//! Minimal Unity Catalog REST client for the Delta **write** path.
//!
//! This mirrors the read-side `query-sidecar` flow but vends `READ_WRITE`
//! credentials: it resolves a table's `storage_location` and `table_id` via
//! `GET /api/2.1/unity-catalog/tables/{full_name}`, then exchanges the
//! `table_id` for temporary AWS credentials via
//! `POST /api/2.1/unity-catalog/temporary-table-credentials`. The caller's
//! bearer token (a UC JWT forwarded from the lineage service) is attached when
//! present; UC validates it (and, when authorization is disabled, ignores it).

use std::collections::HashMap;

use serde::Deserialize;

/// Unity Catalog REST API prefix.
pub const CATALOG_BASE: &str = "/api/2.1/unity-catalog";

/// Thin async UC REST client. Cheap to clone (wraps a `reqwest::Client`).
#[derive(Debug, Clone)]
pub struct UnityClient {
    base_url: String,
    http: reqwest::Client,
}

/// Storage coordinates resolved from a UC table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub storage_location: String,
    pub table_id: String,
}

/// Temporary AWS credentials vended by Unity Catalog, scoped to a table's
/// storage prefix.
#[derive(Debug, Clone)]
pub struct TempCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    /// Absolute expiry as epoch milliseconds, when UC reports it.
    pub expiration_time_ms: Option<i64>,
}

#[derive(Debug, thiserror::Error)]
pub enum UnityError {
    #[error("unity http: {0}")]
    Http(String),
    #[error("unity: status {status} from {url}")]
    Status { status: u16, url: String },
    #[error("unity: table {0} has no storage_location")]
    NoStorageLocation(String),
    #[error("unity: vend response had no aws_temp_credentials")]
    NoCredentials,
}

impl UnityClient {
    /// Build a client against `base_url` (trailing slash trimmed) using a
    /// default `reqwest::Client`.
    pub fn from_url(base_url: &str) -> Self {
        Self::new(base_url, reqwest::Client::new())
    }

    /// Build a client with an injected `reqwest::Client` (used by tests).
    pub fn new(base_url: &str, http: reqwest::Client) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
        }
    }

    /// `GET /api/2.1/unity-catalog/tables/{full_name}` -> storage_location +
    /// table_id. `full_name` is `catalog.schema.table`.
    pub async fn get_table(
        &self,
        token: Option<&str>,
        full_name: &str,
    ) -> Result<TableInfo, UnityError> {
        let url = table_url(&self.base_url, full_name);
        let mut rb = self.http.get(&url);
        if let Some(t) = token {
            rb = rb.bearer_auth(t);
        }
        let resp = rb
            .send()
            .await
            .map_err(|e| UnityError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(UnityError::Status {
                status: resp.status().as_u16(),
                url,
            });
        }
        let body: GetTableResponse = resp
            .json()
            .await
            .map_err(|e| UnityError::Http(e.to_string()))?;
        if body.storage_location.is_empty() {
            return Err(UnityError::NoStorageLocation(full_name.to_string()));
        }
        Ok(TableInfo {
            storage_location: body.storage_location,
            table_id: body.table_id,
        })
    }

    /// `POST /api/2.1/unity-catalog/temporary-table-credentials` with
    /// `{table_id, operation}` -> temporary AWS credentials. `operation` is
    /// typically `"READ_WRITE"` for the write path.
    pub async fn vend_credentials(
        &self,
        token: Option<&str>,
        table_id: &str,
        operation: &str,
    ) -> Result<TempCredentials, UnityError> {
        let url = temp_creds_url(&self.base_url);
        let mut rb = self.http.post(&url).json(&serde_json::json!({
            "table_id": table_id,
            "operation": operation,
        }));
        if let Some(t) = token {
            rb = rb.bearer_auth(t);
        }
        let resp = rb
            .send()
            .await
            .map_err(|e| UnityError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(UnityError::Status {
                status: resp.status().as_u16(),
                url,
            });
        }
        let body: VendResponse = resp
            .json()
            .await
            .map_err(|e| UnityError::Http(e.to_string()))?;
        let creds = body.aws_temp_credentials.ok_or(UnityError::NoCredentials)?;
        Ok(TempCredentials {
            access_key_id: creds.access_key_id,
            secret_access_key: creds.secret_access_key,
            session_token: creds.session_token,
            expiration_time_ms: body.expiration_time,
        })
    }
}

/// Build the `GET /tables/{full_name}` URL.
pub(crate) fn table_url(base: &str, full_name: &str) -> String {
    format!("{base}{CATALOG_BASE}/tables/{full_name}")
}

/// Build the `POST /temporary-table-credentials` URL.
pub(crate) fn temp_creds_url(base: &str) -> String {
    format!("{base}{CATALOG_BASE}/temporary-table-credentials")
}

/// Map vended credentials into deltalake/object_store storage options. Keys
/// are the lowercase names deltalake expects. `region` is layered in when the
/// service has an `AWS_REGION` configured (UC does not return one).
pub fn storage_options_from_creds(
    creds: &TempCredentials,
    region: Option<&str>,
) -> HashMap<String, String> {
    let mut opts = HashMap::new();
    opts.insert("aws_access_key_id".to_string(), creds.access_key_id.clone());
    opts.insert(
        "aws_secret_access_key".to_string(),
        creds.secret_access_key.clone(),
    );
    opts.insert("aws_session_token".to_string(), creds.session_token.clone());
    if let Some(r) = region
        && !r.is_empty()
    {
        opts.insert("aws_region".to_string(), r.to_string());
    }
    opts
}

#[derive(Debug, Deserialize)]
struct GetTableResponse {
    #[serde(default)]
    storage_location: String,
    #[serde(default)]
    table_id: String,
}

#[derive(Debug, Deserialize)]
struct VendResponse {
    #[serde(default)]
    aws_temp_credentials: Option<AwsTempCreds>,
    #[serde(default)]
    expiration_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct AwsTempCreds {
    #[serde(default)]
    access_key_id: String,
    #[serde(default)]
    secret_access_key: String,
    #[serde(default)]
    session_token: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn test_table_url() {
        assert_eq!(
            table_url("https://uc.example.dev", "lineage.ol.events"),
            "https://uc.example.dev/api/2.1/unity-catalog/tables/lineage.ol.events",
        );
    }

    #[test]
    fn test_temp_creds_url() {
        assert_eq!(
            temp_creds_url("https://uc.example.dev"),
            "https://uc.example.dev/api/2.1/unity-catalog/temporary-table-credentials",
        );
    }

    #[test]
    fn test_storage_options_from_creds() {
        let creds = TempCredentials {
            access_key_id: "AKIA".into(),
            secret_access_key: "secret".into(),
            session_token: "sess".into(),
            expiration_time_ms: Some(1234),
        };
        let opts = storage_options_from_creds(&creds, Some("us-west-2"));
        assert_eq!(opts.get("aws_access_key_id").unwrap(), "AKIA");
        assert_eq!(opts.get("aws_secret_access_key").unwrap(), "secret");
        assert_eq!(opts.get("aws_session_token").unwrap(), "sess");
        assert_eq!(opts.get("aws_region").unwrap(), "us-west-2");

        let opts_no_region = storage_options_from_creds(&creds, None);
        assert!(!opts_no_region.contains_key("aws_region"));
    }

    #[tokio::test]
    async fn test_get_table_resolves_location_and_id() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/2.1/unity-catalog/tables/lineage.ol.events"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "events",
                "storage_location": "s3://bucket/__unitystorage/tables/uuid",
                "table_id": "abc-123",
            })))
            .mount(&server)
            .await;

        let client = UnityClient::from_url(&server.uri());
        let info = client
            .get_table(Some("jwt"), "lineage.ol.events")
            .await
            .unwrap();
        assert_eq!(
            info.storage_location,
            "s3://bucket/__unitystorage/tables/uuid"
        );
        assert_eq!(info.table_id, "abc-123");
    }

    #[tokio::test]
    async fn test_get_table_missing_storage_location_errors() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/2.1/unity-catalog/tables/lineage.ol.events"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "events",
                "table_id": "abc-123",
            })))
            .mount(&server)
            .await;

        let client = UnityClient::from_url(&server.uri());
        let err = client
            .get_table(None, "lineage.ol.events")
            .await
            .unwrap_err();
        assert!(matches!(err, UnityError::NoStorageLocation(_)));
    }

    #[tokio::test]
    async fn test_vend_credentials_posts_table_id_and_operation() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.1/unity-catalog/temporary-table-credentials"))
            .and(body_json(serde_json::json!({
                "table_id": "abc-123",
                "operation": "READ_WRITE",
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "aws_temp_credentials": {
                    "access_key_id": "AKIA",
                    "secret_access_key": "secret",
                    "session_token": "sess",
                },
                "expiration_time": 1700000000000i64,
            })))
            .mount(&server)
            .await;

        let client = UnityClient::from_url(&server.uri());
        let creds = client
            .vend_credentials(Some("jwt"), "abc-123", "READ_WRITE")
            .await
            .unwrap();
        assert_eq!(creds.access_key_id, "AKIA");
        assert_eq!(creds.secret_access_key, "secret");
        assert_eq!(creds.session_token, "sess");
        assert_eq!(creds.expiration_time_ms, Some(1700000000000));
    }

    #[tokio::test]
    async fn test_vend_credentials_rejects_non_2xx() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.1/unity-catalog/temporary-table-credentials"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let client = UnityClient::from_url(&server.uri());
        let err = client
            .vend_credentials(Some("jwt"), "abc-123", "READ_WRITE")
            .await
            .unwrap_err();
        assert!(matches!(err, UnityError::Status { status: 403, .. }));
    }
}
