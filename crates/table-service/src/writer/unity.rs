//! Unity Catalog-backed Delta sink.
//!
//! Resolves the target table's `storage_location` and vends temporary
//! `READ_WRITE` credentials from Unity Catalog (see [`crate::unity`]), then
//! writes the batch with the existing [`DeltaWriter`] against that location.
//!
//! The per-request bearer token (the caller's UC JWT, forwarded by the lineage
//! service) is used for vending so credentials are scoped to the calling user.
//! When no token is forwarded the configured service token is used; when UC
//! authorization is disabled both are ignored by UC.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use deltalake::arrow::array::RecordBatch;
use tokio::sync::Mutex;

use crate::config::UnityConfig;
use crate::unity::{TempCredentials, UnityClient, storage_options_from_creds};
use crate::writer::delta::DeltaWriter;
use crate::writer::sink::{SinkError, TableSink};

/// Refresh vended credentials this many milliseconds before their stated
/// expiry to avoid racing an expiry mid-write.
const EXPIRY_SKEW_MS: i64 = 60_000;

/// Default cache lifetime when UC does not report an expiry.
const DEFAULT_TTL_MS: i64 = 600_000;

struct CachedVend {
    storage_location: String,
    storage_options: HashMap<String, String>,
    expires_at_ms: i64,
}

/// Delta sink that writes into a Unity Catalog-managed table using vended
/// credentials.
pub struct UnityDeltaSink {
    client: UnityClient,
    cfg: UnityConfig,
    partition_cols: Vec<String>,
    /// AWS_* options (region, endpoint, unsafe-rename) layered under the
    /// vended credentials — e.g. for MinIO/local S3 testing.
    aws_options: HashMap<String, String>,
    /// Per-token cache of resolved location + vended credentials.
    cache: Mutex<HashMap<String, CachedVend>>,
}

impl UnityDeltaSink {
    pub fn new(
        cfg: UnityConfig,
        partition_cols: Vec<String>,
        base_storage_options: HashMap<String, String>,
    ) -> Self {
        // Keep only AWS_* keys; the raw config map also carries UC url/token
        // which are not valid deltalake storage options.
        let aws_options = base_storage_options
            .into_iter()
            .filter(|(k, _)| k.starts_with("aws_"))
            .collect();
        Self {
            client: UnityClient::from_url(&cfg.url),
            cfg,
            partition_cols,
            aws_options,
            cache: Mutex::new(HashMap::new()),
        }
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    /// Effective bearer token: the per-request caller token if present,
    /// otherwise the configured service fallback token.
    fn effective_token<'a>(&'a self, token: Option<&'a str>) -> Option<&'a str> {
        token
            .filter(|t| !t.is_empty())
            .or(self.cfg.token.as_deref())
    }

    /// Resolve the table location and vended credentials for `token`, using a
    /// short-lived per-token cache.
    async fn resolve(
        &self,
        token: Option<&str>,
    ) -> Result<(String, HashMap<String, String>), SinkError> {
        let effective = self.effective_token(token);
        let cache_key = effective.unwrap_or("").to_string();
        let now = Self::now_ms();

        {
            let cache = self.cache.lock().await;
            if let Some(c) = cache.get(&cache_key)
                && now + EXPIRY_SKEW_MS < c.expires_at_ms
            {
                return Ok((c.storage_location.clone(), c.storage_options.clone()));
            }
        }

        let full_name = self.cfg.full_name();
        let info = self
            .client
            .get_table(effective, &full_name)
            .await
            .map_err(|e| SinkError::Unity(e.to_string()))?;
        let creds = self
            .client
            .vend_credentials(effective, &info.table_id, "READ_WRITE")
            .await
            .map_err(|e| SinkError::Unity(e.to_string()))?;

        let storage_options = self.build_storage_options(&creds);
        let expires_at_ms = creds.expiration_time_ms.unwrap_or(now + DEFAULT_TTL_MS);

        let mut cache = self.cache.lock().await;
        cache.insert(
            cache_key,
            CachedVend {
                storage_location: info.storage_location.clone(),
                storage_options: storage_options.clone(),
                expires_at_ms,
            },
        );
        Ok((info.storage_location, storage_options))
    }

    fn build_storage_options(&self, creds: &TempCredentials) -> HashMap<String, String> {
        let region = self.aws_options.get("aws_region").map(String::as_str);
        let mut opts = storage_options_from_creds(creds, region);
        // Layer in any non-credential AWS options (endpoint, unsafe-rename),
        // but never let a static key overwrite the freshly vended creds.
        for (k, v) in &self.aws_options {
            opts.entry(k.clone()).or_insert_with(|| v.clone());
        }
        opts
    }
}

#[async_trait]
impl TableSink for UnityDeltaSink {
    fn name(&self) -> &'static str {
        "unity-delta"
    }

    async fn append(&self, batch: RecordBatch) -> Result<(), SinkError> {
        self.append_with_token(batch, None).await
    }

    async fn append_with_token(
        &self,
        batch: RecordBatch,
        token: Option<&str>,
    ) -> Result<(), SinkError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let (location, storage_options) = self.resolve(token).await?;
        let writer =
            DeltaWriter::from_parts(location, storage_options, self.partition_cols.clone());
        writer
            .append(batch)
            .await
            .map_err(|e| SinkError::Unity(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> UnityConfig {
        UnityConfig {
            url: "https://uc.example.dev".into(),
            token: Some("service-token".into()),
            catalog: "lineage".into(),
            schema: "ol".into(),
            table: "events".into(),
        }
    }

    #[test]
    fn name_is_unity_delta() {
        let sink = UnityDeltaSink::new(cfg(), vec![], HashMap::new());
        assert_eq!(sink.name(), "unity-delta");
    }

    #[test]
    fn new_filters_non_aws_storage_options() {
        let mut base = HashMap::new();
        base.insert("aws_region".to_string(), "us-west-2".to_string());
        base.insert(
            "aws_endpoint_url".to_string(),
            "http://minio:9000".to_string(),
        );
        base.insert("unity_catalog_url".to_string(), "https://uc".to_string());
        base.insert("unity_catalog_token".to_string(), "tok".to_string());
        let sink = UnityDeltaSink::new(cfg(), vec![], base);
        assert!(sink.aws_options.contains_key("aws_region"));
        assert!(sink.aws_options.contains_key("aws_endpoint_url"));
        assert!(!sink.aws_options.contains_key("unity_catalog_url"));
        assert!(!sink.aws_options.contains_key("unity_catalog_token"));
    }

    #[test]
    fn effective_token_prefers_caller_then_falls_back_to_service() {
        let sink = UnityDeltaSink::new(cfg(), vec![], HashMap::new());
        assert_eq!(sink.effective_token(Some("caller")), Some("caller"));
        assert_eq!(sink.effective_token(Some("")), Some("service-token"));
        assert_eq!(sink.effective_token(None), Some("service-token"));

        let no_service = UnityConfig {
            token: None,
            ..cfg()
        };
        let sink = UnityDeltaSink::new(no_service, vec![], HashMap::new());
        assert_eq!(sink.effective_token(None), None);
    }

    #[test]
    fn build_storage_options_layers_region_and_endpoint() {
        let mut base = HashMap::new();
        base.insert("aws_region".to_string(), "us-west-2".to_string());
        base.insert(
            "aws_endpoint_url".to_string(),
            "http://minio:9000".to_string(),
        );
        let sink = UnityDeltaSink::new(cfg(), vec![], base);
        let creds = TempCredentials {
            access_key_id: "AKIA".into(),
            secret_access_key: "secret".into(),
            session_token: "sess".into(),
            expiration_time_ms: None,
        };
        let opts = sink.build_storage_options(&creds);
        assert_eq!(opts.get("aws_access_key_id").unwrap(), "AKIA");
        assert_eq!(opts.get("aws_session_token").unwrap(), "sess");
        assert_eq!(opts.get("aws_region").unwrap(), "us-west-2");
        assert_eq!(opts.get("aws_endpoint_url").unwrap(), "http://minio:9000");
    }

    #[tokio::test]
    async fn empty_batch_is_a_noop() {
        use crate::writer::schema::arrow_schema;
        let sink = UnityDeltaSink::new(cfg(), vec![], HashMap::new());
        // No UC server is running; an empty batch must short-circuit before
        // any network call.
        let empty = RecordBatch::new_empty(arrow_schema());
        sink.append_with_token(empty, Some("jwt")).await.unwrap();
    }
}
