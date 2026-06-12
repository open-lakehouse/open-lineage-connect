use std::collections::HashMap;
use std::env;

/// Which lakehouse sink the service should fan a `WriteBatch` out to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkKind {
    Delta,
    Iceberg,
}

/// Legacy storage tag retained for source-compatibility with older callers
/// that built `Config` directly. New code should use `SinkKind`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageBackend {
    Local,
    S3,
    Unity,
}

#[derive(Debug, Clone)]
pub struct DeltaConfig {
    pub table_path: String,
    pub partition_cols: Vec<String>,
}

impl Default for DeltaConfig {
    fn default() -> Self {
        Self {
            table_path: "/data/events".into(),
            partition_cols: vec!["event_kind".into()],
        }
    }
}

/// Configuration for writing into a Unity Catalog-managed Delta table.
///
/// The sink resolves the table's `storage_location` and vends temporary
/// `READ_WRITE` credentials from the UC REST API (mirroring the read-side
/// `query-sidecar` flow), then writes Delta with those credentials.
#[derive(Debug, Clone)]
pub struct UnityConfig {
    /// Base URL of the Unity Catalog server, e.g.
    /// `https://uc.openlakehousedemos.dev` (no trailing slash).
    pub url: String,
    /// Fallback service bearer token used when no per-request caller token is
    /// forwarded (and ignored entirely when UC authorization is disabled).
    pub token: Option<String>,
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

impl UnityConfig {
    /// Fully-qualified table name in `catalog.schema.table` form, as expected
    /// by the UC REST `GET /tables/{full_name}` endpoint.
    pub fn full_name(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

#[derive(Debug, Clone)]
pub struct IcebergConfig {
    /// Iceberg REST catalog URI. For Lakekeeper this looks like
    /// `http://lakekeeper:8181/catalog`.
    pub catalog_uri: String,
    /// REST `warehouse` property — for Lakekeeper this is the warehouse name
    /// (e.g. `lineage`), not an S3 path. For other servers it may be an
    /// `s3://bucket/prefix` URI.
    pub warehouse: String,
    pub namespace: String,
    pub table: String,
    /// Identity-transform partition columns. Empty means an unpartitioned
    /// table.
    pub partition_cols: Vec<String>,
    /// Optional bearer token to attach to REST requests (Lakekeeper OIDC).
    /// Forwarded to the catalog as the REST `token` property by
    /// `iceberg::build_rest_props` (sourced from the `ICEBERG_TOKEN` env var).
    pub token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub sinks: Vec<SinkKind>,
    /// Legacy single-backend tag, kept for source compatibility.
    pub storage: StorageBackend,
    pub delta: DeltaConfig,
    pub iceberg: Option<IcebergConfig>,
    /// Present when `DELTA_STORAGE=unity`: the Delta sink writes into a
    /// UC-managed table using vended credentials.
    pub unity: Option<UnityConfig>,
    pub storage_options: HashMap<String, String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 8091,
            sinks: vec![SinkKind::Delta],
            storage: StorageBackend::Local,
            delta: DeltaConfig::default(),
            iceberg: None,
            unity: None,
            storage_options: HashMap::new(),
        }
    }
}

impl Config {
    pub fn from_env() -> Self {
        let port = env::var("TABLE_SERVICE_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8091);

        let sinks = parse_sinks(&env::var("TABLE_SINKS").unwrap_or_else(|_| "delta".into()));

        let storage_str = env::var("DELTA_STORAGE").unwrap_or_else(|_| "local".into());
        let storage = match storage_str.as_str() {
            "s3" => StorageBackend::S3,
            "unity" => StorageBackend::Unity,
            _ => StorageBackend::Local,
        };

        let table_path = env::var("DELTA_TABLE_PATH").unwrap_or_else(|_| "/data/events".into());

        let partition_cols = env::var("DELTA_PARTITION_COLS")
            .unwrap_or_else(|_| "event_kind".into())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let mut storage_options = HashMap::new();
        Self::add_env(&mut storage_options, "AWS_REGION");
        Self::add_env(&mut storage_options, "AWS_ACCESS_KEY_ID");
        Self::add_env(&mut storage_options, "AWS_SECRET_ACCESS_KEY");
        Self::add_env(&mut storage_options, "AWS_ENDPOINT_URL");
        Self::add_env(&mut storage_options, "AWS_S3_ALLOW_UNSAFE_RENAME");
        Self::add_env(&mut storage_options, "UNITY_CATALOG_URL");
        Self::add_env(&mut storage_options, "UNITY_CATALOG_TOKEN");

        let iceberg = if sinks.contains(&SinkKind::Iceberg) {
            Some(iceberg_from_env())
        } else {
            None
        };

        let unity = if storage == StorageBackend::Unity {
            Some(unity_from_env())
        } else {
            None
        };

        Config {
            port,
            sinks,
            storage,
            delta: DeltaConfig {
                table_path,
                partition_cols,
            },
            iceberg,
            unity,
            storage_options,
        }
    }

    fn add_env(opts: &mut HashMap<String, String>, key: &str) {
        if let Ok(val) = env::var(key) {
            opts.insert(key.to_lowercase(), val);
        }
    }
}

fn parse_sinks(raw: &str) -> Vec<SinkKind> {
    let parsed: Vec<SinkKind> = raw
        .split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .filter_map(|s| match s.as_str() {
            "delta" => Some(SinkKind::Delta),
            "iceberg" => Some(SinkKind::Iceberg),
            other => {
                tracing::warn!("ignoring unknown sink kind '{other}' in TABLE_SINKS");
                None
            }
        })
        .collect();
    if parsed.is_empty() {
        vec![SinkKind::Delta]
    } else {
        parsed
    }
}

fn unity_from_env() -> UnityConfig {
    let url = env::var("UNITY_CATALOG_URL")
        .unwrap_or_else(|_| "http://localhost:8080".into())
        .trim_end_matches('/')
        .to_string();
    let token = env::var("UNITY_CATALOG_TOKEN")
        .ok()
        .filter(|s| !s.is_empty());
    let catalog = env::var("UC_CATALOG").unwrap_or_else(|_| "lineage".into());
    let schema = env::var("UC_SCHEMA").unwrap_or_else(|_| "lineage".into());
    let table = env::var("UC_TABLE").unwrap_or_else(|_| "events".into());
    UnityConfig {
        url,
        token,
        catalog,
        schema,
        table,
    }
}

fn iceberg_from_env() -> IcebergConfig {
    let catalog_uri =
        env::var("ICEBERG_CATALOG_URI").unwrap_or_else(|_| "http://localhost:8181/catalog".into());
    let warehouse = env::var("ICEBERG_WAREHOUSE").unwrap_or_else(|_| "lineage".into());
    let namespace = env::var("ICEBERG_NAMESPACE").unwrap_or_else(|_| "lineage".into());
    let table = env::var("ICEBERG_TABLE").unwrap_or_else(|_| "events".into());
    let partition_cols = env::var("ICEBERG_PARTITION_COLS")
        .unwrap_or_else(|_| "event_kind".into())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let token = env::var("ICEBERG_TOKEN").ok().filter(|s| !s.is_empty());
    IcebergConfig {
        catalog_uri,
        warehouse,
        namespace,
        table,
        partition_cols,
        token,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let cfg = Config::default();
        assert_eq!(cfg.port, 8091);
        assert_eq!(cfg.delta.table_path, "/data/events");
        assert!(matches!(cfg.storage, StorageBackend::Local));
        assert_eq!(cfg.delta.partition_cols, vec!["event_kind"]);
        assert_eq!(cfg.sinks, vec![SinkKind::Delta]);
        assert!(cfg.iceberg.is_none());
    }

    #[test]
    fn test_storage_variants() {
        assert!(matches!(StorageBackend::S3, StorageBackend::S3));
        assert!(matches!(StorageBackend::Unity, StorageBackend::Unity));
        assert!(matches!(StorageBackend::Local, StorageBackend::Local));
    }

    #[test]
    fn test_default_has_no_unity_config() {
        assert!(Config::default().unity.is_none());
    }

    #[test]
    fn test_unity_full_name() {
        let uc = UnityConfig {
            url: "https://uc.example.dev".into(),
            token: Some("t".into()),
            catalog: "lineage".into(),
            schema: "ol".into(),
            table: "events".into(),
        };
        assert_eq!(uc.full_name(), "lineage.ol.events");
    }

    #[test]
    fn test_parse_sinks_default_when_empty() {
        assert_eq!(parse_sinks(""), vec![SinkKind::Delta]);
    }

    #[test]
    fn test_parse_sinks_single_iceberg() {
        assert_eq!(parse_sinks("iceberg"), vec![SinkKind::Iceberg]);
    }

    #[test]
    fn test_parse_sinks_dual_write_order_preserved() {
        assert_eq!(
            parse_sinks(" Delta , iceberg "),
            vec![SinkKind::Delta, SinkKind::Iceberg]
        );
    }

    #[test]
    fn test_parse_sinks_skips_unknown() {
        assert_eq!(
            parse_sinks("hudi,delta"),
            vec![SinkKind::Delta],
            "unknown sinks are ignored, valid ones are kept",
        );
    }

    #[test]
    fn test_parse_sinks_only_unknown_falls_back_to_delta() {
        assert_eq!(parse_sinks("hudi"), vec![SinkKind::Delta]);
    }
}
