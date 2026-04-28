use std::collections::HashMap;
use std::env;

#[derive(Debug, Clone)]
pub enum StorageBackend {
    Local,
    S3,
    Unity,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub storage: StorageBackend,
    pub table_path: String,
    pub partition_cols: Vec<String>,
    pub storage_options: HashMap<String, String>,
}

impl Config {
    pub fn from_env() -> Self {
        let port = env::var("TABLE_SERVICE_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8091);

        let storage_str = env::var("DELTA_STORAGE").unwrap_or_else(|_| "local".into());
        let storage = match storage_str.as_str() {
            "s3" => StorageBackend::S3,
            "unity" => StorageBackend::Unity,
            _ => StorageBackend::Local,
        };

        let table_path =
            env::var("DELTA_TABLE_PATH").unwrap_or_else(|_| "/data/events".into());

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

        Config {
            port,
            storage,
            table_path,
            partition_cols,
            storage_options,
        }
    }

    fn add_env(opts: &mut HashMap<String, String>, key: &str) {
        if let Ok(val) = env::var(key) {
            opts.insert(key.to_lowercase(), val);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let cfg = Config {
            port: 8091,
            storage: StorageBackend::Local,
            table_path: "/data/events".into(),
            partition_cols: vec!["event_kind".into()],
            storage_options: HashMap::new(),
        };
        assert_eq!(cfg.port, 8091);
        assert_eq!(cfg.table_path, "/data/events");
        assert!(matches!(cfg.storage, StorageBackend::Local));
        assert_eq!(cfg.partition_cols, vec!["event_kind"]);
    }

    #[test]
    fn test_storage_variants() {
        assert!(matches!(StorageBackend::S3, StorageBackend::S3));
        assert!(matches!(StorageBackend::Unity, StorageBackend::Unity));
        assert!(matches!(StorageBackend::Local, StorageBackend::Local));
    }
}
