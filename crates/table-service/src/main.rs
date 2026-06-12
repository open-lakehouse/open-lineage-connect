use std::sync::Arc;

use axum::{Json, Router, routing::get};
use serde_json::json;
use tracing_subscriber::EnvFilter;

use table_service::config::{Config, SinkKind, StorageBackend};
use table_service::service::TableWriterServiceImpl;
use table_service::table::v1::TableWriterServiceExt;
use table_service::version;
use table_service::writer::delta::DeltaWriter;
use table_service::writer::iceberg::IcebergSink;
use table_service::writer::sink::TableSink;
use table_service::writer::unity::UnityDeltaSink;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let ver = version();
    tracing::info!("table-service version {} starting", ver);

    let cfg = Config::from_env();
    let sinks = build_sinks(&cfg).await;
    let svc = Arc::new(TableWriterServiceImpl::new(sinks));

    let connect_router = svc.register(connectrpc::Router::new());

    let health_version = ver.clone();
    let app = Router::new()
        .route(
            "/health",
            get(move || {
                let v = health_version.clone();
                async move { Json(json!({"status": "ok", "version": v})) }
            }),
        )
        .fallback_service(connect_router.into_axum_service());

    let addr = format!("0.0.0.0:{}", cfg.port);
    tracing::info!("table-service listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn build_sinks(cfg: &Config) -> Vec<Arc<dyn TableSink>> {
    let mut sinks: Vec<Arc<dyn TableSink>> = Vec::with_capacity(cfg.sinks.len());
    for kind in &cfg.sinks {
        match kind {
            SinkKind::Delta if cfg.storage == StorageBackend::Unity => {
                let uc = cfg
                    .unity
                    .as_ref()
                    .expect("DELTA_STORAGE=unity requires UNITY_CATALOG_URL + UC_CATALOG/UC_SCHEMA/UC_TABLE");
                tracing::info!(
                    "registering unity-catalog delta sink: url={} table={}.{}.{}",
                    uc.url,
                    uc.catalog,
                    uc.schema,
                    uc.table,
                );
                let sink = UnityDeltaSink::new(
                    uc.clone(),
                    cfg.delta.partition_cols.clone(),
                    cfg.storage_options.clone(),
                );
                sinks.push(Arc::new(sink));
            }
            SinkKind::Delta => {
                tracing::info!("registering delta sink at {}", cfg.delta.table_path);
                sinks.push(Arc::new(DeltaWriter::new(cfg)));
            }
            SinkKind::Iceberg => {
                let ic = cfg
                    .iceberg
                    .as_ref()
                    .expect("iceberg sink requires ICEBERG_* config");
                tracing::info!(
                    "registering iceberg sink: catalog={} warehouse={} table={}.{}",
                    ic.catalog_uri,
                    ic.warehouse,
                    ic.namespace,
                    ic.table,
                );
                let sink = IcebergSink::from_config(ic)
                    .await
                    .expect("failed to initialize iceberg sink");
                sinks.push(Arc::new(sink));
            }
        }
    }
    sinks
}
