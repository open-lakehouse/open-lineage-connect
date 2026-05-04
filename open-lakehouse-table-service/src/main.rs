use std::sync::Arc;

use axum::{Router, routing::get};
use tracing_subscriber::EnvFilter;

use open_lakehouse_table_service::config::{Config, SinkKind};
use open_lakehouse_table_service::service::TableWriterServiceImpl;
use open_lakehouse_table_service::table::v1::TableWriterServiceExt;
use open_lakehouse_table_service::writer::delta::DeltaWriter;
use open_lakehouse_table_service::writer::iceberg::IcebergSink;
use open_lakehouse_table_service::writer::sink::TableSink;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg = Config::from_env();
    let sinks = build_sinks(&cfg).await;
    let svc = Arc::new(TableWriterServiceImpl::new(sinks));

    let connect_router = svc.register(connectrpc::Router::new());

    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
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
