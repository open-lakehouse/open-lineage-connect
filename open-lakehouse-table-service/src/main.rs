use std::sync::Arc;

use axum::{Router, routing::get};
use tracing_subscriber::EnvFilter;

use open_lakehouse_table_service::config::Config;
use open_lakehouse_table_service::service::TableWriterServiceImpl;
use open_lakehouse_table_service::table::v1::TableWriterServiceExt;
use open_lakehouse_table_service::writer::delta::DeltaWriter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg = Config::from_env();
    let writer = DeltaWriter::new(&cfg);
    let svc = Arc::new(TableWriterServiceImpl::new(writer));

    let connect_router = svc.register(connectrpc::Router::new());

    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .fallback_service(connect_router.into_axum_service());

    let addr = format!("0.0.0.0:{}", cfg.port);
    tracing::info!("table-service listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
