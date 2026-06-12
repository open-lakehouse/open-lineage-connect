include!(concat!(env!("OUT_DIR"), "/_connectrpc.rs"));

pub mod config;
pub mod service;
pub mod unity;
pub mod writer;

/// The running service version. Sourced from the `SERVICE_VERSION` env var
/// (set by the Dockerfile / deploy scripts / ECS task definition), falling
/// back to the crate version for local `cargo run` builds.
pub fn version() -> String {
    match std::env::var("SERVICE_VERSION") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => env!("CARGO_PKG_VERSION").to_string(),
    }
}
