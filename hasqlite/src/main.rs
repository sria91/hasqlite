//! Entry point for the hasqlite node daemon CLI.
#![forbid(unsafe_code)]

use tracing_subscriber::EnvFilter;

/// Initializes logging from `RUST_LOG` (default `info`) and logs the bootstrap banner.
fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    tracing::info!(
        version = hasqlite_node::NODE_API_VERSION,
        "hasqlite starting — see docs/PLAN.md (implementation in progress)"
    );
}
