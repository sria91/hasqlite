fn main() {
    tracing_subscriber::fmt::init();
    tracing::info!(
        version = hasqlite_node::NODE_API_VERSION,
        "hasqlite starting — see docs/PLAN.md (implementation in progress)"
    );
}
