fn main() {
    tracing_subscriber::fmt::init();
    eprintln!(
        "hasqlite {} — see docs/PLAN.md (implementation in progress)",
        hasqlite_node::NODE_API_VERSION
    );
}
