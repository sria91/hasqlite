//! Controller reconcile and promotion gates — implementation lands in Phase 1.

use hasqlite_lease::{FreshnessLedger, LeaseRecord};

/// Placeholder for the tick outcome type (will match rsqlite-rsync `HaController`).
#[derive(Debug, Clone)]
pub struct ControllerTick {
    pub lease: LeaseRecord,
    pub freshness: Option<FreshnessLedger>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_bootstraps() {
        let tick = ControllerTick {
            lease: LeaseRecord {
                holder_node_id: Some("node-1".into()),
                generation: 1,
                renewed_at_secs: 100,
                ttl_secs: 10,
            },
            freshness: None,
        };
        assert_eq!(tick.lease.generation, 1);
    }
}
