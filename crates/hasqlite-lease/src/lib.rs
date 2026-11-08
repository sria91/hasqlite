//! Lease records and freshness metadata (Phase 1: types; logic ports from rsqlite-rsync `ha`).

/// Authoritative writer lease (mirrors rsqlite-rsync `LeaseRecord`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRecord {
    pub holder_node_id: Option<String>,
    pub generation: u64,
    pub renewed_at_secs: u64,
    pub ttl_secs: u64,
}

/// Last successful replica sync metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshnessLedger {
    pub source_node_id: String,
    pub source_generation: u64,
    pub synced_at_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn freshness_ledger_requires_source_node() {
        let ledger = FreshnessLedger {
            source_node_id: "node-1".to_string(),
            source_generation: 42,
            synced_at_secs: 1_700_000_000,
        };
        assert_eq!(ledger.source_node_id, "node-1");
        assert_eq!(ledger.source_generation, 42);
        assert_eq!(ledger.synced_at_secs, 1_700_000_000);
    }

    #[test]
    fn lease_record_construction() {
        let lease = LeaseRecord {
            holder_node_id: Some("node-1".to_string()),
            generation: 1,
            renewed_at_secs: 100,
            ttl_secs: 10,
        };
        assert_eq!(lease.holder_node_id.as_deref(), Some("node-1"));
        assert_eq!(lease.generation, 1);
    }
}
