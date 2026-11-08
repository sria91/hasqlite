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
    pub source_node_id: Option<String>,
    pub source_generation: u64,
    pub synced_at_secs: u64,
}
