//! Lease records and freshness metadata (Phase 1: types; logic ports from rsqlite-rsync `ha`).

use hasqlite_core::NodeId;
use serde::{Deserialize, Serialize};

/// Authoritative writer lease (mirrors rsqlite-rsync `LeaseRecord`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRecord {
    /// Identifier of the current lease holder node, or `None` if unassigned.
    pub holder_node_id: Option<NodeId>,
    /// Monotonically increasing lease epoch generation counter.
    pub generation: u64,
    /// Wall-clock timestamp (Unix seconds) when the lease was acquired or renewed.
    pub renewed_at_secs: u64,
    /// Lease time-to-live duration in seconds before expiry.
    pub ttl_secs: u64,
}

impl LeaseRecord {
    /// Returns true if this lease is currently assigned to the given node identity.
    pub fn is_held_by(&self, node_id: &NodeId) -> bool {
        self.holder_node_id.as_ref() == Some(node_id)
    }

    /// Returns true if the lease has expired given the current wall-clock seconds.
    pub fn is_expired_at(&self, now_secs: u64) -> bool {
        now_secs >= self.renewed_at_secs.saturating_add(self.ttl_secs)
    }
}

/// Last successful replica sync metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreshnessLedger {
    /// Node identifier of the writer origin that produced the snapshot.
    pub source_node_id: NodeId,
    /// Generation counter of the writer origin at sync time.
    pub source_generation: u64,
    /// Wall-clock timestamp (Unix seconds) when the sync snapshot was taken.
    pub synced_at_secs: u64,
    /// Node identifier of the destination replica bound to this ledger record.
    #[serde(default)]
    pub destination_node_id: Option<NodeId>,
}

/// Common errors encountered in lease evaluation and coordination.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum LeaseError {
    /// Lease TTL expired without timely renewal.
    #[error("lease expired: renewed at {renewed_at_secs}s with TTL {ttl_secs}s")]
    Expired {
        /// Timestamp when the expired lease was renewed.
        renewed_at_secs: u64,
        /// Time-to-live in seconds.
        ttl_secs: u64,
    },
    /// Compare-and-swap conflict when updating the lease generation.
    #[error("lease CAS conflict: expected generation {expected}, observed {observed}")]
    CasConflict {
        /// Expected lease generation.
        expected: u64,
        /// Observed lease generation in the backend.
        observed: u64,
    },
    /// Action attempted by a node that does not hold the active lease.
    #[error("unauthorized holder: expected {expected}, held by {actual:?}")]
    UnauthorizedHolder {
        /// The node identity expected to hold the lease.
        expected: NodeId,
        /// The actual current lease holder.
        actual: Option<NodeId>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn freshness_ledger_requires_source_node() {
        let ledger = FreshnessLedger {
            source_node_id: NodeId::from("node-1"),
            source_generation: 42,
            synced_at_secs: 1_700_000_000,
            destination_node_id: Some(NodeId::from("node-2")),
        };
        assert_eq!(ledger.source_node_id, NodeId::from("node-1"));
        assert_eq!(ledger.source_node_id.as_str(), "node-1");
        assert_eq!(ledger.source_generation, 42);
        assert_eq!(ledger.synced_at_secs, 1_700_000_000);
        assert_eq!(ledger.destination_node_id, Some(NodeId::from("node-2")));
    }

    #[test]
    fn lease_record_construction_and_expiry() {
        let lease = LeaseRecord {
            holder_node_id: Some(NodeId::from("node-1")),
            generation: 1,
            renewed_at_secs: 100,
            ttl_secs: 10,
        };
        assert_eq!(lease.holder_node_id.as_deref(), Some("node-1"));
        assert_eq!(lease.generation, 1);
        assert!(lease.is_held_by(&NodeId::from("node-1")));
        assert!(!lease.is_held_by(&NodeId::from("node-2")));
        assert!(!lease.is_expired_at(105));
        assert!(lease.is_expired_at(110));
        assert!(lease.is_expired_at(115));
    }

    #[test]
    fn lease_record_is_held_by_unassigned() {
        let lease = LeaseRecord {
            holder_node_id: None,
            generation: 1,
            renewed_at_secs: 100,
            ttl_secs: 10,
        };
        assert!(!lease.is_held_by(&NodeId::from("node-1")));
    }

    #[test]
    fn lease_error_display() {
        let err = LeaseError::CasConflict {
            expected: 2,
            observed: 3,
        };
        assert_eq!(
            err.to_string(),
            "lease CAS conflict: expected generation 2, observed 3"
        );
    }
}
