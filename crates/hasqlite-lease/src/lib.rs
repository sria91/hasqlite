//! Native lease records and freshness metadata, informed by prior HA designs.

use hasqlite_core::NodeId;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Authoritative hasqlite writer lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRecord {
    /// Identifier of the current lease holder node, or `None` if unassigned.
    pub holder_node_id: Option<NodeId>,
    /// Monotonically increasing lease epoch generation counter.
    pub generation: u64,
    /// Wall-clock timestamp (Unix seconds, holder's clock) when the lease was acquired or renewed.
    ///
    /// Informational only: candidates must not compare it with their own wall clock (see [`LeaseObservation`]).
    pub renewed_at_secs: u64,
    /// Lease time-to-live duration in seconds before expiry.
    pub ttl_secs: u64,
    /// Most recent holder that activated as writer, or `None` before genesis.
    ///
    /// Acquisition does not change it, so a candidate that wins the lease but never opens a writable
    /// handle leaves the previous writer's epoch in place and peers synced from that writer stay promotable.
    #[serde(default)]
    pub writer: Option<WriterEpoch>,
}

impl LeaseRecord {
    /// Returns true if this lease is currently assigned to the given node identity.
    pub fn is_held_by(&self, node_id: &NodeId) -> bool {
        self.holder_node_id.as_ref() == Some(node_id)
    }
}

/// Identity and last renewal of the most recent writer that may have accepted commits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterEpoch {
    /// Node that activated as writer.
    pub node_id: NodeId,
    /// Lease generation under which it activated.
    pub generation: u64,
    /// Its last successful renewal or release (Unix seconds, writer's clock).
    ///
    /// Stamped by the same clock as the origin's snapshot timestamps, so replica lag measured against it
    /// is independent of skew between the writer and the candidate.
    pub renewed_at_secs: u64,
}

/// Opaque token that changes on every lease write (Kubernetes `resourceVersion` or a file-lease write counter).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LeaseVersion(String);

impl LeaseVersion {
    /// Wraps a backend-provided version token.
    pub fn new(version: impl Into<String>) -> Self {
        Self(version.into())
    }

    /// Returns the version token.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A candidate's view of lease liveness, measured only with its local monotonic clock.
///
/// The holder stamps `renewed_at_secs` with its own wall clock, so comparing it with a candidate's wall
/// clock lets skew or a clock step expire a live lease. Instead, a candidate treats the lease as expired
/// once its version has stayed unchanged for a full TTL since the candidate first saw that version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseObservation {
    version: LeaseVersion,
    first_seen: Instant,
}

impl LeaseObservation {
    /// Starts tracking a version first read at `now`.
    pub fn new(version: LeaseVersion, now: Instant) -> Self {
        Self {
            version,
            first_seen: now,
        }
    }

    /// Records a fresh read of the lease; a changed version restarts the expiry timer.
    pub fn observe(&mut self, version: LeaseVersion, now: Instant) {
        if version != self.version {
            *self = Self::new(version, now);
        }
    }

    /// Returns the most recently observed version, for use as the expected version in a CAS.
    pub fn version(&self) -> &LeaseVersion {
        &self.version
    }

    /// Returns true once the observed version has been unchanged for at least `ttl`.
    pub fn is_expired(&self, now: Instant, ttl: Duration) -> bool {
        now.saturating_duration_since(self.first_seen) >= ttl
    }
}

/// Writer-side deadline after which commits must stop unless another renewal has succeeded.
///
/// Measured from when the successful renewal request was *sent*: a candidate can only observe the new
/// lease version after that instant, so the writer always stops before any candidate's TTL can elapse,
/// provided `renew_deadline` is shorter than the TTL by the configured fencing margin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RenewalDeadline {
    deadline: Instant,
}

impl RenewalDeadline {
    /// Computes the deadline for a renewal that succeeded after being sent at `sent_at`.
    pub fn from_renewal_sent(sent_at: Instant, renew_deadline: Duration) -> Self {
        Self {
            deadline: sent_at + renew_deadline,
        }
    }

    /// Returns true if a commit may still be accepted at `now`.
    pub fn allows_commit_at(&self, now: Instant) -> bool {
        now < self.deadline
    }
}

/// Last successful replica sync metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreshnessLedger {
    /// Node identifier of the writer origin that produced the snapshot.
    pub source_node_id: NodeId,
    /// Generation counter of the writer origin at sync time.
    pub source_generation: u64,
    /// Snapshot start time (Unix seconds), stamped by the origin's clock.
    pub synced_at_secs: u64,
    /// Node identifier of the replica that received the snapshot; promotion requires it to match.
    pub destination_node_id: NodeId,
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
    #[error(
        "unauthorized holder: expected {expected}, held by {}",
        .actual.as_ref().map_or("nobody", NodeId::as_str)
    )]
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
    fn freshness_ledger_records_source_and_destination() {
        let ledger = FreshnessLedger {
            source_node_id: NodeId::from("node-1"),
            source_generation: 42,
            synced_at_secs: 1_700_000_000,
            destination_node_id: NodeId::from("node-2"),
        };
        assert_eq!(ledger.source_node_id, NodeId::from("node-1"));
        assert_eq!(ledger.source_node_id.as_str(), "node-1");
        assert_eq!(ledger.source_generation, 42);
        assert_eq!(ledger.synced_at_secs, 1_700_000_000);
        assert_eq!(ledger.destination_node_id, NodeId::from("node-2"));
    }

    #[test]
    fn freshness_ledger_without_destination_is_rejected() {
        let error = toml::from_str::<FreshnessLedger>(
            "source_node_id = \"node-1\"\nsource_generation = 1\nsynced_at_secs = 1",
        )
        .unwrap_err();
        assert!(error.to_string().contains("destination_node_id"));
    }

    #[test]
    fn lease_record_construction_and_holder() {
        let lease = LeaseRecord {
            holder_node_id: Some(NodeId::from("node-1")),
            generation: 1,
            renewed_at_secs: 100,
            ttl_secs: 10,
            writer: None,
        };
        assert_eq!(lease.holder_node_id.as_deref(), Some("node-1"));
        assert_eq!(lease.generation, 1);
        assert!(lease.is_held_by(&NodeId::from("node-1")));
        assert!(!lease.is_held_by(&NodeId::from("node-2")));
    }

    #[test]
    fn lease_record_is_held_by_unassigned() {
        let lease = LeaseRecord {
            holder_node_id: None,
            generation: 1,
            renewed_at_secs: 100,
            ttl_secs: 10,
            writer: None,
        };
        assert!(!lease.is_held_by(&NodeId::from("node-1")));
    }

    #[test]
    fn lease_observation_expires_after_ttl_of_unchanged_version() {
        let t0 = Instant::now();
        let ttl = Duration::from_secs(10);
        let observation = LeaseObservation::new(LeaseVersion::new("1"), t0);
        assert_eq!(observation.version().as_str(), "1");
        assert!(!observation.is_expired(t0, ttl));
        assert!(!observation.is_expired(t0 + Duration::from_secs(9), ttl));
        assert!(observation.is_expired(t0 + ttl, ttl));
    }

    #[test]
    fn lease_observation_restarts_timer_on_version_change() {
        let t0 = Instant::now();
        let ttl = Duration::from_secs(10);
        let mut observation = LeaseObservation::new(LeaseVersion::new("1"), t0);

        observation.observe(LeaseVersion::new("2"), t0 + Duration::from_secs(8));
        assert_eq!(observation.version().as_str(), "2");
        assert!(!observation.is_expired(t0 + Duration::from_secs(17), ttl));
        assert!(observation.is_expired(t0 + Duration::from_secs(18), ttl));
    }

    #[test]
    fn lease_observation_keeps_timer_for_unchanged_version() {
        let t0 = Instant::now();
        let ttl = Duration::from_secs(10);
        let mut observation = LeaseObservation::new(LeaseVersion::new("1"), t0);

        observation.observe(LeaseVersion::new("1"), t0 + Duration::from_secs(8));
        assert!(observation.is_expired(t0 + ttl, ttl));
    }

    #[test]
    fn lease_observation_tolerates_earlier_instant() {
        let t0 = Instant::now();
        let observation =
            LeaseObservation::new(LeaseVersion::new("1"), t0 + Duration::from_secs(5));
        assert!(!observation.is_expired(t0, Duration::from_secs(1)));
    }

    #[test]
    fn renewal_deadline_stops_commits_at_deadline() {
        let sent_at = Instant::now();
        let deadline = RenewalDeadline::from_renewal_sent(sent_at, Duration::from_secs(6));
        assert!(deadline.allows_commit_at(sent_at));
        assert!(deadline.allows_commit_at(sent_at + Duration::from_millis(5_999)));
        assert!(!deadline.allows_commit_at(sent_at + Duration::from_secs(6)));
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

    #[test]
    fn lease_expired_and_unauthorized_error_display() {
        let expired = LeaseError::Expired {
            renewed_at_secs: 100,
            ttl_secs: 10,
        };
        assert_eq!(
            expired.to_string(),
            "lease expired: renewed at 100s with TTL 10s"
        );

        let unauthorized = LeaseError::UnauthorizedHolder {
            expected: NodeId::from("node-1"),
            actual: Some(NodeId::from("node-2")),
        };
        assert_eq!(
            unauthorized.to_string(),
            "unauthorized holder: expected node-1, held by node-2"
        );

        let unassigned = LeaseError::UnauthorizedHolder {
            expected: NodeId::from("node-1"),
            actual: None,
        };
        assert_eq!(
            unassigned.to_string(),
            "unauthorized holder: expected node-1, held by nobody"
        );
    }
}
