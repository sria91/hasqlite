//! Native lease records and freshness metadata, informed by prior HA designs.
#![forbid(unsafe_code)]

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
    /// The holder's configured `lease.ttl_secs`.
    ///
    /// Candidates wait `max(their own lease.ttl_secs, this value)` before treating the lease as expired
    /// (see [`LeaseObservation::is_expired_for`]). The holder validated `ttl_secs ≥ renew_deadline_secs +
    /// fencing margin` against its *own* deadline, so honouring the holder's TTL keeps the fencing
    /// argument intact even while nodes run different lease timing during a rolling config change.
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

/// Opaque hasqlite-owned token that changes on every lease write made *by hasqlite* (acquire, activate,
/// renew, release): a renewal counter stored inside the lease record or, on Kubernetes, in a hasqlite
/// annotation.
///
/// It is deliberately not the backend's own revision (Kubernetes `resourceVersion`), because that also
/// changes when an unrelated actor annotates or labels the Lease object, which would restart every
/// candidate's expiry timer on a dead writer. Backends still use their native revision as the
/// compare-and-swap precondition, and a lease write must atomically bump this token and the native
/// revision together.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LeaseVersion(String);

impl LeaseVersion {
    /// Wraps a hasqlite-owned renewal token read from the lease store.
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

    /// Returns true once the observed version has been unchanged for the effective TTL: the longer of the
    /// candidate's own `local_ttl` and the TTL the holder recorded in `observed`.
    ///
    /// Using the holder's TTL closes the gap during rolling lease-timing changes: a candidate configured
    /// with a shorter TTL than the holder's renewal deadline would otherwise campaign while the holder may
    /// still commit.
    pub fn is_expired_for(
        &self,
        now: Instant,
        local_ttl: Duration,
        observed: &LeaseRecord,
    ) -> bool {
        self.is_expired(now, effective_ttl(local_ttl, observed))
    }
}

/// The TTL a candidate must wait on `observed`: the longer of its own `local_ttl` and the holder's recorded TTL.
pub fn effective_ttl(local_ttl: Duration, observed: &LeaseRecord) -> Duration {
    local_ttl.max(Duration::from_secs(observed.ttl_secs))
}

/// Writer-side time basis for every timestamp the writer stamps into the lease (`renewed_at_secs`) and
/// into snapshots (`synced_at_secs`).
///
/// Safety contract rule 4 requires these stamps to be non-decreasing so a backward step of the writer's
/// wall clock cannot make `writer.renewed_at_secs − synced_at_secs` saturate to zero and let a stale
/// snapshot pass the lag gate. The clock therefore never returns a value lower than the last one it issued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterClock {
    last_stamp_secs: u64,
}

impl WriterClock {
    /// Starts the basis at `last_stamp_secs`, normally the writer epoch's last renewal read from the lease
    /// (or 0 for genesis), so stamps also never move backwards across a writer restart.
    pub fn new(last_stamp_secs: u64) -> Self {
        Self { last_stamp_secs }
    }

    /// Returns `max(wall_now_secs, last stamp)` and remembers it as the new floor.
    pub fn stamp(&mut self, wall_now_secs: u64) -> u64 {
        self.last_stamp_secs = self.last_stamp_secs.max(wall_now_secs);
        self.last_stamp_secs
    }

    /// Returns the most recent stamp issued (or the initial floor).
    pub fn last_stamp_secs(&self) -> u64 {
        self.last_stamp_secs
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
    ///
    /// If the deadline is not representable it collapses to `sent_at`, so overflow fails closed and the
    /// writer stops committing instead of treating the lease as never expiring.
    pub fn from_renewal_sent(sent_at: Instant, renew_deadline: Duration) -> Self {
        Self {
            deadline: sent_at.checked_add(renew_deadline).unwrap_or(sent_at),
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

/// Errors returned by lease store operations (safety contract rule 6).
///
/// There is deliberately no wall-clock "expired" variant: candidates decide expiry with
/// [`LeaseObservation`] on their monotonic clock (rule 4), never by comparing `renewed_at_secs`.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum LeaseError {
    /// Compare-and-swap conflict: the lease version changed since the caller's observation.
    #[error(
        "lease CAS conflict: expected version {}, observed {}",
        .expected.as_str(),
        .observed.as_str()
    )]
    CasConflict {
        /// Version the caller observed and expected to be unchanged.
        expected: LeaseVersion,
        /// Version currently stored in the backend.
        observed: LeaseVersion,
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
        assert_eq!(
            lease.holder_node_id.as_ref().map(NodeId::as_str),
            Some("node-1")
        );
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
    fn lease_observation_waits_for_the_holders_longer_ttl() {
        let t0 = Instant::now();
        let local_ttl = Duration::from_secs(4);
        // Holder runs ttl 10 / renew deadline 6; a candidate rolled out with ttl 4 must not expire it at 4s.
        let observed = LeaseRecord {
            holder_node_id: Some(NodeId::from("node-1")),
            generation: 3,
            renewed_at_secs: 100,
            ttl_secs: 10,
            writer: None,
        };
        let observation = LeaseObservation::new(LeaseVersion::new("7"), t0);
        assert_eq!(effective_ttl(local_ttl, &observed), Duration::from_secs(10));
        assert!(observation.is_expired(t0 + Duration::from_secs(4), local_ttl));
        assert!(!observation.is_expired_for(t0 + Duration::from_secs(9), local_ttl, &observed));
        assert!(observation.is_expired_for(t0 + Duration::from_secs(10), local_ttl, &observed));
    }

    #[test]
    fn lease_observation_keeps_the_longer_local_ttl() {
        let t0 = Instant::now();
        let local_ttl = Duration::from_secs(10);
        let observed = LeaseRecord {
            holder_node_id: Some(NodeId::from("node-1")),
            generation: 3,
            renewed_at_secs: 100,
            ttl_secs: 4,
            writer: None,
        };
        let observation = LeaseObservation::new(LeaseVersion::new("7"), t0);
        assert_eq!(effective_ttl(local_ttl, &observed), local_ttl);
        assert!(!observation.is_expired_for(t0 + Duration::from_secs(9), local_ttl, &observed));
        assert!(observation.is_expired_for(t0 + Duration::from_secs(10), local_ttl, &observed));
    }

    #[test]
    fn writer_clock_never_steps_backwards() {
        let mut clock = WriterClock::new(1_000);
        assert_eq!(clock.last_stamp_secs(), 1_000);
        assert_eq!(clock.stamp(1_005), 1_005);
        // Wall clock stepped back 30s: the stamp holds at the last issued value.
        assert_eq!(clock.stamp(975), 1_005);
        assert_eq!(clock.last_stamp_secs(), 1_005);
        // Once the wall clock catches up, stamps advance again.
        assert_eq!(clock.stamp(1_006), 1_006);
    }

    #[test]
    fn writer_clock_floor_prevents_stale_snapshot_from_looking_fresh() {
        // Writer renewed at 1_000, then its clock stepped back to 900 before taking a snapshot.
        let mut clock = WriterClock::new(1_000);
        let renewed_at = clock.stamp(1_000);
        let synced_at = clock.stamp(900);
        // Without the floor the snapshot would be stamped 900 and look 100s stale; with it, lag is 0 and
        // the snapshot is correctly treated as taken at or after the renewal.
        assert_eq!(synced_at, 1_000);
        assert_eq!(renewed_at.saturating_sub(synced_at), 0);
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
    fn renewal_deadline_overflow_fails_closed() {
        let sent_at = Instant::now();
        let deadline = RenewalDeadline::from_renewal_sent(sent_at, Duration::MAX);
        assert!(!deadline.allows_commit_at(sent_at));
    }

    #[test]
    fn lease_error_display() {
        let err = LeaseError::CasConflict {
            expected: LeaseVersion::new("2"),
            observed: LeaseVersion::new("3"),
        };
        assert_eq!(
            err.to_string(),
            "lease CAS conflict: expected version 2, observed 3"
        );
    }

    #[test]
    fn lease_unauthorized_error_display() {
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
