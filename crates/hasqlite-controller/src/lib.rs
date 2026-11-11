//! Controller reconcile and promotion gates — implementation lands in Phase 1.

use hasqlite_core::{FreshnessSection, NodeId, PromotionSection};
use hasqlite_lease::{FreshnessLedger, LeaseRecord};

/// Placeholder for the hasqlite controller tick outcome.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ControllerTick {
    /// Active writer lease status observed during this tick.
    pub lease: LeaseRecord,
    /// Freshness ledger record observed on the local node, if any.
    pub freshness: Option<FreshnessLedger>,
}

/// Promotion validation errors reflecting §7 of the Safety Contract.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum PromotionError {
    /// No freshness ledger was found for the candidate replica.
    #[error("no freshness ledger recorded for replica")]
    NoFreshnessRecord,
    /// The lease records no activated writer, so only the genesis procedure can establish one.
    #[error("lease records no activated writer; use the genesis procedure")]
    NoActivatedWriter,
    /// Replica snapshot lags the writer's last lease renewal beyond the permitted bound.
    #[error("replica lags the writer's last renewal by {lag_secs}s (max allowed {max_lag_secs}s)")]
    StaleReplica {
        /// Seconds between the snapshot start and the writer's last renewal.
        lag_secs: u64,
        /// Maximum allowed lag in seconds.
        max_lag_secs: u64,
    },
    /// The snapshot came from a different writer generation than the lease's activated writer.
    #[error(
        "freshness ledger is from generation {ledger_generation}, but the last activated writer has generation {writer_generation}"
    )]
    GenerationMismatch {
        /// Generation recorded in the freshness ledger.
        ledger_generation: u64,
        /// Generation of the lease's last activated writer.
        writer_generation: u64,
    },
    /// The snapshot came from a different node than the lease's activated writer.
    #[error("freshness ledger source {source_node} is not the last activated writer {writer_node}")]
    SourceMismatch {
        /// Source node recorded in the freshness ledger.
        source_node: NodeId,
        /// Node recorded as the lease's last activated writer.
        writer_node: NodeId,
    },
    /// Replica snapshot timestamp is ahead of local wall-clock time beyond tolerance.
    #[error(
        "snapshot timestamp {synced_at_secs}s is in the future by {skew_secs}s (max allowed {max_skew_secs}s)"
    )]
    ClockSkewExceeded {
        /// Recorded sync timestamp in seconds.
        synced_at_secs: u64,
        /// Calculated forward clock skew in seconds.
        skew_secs: u64,
        /// Maximum allowed future skew in seconds.
        max_skew_secs: u64,
    },
    /// Freshness ledger is pinned to a different destination replica.
    #[error("freshness ledger bound to node {bound_to}, but promoting node is {promoting_node}")]
    ReplicaMismatch {
        /// Node ID the ledger was recorded for.
        bound_to: NodeId,
        /// Node ID attempting promotion.
        promoting_node: NodeId,
    },
    /// Source writer origin node ID is empty or invalid.
    #[error("freshness ledger contains empty or invalid source node ID")]
    InvalidSourceNode,
    /// Candidate's freshness record was produced by the candidate itself.
    #[error("freshness ledger for node {node_id} is self-sourced")]
    SelfSourcedLedger {
        /// Node whose own prior writer snapshot is being considered for promotion.
        node_id: NodeId,
    },
}

/// Thresholds a freshness ledger must satisfy before its replica may be promoted (§7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromotionPolicy {
    /// Maximum allowed lag of the snapshot behind the writer's last lease renewal, in seconds.
    pub max_lag_secs: u64,
    /// Maximum allowed forward clock skew of the sync timestamp, in seconds.
    pub max_future_skew_secs: u64,
}

impl PromotionPolicy {
    /// Builds the policy from configuration sections that have passed `ClusterConfig::validate`,
    /// which bounds `max_future_skew_secs` and requires a non-zero `max_lag_secs`.
    pub fn from_config(freshness: &FreshnessSection, promotion: &PromotionSection) -> Self {
        Self {
            max_lag_secs: freshness.max_lag_secs,
            max_future_skew_secs: promotion.max_future_skew_secs,
        }
    }
}

/// Validates whether a replica meets all freshness and fencing criteria to promote to writer (§7).
///
/// `observed_lease` is the lease the candidate is about to replace. The ledger must come from that lease's
/// last activated writer, and lag is measured against that writer's last renewal; both timestamps are
/// stamped by the writer's clock, so the result does not depend on how long failure detection took or
/// on skew between the writer and the candidate. The inputs do not change while the candidate campaigns
/// (scheduled sync is paused), so a successful acquisition CAS needs no second freshness check.
pub fn validate_promotion(
    freshness: Option<&FreshnessLedger>,
    promoting_node: &NodeId,
    observed_lease: &LeaseRecord,
    now_secs: u64,
    policy: PromotionPolicy,
) -> Result<(), PromotionError> {
    let ledger = freshness.ok_or(PromotionError::NoFreshnessRecord)?;

    if ledger.source_node_id.as_str().trim().is_empty() {
        return Err(PromotionError::InvalidSourceNode);
    }
    if &ledger.source_node_id == promoting_node {
        return Err(PromotionError::SelfSourcedLedger {
            node_id: promoting_node.clone(),
        });
    }
    if &ledger.destination_node_id != promoting_node {
        return Err(PromotionError::ReplicaMismatch {
            bound_to: ledger.destination_node_id.clone(),
            promoting_node: promoting_node.clone(),
        });
    }

    let writer = observed_lease
        .writer
        .as_ref()
        .ok_or(PromotionError::NoActivatedWriter)?;
    if ledger.source_generation != writer.generation {
        return Err(PromotionError::GenerationMismatch {
            ledger_generation: ledger.source_generation,
            writer_generation: writer.generation,
        });
    }
    if ledger.source_node_id != writer.node_id {
        return Err(PromotionError::SourceMismatch {
            source_node: ledger.source_node_id.clone(),
            writer_node: writer.node_id.clone(),
        });
    }

    let skew = ledger.synced_at_secs.saturating_sub(now_secs);
    if skew > policy.max_future_skew_secs {
        return Err(PromotionError::ClockSkewExceeded {
            synced_at_secs: ledger.synced_at_secs,
            skew_secs: skew,
            max_skew_secs: policy.max_future_skew_secs,
        });
    }

    let lag = writer.renewed_at_secs.saturating_sub(ledger.synced_at_secs);
    if lag > policy.max_lag_secs {
        return Err(PromotionError::StaleReplica {
            lag_secs: lag,
            max_lag_secs: policy.max_lag_secs,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hasqlite_lease::WriterEpoch;
    use proptest::prelude::*;

    const WRITER: &str = "node-1";
    const CANDIDATE: &str = "node-2";
    const WRITER_GENERATION: u64 = 5;
    const WRITER_RENEWED_AT: u64 = 1_000;

    /// Promotion policy with a 10s lag bound and 2s future-skew tolerance.
    fn policy() -> PromotionPolicy {
        PromotionPolicy {
            max_lag_secs: 10,
            max_future_skew_secs: 2,
        }
    }

    /// Expired lease whose last activated writer is `node-1` at generation `WRITER_GENERATION`, last renewed at `WRITER_RENEWED_AT`.
    fn lease() -> LeaseRecord {
        LeaseRecord {
            holder_node_id: Some(NodeId::from(WRITER)),
            generation: WRITER_GENERATION,
            renewed_at_secs: WRITER_RENEWED_AT,
            ttl_secs: 10,
            writer: Some(WriterEpoch {
                node_id: NodeId::from(WRITER),
                generation: WRITER_GENERATION,
                renewed_at_secs: WRITER_RENEWED_AT,
            }),
        }
    }

    /// Ledger for a snapshot of `node-1` generation `WRITER_GENERATION` delivered to `node-2`.
    fn ledger(synced_at_secs: u64) -> FreshnessLedger {
        FreshnessLedger {
            source_node_id: NodeId::from(WRITER),
            source_generation: WRITER_GENERATION,
            synced_at_secs,
            destination_node_id: NodeId::from(CANDIDATE),
        }
    }

    /// Validates `ledger` for `node-2` against `lease` with the default test policy.
    fn validate(
        ledger: &FreshnessLedger,
        lease: &LeaseRecord,
        now_secs: u64,
    ) -> Result<(), PromotionError> {
        validate_promotion(
            Some(ledger),
            &NodeId::from(CANDIDATE),
            lease,
            now_secs,
            policy(),
        )
    }

    #[test]
    fn promotion_policy_maps_config_sections() {
        let freshness = FreshnessSection {
            path: "/var/lib/hasqlite/.hasqlite.freshness".into(),
            max_lag_secs: 10,
        };
        let promotion = PromotionSection {
            max_future_skew_secs: 2,
        };
        assert_eq!(
            PromotionPolicy::from_config(&freshness, &promotion),
            policy()
        );
    }

    #[test]
    fn validate_promotion_accepts_fresh_replica() {
        assert!(validate(&ledger(995), &lease(), 1_020).is_ok());
    }

    #[test]
    fn validate_promotion_ignores_failure_detection_delay() {
        // The writer stopped renewing long ago; lag is still measured against its last renewal.
        assert!(validate(&ledger(995), &lease(), 10_000).is_ok());
    }

    #[test]
    fn validate_promotion_accepts_snapshot_after_last_renewal() {
        assert!(validate(&ledger(1_003), &lease(), 1_003).is_ok());
    }

    #[test]
    fn validate_promotion_rejects_missing_ledger() {
        let result = validate_promotion(None, &NodeId::from(CANDIDATE), &lease(), 1_000, policy());
        assert_eq!(result.unwrap_err(), PromotionError::NoFreshnessRecord);
    }

    #[test]
    fn validate_promotion_rejects_lease_without_activated_writer() {
        let mut lease = lease();
        lease.writer = None;
        assert_eq!(
            validate(&ledger(995), &lease, 1_000).unwrap_err(),
            PromotionError::NoActivatedWriter
        );
    }

    #[test]
    fn validate_promotion_accepts_lag_at_exact_limit() {
        assert!(validate(&ledger(990), &lease(), 1_020).is_ok());
    }

    #[test]
    fn validate_promotion_rejects_lag_one_past_limit() {
        assert_eq!(
            validate(&ledger(989), &lease(), 1_020).unwrap_err(),
            PromotionError::StaleReplica {
                lag_secs: 11,
                max_lag_secs: 10
            }
        );
    }

    #[test]
    fn validate_promotion_rejects_ledger_from_older_writer_generation() {
        let mut ledger = ledger(999);
        ledger.source_generation = 4;
        assert_eq!(
            validate(&ledger, &lease(), 1_020).unwrap_err(),
            PromotionError::GenerationMismatch {
                ledger_generation: 4,
                writer_generation: WRITER_GENERATION
            }
        );
    }

    #[test]
    fn validate_promotion_survives_aborted_acquisition() {
        // A peer acquired generation 6 but never activated, so the generation-5 writer epoch still applies.
        let mut lease = lease();
        lease.holder_node_id = Some(NodeId::from("node-3"));
        lease.generation = 6;
        lease.renewed_at_secs = 1_015;
        assert!(validate(&ledger(995), &lease, 1_030).is_ok());
    }

    #[test]
    fn validate_promotion_rejects_ledger_from_other_source() {
        let mut ledger = ledger(999);
        ledger.source_node_id = NodeId::from("node-3");
        assert_eq!(
            validate(&ledger, &lease(), 1_020).unwrap_err(),
            PromotionError::SourceMismatch {
                source_node: NodeId::from("node-3"),
                writer_node: NodeId::from(WRITER)
            }
        );
    }

    #[test]
    fn validate_promotion_rejects_excessive_future_skew() {
        assert_eq!(
            validate(&ledger(1_010), &lease(), 1_000).unwrap_err(),
            PromotionError::ClockSkewExceeded {
                synced_at_secs: 1_010,
                skew_secs: 10,
                max_skew_secs: 2
            }
        );
    }

    #[test]
    fn validate_promotion_accepts_skew_at_exact_limit() {
        assert!(validate(&ledger(1_002), &lease(), 1_000).is_ok());
    }

    #[test]
    fn validate_promotion_rejects_skew_one_past_limit() {
        assert_eq!(
            validate(&ledger(1_003), &lease(), 1_000).unwrap_err(),
            PromotionError::ClockSkewExceeded {
                synced_at_secs: 1_003,
                skew_secs: 3,
                max_skew_secs: 2
            }
        );
    }

    #[test]
    fn validate_promotion_rejects_replica_mismatch() {
        let mut ledger = ledger(999);
        ledger.destination_node_id = NodeId::from("node-3");
        assert_eq!(
            validate(&ledger, &lease(), 1_002).unwrap_err(),
            PromotionError::ReplicaMismatch {
                bound_to: NodeId::from("node-3"),
                promoting_node: NodeId::from(CANDIDATE)
            }
        );
    }

    #[test]
    fn validate_promotion_rejects_empty_source_node() {
        let mut ledger = ledger(999);
        ledger.source_node_id = NodeId::from("   ");
        assert_eq!(
            validate(&ledger, &lease(), 1_005).unwrap_err(),
            PromotionError::InvalidSourceNode
        );
    }

    #[test]
    fn validate_promotion_rejects_self_sourced_ledger() {
        let mut ledger = ledger(999);
        ledger.source_node_id = NodeId::from(CANDIDATE);
        assert_eq!(
            validate(&ledger, &lease(), 1_001).unwrap_err(),
            PromotionError::SelfSourcedLedger {
                node_id: NodeId::from(CANDIDATE)
            }
        );
    }

    proptest! {
        #[test]
        fn prop_promotion_lag_invariant(
            renewed_at in 1_000u64..1_000_000,
            delta in 0u64..100,
            max_lag in 1u64..50,
            detection_delay in 0u64..10_000,
        ) {
            let mut lease = lease();
            lease.writer.as_mut().unwrap().renewed_at_secs = renewed_at;
            let synced = renewed_at - delta;
            let policy = PromotionPolicy { max_lag_secs: max_lag, max_future_skew_secs: 5 };

            let res = validate_promotion(
                Some(&ledger(synced)),
                &NodeId::from(CANDIDATE),
                &lease,
                renewed_at + detection_delay,
                policy,
            );
            if delta <= max_lag {
                prop_assert!(res.is_ok());
            } else {
                prop_assert_eq!(res.unwrap_err(), PromotionError::StaleReplica { lag_secs: delta, max_lag_secs: max_lag });
            }
        }

        #[test]
        fn prop_ledger_from_any_other_generation_is_rejected(
            writer_generation in 1u64..1_000,
            ledger_generation in 0u64..1_000,
        ) {
            prop_assume!(writer_generation != ledger_generation);
            let mut lease = lease();
            lease.writer.as_mut().unwrap().generation = writer_generation;
            let mut ledger = ledger(1_000);
            ledger.source_generation = ledger_generation;

            prop_assert_eq!(
                validate(&ledger, &lease, 1_000).unwrap_err(),
                PromotionError::GenerationMismatch { ledger_generation, writer_generation }
            );
        }
    }
}
