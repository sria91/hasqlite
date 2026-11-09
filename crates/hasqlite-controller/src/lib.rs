//! Controller reconcile and promotion gates — implementation lands in Phase 1.

use hasqlite_core::NodeId;
use hasqlite_lease::{FreshnessLedger, LeaseRecord};

/// Placeholder for the tick outcome type (will match rsqlite-rsync `HaController`).
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
    /// Replica data age exceeds the maximum permissible threshold.
    #[error("freshness age {age_secs}s exceeds max allowed age {max_age_secs}s")]
    StaleReplica {
        /// Age of the replica snapshot in seconds.
        age_secs: u64,
        /// Maximum allowed freshness age in seconds.
        max_age_secs: u64,
    },
    /// Replica generation is strictly lower than the minimum required generation.
    #[error("source generation {source_gen} is below minimum allowed generation {min_gen}")]
    GenerationTooLow {
        /// Generation counter observed on the replica.
        source_gen: u64,
        /// Minimum permissible generation counter.
        min_gen: u64,
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
}

/// Validates whether a replica meets all freshness and fencing criteria to promote to writer (§7).
pub fn validate_promotion(
    freshness: Option<&FreshnessLedger>,
    promoting_node: &NodeId,
    now_secs: u64,
    max_freshness_age_secs: u64,
    min_source_generation: u64,
    max_future_skew_secs: u64,
) -> Result<(), PromotionError> {
    let ledger = freshness.ok_or(PromotionError::NoFreshnessRecord)?;

    if ledger.source_node_id.as_str().trim().is_empty() {
        return Err(PromotionError::InvalidSourceNode);
    }

    if let Some(ref bound_node) = ledger.destination_node_id
        && bound_node != promoting_node
    {
        return Err(PromotionError::ReplicaMismatch {
            bound_to: bound_node.clone(),
            promoting_node: promoting_node.clone(),
        });
    }

    if ledger.source_generation < min_source_generation {
        return Err(PromotionError::GenerationTooLow {
            source_gen: ledger.source_generation,
            min_gen: min_source_generation,
        });
    }

    if ledger.synced_at_secs > now_secs {
        let skew = ledger.synced_at_secs - now_secs;
        if skew > max_future_skew_secs {
            return Err(PromotionError::ClockSkewExceeded {
                synced_at_secs: ledger.synced_at_secs,
                skew_secs: skew,
                max_skew_secs: max_future_skew_secs,
            });
        }
    } else {
        let age = now_secs - ledger.synced_at_secs;
        if age > max_freshness_age_secs {
            return Err(PromotionError::StaleReplica {
                age_secs: age,
                max_age_secs: max_freshness_age_secs,
            });
        }
    }

    Ok(())
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

    #[test]
    fn validate_promotion_accepts_fresh_replica() {
        let promoting_node = NodeId::from("node-2");
        let ledger = FreshnessLedger {
            source_node_id: NodeId::from("node-1"),
            source_generation: 5,
            synced_at_secs: 1_000,
            destination_node_id: Some(promoting_node.clone()),
        };

        let result = validate_promotion(Some(&ledger), &promoting_node, 1_005, 10, 1, 2);
        assert!(result.is_ok());
    }

    #[test]
    fn validate_promotion_rejects_missing_ledger() {
        let promoting_node = NodeId::from("node-2");
        let result = validate_promotion(None, &promoting_node, 1_000, 10, 1, 2);
        assert_eq!(result.unwrap_err(), PromotionError::NoFreshnessRecord);
    }

    #[test]
    fn validate_promotion_rejects_stale_ledger() {
        let promoting_node = NodeId::from("node-2");
        let ledger = FreshnessLedger {
            source_node_id: NodeId::from("node-1"),
            source_generation: 5,
            synced_at_secs: 1_000,
            destination_node_id: Some(promoting_node.clone()),
        };

        let result = validate_promotion(Some(&ledger), &promoting_node, 1_015, 10, 1, 2);
        assert_eq!(
            result.unwrap_err(),
            PromotionError::StaleReplica {
                age_secs: 15,
                max_age_secs: 10
            }
        );
    }

    #[test]
    fn validate_promotion_rejects_generation_below_minimum() {
        let promoting_node = NodeId::from("node-2");
        let ledger = FreshnessLedger {
            source_node_id: NodeId::from("node-1"),
            source_generation: 2,
            synced_at_secs: 1_000,
            destination_node_id: None,
        };

        let result = validate_promotion(Some(&ledger), &promoting_node, 1_002, 10, 5, 2);
        assert_eq!(
            result.unwrap_err(),
            PromotionError::GenerationTooLow {
                source_gen: 2,
                min_gen: 5
            }
        );
    }

    #[test]
    fn validate_promotion_rejects_excessive_future_skew() {
        let promoting_node = NodeId::from("node-2");
        let ledger = FreshnessLedger {
            source_node_id: NodeId::from("node-1"),
            source_generation: 5,
            synced_at_secs: 1_010,
            destination_node_id: None,
        };

        let result = validate_promotion(Some(&ledger), &promoting_node, 1_000, 10, 1, 2);
        assert_eq!(
            result.unwrap_err(),
            PromotionError::ClockSkewExceeded {
                synced_at_secs: 1_010,
                skew_secs: 10,
                max_skew_secs: 2
            }
        );
    }

    #[test]
    fn validate_promotion_rejects_replica_mismatch() {
        let promoting_node = NodeId::from("node-2");
        let ledger = FreshnessLedger {
            source_node_id: NodeId::from("node-1"),
            source_generation: 5,
            synced_at_secs: 1_000,
            destination_node_id: Some(NodeId::from("node-3")),
        };

        let result = validate_promotion(Some(&ledger), &promoting_node, 1_002, 10, 1, 2);
        assert_eq!(
            result.unwrap_err(),
            PromotionError::ReplicaMismatch {
                bound_to: NodeId::from("node-3"),
                promoting_node: NodeId::from("node-2")
            }
        );
    }

    #[test]
    fn validate_promotion_rejects_empty_source_node() {
        let promoting_node = NodeId::from("node-2");
        let ledger = FreshnessLedger {
            source_node_id: NodeId::from("   "),
            source_generation: 5,
            synced_at_secs: 1_000,
            destination_node_id: Some(promoting_node.clone()),
        };

        let result = validate_promotion(Some(&ledger), &promoting_node, 1_005, 10, 1, 2);
        assert_eq!(result.unwrap_err(), PromotionError::InvalidSourceNode);
    }

    #[test]
    fn validate_promotion_accepts_zero_age_exact_sync() {
        let promoting_node = NodeId::from("node-2");
        let ledger = FreshnessLedger {
            source_node_id: NodeId::from("node-1"),
            source_generation: 5,
            synced_at_secs: 1_000,
            destination_node_id: Some(promoting_node.clone()),
        };

        let result = validate_promotion(Some(&ledger), &promoting_node, 1_000, 10, 1, 2);
        assert!(result.is_ok());
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_promotion_freshness_age_invariant(
            now_secs in 1_000u64..1_000_000,
            delta in 0u64..100,
            max_age in 1u64..50,
        ) {
            let promoting = NodeId::from("node-self");
            let synced = now_secs.saturating_sub(delta);
            let ledger = FreshnessLedger {
                source_node_id: NodeId::from("node-src"),
                source_generation: 10,
                synced_at_secs: synced,
                destination_node_id: Some(promoting.clone()),
            };

            let res = validate_promotion(Some(&ledger), &promoting, now_secs, max_age, 1, 5);
            let age = now_secs - synced;
            if age <= max_age {
                prop_assert!(res.is_ok());
            } else {
                prop_assert_eq!(res.unwrap_err(), PromotionError::StaleReplica { age_secs: age, max_age_secs: max_age });
            }
        }
    }
}
