//! Controller reconcile and promotion gates — the reconcile loop lands in Phase 1.
#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use hasqlite_core::{DatabaseName, FreshnessSection, NodeId, PromotionSection};
use hasqlite_lease::{FreshnessLedger, LeaseRecord, WriterEpoch};

/// Promotion validation errors reflecting rule 10 of the safety contract.
///
/// Variants are listed in the order the checks run, and the first failure is reported. The lease must
/// first record an activated writer, because its database set says which ledger records to check. The
/// remaining checks run class by class — ledger shape, then writer epoch, then time bounds — and within a
/// class databases are checked in name order (REQ-PROMO-006). The order is part of the contract for the
/// `hasqlite_promotion_denied_total` metric, which is labelled by [`PromotionError::metric_label`].
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum PromotionError {
    /// The lease records no activated writer, so only the genesis procedure can establish one.
    #[error("lease records no activated writer; use the genesis procedure")]
    NoActivatedWriter,
    /// No valid freshness ledger record exists for a database in the writer's set.
    #[error("no freshness ledger recorded for replica")]
    NoFreshnessRecord,
    /// The ledger record found for a database describes a different database.
    #[error("freshness ledger record describes database {recorded}")]
    DatabaseMismatch {
        /// Database named inside the record.
        recorded: DatabaseName,
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
    /// Freshness ledger is pinned to a different destination replica.
    #[error("freshness ledger bound to node {bound_to}, but promoting node is {promoting_node}")]
    ReplicaMismatch {
        /// Node ID the ledger was recorded for.
        bound_to: NodeId,
        /// Node ID attempting promotion.
        promoting_node: NodeId,
    },
    /// The ledger record is of another incarnation than the writer's set entry (removed and re-added name).
    #[error(
        "freshness ledger is of incarnation {ledger_incarnation}, but the database set has incarnation {set_incarnation}"
    )]
    IncarnationMismatch {
        /// Incarnation recorded in the ledger.
        ledger_incarnation: u64,
        /// Incarnation in the writer epoch's database set.
        set_incarnation: u64,
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
    /// Replica snapshot lags the writer's last lease renewal beyond the permitted bound.
    #[error("replica lags the writer's last renewal by {lag_secs}s (max allowed {max_lag_secs}s)")]
    StaleReplica {
        /// Seconds between the snapshot start and the writer's last renewal.
        lag_secs: u64,
        /// Maximum allowed lag in seconds.
        max_lag_secs: u64,
    },
}

impl PromotionError {
    /// Stable, low-cardinality label for the `reason` of `hasqlite_promotion_denied_total`.
    pub fn metric_label(&self) -> &'static str {
        match self {
            Self::NoActivatedWriter => "no_activated_writer",
            Self::NoFreshnessRecord => "no_freshness_record",
            Self::DatabaseMismatch { .. } => "database_mismatch",
            Self::InvalidSourceNode => "invalid_source_node",
            Self::SelfSourcedLedger { .. } => "self_sourced_ledger",
            Self::ReplicaMismatch { .. } => "replica_mismatch",
            Self::IncarnationMismatch { .. } => "incarnation_mismatch",
            Self::GenerationMismatch { .. } => "generation_mismatch",
            Self::SourceMismatch { .. } => "source_mismatch",
            Self::ClockSkewExceeded { .. } => "clock_skew_exceeded",
            Self::StaleReplica { .. } => "stale_replica",
        }
    }
}

/// A denied promotion: the first failed check and, unless it concerns the lease as a whole, its database.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
#[error("promotion denied{}: {reason}", .database.as_ref().map(|db| format!(" for database {db}")).unwrap_or_default())]
pub struct PromotionDenied {
    /// Database whose ledger record failed, or `None` for [`PromotionError::NoActivatedWriter`].
    pub database: Option<DatabaseName>,
    /// The failed check.
    #[source]
    pub reason: PromotionError,
}

/// What a passed promotion gate measured, for the freshest-first delay and the activation record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromotionAssessment {
    /// Largest lag over the database set (REQ-PROMO-007).
    pub max_lag_secs: u64,
    /// Oldest snapshot stamp over the database set, recorded in `writer.promoted_from` (REQ-PROMO-009).
    pub oldest_synced_at_secs: u64,
    /// Writer generation every record was sourced from.
    pub source_generation: u64,
}

/// Thresholds a freshness ledger must satisfy before its replica may be promoted (safety contract rule 10).
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

/// Validates whether a replica meets all freshness and fencing criteria to promote to writer (rule 10).
///
/// `observed_lease` is the lease the candidate is about to replace; its writer epoch's database set lists
/// the databases that must each have a passing record in `ledgers`, keyed by database name. Records that
/// are missing, unreadable or unparseable must simply be absent from the map. Records for databases
/// outside the set are ignored.
///
/// Every record must come from that lease's last activated writer, and lag is measured against that
/// writer's last renewal; both timestamps are stamped by the writer's clock, so the result does not depend
/// on how long failure detection took or on skew between the writer and the candidate. The inputs do not
/// change while the candidate campaigns (scheduled sync is paused), so a successful acquisition CAS needs
/// no second freshness check.
pub fn validate_promotion(
    ledgers: &BTreeMap<DatabaseName, FreshnessLedger>,
    promoting_node: &NodeId,
    observed_lease: &LeaseRecord,
    now_secs: u64,
    policy: PromotionPolicy,
) -> Result<PromotionAssessment, PromotionDenied> {
    let writer = observed_lease.writer.as_ref().ok_or(PromotionDenied {
        database: None,
        reason: PromotionError::NoActivatedWriter,
    })?;

    // Ledger shape: every database in the set has a well-formed record bound to this replica.
    let mut records = Vec::with_capacity(writer.databases.len());
    for entry in writer.databases.entries() {
        let deny = |reason| PromotionDenied {
            database: Some(entry.name.clone()),
            reason,
        };
        let ledger = ledgers
            .get(&entry.name)
            .ok_or_else(|| deny(PromotionError::NoFreshnessRecord))?;
        check_ledger_shape(ledger, &entry.name, entry.incarnation, promoting_node).map_err(deny)?;
        records.push(ledger);
    }

    // Writer epoch: every record was sourced from the lease's last activated writer.
    for ledger in &records {
        check_writer_epoch(ledger, writer).map_err(|reason| PromotionDenied {
            database: Some(ledger.database.clone()),
            reason,
        })?;
    }

    // Time bounds: no record is stamped in the future or lags the writer's last renewal too far.
    let mut assessment = PromotionAssessment {
        max_lag_secs: 0,
        oldest_synced_at_secs: u64::MAX,
        source_generation: writer.generation,
    };
    for ledger in &records {
        let lag = check_time_bounds(ledger, writer, now_secs, policy).map_err(|reason| {
            PromotionDenied {
                database: Some(ledger.database.clone()),
                reason,
            }
        })?;
        assessment.max_lag_secs = assessment.max_lag_secs.max(lag);
        assessment.oldest_synced_at_secs =
            assessment.oldest_synced_at_secs.min(ledger.synced_at_secs);
    }
    Ok(assessment)
}

/// Checks that `ledger` describes `database` at `incarnation`, has a valid foreign source and is bound to
/// `promoting_node`.
fn check_ledger_shape(
    ledger: &FreshnessLedger,
    database: &DatabaseName,
    incarnation: u64,
    promoting_node: &NodeId,
) -> Result<(), PromotionError> {
    if &ledger.database != database {
        return Err(PromotionError::DatabaseMismatch {
            recorded: ledger.database.clone(),
        });
    }
    if !ledger.source_node_id.is_valid() {
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
    if ledger.incarnation != incarnation {
        return Err(PromotionError::IncarnationMismatch {
            ledger_incarnation: ledger.incarnation,
            set_incarnation: incarnation,
        });
    }
    Ok(())
}

/// Checks that `ledger` was sourced from `writer`'s node and generation.
fn check_writer_epoch(
    ledger: &FreshnessLedger,
    writer: &WriterEpoch,
) -> Result<(), PromotionError> {
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
    Ok(())
}

/// Checks future skew against `now_secs` and lag behind `writer`'s last renewal; returns the lag.
fn check_time_bounds(
    ledger: &FreshnessLedger,
    writer: &WriterEpoch,
    now_secs: u64,
    policy: PromotionPolicy,
) -> Result<u64, PromotionError> {
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
    Ok(lag)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hasqlite_lease::{DatabaseEntry, DatabaseSet, Tombstones};
    use proptest::prelude::*;

    const WRITER: &str = "node-1";
    const CANDIDATE: &str = "node-2";
    const WRITER_GENERATION: u64 = 5;
    const WRITER_RENEWED_AT: u64 = 1_000;
    /// Database set of the test writer epoch, in name order, with incarnations.
    const DATABASES: [(&str, u64); 2] = [("app.db", 1), ("tenants/acme.db", 3)];

    /// Promotion policy with a 10s lag bound and 2s future-skew tolerance.
    fn policy() -> PromotionPolicy {
        PromotionPolicy {
            max_lag_secs: 10,
            max_future_skew_secs: 2,
        }
    }

    /// Parses a database name.
    fn db(name: &str) -> DatabaseName {
        DatabaseName::parse(name).unwrap()
    }

    /// Expired lease whose last activated writer is `node-1` at generation `WRITER_GENERATION`, last
    /// renewed at `WRITER_RENEWED_AT`, replicating `DATABASES`.
    fn lease() -> LeaseRecord {
        LeaseRecord {
            holder_node_id: Some(NodeId::new(WRITER)),
            generation: WRITER_GENERATION,
            renewed_at_secs: WRITER_RENEWED_AT,
            ttl_secs: 10,
            writer: Some(WriterEpoch {
                node_id: NodeId::new(WRITER),
                generation: WRITER_GENERATION,
                renewed_at_secs: WRITER_RENEWED_AT,
                databases: DatabaseSet::new(
                    DATABASES
                        .iter()
                        .map(|&(name, incarnation)| DatabaseEntry {
                            name: db(name),
                            incarnation,
                        })
                        .collect(),
                )
                .unwrap(),
                next_incarnation: 4,
                removed: Tombstones::default(),
            }),
        }
    }

    /// Ledger record for `database` (at its set incarnation) of a snapshot of `node-1` generation
    /// `WRITER_GENERATION`, delivered to `node-2`.
    fn record(database: &str, synced_at_secs: u64) -> FreshnessLedger {
        let incarnation = DATABASES
            .iter()
            .find(|(name, _)| *name == database)
            .map_or(1, |&(_, incarnation)| incarnation);
        FreshnessLedger {
            database: db(database),
            incarnation,
            source_node_id: NodeId::new(WRITER),
            source_generation: WRITER_GENERATION,
            synced_at_secs,
            destination_node_id: NodeId::new(CANDIDATE),
        }
    }

    /// Passing records for every database in `DATABASES`, all synced at `synced_at_secs`.
    fn ledgers(synced_at_secs: u64) -> BTreeMap<DatabaseName, FreshnessLedger> {
        DATABASES
            .iter()
            .map(|&(name, _)| (db(name), record(name, synced_at_secs)))
            .collect()
    }

    /// Applies `mutate` to the `app.db` record of passing ledgers synced at `synced_at_secs`.
    fn with_app(
        synced_at_secs: u64,
        mutate: impl FnOnce(&mut FreshnessLedger),
    ) -> BTreeMap<DatabaseName, FreshnessLedger> {
        let mut ledgers = ledgers(synced_at_secs);
        mutate(ledgers.get_mut(&db("app.db")).unwrap());
        ledgers
    }

    /// Validates `ledgers` for `node-2` against `lease` with the default test policy.
    fn validate(
        ledgers: &BTreeMap<DatabaseName, FreshnessLedger>,
        lease: &LeaseRecord,
        now_secs: u64,
    ) -> Result<PromotionAssessment, PromotionDenied> {
        validate_promotion(ledgers, &NodeId::new(CANDIDATE), lease, now_secs, policy())
    }

    /// Validates and returns the denial reason, asserting it names `database`.
    fn denial(
        ledgers: &BTreeMap<DatabaseName, FreshnessLedger>,
        lease: &LeaseRecord,
        now_secs: u64,
        database: &str,
    ) -> PromotionError {
        let denied = validate(ledgers, lease, now_secs).unwrap_err();
        assert_eq!(denied.database, Some(db(database)), "denial: {denied}");
        denied.reason
    }

    #[test]
    fn promotion_policy_maps_config_sections() {
        let freshness = FreshnessSection {
            dir: "/var/lib/hasqlite/.hasqlite.freshness".into(),
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
    fn validate_promotion_accepts_fresh_replica_and_reports_worst_lag() {
        let mut ledgers = ledgers(995);
        ledgers
            .get_mut(&db("tenants/acme.db"))
            .unwrap()
            .synced_at_secs = 998;
        assert_eq!(
            validate(&ledgers, &lease(), 1_020).unwrap(),
            PromotionAssessment {
                max_lag_secs: 5,
                oldest_synced_at_secs: 995,
                source_generation: WRITER_GENERATION,
            }
        );
    }

    #[test]
    fn validate_promotion_ignores_failure_detection_delay() {
        // The writer stopped renewing long ago; lag is still measured against its last renewal.
        assert!(validate(&ledgers(995), &lease(), 10_000).is_ok());
    }

    #[test]
    fn validate_promotion_accepts_snapshot_after_last_renewal() {
        let assessment = validate(&ledgers(1_003), &lease(), 1_003).unwrap();
        assert_eq!(assessment.max_lag_secs, 0);
    }

    #[test]
    fn validate_promotion_ignores_records_outside_the_set() {
        let mut ledgers = ledgers(995);
        let mut stray = record("removed.db", 1);
        stray.source_generation = 1;
        ledgers.insert(db("removed.db"), stray);
        assert!(validate(&ledgers, &lease(), 1_000).is_ok());
    }

    #[test]
    fn validate_promotion_rejects_missing_record_for_any_database() {
        let mut ledgers = ledgers(995);
        ledgers.remove(&db("tenants/acme.db"));
        assert_eq!(
            denial(&ledgers, &lease(), 1_000, "tenants/acme.db"),
            PromotionError::NoFreshnessRecord
        );
        assert_eq!(
            denial(&BTreeMap::new(), &lease(), 1_000, "app.db"),
            PromotionError::NoFreshnessRecord
        );
    }

    #[test]
    fn validate_promotion_rejects_lease_without_activated_writer() {
        let mut lease = lease();
        lease.writer = None;
        let denied = validate(&ledgers(995), &lease, 1_000).unwrap_err();
        assert_eq!(denied.database, None);
        assert_eq!(denied.reason, PromotionError::NoActivatedWriter);
        assert_eq!(
            denied.to_string(),
            "promotion denied: lease records no activated writer; use the genesis procedure"
        );
    }

    #[test]
    fn validate_promotion_accepts_lag_at_exact_limit() {
        assert!(validate(&ledgers(990), &lease(), 1_020).is_ok());
    }

    #[test]
    fn validate_promotion_rejects_lag_one_past_limit() {
        let ledgers = with_app(995, |r| r.synced_at_secs = 989);
        assert_eq!(
            denial(&ledgers, &lease(), 1_020, "app.db"),
            PromotionError::StaleReplica {
                lag_secs: 11,
                max_lag_secs: 10
            }
        );
    }

    #[test]
    fn validate_promotion_rejects_ledger_from_older_writer_generation() {
        let ledgers = with_app(999, |r| r.source_generation = 4);
        assert_eq!(
            denial(&ledgers, &lease(), 1_020, "app.db"),
            PromotionError::GenerationMismatch {
                ledger_generation: 4,
                writer_generation: WRITER_GENERATION
            }
        );
    }

    #[test]
    fn validate_promotion_rejects_record_of_another_incarnation() {
        // `tenants/acme.db` was removed and re-added; the replica still holds the old incarnation's record.
        let mut ledgers = ledgers(999);
        ledgers.get_mut(&db("tenants/acme.db")).unwrap().incarnation = 2;
        assert_eq!(
            denial(&ledgers, &lease(), 1_000, "tenants/acme.db"),
            PromotionError::IncarnationMismatch {
                ledger_incarnation: 2,
                set_incarnation: 3
            }
        );
    }

    #[test]
    fn validate_promotion_rejects_record_filed_under_another_database() {
        let ledgers = with_app(999, |r| r.database = db("other.db"));
        assert_eq!(
            denial(&ledgers, &lease(), 1_000, "app.db"),
            PromotionError::DatabaseMismatch {
                recorded: db("other.db")
            }
        );
    }

    #[test]
    fn validate_promotion_survives_aborted_acquisition() {
        // A peer acquired generation 6 but never activated, so the generation-5 writer epoch still applies.
        let mut lease = lease();
        lease.holder_node_id = Some(NodeId::new("node-3"));
        lease.generation = 6;
        lease.renewed_at_secs = 1_015;
        assert!(validate(&ledgers(995), &lease, 1_030).is_ok());
    }

    #[test]
    fn validate_promotion_rejects_ledger_from_other_source() {
        let ledgers = with_app(999, |r| r.source_node_id = NodeId::new("node-3"));
        assert_eq!(
            denial(&ledgers, &lease(), 1_020, "app.db"),
            PromotionError::SourceMismatch {
                source_node: NodeId::new("node-3"),
                writer_node: NodeId::new(WRITER)
            }
        );
    }

    #[test]
    fn validate_promotion_rejects_excessive_future_skew() {
        let ledgers = with_app(999, |r| r.synced_at_secs = 1_010);
        assert_eq!(
            denial(&ledgers, &lease(), 1_000, "app.db"),
            PromotionError::ClockSkewExceeded {
                synced_at_secs: 1_010,
                skew_secs: 10,
                max_skew_secs: 2
            }
        );
    }

    #[test]
    fn validate_promotion_accepts_skew_at_exact_limit() {
        assert!(validate(&ledgers(1_002), &lease(), 1_000).is_ok());
    }

    #[test]
    fn validate_promotion_rejects_skew_one_past_limit() {
        let ledgers = with_app(1_000, |r| r.synced_at_secs = 1_003);
        assert_eq!(
            denial(&ledgers, &lease(), 1_000, "app.db"),
            PromotionError::ClockSkewExceeded {
                synced_at_secs: 1_003,
                skew_secs: 3,
                max_skew_secs: 2
            }
        );
    }

    #[test]
    fn validate_promotion_rejects_replica_mismatch() {
        let ledgers = with_app(999, |r| r.destination_node_id = NodeId::new("node-3"));
        assert_eq!(
            denial(&ledgers, &lease(), 1_002, "app.db"),
            PromotionError::ReplicaMismatch {
                bound_to: NodeId::new("node-3"),
                promoting_node: NodeId::new(CANDIDATE)
            }
        );
    }

    #[test]
    fn validate_promotion_rejects_empty_or_unsafe_source_node() {
        for source in ["   ", "node 1", ""] {
            let ledgers = with_app(999, |r| r.source_node_id = NodeId::new(source));
            assert_eq!(
                denial(&ledgers, &lease(), 1_005, "app.db"),
                PromotionError::InvalidSourceNode,
                "source '{source}'"
            );
        }
    }

    #[test]
    fn validate_promotion_rejects_self_sourced_ledger() {
        let ledgers = with_app(999, |r| r.source_node_id = NodeId::new(CANDIDATE));
        assert_eq!(
            denial(&ledgers, &lease(), 1_001, "app.db"),
            PromotionError::SelfSourcedLedger {
                node_id: NodeId::new(CANDIDATE)
            }
        );
    }

    #[test]
    fn validate_promotion_reports_errors_in_documented_precedence() {
        // No activated writer wins over everything: without it there is no database set to check.
        let mut no_writer = lease();
        no_writer.writer = None;
        assert_eq!(
            validate(&BTreeMap::new(), &no_writer, 1_000)
                .unwrap_err()
                .reason,
            PromotionError::NoActivatedWriter
        );

        // Ledger shape on a later database precedes writer epoch and time bounds on an earlier one.
        let mut ledgers = with_app(5_000, |r| r.source_generation = 4);
        ledgers
            .get_mut(&db("tenants/acme.db"))
            .unwrap()
            .destination_node_id = NodeId::new("node-3");
        assert!(matches!(
            denial(&ledgers, &lease(), 1_000, "tenants/acme.db"),
            PromotionError::ReplicaMismatch { .. }
        ));

        // Within ledger shape: self-sourced precedes binding, which precedes incarnation.
        let ledgers = with_app(999, |r| {
            r.source_node_id = NodeId::new(CANDIDATE);
            r.destination_node_id = NodeId::new("node-3");
            r.incarnation = 9;
        });
        assert!(matches!(
            denial(&ledgers, &lease(), 1_000, "app.db"),
            PromotionError::SelfSourcedLedger { .. }
        ));
        let ledgers = with_app(999, |r| {
            r.destination_node_id = NodeId::new("node-3");
            r.incarnation = 9;
        });
        assert!(matches!(
            denial(&ledgers, &lease(), 1_000, "app.db"),
            PromotionError::ReplicaMismatch { .. }
        ));

        // Writer epoch on a later database precedes time bounds on an earlier one.
        let mut ledgers = with_app(5_000, |_| {});
        ledgers
            .get_mut(&db("tenants/acme.db"))
            .unwrap()
            .source_generation = 4;
        assert!(matches!(
            denial(&ledgers, &lease(), 1_000, "tenants/acme.db"),
            PromotionError::GenerationMismatch { .. }
        ));

        // Within time bounds, databases go in name order, and future skew precedes staleness.
        let mut ledgers = ledgers_for_time_precedence();
        assert!(matches!(
            denial(&ledgers, &lease(), 1_000, "app.db"),
            PromotionError::StaleReplica { .. }
        ));
        ledgers.get_mut(&db("app.db")).unwrap().synced_at_secs = 1_010;
        assert!(matches!(
            denial(&ledgers, &lease(), 1_000, "app.db"),
            PromotionError::ClockSkewExceeded { .. }
        ));
    }

    /// `app.db` stale and `tenants/acme.db` future-skewed.
    fn ledgers_for_time_precedence() -> BTreeMap<DatabaseName, FreshnessLedger> {
        let mut ledgers = with_app(0, |r| r.synced_at_secs = 900);
        ledgers
            .get_mut(&db("tenants/acme.db"))
            .unwrap()
            .synced_at_secs = 1_010;
        ledgers
    }

    #[test]
    fn denial_display_names_the_database() {
        let ledgers = with_app(999, |r| r.source_generation = 4);
        assert_eq!(
            validate(&ledgers, &lease(), 1_000).unwrap_err().to_string(),
            "promotion denied for database app.db: freshness ledger is from generation 4, but the last activated writer has generation 5"
        );
    }

    #[test]
    fn metric_labels_are_unique() {
        let errors = [
            PromotionError::NoActivatedWriter,
            PromotionError::NoFreshnessRecord,
            PromotionError::DatabaseMismatch {
                recorded: db("a.db"),
            },
            PromotionError::InvalidSourceNode,
            PromotionError::SelfSourcedLedger {
                node_id: NodeId::new(CANDIDATE),
            },
            PromotionError::ReplicaMismatch {
                bound_to: NodeId::new(WRITER),
                promoting_node: NodeId::new(CANDIDATE),
            },
            PromotionError::IncarnationMismatch {
                ledger_incarnation: 1,
                set_incarnation: 2,
            },
            PromotionError::GenerationMismatch {
                ledger_generation: 1,
                writer_generation: 2,
            },
            PromotionError::SourceMismatch {
                source_node: NodeId::new(WRITER),
                writer_node: NodeId::new(CANDIDATE),
            },
            PromotionError::ClockSkewExceeded {
                synced_at_secs: 1,
                skew_secs: 1,
                max_skew_secs: 0,
            },
            PromotionError::StaleReplica {
                lag_secs: 1,
                max_lag_secs: 0,
            },
        ];
        let labels: std::collections::HashSet<_> =
            errors.iter().map(PromotionError::metric_label).collect();
        assert_eq!(labels.len(), errors.len());
    }

    proptest! {
        #[test]
        fn prop_promotion_lag_invariant(
            renewed_at in 1_000u64..1_000_000,
            delta in 0u64..100,
            other_delta in 0u64..100,
            max_lag in 1u64..50,
            detection_delay in 0u64..10_000,
        ) {
            let mut lease = lease();
            lease.writer.as_mut().unwrap().renewed_at_secs = renewed_at;
            let mut ledgers = with_app(renewed_at - other_delta, |r| r.synced_at_secs = renewed_at - delta);
            ledgers.get_mut(&db("tenants/acme.db")).unwrap().synced_at_secs = renewed_at - other_delta;
            let policy = PromotionPolicy { max_lag_secs: max_lag, max_future_skew_secs: 5 };

            let res = validate_promotion(
                &ledgers,
                &NodeId::new(CANDIDATE),
                &lease,
                renewed_at + detection_delay,
                policy,
            );
            match (delta <= max_lag, other_delta <= max_lag) {
                (true, true) => prop_assert_eq!(res.unwrap().max_lag_secs, delta.max(other_delta)),
                (false, _) => prop_assert_eq!(
                    res.unwrap_err(),
                    PromotionDenied {
                        database: Some(db("app.db")),
                        reason: PromotionError::StaleReplica { lag_secs: delta, max_lag_secs: max_lag },
                    }
                ),
                (true, false) => prop_assert_eq!(
                    res.unwrap_err(),
                    PromotionDenied {
                        database: Some(db("tenants/acme.db")),
                        reason: PromotionError::StaleReplica { lag_secs: other_delta, max_lag_secs: max_lag },
                    }
                ),
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
            let mut ledgers = ledgers(1_000);
            for record in ledgers.values_mut() {
                record.source_generation = writer_generation;
            }
            ledgers.get_mut(&db("tenants/acme.db")).unwrap().source_generation = ledger_generation;

            prop_assert_eq!(
                validate(&ledgers, &lease, 1_000).unwrap_err(),
                PromotionDenied {
                    database: Some(db("tenants/acme.db")),
                    reason: PromotionError::GenerationMismatch { ledger_generation, writer_generation },
                }
            );
        }
    }
}
