# hasqlite — Operations and Risks

## 9. Observability (planned)

| Signal | Format |
|--------|--------|
| Audit log | append-only JSON lines on the PVC (action, from, to, lease gen, first and last commit time per generation) |
| Role state file | `replica` / `writer:<gen>` (informational; never used as authority) |
| Metrics | Prometheus: `hasqlite_sync_lag_seconds`, `hasqlite_lease_held`, `hasqlite_promotion_denied_total` (by `PromotionError` variant), `hasqlite_renewal_deadline_misses_total`, `hasqlite_origin_wal_bytes`, `hasqlite_snapshot_future_skew_seconds` (replica: origin-stamped snapshot start minus local wall time at ledger publication) |
| Tracing | `tracing` spans: `tick`, `sync`, `promote` |

Alerts (recommended):

- `sync_lag > freshness.max_lag_secs` for > 2 intervals
- origin WAL size growing across checkpoints
- promotion denied spike
- lease holder missing > TTL
- snapshot future skew approaching `max_future_skew_secs` (writer clock ahead; failover would be denied)

---

## 10. Risks and mitigations

This section distinguishes architectural limitations, expected failure behavior,
and unresolved guarantees. Mitigations and test assertions elsewhere in this
plan are delivery requirements, not evidence that they are already implemented
or verified. The [safety contract](safety-contract.md) remains normative.

### 10.1 Risk summary

This is a quick-reference overview; [§10.2](#102-data-safety-and-durability-limitations)–[§10.5](#105-deployment-access-and-compatibility-constraints) expand the failure conditions and
constraints, and [§10.6](#106-unresolved-guarantees-and-recovery-policy) identifies unresolved guarantees and recovery policy.

| Risk | Mitigation |
|------|------------|
| Stale replica promoted | Lag gate against the writer epoch's last renewal + writer-epoch match + default deny; manual recovery runbook ([§10.3](#103-availability-and-promotion-failure-modes)) |
| Dual writer during partition | Candidates expire leases on local monotonic time; writer stops commits at its renewal deadline; renewals are CAS; gateway write fence before `COMMIT` ([§10.2](#102-data-safety-and-durability-limitations), [§10.3](#103-availability-and-promotion-failure-modes)) |
| Writer stalled inside a commit longer than the fencing margin | Residual risk (no storage-level fence): margin ≥ 2s enforced, pause-injection chaos tests, audit log lets operators detect overlap ([§10.2](#102-data-safety-and-durability-limitations), [§10.6](#106-unresolved-guarantees-and-recovery-policy)) |
| Torn replica promoted after crash during sync | Ledger invalidated before apply, published only after fsync (Phase 2); atomic apply in Track N ([§10.2](#102-data-safety-and-durability-limitations)) |
| Writer dies before any replica syncs from it | Fail closed; manual promotion runbook; alert on lag ([§10.3](#103-availability-and-promotion-failure-modes)) |
| Lost or duplicated client writes on failover | Client retries only definite `NOT_LEADER`; "outcome unknown" surfaced; optional idempotency key, whose failover-safe deduplication semantics are unresolved ([§10.2](#102-data-safety-and-durability-limitations), [§10.6](#106-unresolved-guarantees-and-recovery-policy)) |
| Large catch-up after failover | PreStop sync + pre-activation catch-up from a fresher peer; monitor lag ([§10.3](#103-availability-and-promotion-failure-modes)) |
| Upstream API changes during transition | Pin the adapter by commit, build with `--locked`, isolate it; remove the integration at native-backend cutover ([§10.5](#105-deployment-access-and-compatibility-constraints)) |
| StatefulSet rollout stall or repeated failovers | Document `/healthz` vs `/ready`; writer-last rollout with planned switchover; PodDisruptionBudget ([§10.5](#105-deployment-access-and-compatibility-constraints)) |

### 10.2 Data safety and durability limitations

| Condition | Consequence and requirement |
|-----------|-----------------------------|
| Asynchronous replication | An unplanned failover can lose acknowledged writes. The target loss window is at most `freshness.max_lag_secs + lease.renew_deadline_secs` (14 seconds with the example configuration), not zero RPO. This is a time-window target, not a bound on the number of lost transactions; it assumes the writer's wall clock accurately measures elapsed time and no commit stalls beyond the fencing margin (next rows). |
| No storage-level generation fence | Each node writes its own disk; a generation counter does not physically reject an old writer's disk writes. Split-brain prevention depends on cooperative self-fencing and the timing assumption below. |
| Writer stalls inside `COMMIT` beyond the fencing margin | A commit can finish after a peer promotes. The default margin is 4 seconds and the minimum is 2 seconds. The `enforce_write_fence` check before `COMMIT` and `sqlite3_interrupt` at the deadline do not eliminate this residual risk. |
| Replica crashes during in-place page application | The replica can contain a partially applied snapshot. Invalidate and fsync the ledger before application; deny promotion until a full sync and `PRAGMA integrity_check` pass. Track N provides atomic/locked application. |
| Disk full during page application or ledger publication | Sync fails and the ledger remains invalid; the affected node cannot promote. |
| Former writer contains divergent commits | Preserve its database before resync. Those commits are not automatically merged into the new writer; recovery may require operator reconciliation. |
| Commit completes but its response is lost | The client has an unknown outcome. Blind retries can duplicate operations; surface uncertainty and retry automatically only when rejection before execution is definite. |

SQLite transaction atomicity and replication durability are separate concerns.
Power loss during normal SQLite writes should be recoverable when journaling,
sync settings, and storage behave correctly; that does not make external,
in-place replica page replacement transactional. Nor does local crash recovery
restore writes absent from the promoted replica.

### 10.3 Availability and promotion failure modes

| Condition | Consequence and requirement |
|-----------|-----------------------------|
| Writer loses access to coordination | It stops committing at its renewal deadline and demotes, even if its database and client network remain healthy. |
| Lease store is unreachable from all nodes | The writer demotes; no replacement can acquire authority until the store returns. A local expiry timer alone never authorizes promotion. |
| Writer is partitioned from replicas but can renew | Writes continue while replica lag grows. If the writer later fails, all replicas may be too stale to promote. |
| All replicas exceed the freshness limit | Automatic promotion is denied; availability is sacrificed to limit data loss. While the activated writer is still serving, catch-up sync from it restores eligibility. Once it has failed, the failed writer remains the last activated writer in the observed lease, so a stale ledger may still match its epoch, and `validate_promotion` rejects promotion because lag exceeds the limit; automatic promotion is blocked and manual recovery is required. |
| Activated writer dies before any replica syncs from it | Replicas have the wrong writer epoch and automatic failover is blocked. An acquired but never-activated generation does not have this effect. |
| Ledger is missing, invalid, bound to another destination, self-sourced, or from the wrong writer epoch | Promotion fails closed; apparent file recency is insufficient. |
| Snapshot timestamp exceeds the allowed future skew | Promotion is denied while the snapshot's timestamp still exceeds the skew limit at promotion time. Lease expiry uses monotonic time, but the promotion future-timestamp check compares the writer-stamped snapshot time with the candidate's local wall time (safety contract rule 10). If the snapshot ages until its timestamp is within the skew limit, promotion is no longer denied on this basis alone; denial therefore applies only as long as the skew condition persists. A writer whose clock runs more than `max_future_skew_secs` ahead of every replica keeps all replicas unpromotable for as long as it continues stamping snapshots beyond the limit; alert on `hasqlite_snapshot_future_skew_seconds` ([§8](configuration.md#8-configuration-contract-v1)). |
| Acquisition or activation CAS loses a race | The candidate remains a replica. Acquisition without activation never permits writable SQL handles. |
| Candidate crashes between acquisition and activation | The previous writer epoch remains valid for eligible peers, but another acquisition may have to wait for the abandoned holder's TTL. |
| Writer crashes without releasing the lease | Each candidate waits for an unchanged lease version for a full locally measured TTL before campaigning; failover is not instantaneous. |
| Slow storage, coordination, or catch-up | Recovery can exceed the p95 RTO target of 15 seconds. That target applies to the specified test environment, not every deployment. |
| Lag-proportional acquisition delay | It favors the freshest eligible replica but does not guarantee that replica wins the CAS race. |

### 10.4 Replication and request-serving limits

| Condition | Consequence and requirement |
|-----------|-----------------------------|
| Single writer | Replicas do not scale write execution; multi-writer SQL and cross-region active/active are outside v1 scope. |
| One replicated database per cluster in v1 | One freshness ledger covers one database. Standalone multi-database serving is not clustered multi-database replication. |
| Replica reads with the transitional adapter | Unsupported because concurrent in-place application can expose torn pages. Track N must provide safe atomic/locked application first. |
| Replica reads after safe application is implemented | Reads can still be stale; they are not automatically equivalent to reading the writer. |
| Sync transport, credential, or TLS failure | Retry and alert; snapshots stop advancing and affected replicas can become unpromotable. |
| Stalled snapshot session | Its read transaction can block WAL checkpoint progress and grow the origin WAL. Track N must enforce a snapshot deadline and stop replication sessions before the shutdown checkpoint. |
| Switchover write-service gap | Requests can be rejected while the old writer stops and a successor activates. The plan does not provide a durable SQL request queue. |
| Final-sync or drain timeout during planned switchover | A timeout does not establish a lossless handover. The intended successor must durably contain every final committed write to meet the planned-switchover target; see unresolved policy in [§10.6](#106-unresolved-guarantees-and-recovery-policy). |

### 10.5 Deployment, access, and compatibility constraints

| Condition | Consequence and requirement |
|-----------|-----------------------------|
| File lease backend | Single-host only; not a supported cross-host coordination mechanism. |
| Standalone storage and lock identity | Requires local storage and the canonical, never-replaced lock file. NFS, CIFS/SMB, and FUSE are refused. Independent lock paths or replacing the lock inode can defeat mutual exclusion. |
| Standalone-to-cluster migration | Fence the standalone writer before switching coordination backends and seed generation continuity. File locks and Kubernetes leases do not exclude each other. |
| Kubernetes routing propagation delay | Role labels and Service endpoints can lag leadership. Old connections may still reach the former writer; write fencing, not routing, must reject writes. `/ready=503` alone does not enforce the writer Service's routing. |
| Wrong probes or rollout ordering | Can stall StatefulSet rollout or cause repeated failovers. Use `/healthz` for rollout health, writer-role labels for write routing, and update replicas before the writer. |
| Missing Kubernetes API access or RBAC | Clustered Kubernetes operation requires the in-process Lease client and pod-label permissions. A separate lease updater sidecar is out of scope for v1. |
| Insufficient shutdown grace | Drain, final sync, and checkpoint may be cut short by forced termination. Grace periods must cover the complete sequence. |
| Checkpoint blocked by readers or slow I/O | Shutdown may use the documented passive-checkpoint fallback. Remaining WAL must be preserved for SQLite recovery. |
| Unwritable data paths or incompatible UID/GID | Startup fails with the offending path and required permission rather than serving with partially accessible files. |
| No built-in gateway TLS termination | Remote traffic requires an external TLS endpoint. Plaintext replication origins are restricted to loopback. The sidecar-to-gateway hop is plaintext on loopback, so anything sharing the pod's network namespace can observe SQL and replication traffic, including bearer tokens. |
| Private-CA certificates with the transitional adapter | Unsupported by its public-WebPKI-only trust configuration. Native TLS support gates Kubernetes delivery. |
| Authentication requirements | Clustered mode requires authentication even behind a loopback TLS proxy; SQL and replication credentials must be distinct. Unauthenticated mode is limited to standalone loopback use. |
| Restricted SQLite feature surface | `ATTACH`, `DETACH`, `VACUUM INTO`, extension loading, and non-allowlisted pragmas are denied; this is not unrestricted access to every SQLite feature. |
| Rolling protocol compatibility | The stated compatibility window is version N with N-1; arbitrary mixed versions are not promised. |
| Transitional upstream dependency | Keep the adapter pinned and isolated until native cutover. Native replication, runtime integration, deployment tooling, and tests are milestone deliverables, not guarantees supplied by this document alone. |
| Upstream CLI compatibility | Drop-in compatibility with the `rsqlite-rsync` standalone sync CLI is not a v1 goal. |

### 10.6 Unresolved guarantees and recovery policy

The following are gaps to resolve explicitly, not additional guarantees:

- **Arbitrary corruption and storage faults:** the plan does not establish
  comprehensive protection against disk corruption, dishonest flush behavior,
  filesystem faults, or faulty memory. Hash convergence and freshness metadata
  do not prove application-level data correctness.
- **Corrupted-writer recovery:** automatic diagnosis, quarantine, selection of
  a verified healthy recovery source, and repair of an already-corrupted writer
  are not specified end to end.
- **Independent backups and point-in-time recovery:** replication is not a
  substitute for backups; corruption or unwanted logical changes can propagate.
  Backup retention, restore verification, and point-in-time recovery are not
  defined here.
- **Exactly-once execution across failover:** an optional idempotency key alone
  does not specify durable, failover-safe deduplication or its retention window.
  Unknown outcomes must remain visible until such semantics are defined.
- **Lossless switchover on timeout:** specify whether final-sync/drain failure
  aborts handover or enters an explicitly bounded-loss recovery path. Also
  define how the final-snapshot-bearing successor is selected and verified;
  passing the ordinary freshness gate alone is not proof of zero loss.
- **Unbounded pauses:** neither `sqlite3_interrupt` nor repeating the
  `enforce_write_fence` check closes the documented stalled-commit window. A stronger guarantee requires
  an enforceable external/storage fence or a different replication/commit
  architecture, not merely a larger timeout.
- **Damaged authoritative lease state:** the dual-claim chaos assertion in
  [§6.3](testing.md#63-fault-injection-testschaos-feature-chaos) does not define recovery from arbitrary lease corruption or loss.
  Generation rules depend on an intact authoritative history; reconstruction
  must not silently reset generations or bootstrap competing writers.

---
