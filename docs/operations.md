# hasqlite — Operations and Risks

## 9. Observability (planned)

| Signal | Format |
|--------|--------|
| Audit log | append-only JSON lines on the PVC (action, from, to, lease gen, first and last commit time per generation) |
| Role state file | `replica` / `writer:<gen>` (informational; never used as authority) |
| Metrics | Prometheus: `hasqlite_sync_lag_seconds`, `hasqlite_origin_wal_bytes` and `hasqlite_snapshot_future_skew_seconds` carry a `database` label (at most 64 values, one per database in the set); `hasqlite_databases` (size of the observed database set); `hasqlite_unmanaged_database_files` (database files in neither the set nor the tombstones); `hasqlite_lease_held`, `hasqlite_promotion_denied_total` (by `PromotionError` variant), `hasqlite_renewal_deadline_misses_total`, `hasqlite_origin_wal_bytes`, `hasqlite_snapshot_future_skew_seconds` (replica: origin-stamped snapshot start minus local wall time at ledger publication), `hasqlite_diverged_files` and `hasqlite_diverged_bytes` (finished `diverged/<db>.diverged-<id>` copies only), `hasqlite_divergence_preservation_pending` (1 while a former writer is blocked on rule 15 preservation) |
| Tracing | `tracing` spans: `tick`, `sync`, `promote` |

Alerts (recommended):

- `sync_lag > freshness.max_lag_secs` for > 2 intervals, for any database (one stale database makes the whole replica unpromotable)
- origin WAL size growing across checkpoints
- promotion denied spike
- lease holder missing > TTL
- snapshot future skew approaching `max_future_skew_secs` (writer clock ahead; failover would be denied)
- `hasqlite_diverged_files > 0` (unreconciled divergent commits; follow the diverged data recovery runbook)
- `hasqlite_divergence_preservation_pending == 1` for more than 10 minutes (former writer blocked, typically by ENOSPC; a pending preservation stops sync, so sync intervals are not a usable unit)

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
| Large catch-up after failover | PreStop sync; monitor lag. Pre-activation catch-up from a fresher peer is post-v1 (safety contract rule 13) ([§10.3](#103-availability-and-promotion-failure-modes)) |
| Upstream API changes during transition | Pin the adapter by commit, build with `--locked`, isolate it; remove the integration at native-backend cutover ([§10.5](#105-deployment-access-and-compatibility-constraints)) |
| StatefulSet rollout stall or repeated failovers | Document `/healthz` vs `/ready`; writer-last rollout with planned switchover; PodDisruptionBudget ([§10.5](#105-deployment-access-and-compatibility-constraints)) |

### 10.2 Data safety and durability limitations

| Condition | Consequence and requirement |
|-----------|-----------------------------|
| Asynchronous replication | An unplanned failover can lose acknowledged writes. The target loss window is at most `freshness.max_lag_secs + lease.renew_deadline_secs` (14 seconds with the example configuration), not zero RPO. This is a time-window target, not a bound on the number of lost transactions; it assumes the writer's wall clock accurately measures elapsed time and no commit stalls beyond the fencing margin (next rows). |
| No cross-database atomicity | A transaction touches one database (`ATTACH` is denied). After an unplanned failover each database separately holds a prefix of its acknowledged writes within the loss window, but different databases can reflect different points in time. Applications that need several tables to stay mutually consistent across failover must keep them in one database. |
| No storage-level generation fence | Each node writes its own disk; a generation counter does not physically reject an old writer's disk writes. Split-brain prevention depends on cooperative self-fencing and the timing assumption below. |
| Writer stalls inside `COMMIT` beyond the fencing margin | A commit can finish after a peer promotes. The default margin is 4 seconds and the minimum is 2 seconds. The `enforce_write_fence` check before `COMMIT` and `sqlite3_interrupt` at the deadline do not eliminate this residual risk. |
| Replica crashes during in-place page application | The replica can contain a partially applied snapshot. Invalidate and fsync the ledger before application; deny promotion until a full sync and `PRAGMA integrity_check` pass. Track N provides atomic/locked application. |
| Disk full during page application or ledger publication | Sync fails and the ledger remains invalid; the affected node cannot promote. |
| Former writer contains divergent commits | Preserve its database as a diverged copy before resync (safety contract rule 15). The copy includes WAL-only commits, survives crashes during preservation, and blocks resync and promotion if it cannot be written. Those commits are not automatically merged into the new writer; recovery requires operator reconciliation. |
| Former writer's volume is lost | Its diverged commits are unrecoverable. Replicas holding snapshots newer than the promoted one are overwritten on their next resync and are not preserved in v1. |
| Diverged copies accumulate | v1 never deletes them automatically; one preservation writes a copy of every database on the volume, so it can be as large as all of them together. A planned switchover leaves no copy only if, when the former writer returns, the lease's activated writer is still the successor whose `promoted_from` names the former generation, its stop stamp and a later post-stop snapshot stamp (rules 15 and 16). If that successor has already been replaced, the former writer preserves anyway, as it does after crash failovers and timed-out switchovers. Monitor `hasqlite_diverged_bytes` and size the volume for every retained copy, at least one extra copy of every database (two with `retain_promotion_ancestor`), plus rule 15's preservation headroom: free space of `S + max(S / 10, 64 MiB)`, where `S` is the total of every database plus its WAL, on top of copies already kept. Remove reconciled copies with `hasqlite diverged discard --id <id> --confirm-data-loss --reason <text>`, which is audited with the operator's identity. |
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
| Two nodes are unavailable in a 3-node cluster; the surviving node is the writer | The writer may continue serving while it can renew the authoritative lease. The cluster has no replica redundancy; alert and restore a replica. If the writer loses coordination, it self-fences as described above. |
| Two nodes are unavailable in a 3-node cluster; the surviving node is a replica | In v1, the sole survivor may promote after lease release or locally observed expiry if `validate_promotion` passes and acquisition and activation CAS operations succeed against the authoritative lease backend. No quorum of hasqlite nodes is required for ordinary v1 promotion; the lease backend must remain available. If validation or coordination fails, keep the node non-writable. The 2-of-3 peer quorum applies only to post-v1 degraded recovery, not ordinary v1 failover. Promotion may lose acknowledged writes because replication is asynchronous. |
| All replicas exceed the freshness limit | Automatic promotion is denied; availability is sacrificed to limit data loss. While the activated writer is still serving, catch-up sync from it restores eligibility. Once it has failed, the failed writer remains the last activated writer in the observed lease, so a stale ledger may still match its epoch, and `validate_promotion` rejects promotion because lag exceeds the limit; automatic promotion is blocked and manual recovery is required. |
| Activated writer dies before any replica syncs from it | Replicas have the wrong writer epoch and automatic failover is blocked. An acquired but never-activated generation does not have this effect. |
| Ledger is missing, invalid, bound to another destination, self-sourced, or from the wrong writer epoch | Promotion fails closed; apparent file recency is insufficient. |
| Snapshot timestamp exceeds the allowed future skew | Promotion is denied while the snapshot's timestamp still exceeds the skew limit at promotion time. Lease expiry uses monotonic time, but the promotion future-timestamp check compares the writer-stamped snapshot time with the candidate's local wall time (safety contract rule 10). If the snapshot ages until its timestamp is within the skew limit, promotion is no longer denied on this basis alone; denial therefore applies only as long as the skew condition persists. A writer whose clock runs more than `max_future_skew_secs` ahead of every replica keeps all replicas unpromotable for as long as it continues stamping snapshots beyond the limit; alert on `hasqlite_snapshot_future_skew_seconds` ([§8](configuration.md#8-configuration-contract-v1)). |
| Acquisition or activation CAS loses a race | The candidate remains a replica. Acquisition without activation never permits writable SQL handles. |
| Candidate crashes between acquisition and activation | The previous writer epoch remains valid for eligible peers, but another acquisition may have to wait for the abandoned holder's TTL. |
| Writer crashes without releasing the lease | Each candidate waits for an unchanged lease version for a full locally measured effective TTL (`max(its ttl_secs, the holder's recorded ttl_secs)`) before campaigning; failover is not instantaneous. |
| Rolling change of lease timing | Candidates honour the holder's recorded TTL, so a shorter candidate TTL cannot expire a holder that still commits; a longer candidate TTL only delays failover until the rollout completes. |
| Unrelated writes to the Kubernetes Lease object | Labels, annotations or managed-field updates by other actors change `resourceVersion` but not the hasqlite version token, so they neither postpone failover nor count as renewals. They can still make a hasqlite CAS fail and retry on the next tick. |
| Slow storage, coordination, or catch-up | Recovery can exceed the p95 RTO target of 15 seconds. That target applies to the specified test environment, not every deployment. |
| Lag-proportional acquisition delay | It favors the freshest eligible replica but does not guarantee that replica wins the CAS race. |

### 10.4 Replication and request-serving limits

| Condition | Consequence and requirement |
|-----------|-----------------------------|
| Single writer | Replicas do not scale write execution; multi-writer SQL and cross-region active/active are outside v1 scope. |
| Several databases per cluster, one writer | Clustered mode replicates the database set recorded in the lease (1–64 databases, ADR-0020). All databases share one lease and one writer, so they fail over together; per-database leadership is out of scope. A replica is promotable only if every database in the set passes the freshness gate, so one stale or never-synced database blocks promotion of the whole replica. |
| Database added with `hasqlite db add` | No replica is promotable until it has synced the new database once (about one sync round). If the writer dies in that window, promotion fails closed and the manual promotion runbook applies. Add databases when the writer is healthy and replicas are current. |
| Database removed with `hasqlite db remove` | Permanent: requires `--confirm-data-loss`, and every node deletes the database's files and ledger record (the writer immediately, replicas when they observe the tombstone). Take a backup first if the data may be needed. A former writer that had not yet resynced keeps its divergent commits as a diverged copy, because it preserves before deleting. A node that missed more than 64 removals leaves the files of dropped tombstones in place and reports them in `hasqlite_unmanaged_database_files`; delete those by hand. |
| Concurrent sync | A round runs up to `sync.max_concurrent_pulls` pulls at once (default 4). A database's lag includes its own pull plus any wait for a slot, so size `freshness.max_lag_secs` for the slowest pull, or for a full round when there are more databases than slots; the validator checks only `interval_secs + retry_period_secs`. Peak network, disk I/O and origin read transactions (each pinning its database's WAL) grow with the concurrency. Idle databases still cost one pull per round. |
| Concurrent requests on the writer | Requests to different databases run concurrently and do not block each other; requests to one database follow SQLite's single-writer locking. |
| Gateway database names in clustered mode | Only names in the set resolve; other names get `NOT_FOUND`, and SQL cannot create a database. Create databases with `hasqlite db add`. |
| Replica reads with the transitional adapter | Unsupported because concurrent in-place application can expose torn pages. Track N must provide safe atomic/locked application first. |
| Snapshot attribution with the transitional adapter | The adapter stamps neither origin identity nor snapshot time and serves pulls regardless of leadership. Until Track N, the ledger is stamped from the lease observed before the pull (`synced_at_secs := writer.renewed_at_secs`, a lower bound that overstates lag) and published only if the lease is unchanged after the fsync (safety contract rule 9). Lag therefore reads up to one retry period higher than with native stamping; size `freshness.max_lag_secs` accordingly. |
| Concurrent pulls into one replica | Rejected with `SyncInProgress`, in-process and, through an advisory lock in `${data.dir}/.hasqlite.pull-locks/`, across processes on one host; a second node must never be pointed at the same replica file (advisory locks are not reliable on network filesystems). |
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
| Security advisories inherited through the adapter | `cargo audit` runs in the CI `audit` job and is never suppressed. Advisories against the adapter's dependencies are fixed by bumping them upstream, re-pinning the `rsqlite-rsync` rev here and moving the `hasqlite-sync` dev-dependency on `libsqlite3-sys` in lockstep (`links = "sqlite3"` forbids two versions). RUSTSEC-2026-0459 (bundled SQLite CVE) was closed this way; the unmaintained-crate warnings for `bincode` 2 and `serde_yaml`/`unsafe-libyaml` remain upstream and disappear at the Track N cutover. |
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

Automated recovery strategies for addressing these operational gaps are detailed in the [Post-v1 Roadmap](roadmap.md#post-v1--automated-failure-recovery--self-healing).

---
