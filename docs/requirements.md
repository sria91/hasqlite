# hasqlite — Requirements (EARS)

This document restates the v1 plan as testable requirements in the **EARS**
(Easy Approach to Requirements Syntax) style. It is derived from
[PLAN.md](PLAN.md), [architecture.md](architecture.md),
[safety-contract.md](safety-contract.md), [configuration.md](configuration.md),
[operations.md](operations.md), [roadmap.md](roadmap.md) and
[testing.md](testing.md). The decisions behind the requirements are recorded in
[adr/](adr/README.md).

The [safety contract](safety-contract.md) remains normative. If a requirement
here conflicts with it, the safety contract wins and this document is a defect.

## Conventions

### EARS patterns

| Pattern | Template |
|---------|----------|
| Ubiquitous | The `<system>` shall `<response>`. |
| Event-driven | **When** `<trigger>`, the `<system>` shall `<response>`. |
| State-driven | **While** `<state>`, the `<system>` shall `<response>`. |
| Unwanted behaviour | **If** `<condition>`, **then** the `<system>` shall `<response>`. |
| Optional feature | **Where** `<feature is included>`, the `<system>` shall `<response>`. |
| Complex | Combinations, e.g. **While** `<state>`, **when** `<trigger>`, the `<system>` shall `<response>`. |

"shall" corresponds to MUST in the plan documents, "shall not" to MUST NOT,
and "should" to SHOULD.

### Systems named in requirements

| Name | Meaning |
|------|---------|
| node | a `hasqlite-node` process (any role) |
| writer | a node in role `Writer(g)` |
| replica | a node in role `Replica` |
| candidate | a replica evaluating or attempting promotion |
| former writer | a node that was the activated writer of an earlier generation and has rejoined as replica |
| lease store | the `LeaseSource` implementation over the authoritative backend (file or Kubernetes) |
| sync engine | `hasqlite-sync` on a replica |
| origin | the replication service on the writer |
| gateway | the gRPC SQL gateway (`hasqlite-gateway`) |
| client | `hasqlite-client` |
| config validator | `ClusterConfig::from_toml` / `validate()` |

### Columns

- **Source**: safety contract rule (`R<n>`) or plan section (`§n.n`).
- **Verify**: the planned test or suite from [testing.md](testing.md); `unit`
  = §6.1, `sim` = §6.1b, `chaos` = §6.3, `e2e` = §6.4, `sec` = §6.6.
- **Phase**: delivery phase from the [roadmap](roadmap.md). `TN` = Track N.

Requirement IDs are stable and never reused.

---

## 1. Lease and ownership (ADR-0001, ADR-0002, ADR-0004)

| ID | Requirement | Source | Verify | Phase |
|----|-------------|--------|--------|-------|
| REQ-LEASE-001 | The cluster shall have at most one node accepting commits at any instant, across all generations, provided no commit stalls inside `COMMIT` for longer than the fencing margin. | R1, R5 | sim; chaos (writer pause, history checker); e2e audit overlap | 1, 3 |
| REQ-LEASE-002 | The node shall treat the lease as the sole authority for writer identity, generation and activated writer epoch, and shall not use the role file as authority. | R2, §9 | unit | 1 |
| REQ-LEASE-003 | The lease store shall perform every hasqlite lease write (acquire, activate, renew, release) as a compare-and-swap whose precondition is the backend's native revision, and shall change the hasqlite version token atomically with that write. | R2, R6 | unit (mock store) | 1 |
| REQ-LEASE-004 | When an acquire succeeds, the lease store shall set `holder` to the acquiring node and `generation` to the observed generation plus one, derived by the store, and shall leave the writer epoch unchanged. | R3, R6 | unit; proptest (acquire yields `observed + 1`) | 1 |
| REQ-LEASE-005 | The lease store shall not change the generation on renew, activate or release. | R3 | proptest | 1 |
| REQ-LEASE-006 | The lease store shall accept a renew, activate or release only if the version is unchanged since the node's last successful lease write, `holder` equals the node, and the generation is unchanged. | R6 | unit (superseded writer) | 1 |
| REQ-LEASE-007 | If a renew, activate or release conflicts, then the writer shall demote immediately and shall not overwrite the lease. | R6, R14 | unit (superseded writer demotes in the same tick) | 1 |
| REQ-LEASE-008 | The candidate shall treat the lease as expired only when it is released or its hasqlite version token has stayed unchanged for the effective TTL, `max(local lease.ttl_secs, holder's recorded ttl_secs)`, measured on the candidate's local monotonic clock. | R4 | unit (injected `Instant`); chaos (rolling lease-timing change) | 1 |
| REQ-LEASE-009 | The candidate shall not compare the lease's `renewed_at_secs` with its own wall clock for any lease decision. | R4 | unit (wall-clock jumps have no effect); chaos (clock skew) | 1 |
| REQ-LEASE-010 | When the Kubernetes Lease object changes without a change to annotation `hasqlite.io/version`, the candidate shall not treat the change as a renewal and shall not restart its expiry timer. | R2, §10.3 | chaos (unrelated Lease writes) | 4 |
| REQ-LEASE-011 | If the lease store is unreachable, then the candidate shall not promote on the basis of a local expiry timer alone. | §10.3 | chaos (lease store unreachable from all nodes) | 3 |
| REQ-LEASE-012 | When a renewal occurs between a candidate's observation and its acquire, the lease store shall reject the acquire and preserve the live holder. | §3 invariants | unit (CAS acquisition race) | 1 |
| REQ-LEASE-013 | The lease store shall change the activated writer epoch only on a successful activate or on a database-set change by the activated writer (REQ-MDB-005). | R2, R10 | unit; chaos (aborted promotion) | 1 |

## 2. Write fencing and writer clock (ADR-0003, ADR-0006)

| ID | Requirement | Source | Verify | Phase |
|----|-------------|--------|--------|-------|
| REQ-FENCE-001 | While in role writer, the node shall accept a commit only before the send time of its last successful renewal plus `lease.renew_deadline_secs`, measured on its local monotonic clock. | R5 | unit (renewal deadline) | 1 |
| REQ-FENCE-002 | The gateway shall run `enforce_write_fence` for the current generation inside every write transaction immediately before `COMMIT`, and shall reject the commit if the check fails (non-writer, deadline passed or generation mismatch). | R5, R17 | unit (`PromotionViolation`); integration | 1, 1b |
| REQ-FENCE-003 | When the renewal deadline is reached, the writer shall interrupt in-flight statements with `sqlite3_interrupt`, set `/ready` to 503 and demote. | R5 | chaos (partition writer from lease store) | 3 |
| REQ-FENCE-004 | If the renewal-deadline computation overflows, then the node shall treat the deadline as already passed. | §6.1 | unit | 1 |
| REQ-FENCE-005 | The writer shall issue every renew, activate, release, stop and snapshot stamp through `WriterClock`, which returns `max(wall clock, last stamp)` and is seeded from the writer epoch's last renewal on activation. | R4, R16 | unit (writer clock); chaos (writer clock step back) | 1 |

## 3. Promotion (ADR-0004, ADR-0005)

| ID | Requirement | Source | Verify | Phase |
|----|-------------|--------|--------|-------|
| REQ-PROMO-001 | When a replica observes the lease as expired or released, the candidate shall run `validate_promotion` against the observed lease before any acquisition attempt. | R10 | unit (reconcile) | 1 |
| REQ-PROMO-002 | The candidate shall deny promotion unless an activated writer epoch exists and, for every database in that epoch's database set, its freshness ledger record is present, valid, has `destination_node_id` equal to this node and is not self-sourced. | R9, R10 | unit (promotion) | 1 |
| REQ-PROMO-003 | The candidate shall deny promotion unless every database's ledger record has source node and generation equal to the observed lease's activated writer epoch. | R10 | unit; proptest | 1 |
| REQ-PROMO-004 | The candidate shall deny promotion unless, for every database, `writer.renewed_at_secs − synced_at_secs`, computed with saturating subtraction, is at most `freshness.max_lag_secs`. | R10 | unit (lag at and past the limit) | 1 |
| REQ-PROMO-005 | The candidate shall deny promotion while any database's `synced_at_secs` is more than `promotion.max_future_skew_secs` ahead of its local wall time. | R10 | unit; chaos (writer clock ahead) | 1 |
| REQ-PROMO-006 | The candidate shall first deny promotion if the lease has no activated writer epoch (it supplies the database set), and shall then evaluate promotion checks in the class order ledger shape, writer epoch, time bounds, checking databases in name order within each class, and shall report the first failure, with its database name, as the denial reason. | R10 | unit (error precedence) | 1 |
| REQ-PROMO-007 | When `validate_promotion` passes, the candidate shall wait `retry_period_secs × lag_secs / max_lag_secs`, where `lag_secs` is the largest lag over the database set, before its acquire, and if the lease version changes during the wait, then it shall abort the attempt. | §3 | unit; sim (freshest wins majority) | 1 |
| REQ-PROMO-008 | While campaigning, the candidate shall pause scheduled sync until it promotes or remains a replica. | R10, §3 | unit | 2 |
| REQ-PROMO-009 | When the acquire succeeds, the candidate shall perform an activate that records `writer = (self, generation, now, databases)`, with `databases` copied unchanged from the observed lease, and `writer.promoted_from = (ledger source generation, oldest ledger synced_at_secs over the database set, observed writer.stopped_at_secs)` before opening any writable handle. | R10 | unit; integration | 1, 2 |
| REQ-PROMO-010 | When the activate succeeds, the node shall durably write the divergence marker `${data.dir}/.hasqlite.divergence` with id `g<generation>` before opening the writable handle. | R10, R15 | `divergence_marker.rs` | 2 |
| REQ-PROMO-011 | If the acquire or activate fails, the renewal deadline has passed, or the divergence-marker write fails, then the candidate shall not open any writable SQL path and shall remain or become a replica. | R10, R12 | unit; `divergence_marker.rs`; chaos (aborted promotion) | 1, 2 |
| REQ-PROMO-012 | When promotion completes, the node shall open its writable handle on the validated snapshot without post-promotion catch-up, activate the write fence for the new generation, set the role file to `writer:<gen>`, label its pod (Kubernetes), append an audit entry and set `/ready` to 200. | R13, §3 | integration (`local_cluster.rs`) | 3 |
| REQ-PROMO-013 | Where a cluster has no activated writer, the node shall establish the initial generation and writer epoch, including its database set, only through an explicit genesis procedure or a verified seed-snapshot procedure. | R11 | integration | 3 |
| REQ-PROMO-014 | When the cluster cold-restarts with coordination available and at least one eligible replica, a candidate shall promote after observing lease expiry without operator action. | §3, §6.3 | chaos (full-cluster cold restart) | 3 |

## 4. Replication and freshness ledger (ADR-0007, ADR-0008, ADR-0020)

| ID | Requirement | Source | Verify | Phase |
|----|-------------|--------|--------|-------|
| REQ-SYNC-001 | The sync engine shall not write to a database while the node's role is writer. | R7 | unit | 2 |
| REQ-SYNC-002 | If a pull targets a replica path that already has a pull in progress, then the sync engine shall fail with `SyncError::SyncInProgress` before any I/O. | R9 | `exclusive_replica_path.rs` (exists) | 2 |
| REQ-SYNC-003 | The origin shall produce a consistent snapshot as of sync start while it continues to serve reads and writes. | R8 | `live_writes.rs`, `sync_convergence.rs` | 2 |
| REQ-SYNC-004 | If the node is not the current lease holder and activated writer, then the origin shall refuse replication pulls with `FAILED_PRECONDITION`; if the requested database is not in its current database set, then it shall refuse the pull with `NOT_FOUND`. | R8, R9 | `replication_role_gate.rs`, `grpc_replication.rs` | 2 |
| REQ-SYNC-005 | Before writing the first page of a pull, the sync engine shall invalidate that database's freshness ledger record and fsync the invalidation. | R9 | chaos (crash during page apply) | 2 |
| REQ-SYNC-006 | When a pull succeeds, the sync engine shall publish that database's ledger record only after the database file and its directory are fsynced, using temp file + rename + directory fsync, recording `database`, `incarnation`, `source_node_id`, `source_generation`, `synced_at_secs` (snapshot start) and `destination_node_id`. | R9 | `adapter_ledger_protocol.rs` | 2 |
| REQ-SYNC-007 | The node shall keep the freshness ledger on the same filesystem as the database, and pre-flight shall verify this with `statfs`/`st_dev`. | R9, §5 Phase 2 | integration | 2 |
| REQ-SYNC-008 | If a pull or ledger publication fails (including ENOSPC), then the sync engine shall leave the ledger invalidated and the node shall not promote from it. | R9, §10.2 | chaos (disk full, crash during apply) | 2 |
| REQ-SYNC-009 | While the transitional adapter is the replication backend, before a pull the sync engine shall observe the lease and abort if `holder_node_id` differs from `writer.node_id`. | R9 | `adapter_ledger_protocol.rs` | 2 |
| REQ-SYNC-010 | While the transitional adapter is the replication backend, when a pull completes the sync engine shall observe the lease again and publish the ledger only if `version` is unchanged and `holder_node_id` equals `writer.node_id`, stamping it from the pre-pull observation with `synced_at_secs := writer.renewed_at_secs`. | R9 | `adapter_ledger_protocol.rs` | 2 |
| REQ-SYNC-011 | Where the native backend is deployed, the origin shall stamp each snapshot stream with its node ID, lease generation, database name and snapshot start time (origin clock), and the replica shall reject a snapshot whose stamped identity does not match the lease's writer epoch or whose database name differs from the one requested. | R8 | `grpc_replication.rs` | TN |
| REQ-SYNC-012 | Where the native backend is deployed, the sync engine shall apply snapshots atomically (staging file + rename, or under SQLite locking). | §5 TN | chaos (crash during apply) | TN |
| REQ-SYNC-013 | Where the native backend is deployed, the origin shall abort a replication session that exceeds its snapshot deadline and shall stop replication sessions before the shutdown checkpoint. | §10.4 | chaos (stalled replica session) | TN |
| ~~REQ-SYNC-014~~ | Superseded 2026-11-18 by ADR-0020: clustered mode replicates the database set in the lease; see REQ-MDB-001 to REQ-MDB-023. | §10.4 | — | — |
| REQ-SYNC-015 | While the transitional adapter is the replication backend, the node shall not serve replica reads. | §10.4 | integration | 2 |
| REQ-SYNC-016 | Where `allow_replica_reads` is enabled with the native backend, the replica shall serve reads only on read-only connections with `PRAGMA query_only = 1`, reopened after each completed sync. | R17, §5 Phase 2 | `readonly_replica_reads.rs` | TN |
| REQ-SYNC-017 | If a sync transport, credential or TLS failure occurs, then the sync engine shall retry with backoff and raise an alert, and shall not advance the ledger. | §10.4 | chaos (sync transport failure) | 2 |

## 4a. Multiple databases (ADR-0020)

| ID | Requirement | Source | Verify | Phase |
|----|-------------|--------|--------|-------|
| REQ-MDB-001 | The lease's activated writer epoch shall carry the database set `writer.databases`, a list of 1–64 `(name, incarnation)` entries sorted by name with no duplicate names, together with the counter `writer.next_incarnation` and the tombstones `writer.removed`, and the node shall treat the lease as the only authority for which databases the cluster replicates. | R2, R9 | unit (lease parsing) | 1 |
| REQ-MDB-002 | If a database name has a `/`-separated component that is empty, longer than 255 bytes, starts with `.` or contains a character other than ASCII letters, digits, `.`, `_` or `-`; or the name exceeds 255 bytes, does not end in `.db`, `.sqlite` or `.sqlite3`, or starts with the component `diverged` or `ancestors`, then the node shall reject it in lease parsing, genesis and `db add`. | R9 | unit (table-driven) | 1 |
| REQ-MDB-003 | When a cluster is seeded from a standalone volume, the procedure shall record the databases that pre-flight discovery finds as the initial database set, and if any name is invalid or there are more than 64, then it shall refuse to seed. | R11 | integration; runbook test | 3, 4 |
| REQ-MDB-004 | When the candidate activates, it shall copy the database set, `next_incarnation` and the tombstones from the observed lease unchanged. | R10 | unit | 1 |
| REQ-MDB-005 | The lease store shall accept a change to `writer.databases` only as a renew by the activated writer under the renew preconditions, and if it conflicts, then the writer shall demote. | R6, R9 | unit (mock store) | 1 |
| REQ-MDB-006 | When an operator runs `hasqlite db add <name> --reason <text>` on the writer, the node shall refuse it if the set holds 64 names or a file exists at the path or its `-wal`/`-shm`; otherwise it shall create the database in WAL mode, fsync it and its parent directories, perform the renew CAS that adds `(name, next_incarnation)`, increments the counter and drops any tombstone for the name, serve the database only after that CAS succeeds and append an audit entry. | R9 | `database_set.rs` | 3 |
| REQ-MDB-007 | If the CAS adding a database fails, then the node shall not serve the created file, shall demote and shall delete the empty file it created. | R6, R9 | `database_set.rs` | 3 |
| REQ-MDB-008 | When an operator runs `hasqlite db remove <name> --confirm-data-loss --reason <text>` on the writer, the node shall reject new requests for that database, interrupt and drain its in-flight transactions, perform the renew CAS that moves the entry to the tombstones, close its handles, delete the database's files and ledger record (REQ-MDB-017) and append an audit entry; it shall refuse the command without `--confirm-data-loss`, when the name is the last one in the set, or during a planned switchover. | R9, R16 | `database_set.rs` | 3 |
| REQ-MDB-009 | When a node observes a tombstone for a database whose files exist locally, and no divergence preservation is pending, it shall delete those files and the database's ledger record (REQ-MDB-017). | R9 | `database_set.rs`; chaos (crash during database deletion) | 3 |
| REQ-MDB-010 | The sync engine shall keep one ledger record per database in `freshness.dir`, named with the diverged-copy encoding (`%` → `%25`, `/` → `%2F`), and shall invalidate and publish each record independently of the others. | R9 | `adapter_ledger_protocol.rs` | 2 |
| REQ-MDB-011 | In each sync round, the sync engine shall run one pull per database of the observed set, at most `sync.max_concurrent_pulls` at a time, each invalidating and publishing only its own record, and shall skip a pull whose database is not in the set of its own pre-pull lease observation or has a different incarnation there. | R9 | `multi_database_sync.rs` | 2 |
| REQ-MDB-012 | While the transitional adapter is the replication backend, the sync engine shall bracket each database's pull with its own pre- and post-pull lease observations and stamp its record from its own pre-pull observation. | R9 | `adapter_ledger_protocol.rs` | 2 |
| REQ-MDB-013 | If any database in the observed lease's database set has no valid ledger record, or its record's incarnation differs from the set entry's, then the candidate shall deny promotion. | R10 | unit (promotion); chaos (writer dies after `db add`) | 1, 3 |
| REQ-MDB-014 | While in clustered mode, the gateway shall resolve only names in the current database set, shall reject any other name with `NOT_FOUND` as a definite rejection, and shall never open a database with `SQLITE_OPEN_CREATE`. | R17 | `database_set.rs`; sec | 3 |
| REQ-MDB-015 | While a planned switchover is in progress, the writer shall refuse database-set changes. | R16 | `planned_switchover.rs` | 3 |
| REQ-MDB-016 | When preserving diverged data, the node shall copy every database that pre-flight discovery finds on its volume, including tombstoned ones, one at a time in name order, all under one divergence id. | R15 | chaos (divergence: multiple databases) | 2 |
| REQ-MDB-017 | When deleting a database's files, the node shall do so only with no open handle on the database and no pull in progress for it, unlinking the `-wal` file, then the `-shm` file, then the database file, treating a missing file as already deleted, then deleting its ledger record and fsyncing the directories; if it crashes during deletion, it shall finish the deletion when it next observes the tombstone. | R9 | `database_set.rs`; chaos (crash during database deletion) | 3 |
| REQ-MDB-018 | Before the first pull of a database whose local ledger record is missing or carries a different incarnation than the set entry, the replica shall delete any local files of that name (REQ-MDB-017). | R9 | `multi_database_sync.rs` (remove and re-add) | 2 |
| REQ-MDB-019 | While a divergence preservation is pending, the node shall not delete any database files. | R9, R15 | chaos (divergence: multiple databases) | 2 |
| REQ-MDB-020 | The writer shall keep at most 64 tombstones and, when adding one to a full list, shall drop the oldest. | R9 | unit | 1 |
| REQ-MDB-021 | The node shall not delete database files that are in neither the database set nor the tombstones, and shall report them in `hasqlite_unmanaged_database_files`. | R9 | `database_set.rs` | 3 |
| REQ-MDB-022 | While in role writer, the gateway shall execute requests on different databases concurrently, each database with its own connections and SQLite write lock, and shall run `enforce_write_fence` before every commit on every database. | R5, R7 | integration (concurrent writes to several databases); sim | 1b, 3 |
| REQ-MDB-023 | The origin shall serve concurrent replication sessions for different databases, each with its own snapshot deadline. | R7, §10.4 | `multi_database_sync.rs`; chaos (stalled replica session) | 2, TN |

## 5. Demotion and diverged-data preservation (ADR-0010)

| ID | Requirement | Source | Verify | Phase |
|----|-------------|--------|--------|-------|
| REQ-DEM-001 | When the writer has a renewal conflict, misses its renewal deadline or loses the lease, the node shall stop accepting writes, interrupt in-flight statements, close writable handles, remove its pod label, set the role file to `replica` and update probes. | R14, §3 | chaos; integration | 3 |
| REQ-DEM-002 | While not the writer, when the gateway receives a mutating request, the gateway shall reject it with `NOT_LEADER`, a redirect hint and the current generation. | R14, §5 Phase 1a | `gateway_writer_only.rs` | 1b, 3 |
| REQ-DIV-001 | While a divergence preservation is pending, the node shall not run sync pulls, serve replica reads or attempt promotion. | R15 | chaos (divergence: crash during preservation) | 2 |
| REQ-DIV-002 | When a node starts in clustered mode, it shall treat preservation as pending if the divergence marker exists; if the lease's writer epoch names this node while another node holds the lease (id `g<writer generation>`); if the marker is unreadable while database files exist; or if the volume holds database files and a standalone lease record but has never run in clustered mode (id `standalone-g<generation>`). | R15 | chaos (lost role file, lost marker); `divergence_marker.rs` | 2 |
| REQ-DIV-003 | The node shall not use the role file as a preservation trigger. | R15 | chaos (lost role file) | 2 |
| REQ-DIV-004 | When preservation is pending for generation `g`, if the current activated writer's `promoted_from` names `g`, carries `g`'s stop stamp and has a snapshot stamp later than it, then the node shall append an audit entry and remove the marker without making a copy. | R15, R16 | chaos (divergence: planned switchover) | 2 |
| REQ-DIV-005 | While the lease shows generation `g` released with `stopped_at_secs` and no successor activated, the node shall keep preservation pending and repeat the lossless-handover check on each tick. | R15 | integration | 2 |
| REQ-DIV-006 | If the lossless-handover check does not pass (including when the lease has moved past the successor), then the node shall preserve a diverged copy. | R15 | chaos (divergence: WAL-only commits, timed-out switchover) | 2 |
| REQ-DIV-007 | If the divergence marker is unreadable, then the node shall hard-link it to `${data.dir}/.hasqlite.divergence.unreadable-<unix secs>`, fsync the directory, write a replacement marker with id `unknown-<unix secs>` to a temp file, fsync it, rename it over the unreadable marker and fsync the directory; on later restarts it shall reuse that id. | R15 | chaos (divergence: unreadable marker) | 2 |
| REQ-DIV-008 | The node shall take each diverged copy with the SQLite online backup API from a local read connection with no writable handles open, including all WAL-only frames, and shall not rename or copy the source files. | R15 | chaos (divergence: WAL-only commits) | 2 |
| REQ-DIV-009 | The node shall name the copy `${data.dir}/diverged/<db>.diverged-<id>`, where `<db>` is the database path relative to `data.dir` with `%` encoded as `%25` and `/` as `%2F`. | R15 | `diverged_paths.rs` | 2 |
| REQ-DIV-010 | The node shall write the copy to `<name>.tmp`, fsync it, run `PRAGMA integrity_check`, rename it to `<name>` and fsync the directory, and only when every copy of the preservation is durable append an audit entry listing each copy, remove the marker and fsync the directory. | R15 | chaos (crash during preservation) | 2 |
| REQ-DIV-011 | When preservation restarts, the node shall delete and retake a leftover `.tmp` copy, and shall verify an existing finished copy with the same id with `integrity_check` and reuse it rather than overwrite it. | R15 | chaos (crash during preservation) | 2 |
| REQ-DIV-012 | If the space available to the node's UID on the `data.dir` filesystem (`statvfs` `f_bavail × f_frsize`) is less than `S + max(S / 10, 64 MiB)`, where `S` is the sum, over every database still needing a copy, of its database file size plus its `-wal` file size, measured once before the first copy, or if the copy fails, then the node shall not resync or promote, shall report `hasqlite_divergence_preservation_pending = 1` and shall raise an alert. | R15 | unit (space check boundaries); chaos (divergence: disk full) | 2 |
| REQ-DIV-013 | The node shall create `diverged/` with mode `0700` and copies with mode `0600` owned by its UID, and shall open every path relative to a `data.dir` directory handle (`openat`) with `O_NOFOLLOW`. | R15 | `diverged_paths.rs` | 2 |
| REQ-DIV-014 | If `diverged/` exists but is a symlink, is not a directory or is owned by another UID, then the node shall fail startup. | R15 | `diverged_paths.rs` | 2 |
| REQ-DIV-015 | The node shall exclude `${data.dir}/diverged/` and `${data.dir}/ancestors/` from pre-flight database discovery, gateway database-name resolution and replication serving, and shall never open a diverged copy for writes, serve it or promote it. | R15 | `diverged_isolation.rs` | 2 |
| REQ-DIV-016 | The node shall not delete diverged copies automatically. | R15 | `diverged_isolation.rs` | 2 |
| REQ-DIV-017 | When an operator runs `hasqlite diverged discard --id <id> --confirm-data-loss --reason <text>`, the node shall delete every copy for `<id>`, or remove the pending marker for `<id>` without preserving, and shall append an audit entry with the id, each deleted copy's name and size, reason, invoking UID and user name, and whether a pending preservation was bypassed. | R15 | `diverged_discard.rs` | 2 |
| REQ-DIV-018 | If `diverged discard` is run without `--confirm-data-loss`, or with an id matching neither a copy nor the pending marker, then the node shall refuse it. | R15 | `diverged_discard.rs` | 2 |
| REQ-DIV-019 | The node shall count only finished copies directly under `diverged/` named `<db>.diverged-<id>` in `hasqlite_diverged_files` and `hasqlite_diverged_bytes`. | R15, §9 | `diverged_isolation.rs` | 2 |
| REQ-DIV-020 | Where the node runs in standalone mode, it shall not write a divergence marker. | R15 | `divergence_marker.rs` | 2 |

## 6. Planned switchover and shutdown (ADR-0011)

| ID | Requirement | Source | Verify | Phase |
|----|-------------|--------|--------|-------|
| REQ-SWO-001 | When the writer begins a planned switchover, it shall reject new write requests on every database, drain or abort (`sqlite3_interrupt`) every in-flight write transaction on every database, and wait until no write transaction is open and every in-flight `COMMIT` has returned. | R16 | `planned_switchover.rs` | 3 |
| REQ-SWO-002 | When writes have stopped, the writer shall issue a stop stamp through `WriterClock`. | R16 | `planned_switchover.rs` | 3 |
| REQ-SWO-003 | After the stop stamp, the writer shall stamp every snapshot strictly after the stop stamp, waiting for `WriterClock` to pass it if needed, bounded by the final-sync timeout. | R16 | chaos (writer clock step back) | 3 |
| REQ-SWO-004 | While the transitional adapter is the replication backend, after issuing the stop stamp the writer shall perform a renew CAS stamped strictly after the stop stamp. | R9 | `planned_switchover.rs` | 3 |
| REQ-SWO-005 | When writes have stopped, the writer shall wait a bounded time for replicas to complete, for every database in the set, a snapshot started after the stop stamp, then release the lease (holder cleared, generation unchanged, writer renewal time updated, `writer.stopped_at_secs` set to the stop stamp). | R16 | `planned_switchover.rs` | 3 |
| REQ-SWO-006 | If the final-sync timeout elapses (including any `WriterClock` wait), then the node shall treat the switchover as a timed-out final sync and shall not report it as lossless. | R16, §10.4 | `planned_switchover.rs`; e2e | 3 |
| REQ-SWO-007 | When a replica receives a preStop or shutdown signal, it shall attempt a final catch-up sync before terminating. | R20 | `shutdown_sync.rs` | 3 |
| REQ-SWO-008 | When the writer has released the lease during shutdown, it shall checkpoint the WAL before exiting. | §3 | `planned_switchover.rs` | 3 |

## 7. SQL gateway (ADR-0012)

| ID | Requirement | Source | Verify | Phase |
|----|-------------|--------|--------|-------|
| REQ-GW-001 | The gateway shall classify each statement as read-only or mutating with `sqlite3_stmt_readonly()` after preparation and shall not inspect SQL text for classification. | R17 | `gateway_hardening.rs` (CTE-wrapped `INSERT`, comment-prefixed DML) | 1b |
| REQ-GW-002 | The gateway shall deny `ATTACH`, `DETACH`, `VACUUM INTO` and pragmas outside an allowlist through an authorizer callback. | R17 | `gateway_hardening.rs` | 1b |
| REQ-GW-003 | The gateway shall enable `SQLITE_DBCONFIG_DEFENSIVE`, disable extension loading and `trusted_schema`, and cap SQL length and attached databases with `sqlite3_limit`. | R17, §5 Phase 1b | `gateway_hardening.rs` (`load_extension`, `writable_schema`, oversized SQL) | 1b |
| REQ-GW-004 | The gateway shall bound statement run time with a progress handler, and shall abort in-flight statements with `sqlite3_interrupt` on demotion or shutdown. | §5 Phase 1b | integration | 1b |
| REQ-GW-005 | The gateway shall resolve database names strictly inside `data.dir`, and if a name traverses outside it or follows a symlink out of it, then the gateway shall reject the request. | §5 Phase 1b | sec (path traversal, symlink escape) | 1b |
| REQ-GW-006 | The gateway shall open replica connections read-only with `PRAGMA query_only = 1`. | R17 | `gateway_hardening.rs` (replica) | 1b |
| REQ-GW-007 | The gateway shall distinguish in each response a request rejected before execution from a request whose outcome is unknown. | §5 Phase 1a | `client_failover.rs` | 1a |

## 8. Wire API and client (ADR-0014)

| ID | Requirement | Source | Verify | Phase |
|----|-------------|--------|--------|-------|
| REQ-API-001 | The wire API shall carry a protocol version field, and version N shall serve version N-1 clients and replicas. | §5 Phase 1a | adjacent-version compatibility tests; chaos (mixed-version rolling upgrade) | 1a |
| REQ-API-002 | The wire API shall return `NOT_LEADER` with the current holder's advertised address and the current generation. | §5 Phase 1a | `gateway_writer_only.rs` | 1a |
| REQ-API-003 | The replication stream header shall carry origin node ID, lease generation and snapshot start time. | §5 Phase 1a, R8 | `grpc_replication.rs` | 1a, TN |
| REQ-API-004 | Where a mutating request carries an idempotency key, the wire API shall transport it; this shall not be presented as a failover-safe deduplication guarantee. | §5 Phase 1a, §10.6 | proto tests | 1a |
| REQ-CLIENT-001 | When the client receives `NOT_LEADER`, it shall retry the request against the redirect target. | §1 goal 4 | `client_failover.rs` | 3 |
| REQ-CLIENT-002 | If a request's outcome is unknown, then the client shall surface an "outcome unknown" error to the application and shall not retry it automatically. | §1 goal 4, §10.2 | `client_failover.rs` (writer killed after `COMMIT`, no duplicate row) | 3 |
| REQ-CLIENT-003 | The client shall retry a mutating request automatically only when a response proves the statement never executed. | §10.2 | `client_failover.rs` | 3 |

## 9. Security (ADR-0013)

| ID | Requirement | Source | Verify | Phase |
|----|-------------|--------|--------|-------|
| REQ-SEC-001 | The gateway shall require a bearer token on every RPC unless the node is in standalone mode, bound to loopback, with `allow_unauthenticated = true`. | R18, §7 | sec | 1b |
| REQ-SEC-002 | If `allow_unauthenticated` is set in clustered mode, then the config validator shall reject the configuration, including on a loopback bind. | R18, §7 | sec; unit | 1b |
| REQ-SEC-003 | The SQL gateway shall reject the replication token, and the replication service shall reject the SQL token. | R18, §7 | sec | 1a |
| REQ-SEC-004 | The node shall compare bearer tokens in constant time. | R18 | unit | 1a |
| REQ-SEC-005 | When the resolved replication and gateway token values are equal, the node shall refuse to start. | §8 | unit | 1a |
| REQ-SEC-006 | The node shall not write tokens to logs or error messages. | §6.6 | sec; `token_rotation.rs` | 1b |
| REQ-SEC-007 | When a token file is rotated, the node shall use the new value on its next use without a restart, and if the token is rejected, then the sync engine shall leave an existing replica file untouched. | §6.2, §6.6 | `token_rotation.rs` (exists) | 2 |
| REQ-SEC-008 | If `sync.origin_endpoint` uses plaintext `grpc://` with a host other than a literal loopback IP or `localhost` (including `*.localhost`), then the config validator shall reject it. | §8 | unit | 0 (exists) |
| REQ-SEC-009 | Where `sync.tls_ca_file` is configured, the sync engine shall trust that private CA, shall reject a certificate whose SAN does not match, and shall not fall back to plaintext. | §6.6, §5 TN | sec | TN |
| REQ-SEC-010 | The node should read secrets from files or mounted secrets, and shall accept exactly one of the file and environment-variable forms for each credential. | §7, §8 | unit | 1b |

## 10. Configuration (ADR-0017)

| ID | Requirement | Source | Verify | Phase |
|----|-------------|--------|--------|-------|
| REQ-CFG-001 | If the configuration contains an unknown key or section, then the config validator shall reject it. | §8 | unit | 0 |
| REQ-CFG-002 | If clustered mode lacks `[freshness]`, `[promotion]`, `[sync]` or `sync.origin_endpoint`, then the config validator shall reject the configuration. | §8 | unit | 0 |
| REQ-CFG-003 | If `node.id` is not 1–253 ASCII letters, digits, `.`, `_` or `-`, or if `lease.kubernetes.lease_name` or `namespace` is not a DNS-1123 subdomain, then the config validator shall reject it. | §8 | unit (exists) | 0 |
| REQ-CFG-004 | If lease timing violates `retry_period_secs > 0`, `2 × retry_period_secs ≤ renew_deadline_secs`, `ttl_secs ≥ renew_deadline_secs + 2` (checked arithmetic) or `ttl_secs ≤ 3600`, then the config validator shall reject it. | §8, R5 | unit (exists) | 0 |
| REQ-CFG-005 | If `freshness.max_lag_secs ≤ sync.interval_secs + lease.retry_period_secs`, then the config validator shall reject it. | §8 | unit (exists) | 0 |
| REQ-CFG-006 | If standalone mode sets `sync.origin_endpoint`, uses a lease source other than `file`, or sets `lease.file.path` to anything other than `${data.dir}/.hasqlite.lease`, then the config validator shall reject it. | §3, §8 | unit | 0, 1b |
| REQ-CFG-007 | If `sync.origin_endpoint` is set without exactly one sync credential (`transport_auth_token_file` or `transport_auth_token_env`), then the config validator shall reject it. | §8 | unit | 0 |
| REQ-CFG-008 | The node shall load its configuration from one TOML/YAML file (`hasqlite run --config <file>`) with environment overrides. | §8 | integration | 1b |
| REQ-CFG-009 | While in clustered mode, the node shall require `sync.origin_endpoint` on every node, including the current writer. | §8 | unit | 0 |
| REQ-CFG-010 | The config validator shall accept `freshness.dir` and shall reject `freshness.path`; at pre-flight the node shall verify that `freshness.dir` is a directory owned by its UID with mode `0700`, is not a symlink and is on the same filesystem as `data.dir`. | §8, R9 | unit; integration | 0, 2 |
| REQ-CFG-011 | If `sync.max_concurrent_pulls` is outside 1–64, then the config validator shall reject it; the default shall be 4. | §8 | unit | 0 |

## 11. Standalone mode (ADR-0015)

| ID | Requirement | Source | Verify | Phase |
|----|-------------|--------|--------|-------|
| REQ-STA-001 | When a node starts in standalone mode, it shall acquire the file lease in mutex mode and transition directly from `Starting` to `Writer(g)` without peer freshness checks. | §3 | `docker_standalone.rs` | 1b |
| REQ-STA-002 | When a standalone node starts on a volume with neither a lease record nor database files, it shall use generation 1; when a lease record exists, it shall write generation `g_prior + 1` and an activated writer epoch for itself while holding the lock. | §3, R3 | integration | 1b |
| REQ-STA-003 | If database files exist but the lease record is missing or unparseable, then the standalone node shall refuse to start; `hasqlite genesis --generation <g>` shall be the audited operator override. | §3 | integration | 1b |
| REQ-STA-004 | The node shall not store its generation in `PRAGMA user_version` or any secondary metadata file. | §3 | unit | 1b |
| REQ-STA-005 | The node shall use `${data.dir}/.hasqlite.lease` as the lock file and shall never replace, rename or delete it; it shall write the record to `${data.dir}/.hasqlite.lease.record` by temp file + rename + directory fsync while holding the lock. | §3 | `file_lease_lock.rs` | 1 |
| REQ-STA-006 | While in standalone mode, the node shall hold `flock(LOCK_EX \| LOCK_NB)` on the lock file for the process lifetime and shall not use process-associated `fcntl` locks. | §3 | `file_lease_lock.rs` (reopen/close does not release) | 1 |
| REQ-STA-007 | If another process holds the standalone lock, then the node shall fail fast with `EWOULDBLOCK`. | §3 | `file_lease_lock.rs`; `docker_standalone.rs` (second container rejected) | 1, 1b |
| REQ-STA-008 | Where the file lease is used in clustered (CAS) mode on a single host, the node shall hold the lock only for one read-compare-write with a bounded wait, and if a mutex-mode holder exists, then it shall fail with `EWOULDBLOCK`. | §3 | integration | 1 |
| REQ-STA-009 | If `data.dir` is on NFS, CIFS/SMB or FUSE (`statfs`), then the node shall refuse to start. | §3, §10.5 | `file_lease_lock.rs` | 1 |
| REQ-STA-010 | When a standalone node starts, it shall verify write access to `data.dir`, every discovered database and sidecar file and every database's parent directory, and if any check fails, then it shall exit, logging the path, the required operation and the active UID/GID. | §5 Phase 1b | `docker_standalone.rs` (unwritable volume) | 1b |
| REQ-STA-011 | While in standalone mode, the node shall not run peer-replication work. | §1 goal 7, §3 | integration; benchmarks | 1b |
| REQ-STA-012 | When a standalone node receives `SIGTERM` or `SIGINT`, it shall set `/ready` to 503, reject new gRPC calls with `UNAVAILABLE`, drain in-flight work up to a bounded timeout, run `SQLITE_CHECKPOINT_TRUNCATE`, release the lock and exit with status 0 within 10 seconds. | §3 | `docker_standalone.rs` (`docker stop` exits 0, not 137) | 1b |
| REQ-STA-013 | If the `TRUNCATE` checkpoint cannot complete before the shutdown deadline, then the node shall log a warning, run a `PASSIVE` checkpoint and continue shutdown. | §3 | integration | 1b |
| REQ-STA-014 | When a standalone volume is migrated to a cluster, the procedure shall stop the standalone writer and verify its lock is released, then seed the Kubernetes Lease with generation `g_prior` and the activated writer epoch, including the database set discovered on the volume, before any cluster acquire. | §3, §10.5 | runbook test | 4 |
| REQ-STA-015 | The container image shall run as a configurable non-root numeric `USER` (default `1000:1000`) and shall reject root builds. | §5 Phase 1b | `docker_standalone.rs` (default and custom UID/GID) | 1b |

## 12. Probes and Kubernetes (ADR-0009, ADR-0016)

| ID | Requirement | Source | Verify | Phase |
|----|-------------|--------|--------|-------|
| REQ-PROBE-001 | The node shall report on `/live` only that its controller loop is alive and shall not access SQLite or the lease store to answer it. | R19 | integration | 3 |
| REQ-PROBE-002 | The node shall return 200 on `/ready` only while it is the activated writer and 503 otherwise. | §3 | integration | 1b, 3 |
| REQ-PROBE-003 | The node shall report service readiness on `/healthz` independent of role. | R19 | e2e (rolling update) | 3 |
| REQ-K8S-001 | When the node starts, it shall reconcile the `hasqlite.io/role=writer` pod label with its current role; when it activates, it shall apply the label; when it demotes, it shall remove the label before demotion completes; if a label update fails, then it shall retry with backoff. | R19, §3 | e2e | 4 |
| REQ-K8S-002 | The node shall run the Kubernetes Lease client in-process, store its version token in annotation `hasqlite.io/version` and use `resourceVersion` only as the CAS precondition. | §5 Phase 4 | e2e | 4 |
| REQ-K8S-003 | The deployment manifests shall provide a writer Service selecting `hasqlite.io/role=writer`, a PodDisruptionBudget with `maxUnavailable: 1`, separate Secrets for the SQL and replication tokens (mode `0440`, restricted `fsGroup`), RBAC limited to `get`/`update` on the hasqlite Lease and `patch` on the StatefulSet's own pods, and `terminationGracePeriodSeconds` covering drain, final sync and checkpoint. | §3, §5 Phase 4 | manifest validation (kubeconform) | 4 |
| REQ-K8S-004 | The rollout procedure shall update replicas before the writer and shall cause at most one failover per rolling update without losing an acknowledged write. | §6.4 | e2e (k3d rolling update under write load) | 4 |

## 13. Observability

| ID | Requirement | Source | Verify | Phase |
|----|-------------|--------|--------|-------|
| REQ-OBS-001 | The node shall append audit entries as JSON lines to an append-only log on its data volume, recording action, from-state, to-state, lease generation, and first and last commit time per generation. | §9 | e2e (audit overlap check) | 3 |
| REQ-OBS-002 | The node shall export the Prometheus metrics `hasqlite_sync_lag_seconds`, `hasqlite_lease_held`, `hasqlite_promotion_denied_total` (labelled by `PromotionError` variant), `hasqlite_renewal_deadline_misses_total`, `hasqlite_origin_wal_bytes`, `hasqlite_snapshot_future_skew_seconds`, `hasqlite_diverged_files`, `hasqlite_diverged_bytes` and `hasqlite_divergence_preservation_pending`, with `hasqlite_sync_lag_seconds`, `hasqlite_origin_wal_bytes` and `hasqlite_snapshot_future_skew_seconds` labelled by `database`, plus `hasqlite_databases` (size of the observed database set) and `hasqlite_unmanaged_database_files`. | §9 | integration | 2, 5 |
| REQ-OBS-003 | The node shall emit `tracing` spans `tick`, `sync` and `promote`. | §9 | unit | 3 |
| REQ-OBS-004 | The deployment shall ship recommended alerts for sync lag above `max_lag_secs` for more than two intervals, growing origin WAL, promotion-denied spikes, lease holder missing for more than TTL, future skew approaching the limit, `hasqlite_diverged_files > 0`, and `hasqlite_divergence_preservation_pending == 1` for more than 10 minutes. | §9 | manifest review | 4, 5 |

## 14. Dependency transition (ADR-0008)

| ID | Requirement | Source | Verify | Phase |
|----|-------------|--------|--------|-------|
| REQ-DEP-001 | While the transitional adapter is in use, the build shall pin `rsqlite-rsync` by commit `rev` and build with `--locked`. | §4 | CI | 0 (exists) |
| REQ-DEP-002 | The workspace shall confine `rsqlite-rsync` usage to `hasqlite-sync`, and public `hasqlite-sync` errors shall not expose upstream types. | §4 | code review; `cargo tree` | 2 |
| REQ-DEP-003 | When the native backend cuts over, `cargo tree --workspace` and the lockfile shall contain no `rsqlite-rsync` package or Git source, and a clean build and test shall pass without access to the upstream repository. | §4 | CI | TN, 5 |
| REQ-DEP-004 | Before cutover, a differential suite shall apply the same workloads through the adapter and the native backend and compare page hashes. | §4 E2 | differential suite | TN |

## 15. Quality and verification

| ID | Requirement | Source | Verify | Phase |
|----|-------------|--------|--------|-------|
| REQ-QA-001 | CI shall enforce at least 95% line coverage on `hasqlite-controller` and `hasqlite-lease` and at least 90% on `hasqlite-sync`. | §6.1 | CI `coverage` job (exists) | 0 |
| REQ-QA-002 | Every `PromotionError` and `LeaseError` variant shall have a test. | §5 Phase 1 | coverage review | 1 |
| REQ-QA-003 | `hasqlite-controller` and `hasqlite-lease` shall perform no SQLite I/O. | §5 Phase 1 | code review | 1 |
| REQ-QA-004 | The deterministic simulation shall run 1,000 seeds per pull request and 100,000 nightly, log seeds for replay, and at each step check at most one node inside its commit window, generation monotonicity with acquire and activate preceding every promotion, and promotion only from the current writer epoch. | §6.1b | CI `simulation` job | 1 |
| REQ-QA-005 | After every chaos and end-to-end run, a history checker shall verify that acknowledged writes on the surviving writer form a prefix with bounded loss and that no two nodes' audit logs show overlapping commit intervals once the fencing margin is accounted for. | §6.3, §6.4 | chaos; e2e | 3 |
| REQ-QA-006 | CI shall run `cargo fmt --check`, `clippy -D warnings`, `cargo doc` with `-D missing_docs`, `cargo test` and `cargo audit`, all with `--locked`, and shall not suppress `cargo audit` advisories. | §6.7, §10.5 | CI (exists) | 0 |
| REQ-QA-007 | Until resource budgets are ratified and the runner is shown to be repeatable, the resource-benchmark job shall be report-only and shall not label the resource goal as passed. | §6.5 | `resource-benchmarks.yml` | 5 |
| REQ-QA-008 | Once enabled as a gate, the resource-benchmark job shall fail on a missing required measurement, an active scenario with zero successful operations, or a budget or regression breach against the declared direction and tolerance. | §6.5 | `resource-benchmarks.yml` | 5 |
| REQ-QA-009 | Benchmark reports and CI artifacts shall contain only sanitized summaries and metric measurements, and shall not contain datasets, database files, secrets, or unsanitized environment or configuration dumps. | §6.5 | review of artifacts | 5 |
| REQ-QA-010 | Background work should avoid busy polling and unbounded buffering. | §1 goal 7 | benchmarks (idle scenarios) | 5 |

---

## 16. Operational targets (not requirements)

These are v1 success criteria from [PLAN §1](PLAN.md#success-criteria-v10).
They are goals verified in the stated environment, not guarantees for every
deployment.

| Target | Value | Verified by |
|--------|-------|-------------|
| Split-brain write window | 0 commits after the renewal deadline (assumes no commit stalls longer than the fencing margin) | sim; chaos; e2e audit overlap |
| Steady-state sync interval | configurable, 1–5 s default | config |
| Promotion block on stale replica | 100% when lag > `max_lag_secs` or ledger not from the writer epoch | unit; chaos |
| Failover RTO | p95 < 15 s on 3-node k3s, node-local disk, default lease timing (TTL 10 s, deadline 6 s, retry 2 s) | `failover_rto` bench; e2e |
| Data after planned switchover | page-hash match when the final snapshot is durable on the promoted replica; never claimed after a timed-out final sync | `planned_switchover.rs`; e2e |
| Data after unplanned failover | prefix of acknowledged writes; loss window ≤ `max_lag_secs + renew_deadline_secs` | chaos (crash mid-write) |
| Resource footprint | ratified per-scenario CPU, memory, network and disk-I/O budgets pass; numeric values TBD | §6.5 benchmark process |

## 17. Open issues

These are unresolved in the plan and are deliberately **not** requirements.
Each needs a decision (and an ADR) before it can become one.

| ID | Issue | Source |
|----|-------|--------|
| OPEN-001 | Policy when the final sync or drain times out during a planned switchover (abort the handover, or enter an explicitly bounded-loss path), and how the final-snapshot-bearing successor is selected and verified. | §10.6; automated-recovery §5 |
| OPEN-002 | Durable, failover-safe deduplication for idempotency keys and its retention window. | §10.6 |
| OPEN-003 | Closing the stalled-commit window (requires an external or storage-level fence, or a different commit architecture). | §10.6 |
| OPEN-004 | Recovery from damaged or lost authoritative lease state without resetting generations. | §10.6 |
| OPEN-005 | Protection against arbitrary corruption and storage faults; corrupted-writer diagnosis and repair. | §10.6 |
| OPEN-006 | Independent backups, restore verification and point-in-time recovery. | §10.6 |
| ~~OPEN-007~~ | Resolved 2026-11-18: the 64 MiB minimum applies to the headroom, so the requirement is `S + max(S / 10, 64 MiB)`; see REQ-DIV-012 and safety contract rule 15. | R15 |
| OPEN-008 | The acceptance threshold for "freshest replica wins the majority of races" in simulation is set when the harness lands. | §6.1b |
| OPEN-009 | Numeric resource budgets, measurement windows and regression tolerances. | §6.5 |
