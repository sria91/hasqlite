# hasqlite — Development Roadmap

## 5. Development phases

### Phase 0 — Bootstrap (week 1)

- [x] Sibling repo + workspace skeleton
- [x] CI: `cargo fmt`, `clippy -D warnings`, `cargo test`, `cargo doc` (all with `--locked`)
- [x] `hasqlite-core`: `NodeId`, `ClusterConfig`, error types
- [x] Document safety contract (draft)

**Exit:** empty crates compile; PLAN approved.

### Phase 1 — Controller parity (weeks 2–4)

Implement native equivalents of the required pure HA logic in
`hasqlite-controller` and `hasqlite-lease`, using the prior implementation as a behavioral reference:

- `parse_lease_record`, `parse_freshness_ledger` (mandatory destination binding, non-empty source attribution)
- `LeaseObservation` (candidate-side expiry on the local monotonic clock against the effective TTL `max(local, holder's recorded ttl_secs)`; done) and `RenewalDeadline` (writer-side commit cutoff; done)
- `WriterClock` (non-decreasing time basis for every writer stamp, seeded from the writer epoch's last renewal on activation; done) wired into renew, activate, release and snapshot stamping
- `validate_promotion` against the observed lease's activated writer epoch (done), and `enforce_write_fence` checked before `COMMIT`
- Lease store trait with conditional operations only; the store, not the caller, derives the new generation. Every write bumps the hasqlite-owned `LeaseVersion` token and uses the backend's native revision only as the CAS precondition (contract rule 2):
  - `acquire(observed: &VersionedLease) -> Result<VersionedLease, LeaseError>` (generation + 1, writer epoch unchanged)
  - `activate(held: &VersionedLease)` (records the writer epoch before a writable handle opens)
  - `renew(held: &VersionedLease)` and `release(held: &VersionedLease)` (require unchanged version, `holder == self`, unchanged generation)
- `FileLeaseSource`: never-replaced lock file under `flock`, record file written by temp + rename + fsync, version = record write counter. Two disciplines selected by `node.mode` ([§3](architecture.md#standalone-single-node-mode-docker--edge--local)): *mutex mode* (standalone, lock held for the process lifetime) and *CAS mode* (clustered local file lease, lock held only across one read-compare-write). This is the only I/O in Phase 1 and is required by Phase 1b.
- Kubernetes lease mapping (interface only in Phase 1, client in Phase 4): `LeaseVersion` ↔ annotation `hasqlite.io/version`, CAS precondition ↔ `resourceVersion`
- `HaController::tick` equivalent with injectable `LeaseSource` / `FreshnessSource` / `Clock`, gating promotion on acquire and activate success and delaying each candidate's attempt in proportion to its lag
- `ControllerPlan` + `execute_controller_plan` with test doubles modeling concurrent promotion races where multiple replicas observe an expired lease simultaneously

Deliverables:

- ≥95% line coverage on `hasqlite-controller` and `hasqlite-lease`, enforced by the CI `coverage` job; every `PromotionError` and `LeaseError` variant has a test (see [§6.1](testing.md#61-unit-tests-hasqlite-controller-hasqlite-lease))
- Property tests for generation monotonicity, atomic CAS success/loss races, lease observation expiry, and writer-clock monotonicity
- Race tests: a renewal between observation and acquisition prevents replacement of the live holder; a superseded writer's renewal fails and demotes it; a candidate with a shorter configured TTL than the holder's does not campaign before the holder's TTL
- Single-winner verification: dual replica failover simulation asserting exactly one node promotes
- Deterministic simulation harness ([§6.1b](testing.md#61b-deterministic-simulation-testssim)) running the controller against an in-memory lease store
- No SQLite I/O in these crates (fast CI)

### Phase 1a — Wire API (week 3, alongside Phase 1)

`hasqlite-proto` (registered in the workspace as a placeholder) defines the hasqlite-owned API before any of it ships:

- SQL gateway service: statements, batches, `NOT_LEADER` with redirect hint (current holder's advertised address) and current generation; responses distinguish "rejected before execution" from "outcome unknown" so clients know when a retry is safe; optional idempotency key on mutating requests
- Replication service: stream header carrying origin node ID, lease generation and snapshot start time (origin clock); the origin refuses to serve unless it is the current lease holder and activated writer
- Separate credentials: replication requests authenticate with `replication.auth_token_*` (server side) / `sync.transport_auth_token_*` (client side), never with the SQL token; constant-time comparison
- Protocol version field with a stated compatibility rule (version N serves N-1 clients and replicas) so rolling upgrades work

Deliverables: proto definitions, compatibility tests between adjacent versions.

### Phase 1b — Fast-track: Standalone single-node & Docker (weeks 4–7)

*Started right after Phase 1 and 1a and overlapping Phase 2, unlocking a usable standalone Docker container
ahead of multi-node replication and clustering. This phase carries the largest body of new code in the plan
(`hasqlite-gateway`, the node supervisor, signal handling, packaging and Docker CI), so it is allotted three weeks
rather than two; its gateway hardening is not optional and is not deferred.*

Scope:
- Multi-stage `Dockerfile` (optimized Rust musl/debian-slim build, configurable non-root numeric `USER ${UID}:${GID}` defaulting to 1000:1000, root builds rejected, persistent volume at `/var/lib/hasqlite`). Build arguments, UID/GID and host-volume permission guidance are specified once in [configuration.md](configuration.md#standalone-docker-invocation).
- `deploy/docker/docker-compose.standalone.yml` (with `stop_grace_period` covering the shutdown sequence) and container quickstart scripts with host volume permission guidance for default and custom UID/GID execution.
- `hasqlite-gateway` SQL engine hardening (safety contract rule 17):
  - Statements are classified with `sqlite3_stmt_readonly()` after preparation, never by inspecting SQL text.
  - An authorizer callback denies `ATTACH`, `DETACH`, `VACUUM INTO` and pragmas outside an allowlist.
  - `SQLITE_DBCONFIG_DEFENSIVE` on, extension loading off, `trusted_schema` off, and `sqlite3_limit` caps on SQL length and attached databases.
  - A progress handler bounds statement run time; `sqlite3_interrupt` aborts in-flight statements on demotion or shutdown.
  - Database names are resolved strictly inside `data.dir` (no traversal, no symlinks leaving it).
- Standalone mode profile in `hasqlite-core` and `hasqlite-node`:
  - `mode = "standalone"` in `ClusterConfig` (or `--mode standalone` CLI / `HASQLITE_MODE=standalone`).
  - Comprehensive pre-flight check during node startup:
    - Refuses network and FUSE filesystems for `data.dir` (`statfs`).
    - Discovers all supported database files across `data.dir` (root and nested paths matching SQLite conventions such as `*.db`, `*.sqlite`, `*.sqlite3`, plus their `-wal` and `-shm` sidecars) alongside the lease lock and record files.
    - Verifies write permissions on `data.dir`, on each discovered database/sidecar file, and on the parent directory of every database (so SQLite can create WAL and shared-memory files in nested paths).
    - Fails fast on the first permission denial, logging the unwritable path, the required operation, and the active UID/GID, instead of surfacing raw SQLite I/O errors later at runtime.
  - Auto-acquire genesis writer lease through `FileLeaseSource` in mutex mode ([§3](architecture.md#standalone-single-node-mode-docker--edge--local)), writing an authoritative `LeaseRecord` at `g_prior + 1` (`1` on a fresh volume) and an activated writer epoch for itself, without requiring peer sync history or a freshness section. A volume with database files but no readable lease record fails startup; `hasqlite genesis --generation <g>` is the audited operator override.
  - Secret file credential support: `auth_token_file` (or `HASQLITE_GRPC_AUTH_TOKEN_FILE`) for secure credential mounting via Docker secrets or volume files; file-based tokens are the documented default because environment variables are visible in `/proc/*/environ` and pod specs.
  - Minimal node supervisor: bind gRPC SQL gateway engine and HTTP probe server (`/ready`, `/live`, `/healthz`).
  - Graceful shutdown handling: explicit `SIGTERM`/`SIGINT` handlers (PID 1), synchronous `sqlite3_wal_checkpoint_v2(TRUNCATE)`, in-flight connection draining, and lease release within Docker's 10s grace window.

Deliverables:
- Production `Dockerfile` with configurable non-root build arguments and `deploy/docker/docker-compose.standalone.yml`.
- `hasqlite run --mode standalone` or `hasqlite run --config standalone.toml`.
- Standalone container smoke test in CI (`docker build` + boot container + execute SQL via gRPC + assert `/ready` 200 OK), verifying both default and custom UID/GID images, a negative test for fast startup failure on unwritable volumes, a second container on the same volume failing with `EWOULDBLOCK`, and `docker stop` exiting 0 (not 137) within 10s. Feature-gated with `--features=docker-tests`.
- Gateway hardening payload suite ([§6.6](testing.md#66-security-tests)).

### Phase 2 — Sync orchestration (weeks 6–8)

`hasqlite-sync` orchestration on top of the pinned adapter. Native replication is Track N below; this phase does not
depend on it:

- Policies: `Periodic`, `OnLeaseChange`, `PreShutdown`. There is no post-promotion catch-up in v1 (safety contract
  rule 13): under rules 7–8 a replica may pull only from the activated writer, which is unavailable at promotion
  time. The sync engine refuses to run while the node's role is `Writer`, and at most one pull runs per replica path
  (`SyncError::SyncInProgress`; done).
- Crash-safe ledger publication with adapter-era attribution (safety contract rule 9). The adapter writes pages into
  the replica file in place, stamps neither origin identity nor snapshot time, and serves pulls regardless of its
  node's role, so the replica supplies the attribution itself:
  1. Observe the lease (`version`, `holder_node_id`, `writer` epoch). Abort if `holder_node_id != writer.node_id`
     (an acquired-but-not-activated holder, or no activated writer).
  2. Invalidate the ledger and fsync it before the first page is written.
  3. Pull; `pull_replica` fsyncs the replica file and its directory on success.
  4. Observe the lease again. Publish only if `version` is unchanged since step 1; otherwise leave the ledger
     invalidated and retry on the next tick.
  5. Publish the new ledger stamped from the step-1 observation: `source_node_id = writer.node_id`,
     `source_generation = writer.generation`, `synced_at_secs = writer.renewed_at_secs` (a lower bound on the snapshot
     start on the writer's clock; lag is overstated by at most one retry period, never understated), and the local
     `destination_node_id`; temp file + rename + directory fsync.
  6. The ledger lives on the same filesystem as the database (`/var/lib/hasqlite/.hasqlite.freshness` by default);
     the pre-flight check verifies this with `statfs`/`st_dev`.
- Replication role gate: a tonic interceptor in front of the adapter's `ReplicationServer` rejects pull sessions
  with `FAILED_PRECONDITION` unless the node is the current lease holder and activated writer (contract rule 8).
- Replica reads (`allow_replica_reads`) use read-only connections that are reopened after each completed sync. Because the adapter can expose torn pages to concurrent readers, `allow_replica_reads` stays unsupported until Track N applies snapshots through staging or under SQLite locking.
- Clustered v1 replicates exactly one database per cluster, so a single ledger describes the whole replica. Standalone mode may still serve several databases.
- Before its first resync after demotion, a node preserves its database as `*.diverged-g<gen>`.
- Backoff and retry configuration for sync batches.

Deliverables:

- Integration tests: writer emits writes → replica converges within N syncs
- `live_writes` convergence test
- `adapter_ledger_protocol` and `replication_role_gate` tests ([§6.2](testing.md#62-integration-tests-testsintegration)): a lease change between the two observations leaves the ledger invalidated; a non-writer refuses to serve pulls
- Crash-during-apply test ([§6.3](testing.md#63-fault-injection-testschaos-feature-chaos)) showing the node is unpromotable until a full sync completes

### Track N — Native replication backend (weeks 5–12, parallel; gates Phase 4 and M5)

- Native snapshot, page-diff and transfer with a hasqlite-owned pull API (Phase 1a protocol), replacing the adapter integration
- Snapshots applied atomically (staging file + rename, or under SQLite locking), which also makes replica reads safe
- Origin-side snapshot deadline: a stalled replica session is aborted so its read transaction cannot block WAL checkpoints; replication sessions stop before the shutdown checkpoint
- TLS with `sync.tls_ca_file` and optional `sync.tls_server_name` for private-CA certificates, **due by week 8**: the Kubernetes deployment in Phase 4 (week 11) depends on it because the adapter trusts only public WebPKI roots, and the three-week lead is the only slack on the M4 critical path. Origin stamping (node ID, generation, snapshot start) in the stream header, which retires the adapter-era attribution protocol of Phase 2, is due in the same drop.
- Differential suite against the adapter ([§4](architecture.md#4-repository-layout) E2), then dependency removal ([§4](architecture.md#4-repository-layout) criteria)

### Phase 3 — HA cluster supervisor & local 3-node runtime (weeks 8–11)

`hasqlite-node` multi-process cluster runtime:

- Full HA cluster supervisor: background controller tick loop + sync worker task + dynamic gateway failover
- CLI flags mirroring rsync `--ha-*` for migration (deprecation table in docs)
- HTTP probes (`hasqlite-probes`) with Kubernetes readiness vs liveness semantics documented; `/live` never touches SQLite or the lease store
- Writer routing: pod self-labeling (`hasqlite.io/role=writer`) for Kubernetes; for the local and Compose harnesses, an HAProxy front end that health-checks `/ready` provides the stable `sync.origin_endpoint` alias
- Graceful shutdown: replica preStop catch-up sync; writer planned switchover (stop writes, bounded wait for replicas' post-fence snapshots, release the lease), then drain and checkpoint

Deliverables:

- Local 3-node simulation (three data dirs, shared lease file in CAS mode on one host; file leases are single-host only)
- Multi-node Docker Compose test harness (`deploy/docker/docker-compose.cluster.yml`)

### Phase 4 — Kubernetes opinionated deploy (weeks 11–13)

- Port `k3s-ha-stack.yaml` → `deploy/k8s/` with hasqlite image/command
- In-process Kubernetes Lease client (no lease updater sidecar in v1). Liveness token in annotation `hasqlite.io/version`, `resourceVersion` as CAS precondition only. RBAC: `get`/`update` on the hasqlite Lease, `patch` on the StatefulSet's own pods for role labels
- Writer Service selecting `hasqlite.io/role=writer`; PodDisruptionBudget `maxUnavailable: 1`; rollout script that updates replicas first and the writer last via StatefulSet `partition`
- Separate Secrets for the SQL token and the replication token
- Scripts: `apply-stack.sh`, manifest validator test (copy pattern from rsync)
- Runbooks: failover, planned switchover, manual promotion (including a writer that died before any replica synced from it), stale replica rebuild, diverged data recovery

Deliverables:

- k3d CI job (optional, nightly): smoke failover and rolling update under write load
- `docs/runbooks/failover.md`

### Phase 5 — Hardening & release (weeks 13–14)

- Metrics (Prometheus): sync lag, lease holder, promotion denials, origin WAL size
- Soak test harness (24h, scheduled failovers)
- Resource-footprint benchmarks for standalone and clustered scenarios; establish
  documented CPU, memory, network, and disk-I/O budgets from reproducible
  baselines, and meet them before claiming low resource consumption or releasing
  v1 (see [§6.5](testing.md#65-performance--benchmarks))
- CHANGELOG, version 0.1.0, README quickstart

---

### Post-v1 — Automated Failure Recovery & Self-Healing

Post-v1 development transitions hasqlite from its v1 fail-closed stance (requiring operator runbooks on abnormal states) to configurable, automated self-healing while maintaining the core safety contract. The architecture, state machines, protocols and configuration are specified once, in [automated-recovery.md](automated-recovery.md); this roadmap only lists the tiers and their order:

| Tier | Scope | Specification |
|------|-------|---------------|
| 1 | Staged atomic snapshots, replica auto-quarantine and re-seed, in-DB replicated idempotency | [§2](automated-recovery.md#2-tier-1-node-local-storage-self-healing) |
| 2 | Emergency promotion of stale replicas with peer quorum, dead-epoch roll-forward, clock-skew auto-backoff, peer-to-peer post-promotion catch-up (contract rule 13) | [§3](automated-recovery.md#3-tier-2-cluster-consensus--degraded-failover) |
| 3 | Session changeset capture and automated divergence reconciliation | [§4](automated-recovery.md#4-tier-3-divergent-data-capture--reconciliation) |
| 4 | Planned-switchover fallback policies | [§5](automated-recovery.md#5-tier-4-planned-switchover-fallback-policies) |

---
