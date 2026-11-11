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
- `LeaseObservation` (candidate-side expiry on the local monotonic clock) and `RenewalDeadline` (writer-side commit cutoff)
- `validate_promotion` against the observed lease's activated writer epoch, and `enforce_write_fence` checked before `COMMIT`
- Lease store trait with conditional operations only; the store, not the caller, derives the new generation:
  - `acquire(observed: &VersionedLease) -> Result<VersionedLease, LeaseCasError>` (generation + 1, writer epoch unchanged)
  - `activate(held: &VersionedLease)` (records the writer epoch before a writable handle opens)
  - `renew(held: &VersionedLease)` and `release(held: &VersionedLease)` (require unchanged version, `holder == self`, unchanged generation)
- `FileLeaseSource`: never-replaced lock file under `flock`, record file written by temp + rename + fsync, version = record write counter. This is the only I/O in Phase 1 and is required by Phase 1b.
- `HaController::tick` equivalent with injectable `LeaseSource` / `FreshnessSource` / `Clock`, gating promotion on acquire and activate success and delaying each candidate's attempt in proportion to its lag
- `ControllerPlan` + `execute_controller_plan` with test doubles modeling concurrent promotion races where multiple replicas observe an expired lease simultaneously

Deliverables:

- ≥95% line coverage on `hasqlite-controller` and `hasqlite-lease`, enforced by the CI `coverage` job; every `PromotionError` and `LeaseCasError` variant has a test (see [§6.1](testing.md#61-unit-tests-hasqlite-controller-hasqlite-lease))
- Property tests for generation monotonicity, atomic CAS success/loss races, and lease observation expiry
- Race tests: a renewal between observation and acquisition prevents replacement of the live holder; a superseded writer's renewal fails and demotes it
- Single-winner verification: dual replica failover simulation asserting exactly one node promotes
- Deterministic simulation harness ([§6.1b](testing.md#61b-deterministic-simulation-testssim)) running the controller against an in-memory lease store
- No SQLite I/O in these crates (fast CI)

### Phase 1a — Wire API (week 3, alongside Phase 1)

`hasqlite-proto` defines the hasqlite-owned API before any of it ships:

- SQL gateway service: statements, batches, `NOT_LEADER` with redirect hint (current holder's advertised address) and current generation; responses distinguish "rejected before execution" from "outcome unknown" so clients know when a retry is safe; optional idempotency key on mutating requests
- Replication service: stream header carrying origin node ID, lease generation and snapshot start time (origin clock); the origin refuses to serve unless it is the current lease holder and activated writer
- Separate credentials: replication requests authenticate with `replication.auth_token_*` (server side) / `sync.transport_auth_token_*` (client side), never with the SQL token; constant-time comparison
- Protocol version field with a stated compatibility rule (version N serves N-1 clients and replicas) so rolling upgrades work

Deliverables: proto definitions, compatibility tests between adjacent versions.

### Phase 1b — Fast-track: Standalone single-node & Docker (weeks 4–5)

*Delivered right after Phase 1 and 1a, unlocking a usable standalone Docker container weeks
ahead of multi-node replication and clustering.*

Scope:
- Multi-stage `Dockerfile` (optimized Rust musl/debian-slim build, configurable non-root UID/GID defaulting to 1000:1000 via `ARG UID=1000` and `ARG GID=1000`, persistent volume mount at `/var/lib/hasqlite`):
  - The final stage needs no `/etc/passwd` entry: it sets numeric `USER ${UID}:${GID}` and creates `/var/lib/hasqlite` with `COPY --chown=${UID}:${GID}` from the builder stage. This works on distroless and musl bases that have no `/bin/sh` or `groupadd`. `ARG USER`/`ARG GROUP` (default `hasqlite`) add named entries only in the debian-slim variant.
  - Disallows root builds (`UID=0` or `GID=0` validation check in the builder stage).
  - Numeric `USER` guarantees compatibility with Kubernetes `runAsNonRoot: true` and OCI security scanners.
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
  - Auto-acquire genesis writer lease through `FileLeaseSource` ([§3](architecture.md#3-architecture) standalone lock rules), writing an authoritative `LeaseRecord` at `new_gen = g_prior + 1` (defaulting to `1` on first boot, or incrementing `max(g_lease, g_meta)` observed from prior volume state) and an activated writer epoch for itself, without requiring peer sync history or a freshness section.
  - Secret file credential support: `auth_token_file` (or `HASQLITE_GRPC_AUTH_TOKEN_FILE`) for secure credential mounting via Docker secrets or volume files; file-based tokens are the documented default because environment variables are visible in `/proc/*/environ` and pod specs.
  - Minimal node supervisor: bind gRPC SQL gateway engine and HTTP probe server (`/ready`, `/live`, `/healthz`).
  - Graceful shutdown handling: explicit `SIGTERM`/`SIGINT` handlers (PID 1), synchronous `sqlite3_wal_checkpoint_v2(TRUNCATE)`, in-flight connection draining, and lease release within Docker's 10s grace window.

Deliverables:
- Production `Dockerfile` with configurable non-root build arguments and `deploy/docker/docker-compose.standalone.yml`.
- `hasqlite run --mode standalone` or `hasqlite run --config standalone.toml`.
- Standalone container smoke test in CI (`docker build` + boot container + execute SQL via gRPC + assert `/ready` 200 OK), verifying both default and custom UID/GID images, a negative test for fast startup failure on unwritable volumes, a second container on the same volume failing with `EWOULDBLOCK`, and `docker stop` exiting 0 (not 137) within 10s. Feature-gated with `--features=docker-tests`.
- Gateway hardening payload suite ([§6.6](testing.md#66-security-tests)).

### Phase 2 — Sync orchestration (weeks 5–7)

`hasqlite-sync` orchestration on top of the pinned adapter. Native replication is Track N below; this phase does not
depend on it:

- Policies: `Periodic`, `OnLeaseChange`, `PreShutdown`, `PostPromoteCatchUp`
  - `PostPromoteCatchUp` runs only before the writable handle opens. Under Phase 1a's writer-origin requirement (safety contract rules 7–8), catch-up can only pull from an accessible writer origin; peer-to-peer catch-up from a fresher replica after writer failure is deferred until a safe peer-transfer protocol and replica-serving safety requirements are defined. The sync engine refuses to run while the node's role is `Writer`.
- Crash-safe ledger publication (safety contract rule 9). The adapter writes pages into the replica file in place, so an interrupted pull leaves a torn file:
  1. Invalidate the ledger and fsync it before the first page is written.
  2. Pull; `pull_replica` fsyncs the replica file and its directory on success.
  3. Publish the new ledger (origin-stamped source node, generation and snapshot start; destination node) by temp file + rename + directory fsync.
  4. The ledger lives on the same filesystem as the database (`/var/lib/hasqlite/.hasqlite.freshness` by default).
- Replica reads (`allow_replica_reads`) use read-only connections that are reopened after each completed sync. Because the adapter can expose torn pages to concurrent readers, `allow_replica_reads` stays unsupported until Track N applies snapshots through staging or under SQLite locking.
- Clustered v1 replicates exactly one database per cluster, so a single ledger describes the whole replica. Standalone mode may still serve several databases.
- Before its first resync after demotion, a node preserves its database as `*.diverged-g<gen>`.
- Backoff and retry configuration for sync batches.

Deliverables:

- Integration tests: writer emits writes → replica converges within N syncs
- `live_writes` convergence test
- Crash-during-apply test ([§6.3](testing.md#63-fault-injection-testschaos-feature-chaos)) showing the node is unpromotable until a full sync completes

### Track N — Native replication backend (weeks 5–12, parallel; gates Phase 4 and M5)

- Native snapshot, page-diff and transfer with a hasqlite-owned pull API (Phase 1a protocol), replacing the adapter integration
- Snapshots applied atomically (staging file + rename, or under SQLite locking), which also makes replica reads safe
- Origin-side snapshot deadline: a stalled replica session is aborted so its read transaction cannot block WAL checkpoints; replication sessions stop before the shutdown checkpoint
- TLS with `sync.tls_ca_file` and optional `sync.tls_server_name` for private-CA certificates, due by week 10: the Kubernetes deployment in Phase 4 depends on it because the adapter trusts only public WebPKI roots.
- Differential suite against the adapter ([§4](architecture.md#4-repository-layout) E2), then dependency removal ([§4](architecture.md#4-repository-layout) criteria)

### Phase 3 — HA cluster supervisor & local 3-node runtime (weeks 7–10)

`hasqlite-node` multi-process cluster runtime:

- Full HA cluster supervisor: background controller tick loop + sync worker task + dynamic gateway failover
- CLI flags mirroring rsync `--ha-*` for migration (deprecation table in docs)
- HTTP probes (`hasqlite-probes`) with Kubernetes readiness vs liveness semantics documented; `/live` never touches SQLite or the lease store
- Writer routing: pod self-labeling (`hasqlite.io/role=writer`) for Kubernetes; for the local and Compose harnesses, an HAProxy front end that health-checks `/ready` provides the stable `sync.origin_endpoint` alias
- Graceful shutdown: replica preStop catch-up sync; writer planned switchover (stop writes, bounded wait for replicas' post-fence snapshots, release the lease), then drain and checkpoint

Deliverables:

- Local 3-node simulation (three data dirs, shared lease file dir on one host; file leases are single-host only)
- Multi-node Docker Compose test harness (`deploy/docker/docker-compose.cluster.yml`)

### Phase 4 — Kubernetes opinionated deploy (weeks 10–12)

- Port `k3s-ha-stack.yaml` → `deploy/k8s/` with hasqlite image/command
- In-process Kubernetes Lease client (no lease updater sidecar in v1). RBAC: `get`/`update` on the hasqlite Lease, `patch` on the StatefulSet's own pods for role labels
- Writer Service selecting `hasqlite.io/role=writer`; PodDisruptionBudget `maxUnavailable: 1`; rollout script that updates replicas first and the writer last via StatefulSet `partition`
- Separate Secrets for the SQL token and the replication token
- Scripts: `apply-stack.sh`, manifest validator test (copy pattern from rsync)
- Runbooks: failover, planned switchover, manual promotion (including a writer that died before any replica synced from it), stale replica rebuild, diverged data recovery

Deliverables:

- k3d CI job (optional, nightly): smoke failover and rolling update under write load
- `docs/runbooks/failover.md`

### Phase 5 — Hardening & release (weeks 12–13)

- Metrics (Prometheus): sync lag, lease holder, promotion denials, origin WAL size
- Soak test harness (24h, scheduled failovers)
- CHANGELOG, version 0.1.0, README quickstart

---

### Post-v1 — Automated Failure Recovery & Self-Healing

Post-v1 development transitions hasqlite from its v1 fail-closed stance (requiring operator runbooks on abnormal states) to configurable, automated self-healing while maintaining the core safety contract. Detailed architecture, state machines, and protocols are specified in [automated-recovery.md](automated-recovery.md):

#### Tier 1 — Node-Local Self-Healing & Request Idempotency
- **Staged Atomic Snapshots**: Apply incoming page syncs into a staging file (`.db.staging`), verify `PRAGMA integrity_check` and checksums, then atomically replace active database files (`renameat2` / directory fsync) to prevent torn replica states.
- **Auto-Quarantine & Re-seed**: When a replica encounters corrupted pages or invalid ledger state on boot/sync, automatically isolate the damaged database to `quarantine/<timestamp>.db`, invalidate freshness, and initiate a full re-clone from the active writer or a healthy peer.
- **In-DB Replicated Idempotency**: Maintain a dedicated internal table (`_hasqlite_idempotency`) committed transactionally with client writes. Because SQLite pages replicate to standby nodes, clients retrying after an "outcome unknown" failover obtain cached execution results from the newly promoted writer without duplicate statement execution.

#### Tier 2 — Cluster Consensus & Emergency Promotion
- **Emergency Escalation Timers for Stale Replicas**: If an activated writer dies and all surviving replicas exceed `freshness.max_lag_secs`, enter an emergency election window after `emergency_promotion_timeout_secs` (e.g. $3 \times \text{TTL}$), automatically electing the freshest replica and recording an explicit bounded-loss audit event.
- **Peer Quorum Verification**: Candidates ping reachable peers via gRPC to compare `synced_at_secs` and verify quorum before initiating emergency promotion.
- **Dead-Epoch Roll-Forward**: If an activated writer dies before any replica syncs from it (leaving all replicas on epoch $g-1$ while the lease shows epoch $g$), allow surviving nodes via quorum attestation to invalidate the dead epoch and campaign to advance from $g-1$ directly to $g+1$.
- **Clock Skew Auto-Backoff**: Replicas delayed by future-skewed snapshot start timestamps automatically schedule controller re-evaluations as the local monotonic/wall clock catches up, avoiding permanent failover blocks.

#### Tier 3 — Divergent Data Capture & Automated Reconciliation
- **Session Changeset Capture**: Integrate the SQLite Session extension (`sqlite3session`) on the writer path to track committed row changesets in an append-only WAL.
- **Automated Divergence Reconciler**: When a demoted writer rejoins as a replica, inspect `*.diverged-g<gen>` against the common ancestor, export divergent transactions to a Dead-Letter Queue (DLQ), and optionally replay idempotent writes against the new writer.

#### Tier 4 — Switchover Fallback Policies
- **Deterministic Switchover Policy**: Formalize switchover timeout handling with a configurable policy: abort handover if time expires while old writer remains healthy, or force promotion of the freshest replica with bounded-loss acknowledgment when shutdown deadlines are hard.

---
