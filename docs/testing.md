# hasqlite — Test Strategy

## 6. Test strategy

### 6.1 Unit tests (`hasqlite-controller`, `hasqlite-lease`)

| Area | Cases | Technique |
|------|-------|-----------|
| Lease parsing | empty, `none`, malformed, future renew, missing writer epoch | table-driven |
| K8s lease JSON | holder, generation and writer-epoch annotations, missing fields | hasqlite-owned fixtures, informed by prior-art cases |
| Lease observation | version unchanged for TTL → expired; version change restarts timer; wall-clock jumps have no effect; effective TTL is `max(local, holder's recorded)` in both directions | injected `Instant` (exists) |
| Writer clock | stamps never decrease across a backward wall-clock step; floor seeded from the writer epoch; a post-step snapshot cannot read as fresher than the renewal | pure function asserts (exists) |
| Renewal deadline | commit allowed before deadline, refused at and after it; overflow fails closed | injected `Instant` (exists) |
| Config identifiers | `node.id` charset and length, DNS-1123 Kubernetes names, standalone rejects `sync.origin_endpoint`, `2 × retry ≤ deadline`, `max_lag > interval + retry` | table-driven (exists, `hasqlite-core`) |
| Promotion | lag at and past the limit, ledger from older writer generation, other source node, unbound or self-sourced ledger, no activated writer, future skew, aborted acquisition keeps previous epoch valid, documented error precedence (ledger shape → writer epoch → time bounds) | pure function asserts (exists) |
| Write fence | non-writer, deadline passed, generation mismatch | enum `PromotionViolation` |
| Reconcile | steal lease, renew, partition (stale holder), freshest-first attempt delay | state machine scenarios |
| CAS acquisition race | renewal between observation and acquisition | mock lease store (assert CAS fails, holder preserved) |
| Superseded writer | peer acquired g+1, old writer renews | mock lease store (renew fails, writer demotes in the same tick) |
| Action execution | partial failure, continue-on-error | mock executor |

**Property-based** (proptest, mirror `property_tests.rs`):

- Generation never decreases on valid transitions; acquire always yields `observed + 1`; renew, activate and release never change it
- A ledger from any generation other than the writer epoch's is rejected
- The lag calculation is independent of detection delay; promotion outcome depends on lag and writer-epoch match, while future skew can also affect promotion

**Coverage target:** ≥95% line coverage on `hasqlite-controller` and `hasqlite-lease`, and ≥90% on `hasqlite-sync`
(it holds the crash-safe ledger publication path), enforced by the CI `coverage` job
(`cargo llvm-cov --fail-under-lines`).

### 6.1b Deterministic simulation (`tests/sim/`)

N controllers run against an in-memory lease store under `turmoil` (or `madsim`) with an injectable `Clock` that provides
per-node monotonic time and a wall clock with configurable skew and jumps. Random partitions, message delays, process
pauses and crashes are injected. Each step checks:

- At most one node is inside its commit window (before its renewal deadline) at any simulated instant
- Generation is monotonic, and every promotion is preceded by acquire and activate successes
- No node promotes with a ledger from anything but the current writer epoch

Across seeds, the least-lagged eligible replica must win the majority of acquisition races in which several replicas
are eligible (freshest-first delay, [§3](architecture.md#3-architecture)); the threshold is set when the harness lands.

Seeds are logged for replay. 1,000 seeds per PR, 100,000 nightly.

### 6.2 Integration tests (`tests/integration/`)

| Test module | Purpose | Prior-art analogue |
|-------------|---------|-----------------|
| `local_cluster.rs` | 3 nodes, file lease, promote/failover | `ha_mode.rs` |
| `sync_convergence.rs` | replica converges to writer page hashes | `local_sync.rs` |
| `live_writes.rs` | writes during sync | `live_writes.rs` |
| `gateway_writer_only.rs` | NOT_LEADER on replica | `grpc_gateway.rs` |
| `client_failover.rs` | retry only after a definite `NOT_LEADER`; writer killed after `COMMIT` but before the response surfaces an "outcome unknown" error and no duplicate row | `grpc_failover.rs` |
| `grpc_replication.rs` | native replication transport, origin identity stamping, demoted origin refuses to serve | `grpc_replication.rs` |
| `shutdown_sync.rs` | replica preStop catch-up | new |
| `planned_switchover.rs` | writer preStop releases the lease; a replica promotes without waiting a TTL; page hashes match; when the final sync times out, the handover follows the [§10.6](operations.md#106-unresolved-guarantees-and-recovery-policy) timeout policy (once defined) and is never reported as lossless | new |
| `file_lease_lock.rs` | holder rewrites the record 100 times while a second process gets `EWOULDBLOCK` each time; reopening and closing the lock path inside the holder does not release it; NFS/FUSE `data.dir` refused | new (Phase 1) |
| `gateway_hardening.rs` | [§6.6](#66-security-tests) payload suite on writer and replica | new (Phase 1b) |
| `token_rotation.rs` | rotated token file takes effect on the next pull without restart; a rejected token never appears in the error chain and leaves an existing replica file untouched | `hasqlite-sync/tests/pull_replica.rs` (exists) |
| `adapter_ledger_protocol.rs` | lease version changes between the pre-pull observation and the post-fsync re-check → ledger stays invalidated; holder ≠ writer epoch after the pull → invalidated; unchanged lease → ledger stamped with the pre-pull writer epoch and `writer.renewed_at_secs` | new (Phase 2, safety contract rule 9) |
| `replication_role_gate.rs` | a node that is not the activated writer refuses to serve pulls through the adapter's replication server | new (Phase 2) |
| `exclusive_replica_path.rs` | a second pull into a path with a pull in progress fails with `SyncInProgress` before any I/O | `hasqlite-sync` unit tests (exists) |
| `readonly_replica_reads.rs` | `--allow-replica-reads` with a concurrent reader loop running `PRAGMA integrity_check` during sync (Track N) | gateway flags |
| `docker_standalone.rs` | Standalone single-node container boot (default and custom UID/GID), volume persistence, gRPC SQL CRUD, probe health, second container rejected, clean `docker stop` | new (Phase 1b) |

Run: `cargo test --workspace` (default). Heavy cluster tests gated with
`--features=k8s-tests` or env `HASQLITE_K8S_TESTS=1`. Docker smoke tests gated with
`--features=docker-tests` or env `HASQLITE_DOCKER_TESTS=1` so standard `cargo test` passes
in container-less environments.

### 6.3 Fault injection (`tests/chaos/`, feature `chaos`)

| Scenario | Injection | Expected |
|----------|-----------|----------|
| Partition writer from lease store | block lease file writes / fake kubectl | writer stops commits at its renewal deadline, then demotes |
| Writer process pause | `SIGSTOP` writer for `ttl + 2s`, peer promotes, `SIGCONT` | old writer's renewal fails; zero commits in its audit log after its deadline |
| Clock jump / skew | libfaketime: candidate +30s, writer −30s | no promotion while the writer keeps renewing |
| Lease store unreachable from all nodes | block Lease API | writer demotes within renew deadline; no promotion until the store returns |
| Writer partitioned from replicas only | drop replication traffic | replica lag grows; if the writer then dies, promotion is denied (documented manual runbook) |
| Stale replica promote | artificial old freshness | promotion denied |
| Sync transport failure | break gRPC / wrong token | retry, alert, no promote |
| Crash during page apply | `kill -9` replica at a random page in the fine pass | ledger stays invalidated; promotion denied until a full sync and `PRAGMA integrity_check` pass |
| Disk full | ENOSPC during page apply or ledger publish | ledger stays invalidated; no promotion from that node |
| Stalled replica session | replica stops reading mid-stream for 10× interval | origin aborts the session; WAL can be checkpointed |
| Crash mid-write | SIGKILL writer | new writer after failover; its data contains a prefix of acknowledged writes and may include committed writes whose client outcome was unknown; loss ≤ `max_lag_secs` + `renew_deadline_secs` (assumes no commit stalls inside `COMMIT` beyond the fencing margin; see [§10.2](operations.md#102-data-safety-and-durability-limitations)) |
| Aborted promotion | kill the CAS winner between acquire and activate | another replica promotes against the previous writer epoch |
| Writer dies before first sync | kill a newly activated writer before any replica completes a sync from it | automatic promotion denied (wrong writer epoch); alert fires; manual promotion runbook required |
| Writer clock ahead | libfaketime: writer +`max_future_skew_secs` + 1s; wait for a replica to publish a post-adjustment ledger and for the future-skew alert ([§8](configuration.md#8-configuration-contract-v1)) to fire; then kill writer | promotion denied on future skew while the skew condition persists; if the snapshot ages within the limit before a promotion attempt, denial no longer applies on this basis |
| Full-cluster cold restart | stop all nodes for > TTL, restart with coordination available and at least one eligible replica | an eligible replica promotes automatically after observing expiry; acquisition delay favors but does not guarantee the freshest replica |
| Mixed-version rolling upgrade | version N writer with N-1 replicas, and the reverse | replication and failover keep working |
| Dual lease claim | corrupt lease file | single winner via generation rules |
| Rolling lease-timing change | restart replicas with `ttl 4 / deadline 2` while the writer runs `ttl 10 / deadline 6`, then `SIGSTOP` the writer | no replica campaigns before 10s of unchanged version (effective TTL honours the holder's recorded TTL); no overlapping commits |
| Unrelated Lease writes | annotate and label the Kubernetes Lease object every second while the writer is `SIGSTOP`ped | failover happens after the effective TTL regardless; the extra writes neither renew nor delay |
| Writer clock step back | libfaketime steps the writer clock −60s between a renewal and the next snapshot | snapshot stamp holds at the renewal value; lag reads 0, not 60s, and promotion eligibility is unchanged |

Every chaos and E2E run records client operations (sequence number; acknowledged, failed or outcome unknown) and checks
the history afterwards: acknowledged writes on the surviving writer form a prefix with bounded loss, and no two nodes'
audit logs show commits in overlapping intervals once the fencing margin is accounted for.

### 6.4 Kubernetes / E2E

| Suite | Environment | Frequency |
|-------|-------------|-----------|
| Manifest validate | shell + kubeconform | every PR |
| k3d 3-node failover | CI nightly | nightly |
| k3d rolling update under write load | CI nightly | nightly |
| Soak 24h + periodic kill | staging | pre-release |

Assertions:

- No two pods' audit logs record commits in overlapping intervals (the lease API always shows one holder, so checking it proves nothing)
- History checker ([§6.3](#63-fault-injection-testschaos-feature-chaos)) passes after every failover
- Page hashes match after planned switchovers where the final sync completed successfully; after crash failovers, the prefix property holds. A timed-out final sync is never reported as lossless (see [§10.4](operations.md#104-replication-and-request-serving-limits), [§10.6](operations.md#106-unresolved-guarantees-and-recovery-policy)).
- A rolling update causes at most one failover and loses no acknowledged write

### 6.5 Performance & benchmarks

| Bench | Measures | Tool |
|-------|----------|------|
| `sync_catchup` | bytes moved vs churn | native Criterion benchmark |
| `failover_rto` | promote → first successful write | custom harness |
| `controller_tick` | reconcile latency | Criterion |

#### Resource-footprint benchmark process

"Very low resource consumption" is a goal, not a verified property or an
absolute guarantee. Benchmark hasqlite before making that claim or setting
release budgets. The benchmark report MUST separate hasqlite's own overhead
from the resources required by SQLite, the workload, and the deployment
environment.

1. **Define representative scenarios.** Measure at least:
   - standalone idle after startup and warm-up, with no clients;
   - standalone read-heavy and write-heavy service at documented concurrency
     and offered load;
   - clustered steady state with an idle replica and with active replication;
   - replica catch-up under controlled write churn.
   Record startup separately from steady state. Include representative database
   sizes and document the chosen sizes and workload mix; do not generalize
   beyond the measured range.
2. **Choose comparable baselines.** For standalone SQL workloads, run the same
   SQLite version, database, schema, SQL statements, durability settings,
   concurrency, and host against SQLite without the hasqlite process/gateway.
   This estimates incremental service overhead; it is not a comparison of
   equivalent HA guarantees. For clustered scenarios, report absolute use and
   compare only with a clearly described equivalent setup.
3. **Measure all resource dimensions.** Record process and container CPU
   consumption (including idle CPU as CPU time per wall-clock interval),
   steady-state and peak resident memory, disk read/write bytes and I/O
   operations, and network bytes. For active workloads, report CPU-seconds,
   disk-I/O, and network bytes per successfully completed operation as well as
   absolute totals. Do not compute per-operation values for idle runs; report
   zero-client resource use over the sampling interval instead. Report attempted,
   successful, and failed operations separately. For active scenarios, report
   throughput and p50/p95/p99 latency when there are successful operations.
   Represent latency percentiles as `null` with reason `not_applicable` for
   idle scenarios; do not substitute zero. If an active scenario expected to
   complete work has zero successful operations, mark it invalid and fail the
   gate rather than reporting per-operation metrics or percentiles. Track
   database, WAL, and sync-related disk use separately where applicable.
4. **Make runs reproducible.** Use optimized builds and fixed, documented
   inputs. Hold hardware, OS/kernel, CPU and memory limits, storage, config,
   SQLite settings, dataset, and load generator constant between hasqlite and
   baseline runs. Warm up before sampling, run long enough to reach steady
   state, and run each scenario at least five times to report run-to-run
   variation, the median, and tail behavior. Record the exact commit, toolchain,
   dependency lockfile, sanitized commands, runner image and instance class,
   allowlisted non-secret configuration, and raw metric measurements with each
   report. Use synthetic data
   only; never benchmark with production databases or include database contents
   in reports or artifacts. Do not dump the process environment. Redact tokens,
   credentials, authorization headers, credential-bearing URLs, private
   endpoints, and local paths from commands, configuration, logs, and reports
   before storing or publishing them. If the environment is noisy or results
   are not repeatable, report that limitation and rerun on the designated
   stable runner.
5. **Set and enforce budgets.** Use the baseline results and intended
   deployment profiles to propose numeric per-scenario CPU, memory, network,
   and disk-I/O budgets, covering both idle overhead and the declared target
   load. Document the rationale, measurement window, and acceptable regression
   tolerance and comparison direction for each gated metric; do not invent
   thresholds after seeing a failing run. Resource-consumption metrics use an
   upper bound: higher CPU, memory, network, or disk I/O is a regression.
   Service-quality metrics use their appropriate direction: lower throughput
   and higher latency or failure rate are regressions; higher throughput and
   lower latency or failure rate are improvements, not regressions. For
   metrics without an absolute budget, compare against the baseline only using
   this declared direction and its pre-approved tolerance. Ratify and record
   the budgets and directions before treating resource efficiency as achieved.
   Until then, benchmark results are report-only and the resource goal remains
   unverified, not a pass.
6. **Gate changes and release.** Keep a benchmark baseline and run comparable
   measurements on the designated stable CI runner nightly, on pull requests,
   and before release. The implementation deliverables are a harness and
   scenario definitions under `benches/resource_footprint/`, an invocable
   `scripts/bench-resource-footprint.sh` runner, a checked-in,
   machine-readable baseline and ratified budgets under
   `docs/benchmarks/resource-footprint/`, and a
   `.github/workflows/resource-benchmarks.yml` workflow. Before budgets are
   ratified and the runner is shown to be repeatable, the job MUST be report-only
   and MUST NOT label the resource goal as passed. Once enabled as a gate, each
   scenario MUST run the configured repetitions and validate required
   measurements according to its declared scenario type. Idle scenarios MUST
   provide all resource measurements and operation counts, with throughput and
   latency percentiles marked `null`/`not_applicable`; those fields are excluded
   from metric comparisons. Active scenarios MUST provide successful-operation
   counts, throughput, latency percentiles, and resource measurements. An active
   scenario with zero successful operations MUST fail as invalid. The gate
   compares available, applicable median measurements against the absolute
   budgets that exist for those metrics and against the recorded baseline using
   each metric's declared regression direction and pre-approved tolerance.
   Missing required measurements, an invalid scenario, or a budget or regression
   breach MUST fail the gate; baseline changes require review and recorded
   benchmark evidence. Store only sanitized summaries and
   machine-readable baselines in the repository. Keep raw CI reports as
   access-controlled, time-limited artifacts with retention configured in the
   workflow; CI artifacts MUST contain only sanitized reports and metric
   measurements, never datasets, database files, secrets, or unsanitized
   environment/configuration dumps. When a budget is exceeded, profile the
   relevant path, make a targeted optimization, and rerun both benchmarks and
   correctness tests. The v1 release criterion is that every documented
   resource budget passes on its specified scenario and environment.

Use hasqlite-owned sync settings documented in the
[configuration guide](configuration.md#8-configuration-contract-v1). Benchmark
reports MUST include the effective, sanitized sync settings, including
`sync.interval_secs` and any applicable batching settings. These settings
affect both resource use and replication behavior.

### 6.6 Security tests

- Gateway refuses unauthenticated start without explicit insecure flag; clustered mode rejects `allow_unauthenticated` even on a loopback bind
- Path traversal and symlink escape on `--data-dir` / database name
- Token required on all RPCs (client negative tests); the SQL token is rejected by the replication service and the replication token by the SQL service
- Gateway hardening payloads rejected on writer and replica: CTE-wrapped `INSERT`, comment-prefixed DML, `ATTACH`, `VACUUM INTO`, `PRAGMA writable_schema`, `load_extension`, oversized SQL
- TLS: a private-CA certificate is accepted with `sync.tls_ca_file`; a certificate with the wrong SAN is rejected; there is no plaintext fallback
- Tokens never appear in logs or error messages; tokens rotate without restart

### 6.7 CI matrix

The authoritative definition is [`.github/workflows/ci.yml`](../.github/workflows/ci.yml). Today it runs:

```yaml
jobs:
  build-test:   # exists
    - cargo fmt --all -- --check
    - cargo clippy --workspace --all-targets --locked -- -D warnings
    - cargo doc --no-deps --workspace --locked   # RUSTDOCFLAGS=-D warnings -D missing_docs
    - cargo test --workspace --locked
  coverage:     # exists
    - cargo llvm-cov --locked -p hasqlite-controller --fail-under-lines 95
    - cargo llvm-cov --locked -p hasqlite-lease --fail-under-lines 95
    - cargo llvm-cov --locked -p hasqlite-sync --fail-under-lines 90
  audit:        # exists (rustsec/audit-check; the workspace carries a Git dependency)
```

Planned additions, by phase:

```yaml
  simulation:   # Phase 1
    - cargo test --locked --test sim   # 1,000 seeds per PR, 100,000 nightly
  docker:       # Phase 1b, --features docker-tests
    - docker build && standalone smoke test
  k8s:          # Phase 4, nightly
    - k3d cluster create && apply deploy/k8s && run smoke and rolling-update scripts
```

---
