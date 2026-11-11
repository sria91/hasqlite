# hasqlite — Development Plan

This page summarizes the product goals and milestones. Detailed design and
implementation guidance is organized by topic:

- [Architecture and repository layout](architecture.md)
- [Development roadmap](roadmap.md)
- [Test strategy](testing.md)
- [Security defaults and configuration contract](configuration.md)
- [Observability, risks, and operational limits](operations.md)
- [Safety contract](safety-contract.md) — authoritative safety invariants
- [Automated recovery & self-healing architecture](automated-recovery.md) (post-v1)

The documents share these conventions: normative requirements use uppercase
MUST, MUST NOT, SHOULD, and MAY; operational targets are goals, not claims of
implemented or verified behavior; unresolved guarantees remain explicit product
risks. The design is safety-first: when correctness and availability conflict,
the system prefers fail-closed behavior and operator recovery. Section numbers
are global across the document set (§1 and §11–12 here, §2–4 architecture, §5
roadmap, §6 testing, §7–8 configuration, §9–10 operations) so cross-references
like "§10.6" are unambiguous; the safety contract uses rule numbers instead.

### Glossary

- `generation`: the monotonic leader epoch tracked by the lease and used for
  fencing. It increases only when a candidate wins an acquire CAS.
- `writer epoch`: the pair of node identity and generation that was activated as
  the current writer; promotion gates validate that the freshest ledger matches
  this epoch.
- `lease version`: the hasqlite-owned token that changes on every lease write
  made by hasqlite (acquire, activate, renew, release): a counter in the file
  record, or a hasqlite annotation on the Kubernetes Lease. Candidates measure
  liveness by how long it stays unchanged.
- `CAS precondition`: the backend's native revision (Kubernetes
  `resourceVersion`, the file record's write counter) that a lease write must
  match to succeed. It is bumped together with the lease version but is never
  used for liveness, because unrelated actors can change it.
- `effective TTL`: `max(candidate's lease.ttl_secs, holder's recorded ttl_secs)`,
  the time a candidate waits before treating an unchanged lease as expired.
- `freshness ledger`: the replica-side record of the last successful sync from a
  source writer, including origin node, generation, and snapshot metadata.
- `outcome unknown`: a write whose client request may have reached the writer but
  whose final status cannot be definitively known; retries are forbidden unless a
  prior rejection proves the statement never executed.

---

## 1. Goals and future goals

### Goals

1. **Single logical writer** at all times, with **lease-backed fencing** and
   **generation monotonicity** (no split-brain writes).
2. **Fast failover** (target: low-second RTO) with **bounded RPO** via
   incremental rsync-style page sync from the current writer snapshot.
3. **Operator-grade** observability: audit log, readiness/liveness semantics,
   metrics, and Kubernetes-first deployment paths.
4. **Application API**: SQL over gRPC on the writer; safe redirect on replicas;
   Rust client with automatic `NOT_LEADER` failover that never silently retries
   a write whose outcome is unknown; the client and wire API will ultimately be
   owned by hasqlite.
5. **Library-first**: embed the HA controller in custom agents; ship a
   batteries-included `hasqlite` node binary.
6. **First-class standalone single-node Docker support (fast-track)**: Out-of-the-box
   capability to run a single `hasqlite` node in a standalone Docker container
   (file-based lease or auto-granted genesis lease, zero peer dependencies, gRPC SQL gateway,
   HTTP probes) for local development, edge, and single-host production before multi-node failover.
7. **Low resource footprint**: Minimize hasqlite's incremental CPU, memory,
   network, and disk-I/O overhead, including when idle. "Very low" is a
   performance goal to define and measure, not a claim about the current
   implementation. Background work SHOULD avoid busy polling and unbounded
   buffering; standalone mode MUST NOT run peer-replication work. Establish
   workload-specific budgets against reproducible baselines before declaring
   this goal met; see
   [§6.5](testing.md#65-performance--benchmarks).

### Future goals (non-goals for v1)

- Multi-writer / distributed SQL consensus (Raft, Paxos).
- Automated failure mode recovery & self-healing (emergency stale-replica promotion, dead-epoch roll-forward, replica auto-quarantine/re-seed, replicated client idempotency, and diverged-data reconciliation; see [automated-recovery.md](automated-recovery.md) and [roadmap.md](roadmap.md#post-v1--automated-failure-recovery--self-healing)).
- Cross-region active/active.
- Post-promotion catch-up from a peer replica (safety contract rule 13) and
  `allow_replica_reads`; both wait for Track N's atomic snapshot application.
- Drop-in compatibility with the `rsqlite-rsync` standalone sync CLI. hasqlite
  will provide its own commands and implementation. Note: running `hasqlite`
  *itself* as a standalone single-node database
  service in Docker is explicitly a goal and fast-tracked (see [§1.6](#1-goals-and-future-goals) and [§5](roadmap.md#5-development-phases) Phase 1b).
- Built-in TLS termination on the gateway (document mesh/proxy patterns instead).

### Success criteria (v1.0)

| Metric | Target |
|--------|--------|
| Split-brain write window | 0: no node commits after its renewal deadline, which precedes any candidate's TTL by the fencing margin ([§3](architecture.md#3-architecture) Lease timing; assumes no single commit stalls longer than the margin) |
| Steady-state sync interval | Configurable 1–5s default |
| Promotion block on stale replica | 100% when lag behind the activated writer's last renewal > `freshness.max_lag_secs`, or the ledger is not from that writer epoch |
| Failover under synthetic load | RTO p95 < 15s on 3-node k3s (node-local disk) with default lease timing (TTL 10s, renew deadline 6s, retry 2s) |
| Data after planned switchover | Page-hash match vs writer at switchover, provided the promoted replica durably holds the final snapshot before claiming a match; a timed-out final sync does not establish a lossless handover (see [§10.4](operations.md#104-replication-and-request-serving-limits), [§10.6](operations.md#106-unresolved-guarantees-and-recovery-policy)) |
| Data after unplanned failover | New writer holds a prefix of acknowledged writes; loss window ≤ `max_lag_secs` + `renew_deadline_secs`, assuming no commit stalls inside `COMMIT` beyond the fencing margin (see [§10.2](operations.md#102-data-safety-and-durability-limitations)) |
| Resource footprint | Workload-specific CPU, memory, network, and disk-I/O budgets are documented and pass the reproducible benchmark process in [§6.5](testing.md#65-performance--benchmarks). Numeric budgets are TBD until measured against declared baselines; no "very low resource consumption" claim is made before they are set and met. |

### Product posture and unresolved guarantees

This is intentionally a safety-first system. In a cluster with a stale replica,
missing coordination, or a partially applied sync, the default decision is to
refuse promotion and require operator intervention rather than silently merge or
retry in a way that could duplicate a request. That trade-off is deliberate and
is documented in [§10.2](operations.md#102-data-safety-and-durability-limitations)–[§10.6](operations.md#106-unresolved-guarantees-and-recovery-policy).

The plan therefore distinguishes between:

- hard correctness requirements that MUST be enforced in the implementation,
- target operational windows (failover, lag, and sync intervals), and
- unresolved guarantees that remain intentionally out of scope for v1.

The factors above are not a caveat to be hidden; they are the product contract.

---

## 11. Milestone checklist (summary)

| Milestone | Deliverable | Tests |
|-----------|-------------|-------|
| M0 | Workspace + PLAN; all planned crates registered as placeholders | CI green on empty crates (done) |
| M1 (weeks 2–4) | Controller parity | unit + proptest + simulation + coverage gate |
| M1a (week 3) | Wire API (`hasqlite-proto`) | adjacent-version compatibility |
| M1.5 (weeks 4–7, fast-track) | Standalone Single-Node Docker | Multi-stage Dockerfile, hardened standalone gateway, lock tests, probes |
| M2 (weeks 6–8) | Sync worker with adapter-era ledger protocol | integration convergence + crash-during-apply + lease-change-mid-pull |
| M3 (weeks 8–11) | HA cluster runtime | 3-node local HA + chaos suite |
| M4 (weeks 11–13; requires Track N TLS by week 8) | K8s deploy | manifest + k3d smoke + rolling update |
| M5 (weeks 13–14; requires Track N cutover) | 0.1.0 release | soak + security suite + resource benchmark budgets met |

The critical path is M1 → M1.5 → M3 → M4; Track N TLS is the one external gate
on M4 and is scheduled three weeks ahead of it.

---

## 12. References

- [rsqlite-rsync README](https://github.com/sria91/rsqlite-rsync/blob/master/README.md)
- [rsqlite-rsync HA guide](https://github.com/sria91/rsqlite-rsync/blob/master/docs/ha-guide.md)
- [k3s HA runbook](https://github.com/sria91/rsqlite-rsync/blob/master/docs/k3s-ha-runbook.md)
- [Protocol](https://github.com/sria91/rsqlite-rsync/blob/master/docs/protocol.md)
- [plan-k3sHaSqliteSync.prompt.md](https://github.com/sria91/rsqlite-rsync/blob/master/.github/prompts/plan-k3sHaSqliteSync.prompt.md)
