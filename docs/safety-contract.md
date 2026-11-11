# hasqlite Safety Contract (draft)

This document is normative for hasqlite implementation and tests. The
invariants apply independently of any replication implementation or wire
protocol.

## Ownership

1. **One node accepts commits** per cluster at any instant, across all
   generations, under the timing assumption in rule 5.
2. **Lease is authoritative** for writer identity: `holder_node_id`, `generation`,
   `renewed_at_secs`, `ttl_secs`, an opaque `version` that changes on every
   lease write, and the last **activated writer epoch** (`writer`: node ID,
   generation, last renewal). Acquiring the lease does not change `writer`; only
   activation does (rule 10).
3. **Generation increases** on every successful acquisition (never reused).
   Renewals and releases never change it.

## Lease timing and write fencing

4. **Candidates never compare wall clocks for lease decisions.** A candidate treats the lease as
   expired only when it is released (no holder) or its `version` has stayed
   unchanged for `lease.ttl_secs` measured on the candidate's local monotonic
   clock. `renewed_at_secs` is informational. In rule 10, candidates make two
   time-based comparisons that are not wall-clock vs. lease comparisons: (a) the
   freshness check compares the writer-stamped `writer.renewed_at_secs` with the
   writer-stamped `synced_at_secs` (both from the writer's clock) to enforce the
   lag limit, requiring a rollback-safe time basis on the writer (renewal and
   snapshot timestamps must be non-decreasing, so backward steps in the writer
   clock cannot cause saturating subtraction to make stale snapshots appear fresh
   or bypass the lag limit); and (b) the future-skew check compares the
   writer-stamped snapshot timestamp with the candidate's local wall time, which
   can deny promotion but never authorizes it. Candidates do not compare the
   lease's `renewed_at_secs` with their own wall clock.
5. **Writers fence themselves.** A writer accepts a commit only before
   `send time of its last successful renewal + lease.renew_deadline_secs`, on its
   local monotonic clock. `enforce_write_fence` runs inside the write transaction
   immediately before `COMMIT`. At the deadline the writer interrupts in-flight
   statements (`sqlite3_interrupt`), sets `/ready` to 503 and demotes.
   `lease.ttl_secs − lease.renew_deadline_secs` (at least 2 seconds) is the
   fencing margin. Each node writes its own disk and no storage layer rejects
   stale generations, so a writer stalled *inside* a commit for longer than the
   margin can still commit after a peer promotes. This is an explicit
   assumption, not a guarantee.
6. **Every lease write is a compare-and-swap on `version`:**
   - *Acquire* succeeds only if the version is unchanged since the candidate's
     observation. The store sets `holder = self` and
     `generation = observed + 1`; callers never supply the generation.
   - *Renew*, *activate* and *release* succeed only if the version is unchanged
     since the writer's last successful lease write, `holder == self`, and the
     generation is unchanged.
   - A conflict on renew, activate or release means immediate demotion. A
     writer never overwrites a newer holder.

## Replication

7. **Writer is sync origin**; replicas are **exclusive** targets (no concurrent
   writers to replica files). The sync engine never writes to a node whose role
   is `Writer`.
8. **Origin may be live** (reads/writes); sync produces a consistent snapshot as
   of sync start. The origin stamps each snapshot with its node ID, its lease
   generation and the snapshot start time (origin clock). It refuses to serve
   unless it is the current lease holder and activated writer. Replicas reject a
   snapshot whose stamped identity does not match the lease's `writer` epoch.
9. **Freshness ledger** records the last successful sync: `source_node_id`,
   `source_generation`, `synced_at_secs`, `destination_node_id`.
   - **Replica binding**: `destination_node_id` is mandatory and must equal the
     promoting node.
   - **Snapshot start timestamp**: `synced_at_secs` is the origin-stamped
     snapshot start time, not the sync completion time.
   - **Crash-safe publication**: the ledger lives on the same filesystem as the
     database. Before writing the first page, the replica invalidates the ledger
     and fsyncs the invalidation. It publishes a new record only after the sync
     succeeds and the database file and its directory are fsynced. Publication
     uses temp file + rename + directory fsync. An interrupted sync therefore
     always leaves the node unpromotable.

## Promotion

10. A node may **promote to writer** only if:
    - `validate_promotion` passes against the lease it observed. The ledger is
      bound to this replica. Its source node and generation equal the lease's
      `writer` epoch. Its lag behind that writer's last renewal
      (`writer.renewed_at_secs − synced_at_secs`) is at most
      `freshness.max_lag_secs`; both timestamps come from the writer's clock.
      `synced_at_secs` is at most `max_future_skew_secs` ahead of local time.
      *(Post-v1 exceptions)*:
      - **Emergency promotion**: When automated emergency promotion is configured,
        a candidate may promote with lag exceeding `freshness.max_lag_secs` only
        after `recovery.emergency_promotion_timeout_secs` has elapsed on its clock,
        provided the candidate proves freshness among a participating quorum, its
        potential loss window `(writer.renewed_at_secs + lease.renew_deadline_secs) − synced_at_secs`
        does not exceed `recovery.max_emergency_lag_secs`, and the acquisition records
        the emergency annotation and audit log entry.
      - **Dead-epoch roll-forward**: When `recovery.auto_dead_epoch_recovery` is
        configured and surviving replicas certify via quorum attestation that the
        activated writer generation $g_{\text{dead}}$ has been abandoned for
        `recovery.dead_epoch_timeout_secs`, candidates whose ledgers match the prior
        activated epoch $g_{\text{prior}}$ may campaign to acquire the next generation
        derived from the observed lease version ($g_{\text{new}} = g_{\text{observed}} + 1$).
        Attestations do not prove that an unreachable partition lacks a completed snapshot
        at generation $g_{\text{dead}}$; the transition explicitly gates this potential
        loss under the configured active duration limit (`recovery.max_dead_epoch_duration_secs`)
        and audit requirements before clearing the abandoned writer epoch.
    - It then wins the acquisition CAS (rule 6). Scheduled sync is paused while
      it campaigns, so the validated inputs cannot change and no second
      freshness check is needed.
    - Before opening a writable handle, it *activates*: a CAS that records
      `writer = (self, generation, now)`. If activation fails, or the renewal
      deadline has passed, it stays a replica.
    - A lost acquisition or activation CAS denies promotion (fail closed). An
      acquired but never-activated generation leaves the previous `writer` epoch
      in place, so peers synced from that writer remain promotable.
11. **First-writer bootstrap**: for clusters with no activated writer, an
    explicit genesis procedure or verified seed-snapshot procedure establishes
    the initial generation and `writer` epoch. Normal failover always applies
    the full rule 10.
12. **Failed promotion** must not open writable SQL paths (fail closed).
13. **Post-promotion catch-up** may only run before the writable handle opens,
    and only from a peer whose ledger has the same `writer` epoch and a newer
    `synced_at_secs`.

## Demotion

14. On a renewal conflict, missed renewal deadline or lease loss while writer:
    **immediate demotion**. The node stops accepting writes, interrupts in-flight
    statements, and the gateway returns `NOT_LEADER` with a redirect hint.
15. A former writer **rejoins as replica** and must resync before it is eligible
    to promote again. Before its first resync it preserves its database as
    `*.diverged-g<generation>`, because commits it accepted after the last
    replica sync are not on the new writer.
16. **Planned switchover**: a stopping writer stops accepting writes, waits a
    bounded time for replicas to complete a snapshot started after that point,
    then *releases* the lease (holder cleared, generation unchanged, `writer`
    renewal time updated) so a replica can acquire without waiting a TTL.

## Fencing and access control

17. `enforce_write_fence` gates all mutating SQL on the writer path. Statements
    are classified with `sqlite3_stmt_readonly()` after preparation, never by
    inspecting SQL text. Replica connections are opened read-only with
    `PRAGMA query_only = 1`. An authorizer denies `ATTACH`, `DETACH`,
    `VACUUM INTO` and non-allowlisted pragmas. `SQLITE_DBCONFIG_DEFENSIVE` is
    on, extension loading is off, and `trusted_schema` is off.
18. **Authentication** (bearer token) is independent of fencing; both are
    required in production configurations. SQL gateway and replication use
    distinct credentials, compared in constant time. Unauthenticated gateways
    are permitted only in standalone mode on a loopback bind.

## Kubernetes

19. **Rollout probes**: use `/healthz` for pod readiness during rolling updates.
    Write traffic is routed only to the current writer: the node labels its own
    pod `hasqlite.io/role=writer` on activation and removes the label before
    demotion completes, and the writer Service selects that label. `/healthz`
    readiness alone does not provide writer-only routing. `/live` never touches
    SQLite or the lease store.
20. **preStop**: replicas attempt a final sync; the writer performs the planned
    switchover in rule 16 before SIGTERM.

Violations of this contract are **severity-0** bugs.
