# hasqlite Safety Contract (draft)

This document is normative for hasqlite implementation and tests. The
invariants apply independently of any replication implementation or wire
protocol.

## Ownership

1. **One node accepts commits** per cluster at any instant, across all
   generations, under the timing assumption in rule 5.
2. **Lease is authoritative** for writer identity: `holder_node_id`, `generation`,
   `renewed_at_secs`, `ttl_secs` (the holder's configured TTL), a hasqlite-owned
   `version` token that changes on every lease write *made by hasqlite* (acquire,
   activate, renew, release), and the last **activated writer epoch** (`writer`:
   node ID, generation, last renewal). The backend's native revision (Kubernetes
   `resourceVersion`, file record counter) is the compare-and-swap precondition and
   is bumped atomically with `version`, but it is never used as the liveness
   signal: an unrelated actor annotating or labelling the Lease object changes the
   native revision without changing `version`. Acquiring the lease does not change
   `writer`; only activation does (rule 10).
3. **Generation increases** on every successful acquisition (never reused).
   Renewals and releases never change it.

## Lease timing and write fencing

4. **Candidates never compare wall clocks for lease decisions.** A candidate treats the lease as
   expired only when it is released (no holder) or its `version` has stayed
   unchanged for the **effective TTL**, `max(local lease.ttl_secs, observed
   ttl_secs)`, measured on the candidate's local monotonic clock
   (`LeaseObservation::is_expired_for`). Honouring the holder's recorded TTL keeps
   rule 5 valid during a rolling change of lease timing: the holder validated
   `ttl_secs ≥ renew_deadline_secs + margin` against *its own* deadline, so a
   candidate configured with a shorter TTL must not expire it earlier.
   `renewed_at_secs` is informational. In rule 10, candidates make two
   time-based comparisons that are not wall-clock vs. lease comparisons: (a) the
   freshness check compares the writer-stamped `writer.renewed_at_secs` with the
   writer-stamped `synced_at_secs` (both from the writer's clock) to enforce the
   lag limit, requiring a rollback-safe time basis on the writer (renewal and
   snapshot timestamps must be non-decreasing, so backward steps in the writer
   clock cannot cause saturating subtraction to make stale snapshots appear fresh
   or bypass the lag limit; the writer issues every stamp through `WriterClock`,
   which returns `max(wall clock, last stamp)` and is seeded from the writer
   epoch's last renewal on activation); and (b) the future-skew check compares the
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
   - **Exclusive application**: at most one sync writes a given replica file at a
     time (`SyncError::SyncInProgress` otherwise).
   - **Adapter-era attribution (until Track N)**: the transitional backend stamps
     neither origin identity nor snapshot time, and its replication server does
     not refuse to serve when its node is not the activated writer. Until the
     native backend provides origin stamping, the replica MUST (1) read the lease
     before the pull and stamp the ledger's `source_node_id`, `source_generation`
     and `synced_at_secs` from that lease's `writer` epoch (`synced_at_secs :=
     writer.renewed_at_secs` observed before the pull, a lower bound on the
     snapshot start on the writer's clock, so lag is overstated, never
     understated); (2) read the lease again after the database fsync and publish
     the ledger only if `version` is unchanged and `holder_node_id ==
     writer.node_id`; and (3) serve pulls only while it is itself the activated
     writer (a role-gating interceptor in front of the adapter's replication
     server). A lease change observed in (2) leaves the ledger invalidated.

## Promotion

10. A node may **promote to writer** only if:
    - `validate_promotion` passes against the lease it observed. The ledger is
      bound to this replica. Its source node and generation equal the lease's
      `writer` epoch. Its lag behind that writer's last renewal
      (`writer.renewed_at_secs − synced_at_secs`) is at most
      `freshness.max_lag_secs`; both timestamps come from the writer's clock.
      `synced_at_secs` is at most `max_future_skew_secs` ahead of local time.
      Checks are evaluated in a fixed order (ledger shape, writer epoch, time
      bounds) and the first failure is reported; the order is part of the
      `hasqlite_promotion_denied_total` metric contract. The post-v1 emergency
      promotion and dead-epoch roll-forward exceptions are specified in
      [automated-recovery.md](automated-recovery.md#3-tier-2-cluster-consensus--degraded-failover)
      and are not part of v1.
    - It then wins the acquisition CAS (rule 6). Scheduled sync is paused while
      it campaigns, so the validated inputs cannot change and no second
      freshness check is needed.
    - Before opening a writable handle, it *activates*: a CAS that records
      `writer = (self, generation, now)`. If activation fails, or the renewal
      deadline has passed, it stays a replica. After a successful activation and
      still before the writable handle opens, it durably writes the divergence
      marker for that generation (rule 15). If the marker write fails, it does
      not open the handle and demotes.
    - A lost acquisition or activation CAS denies promotion (fail closed). An
      acquired but never-activated generation leaves the previous `writer` epoch
      in place, so peers synced from that writer remain promotable.
11. **First-writer bootstrap**: for clusters with no activated writer, an
    explicit genesis procedure or verified seed-snapshot procedure establishes
    the initial generation and `writer` epoch. Normal failover always applies
    the full rule 10.
12. **Failed promotion** must not open writable SQL paths (fail closed).
13. **Post-promotion catch-up is not part of v1.** Under rules 7–8 a replica may
    pull only from the activated writer, which is unavailable at the moment of
    promotion, so the promoted node opens its writable handle on the snapshot it
    validated in rule 10. A peer-to-peer catch-up (from a peer whose ledger has the
    same `writer` epoch and a newer `synced_at_secs`, run strictly before the
    writable handle opens) is reserved for post-v1 once a safe peer-transfer
    protocol exists ([automated-recovery.md](automated-recovery.md)).

## Demotion

14. On a renewal conflict, missed renewal deadline or lease loss while writer:
    **immediate demotion**. The node stops accepting writes, interrupts in-flight
    statements, and the gateway returns `NOT_LEADER` with a redirect hint.
15. A former writer **rejoins as replica** and must resync before it is eligible
    to promote again. Before its first resync it preserves its database as a
    *diverged copy*, because commits it accepted after the last replica sync are
    not on the new writer. Those commits come from normal replication lag, or
    from a commit that stalled past the fencing margin and finished after a peer
    promoted (rule 5). Preservation is fail-closed:
    - **Trigger.** The trigger is a durable divergence marker,
      `${data.dir}/.hasqlite.divergence`, holding the generation. It is written
      at activation (rule 10) and removed only after the diverged copy is
      durable. The informational role file is never the trigger, because a crash
      or partition usually ends a writer before it can update that file. On
      startup in clustered mode, preservation is also pending in two other cases:
      the lease's `writer` epoch names this node while another node holds the
      lease, or the marker is unreadable while database files exist. A pending
      preservation blocks every sync pull, replica read and promotion attempt
      until it completes. Standalone mode writes no marker, because it has no
      peers to diverge from.
    - **Contents.** The copy must contain every committed transaction, including
      frames that exist only in `-wal`. It is taken with the SQLite online backup
      API (`sqlite3_backup_*`) from a local read connection with no writable
      handles open. The source files are never renamed or copied file by file,
      because a main file without its WAL silently drops the commits being
      preserved.
    - **Durable, idempotent write.** The copy is written to
      `${data.dir}/diverged/<db>.diverged-g<generation>.tmp`, fsynced, checked with
      `PRAGMA integrity_check`, renamed to `<db>.diverged-g<generation>`, and its
      directory is fsynced. Only then is an audit entry appended and the marker
      removed (also followed by a directory fsync). A crash at any step leaves the
      marker in place, and the next start redoes the procedure. A finished copy
      that already exists is verified and reused, not overwritten. If the
      generation cannot be determined (an unreadable marker), the copy is named
      `<db>.diverged-unknown-<unix secs>`.
    - **Insufficient space or I/O failure.** Free space is checked before the
      copy (database plus WAL plus a margin). If space is short, or the copy
      fails, the node does not resync, cannot promote, and raises an alert. The
      only way past this without preserving is an audited operator command
      (`hasqlite diverged discard --generation <g>`) that explicitly accepts the
      loss.
    - **Isolation.** `${data.dir}/diverged/` is excluded from pre-flight database
      discovery, gateway database-name resolution (rule 17) and replication
      serving. A diverged copy is never opened for writes, served or promoted.
    - **Retention.** v1 never deletes diverged copies automatically. They remain
      until an operator reconciles and discards them (diverged data recovery
      runbook). Their count and size are exported as metrics.
    - **Scope.** The diverged copy exists only on the former writer's volume. If
      that volume is lost, its unreplicated commits are lost. Replicas whose
      snapshots are newer than the promoted one are overwritten on their next
      resync and are not preserved in v1.
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
