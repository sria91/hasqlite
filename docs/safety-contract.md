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
   activate, renew, release), the lease `format` (below), and the last
   **activated writer epoch** (`writer`: node ID, generation, last renewal,
   the clock fields `clock_deficit_secs` and `clock_rebased_at_secs`, the
   *database set* `databases`, `promoted_from` and, after a planned
   switchover, `stopped_at_secs`; rules 4, 9, 10 and 16).
   **Lease format.** `format` is an integer that is raised whenever a lease
   field is added that changes how candidates must evaluate promotion. A node
   that reads a lease whose `format` is newer than it supports neither
   acquires nor promotes against it (rule 10), so a candidate can never
   ignore a field it does not understand and promote on an incomplete check.
   Rollouts update replicas before the writer, so a newer format is written
   only once no older candidate remains. A rollback across a format change
   leaves the older nodes unpromotable until they are upgraded again. Format
   1, the first format any campaigning release reads, already includes the
   clock fields of rule 4. The backend's native
   revision (Kubernetes `resourceVersion`, file record counter) is the
   compare-and-swap precondition and
   is bumped atomically with `version`, but it is never used as the liveness
   signal: an unrelated actor annotating or labelling the Lease object changes the
   native revision without changing `version`. Acquiring the lease does not change
   `writer`. Only activation (rule 10) and a database-set change by the
   activated writer (rule 9) do.
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
   epoch's last renewal on activation, and candidates add the recorded clock
   deficit, below); and (b) the future-skew check compares the
   writer-stamped snapshot timestamp with the candidate's local wall time, which
   can deny promotion but never authorizes it. Candidates do not compare the
   lease's `renewed_at_secs` with their own wall clock.
   **Clock deficit.** Non-decreasing stamps alone do not bound lag. While
   `WriterClock` holds at its floor, or while the wall clock runs slow, real
   time passes that the stamps do not show, so the difference between two
   stamps can understate how stale a snapshot is. `WriterClock` therefore
   also keeps a *reference*. On activation the reference is set to the first
   stamp. It then advances with the writer's local monotonic clock and moves
   forward to the wall clock whenever the wall clock reads later. It is never
   behind any stamp. The *deficit* is the reference minus the current stamp.
   Because the deficit is cumulative, it catches one large backward step, a
   run of small ones and a slow-running clock alike.
   - *Recording*: every lease write the writer makes records
     `writer.clock_deficit_secs`. The value is the deficit rounded up to
     whole seconds, or 0 when the deficit is 1 second or less. Candidates
     measure lag as `writer.renewed_at_secs + writer.clock_deficit_secs −
     synced_at_secs` (rule 10). The deficit is written in the same CAS as
     the renewal stamp it corrects, so a held renewal stamp is never visible
     without its correction. A snapshot stamp issued during a deficit only
     makes that snapshot look older. A writer that crashes during a deficit
     therefore leaves a lease that candidates evaluate conservatively, with
     no marker left for anyone to clear. A replica whose corrected lag is
     within `freshness.max_lag_secs` stays promotable. Otherwise the cluster
     needs the operator recovery of ADR-0019, as for any writer that dies
     ahead of its replicas.
   - *Rebase*: after a backward step the wall clock runs at its normal rate
     again, so the deficit never shrinks by itself. Once the wall clock reads
     later than the last stamp, so that stamps advance with it again, the
     writer's next lease write *rebases*. It records that write's stamp as
     `writer.clock_rebased_at_secs` and a deficit of 0. After the CAS
     succeeds, the writer resets the reference to that stamp, as of the
     instant the stamp was taken. Candidates deny promotion from a snapshot
     stamped at or before `writer.clock_rebased_at_secs`, because its gap to
     later renewals is no longer corrected. A rebase is the only way the
     writer clears a deficit of more than 1 second. Until the rebase, the
     correction keeps snapshots taken before the step promotable for as long
     as their corrected lag allows.
   - *Bound*: between rebases, the corrected lag is never less than the
     writer's monotonic time from a snapshot to a later renewal, minus 1
     second. A deficit of 1 second or less is not recorded and can understate
     lag by at most that much. Forward jumps only overstate lag. A wall clock
     that keeps running slow causes a rebase each time its deficit passes 1
     second. Each rebase makes replicas unpromotable until they next sync.
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
   is `Writer`. One writer originates every database in the cluster; there is
   no per-database leadership. The writer serves reads and writes on
   different databases concurrently, and serves concurrent replication
   sessions for them: each database has its own connections and SQLite write
   lock, so work on one never waits for another. `enforce_write_fence` (rule
   5) runs before every commit on every database, and demotion interrupts
   in-flight statements on all of them.
8. **Origin may be live** (reads/writes); sync produces a consistent snapshot as
   of sync start. The origin stamps each snapshot with its node ID, its lease
   generation, the database name and the snapshot start time (origin clock).
   It refuses to serve unless it is the current lease holder and activated
   writer, and refuses a database that is not in its current
   `writer.databases`. Replicas reject a snapshot whose stamped identity does
   not match the lease's `writer` epoch or whose database name differs from
   the one requested.
9. **Freshness ledger** records, for each database, the last successful sync:
   `database`, `incarnation`, `source_node_id`, `source_generation`,
   `synced_at_secs`, `destination_node_id`.
   - **Database set**: the cluster replicates exactly the databases in the
     lease's `writer.databases`, a list of 1–64 entries `(name, incarnation)`,
     sorted by name with no duplicate names. Names are relative to `data.dir`.
     Each `/`-separated component is non-empty, uses only ASCII letters,
     digits, `.`, `_` or `-`, and does not start with `.`. The whole name ends
     in `.db`, `.sqlite` or `.sqlite3`, its first component is not `diverged`
     or `ancestors`, and it is at most 200 bytes when each `/` is counted as
     three bytes (its `%2F` encoding; `%` cannot occur in a name). That bound
     keeps every generated name within the 255-byte file-name limit: the
     encoded ledger record name and its `.tmp` file, and the diverged copy
     `<db>.diverged-<id>` and its `.tmp` file, where `.diverged-`, the longest
     id (`standalone-g<u64>`, 32 bytes) and `.tmp` add at most 46 bytes. The *incarnation* is a number the writer takes
     from the lease counter `writer.next_incarnation` each time a name is
     added, so a name that is removed and added again never reuses an
     incarnation. Node configuration never defines the set. Genesis or the
     seed-snapshot procedure records the initial set (rule 11), and activation
     copies the set, the counter and the tombstones (below) from the observed
     lease unchanged (rule 10). Afterwards only the activated writer changes
     them, with a renew CAS (rule 6) that also writes the new values; a
     conflict means demotion, as for any renew. Both changes are audited with
     the operator's identity and reason:
     - *Add*: the writer validates the name, refuses it if the set already
       holds 64 names or a file exists at that path (or its `-wal`/`-shm`),
       creates the database in WAL mode, and fsyncs it and its parent
       directories. Then it performs the renew CAS, which adds
       `(name, next_incarnation)`, increments the counter and drops any
       tombstone for the name. It serves the database only after the CAS
       succeeds. If the CAS fails, the writer demotes and deletes the empty
       file it created, which nothing has opened. If it crashes before that,
       the file is not in the set, is never served and is reported as
       unmanaged (below).
     - *Remove*: deletes the database's files on every node. The writer
       rejects new requests for the database, interrupts and drains its
       in-flight transactions, and performs the renew CAS, which moves the
       entry from `writer.databases` to the *tombstones* `writer.removed`.
       Then it closes its handles and deletes the files (*file deletion*,
       below). It never removes the last database, and refuses to run during
       a planned switchover. The command requires `--confirm-data-loss`: the
       deletion is permanent and is not preserved anywhere, except as rule
       15's diverged copy on a former writer that had not yet resynced.
     - *Tombstones*: `writer.removed` holds up to 64 `(name, incarnation)`
       entries. When it is full, the oldest is dropped. A node deletes local
       files only for a database named in the tombstones, for a database
       whose files belong to an older incarnation of a name in the set
       (*incarnation check*, below), or for a *dropped removal*: a name in
       neither list for which the node holds a ledger record. A record shows
       that the node replicated that name, and a name leaves the set only by
       removal, so its files belong to a removed incarnation whose tombstone
       was dropped while the node was away (more than 64 removals). The node
       deletes them when it observes such a lease, so the stale files cannot
       later block *Add* of that name. It never deletes files in neither list
       for which it holds no ledger record. Such files are not served or
       replicated, are reported with `hasqlite_unmanaged_database_files`, and
       must be removed by hand.
     - *File deletion*: with no open handle on the database and no pull in
       progress for it, the node unlinks the `-wal` file, then the `-shm`
       file, then the database file, treating a missing file as already
       deleted. It then deletes the database's ledger record and fsyncs the
       directories. A crash leaves the tombstone in place, so each node
       finishes the deletion when it next observes the lease. A leftover
       `-wal` is never paired with a new database file, because *Add*
       refuses a name while any of its three files exists. While a
       divergence preservation is pending (rule 15) the node deletes nothing,
       so a former writer's commits to a database removed by its successor
       are preserved first. Deletion of a name is serialized with pulls of
       that name and, on the writer, with *Add* of that name, under one
       per-name lock. Holding the lock, the node re-reads the lease
       immediately before deleting anything, and proceeds only if the
       current lease still tombstones the name and does not list it in the
       set; or the lease lists the name in neither the set nor the
       tombstones and the node holds a ledger record for it (a dropped
       removal); or, on a replica, the set entry's incarnation differs from
       that of the local files (taken from the local ledger record; files
       without a record count as an older incarnation). If the set lists the
       local files' incarnation, it skips the deletion. *Add* holds the lock
       from its file check until its CAS completes, so a dropped-removal
       deletion never removes a file that *Add* has just created. The ledger
       record is deleted last, so a crash during a dropped-removal deletion
       leaves the record in place and the node finishes the deletion when it
       next observes the lease.
     - *Incarnation check*: before the first pull of a database whose local
       record is missing or carries a different incarnation than the set
       entry, the replica deletes any local files of that name as above. A
       replica therefore never applies a new incarnation's pages on top of an
       old incarnation's file.
   - **One record per database**: records live in the directory
     `freshness.dir`, one file per database, named with the encoding of rule
     15 (`%` → `%25`, `/` → `%2F`). The rules below apply to each record
     independently. An invalidated record keeps its database name and
     incarnation. A record for a database outside the set is ignored and is
     deleted with the database's files.
   - **Replica binding**: `destination_node_id` is mandatory and must equal the
     promoting node.
   - **Snapshot start timestamp**: `synced_at_secs` is the origin-stamped
     snapshot start time, not the sync completion time.
   - **Crash-safe publication**: the ledger lives on the same filesystem as the
     databases. Before writing the first page of a database, the replica
     invalidates that database's record and fsyncs the invalidation. It
     publishes a new record only after the sync
     succeeds and the database file and its directory are fsynced. Publication
     uses temp file + rename + directory fsync. An interrupted sync therefore
     always leaves the node unpromotable.
   - **Exclusive application**: at most one sync writes a given replica file at a
     time (`SyncError::SyncInProgress` otherwise). Pulls of *different*
     databases run concurrently: a sync round starts one pull per database in
     the observed set, at most `sync.max_concurrent_pulls` at a time. Each pull
     invalidates, stamps and publishes only its own record, so pulls neither
     wait for nor affect each other. A pull is skipped if its database is not
     in the set that its own pre-pull lease observation shows, or carries a
     different incarnation there.
   - **Adapter-era attribution (until Track N)**: the transitional backend stamps
     neither origin identity nor snapshot time, and its replication server does
     not refuse to serve when its node is not the activated writer. Until the
     native backend provides origin stamping, the replica MUST (1) read the lease
     before the pull and stamp the ledger's `source_node_id`, `source_generation`
     and `synced_at_secs` from that lease's `writer` epoch (`synced_at_secs :=
     writer.renewed_at_secs` observed before the pull, a lower bound on the
     snapshot start on the writer's clock, so lag is overstated, never
     understated); (2) read the lease again after the database fsync and publish
     the ledger only if `holder_node_id` and `generation` are unchanged since
     the pre-pull observation, `holder_node_id == writer.node_id`, the
     `writer` epoch (node and generation) is unchanged, and the database's set
     entry (name and incarnation) is unchanged; and (3) serve pulls only while
     it is itself the activated writer (a role-gating interceptor in front of
     the adapter's replication server). `version` is not compared in (2): the
     writer renews every retry period, so it changes during most pulls, and a
     renewal by the same writer does not change who served the snapshot. A
     change of writer observed in (2) leaves the ledger invalidated.
     For a planned switchover (rule 16), the pre-pull renewal may predate the
     stop stamp, so it cannot show a pull to be post-stop. Instead, right after
     issuing the stop stamp, the stopping writer performs a renew CAS stamped
     through `WriterClock` strictly after the stop stamp. A replica whose
     pre-pull observation in (1) already contains that renewal starts its pull
     after writes stopped, so its `synced_at_secs := writer.renewed_at_secs`
     is later than the stop stamp and qualifies for the lossless check. A pull
     whose observation predates that renewal carries an earlier stamp and does
     not qualify. Checks (1)–(3) apply unchanged. Each database's pull is
     bracketed by its own observations (1) and (2) and stamped from its own
     pre-pull observation, including when pulls run concurrently.

## Promotion

10. A node may **promote to writer** only if:
    - `validate_promotion` passes against the lease it observed, for **every**
      database in that lease's `writer.databases`. Each database has a valid
      ledger record whose incarnation equals the set entry's and that is
      bound to this replica. Its source node and generation
      equal the lease's `writer` epoch. Its lag behind that writer's last
      renewal, corrected for the clock deficit (`writer.renewed_at_secs +
      writer.clock_deficit_secs − synced_at_secs`, saturating, rule 4), is at
      most `freshness.max_lag_secs`. All three values come from the writer's
      clock. Its `synced_at_secs` is at most `max_future_skew_secs` ahead of
      local time. Its `synced_at_secs` is later than
      `writer.clock_rebased_at_secs` when that is set (rule 4). The observed
      lease's `format` is one this node supports (rule 2). A
      missing record, or one from another incarnation, for any database in
      the set denies promotion.
      The lease format is checked first. The other checks are evaluated in
      a fixed order of classes (ledger shape, writer epoch, time bounds);
      within a class, databases are checked in name order. The first failure
      is reported, naming its database. The class order is part of the
      `hasqlite_promotion_denied_total` metric contract. The freshest-first
      delay ([architecture §3](architecture.md#3-architecture)) uses the
      largest corrected lag over the set. The post-v1 emergency
      promotion and dead-epoch roll-forward exceptions are specified in
      [automated-recovery.md](automated-recovery.md#3-tier-2-cluster-consensus--degraded-failover)
      and are not part of v1.
    - It then wins the acquisition CAS (rule 6). Scheduled sync is paused while
      it campaigns, so the validated inputs cannot change and no second
      freshness check is needed.
    - Before opening a writable handle, it *activates*: a CAS that records
      `writer = (self, generation, now, databases)`, where `databases` is the
      validated set, copied unchanged from the observed lease together with
      `next_incarnation` and the tombstones. If activation
      fails, or the renewal deadline has passed, it stays a replica. The
      activation CAS also records `writer.promoted_from = (source generation,
      synced_at_secs, source stopped_at_secs)`: the first two come from the
      validated ledger, with `synced_at_secs` the *oldest* snapshot stamp over
      the set, and the third is copied from the observed lease's
      `writer.stopped_at_secs` (absent
      if the source epoch was not released by a planned switchover). Activation
      replaces the source epoch, so this copy is the only durable record of the
      stop stamp that rule 15's lossless handover check compares against.
      After a successful activation and still before the writable handle opens,
      it durably writes the divergence marker with id `g<generation>` (rule 15).
      If the marker write fails, it does not open the handle and demotes.
    - A lost acquisition or activation CAS denies promotion (fail closed). An
      acquired but never-activated generation leaves the previous `writer` epoch
      in place, so peers synced from that writer remain promotable.
11. **First-writer bootstrap**: for clusters with no activated writer, an
    explicit genesis procedure or verified seed-snapshot procedure establishes
    the initial generation and `writer` epoch, including its database set
    (rule 9), with incarnations starting at 1 and no tombstones. A seed from
    a standalone volume records the databases that
    pre-flight discovery finds there, and fails if any name is invalid or
    there are more than 64. Normal failover always applies the full rule 10.
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
    **immediate demotion**. The node stops accepting writes on every database,
    interrupts in-flight statements, and the gateway returns `NOT_LEADER` with
    a redirect hint.
15. A former writer **rejoins as replica** and must resync before it is eligible
    to promote again. Before its first resync it preserves its databases as
    *diverged copies*, because commits it accepted after the last replica sync are
    not on the new writer. Those commits come from normal replication lag, or
    from a commit that stalled past the fencing margin and finished after a peer
    promoted (rule 5). Preservation is fail-closed. This rule is the normative
    description of the procedure; other documents only summarize it.
    - **Trigger.** The trigger is a durable divergence marker,
      `${data.dir}/.hasqlite.divergence`, holding a *divergence id*. Activation
      writes it with id `g<generation>` (rule 10). There is one marker per node,
      not per database. It is removed only after every diverged copy is
      durable, after a verified lossless handover (rule 16), or
      by an operator discard. The informational role file is never the trigger,
      because a crash or partition usually ends a writer before it can update
      that file. On startup in clustered mode, preservation is also pending in
      three other cases:
      - the lease's `writer` epoch names this node while another node holds the
        lease (id `g<writer generation>`);
      - the marker is unreadable while database files exist (see *Unknown
        generation*);
      - the volume holds database files and a standalone lease record but has
        never run in clustered mode, so a standalone volume joining a cluster is
        preserved before its first resync (id `standalone-g<standalone
        generation>`).
      A pending preservation blocks every sync pull, replica read and promotion
      attempt until it completes. Standalone mode writes no marker, because it
      has no peers to diverge from; the third case covers its later conversion.
    - **Lossless handover check.** Before copying, the node reads the lease. If
      an activated writer's `promoted_from` names this node's generation `g`,
      carries `g`'s stop stamp, and has a snapshot stamp later than that stop
      stamp (rules 10 and 16), the successor holds every commit of `g`. The node then appends an audit entry
      and removes the marker without making a copy. If the lease still shows
      `g` released with `stopped_at_secs` and no successor has activated yet,
      the node stays pending and checks again on each tick, since it has no
      writer to sync from anyway. In every other case, including a lease that
      has since moved past the successor, it preserves.
    - **Unknown generation.** If the marker is unreadable, the node hard-links it
      (`linkat`) to `${data.dir}/.hasqlite.divergence.unreadable-<unix secs>`
      for forensics and fsyncs the directory, leaving the unreadable marker in
      place as the trigger. It then durably writes a replacement marker with id
      `unknown-<unix secs>`, taken at that attempt, to a temp file, fsyncs it,
      renames it over the unreadable marker and fsyncs the directory. A marker
      exists at every step, so a crash at any point leaves preservation pending;
      a crash before the rename simply repeats this step with a new id. Later
      restarts read the replacement and reuse its id, so the procedure stays
      idempotent. Failure to write the replacement is an I/O failure (below).
    - **Scope of the copy set.** The node preserves every database that
      pre-flight discovery finds on its volume, whether or not it is in the
      current `writer.databases`. This includes databases the cluster has since
      removed (tombstoned), which rule 9 deletes only after preservation
      completes. All copies of one preservation share its divergence id.
    - **Contents.** Each copy must contain every committed transaction, including
      frames that exist only in `-wal`. It is taken with the SQLite online backup
      API (`sqlite3_backup_*`) from a local read connection with no writable
      handles open. The source files are never renamed or copied file by file,
      because a main file without its WAL silently drops the commits being
      preserved.
    - **Naming.** The copy is `${data.dir}/diverged/<db>.diverged-<id>`. `<db>` is
      the database path relative to `data.dir`, with `%` encoded as `%25` and `/`
      as `%2F`, so names are flat and cannot collide.
    - **Durable, idempotent write.** Each copy is written to `<name>.tmp`,
      fsynced, checked with `PRAGMA integrity_check`, renamed to `<name>`, and
      the directory is fsynced. Copies are taken one at a time, in name order.
      Only when every copy is durable is an audit entry appended (listing each
      copy) and the marker removed, followed by a directory fsync. A crash at
      any step leaves the marker in place, and the next start redoes the
      procedure: a leftover `.tmp` is deleted and retaken, and a finished copy
      with the same id is verified with `integrity_check` and reused, not
      overwritten.
    - **Insufficient space or I/O failure.** Before copying, the node requires
      free space of at least `S + max(S / 10, 64 MiB)`, where `S` is the sum,
      over every database that still needs a copy (no verified finished copy
      with this id), of the size of its database file plus the size of its
      `-wal` file, all measured at the check. The check runs once, before the
      first copy. The 64 MiB is a floor on the headroom, not on the total, so a
      small database still keeps a fixed margin. Free space is the space
      available to the node's UID on the `data.dir` filesystem
      (`statvfs`: `f_bavail × f_frsize`), not the space reserved for root.
      If space is short, or the copy fails, the node does
      not resync, cannot promote, reports
      `hasqlite_divergence_preservation_pending = 1` and raises an alert. A
      failure on one database leaves preservation pending for the whole node;
      copies already finished are kept and reused on the next attempt. The
      only way past this without preserving is an operator discard, which
      explicitly accepts the loss.
    - **Permissions and paths.** `diverged/` is created with mode `0700` and
      copies with mode `0600`, owned by the node's UID. Every path is opened
      relative to a directory handle on `data.dir` (`openat`) with `O_NOFOLLOW`.
      Startup fails closed if `diverged/` exists but is a symlink, is not a
      directory, or is owned by another UID.
    - **Isolation.** `${data.dir}/diverged/` and `${data.dir}/ancestors/`
      ([automated-recovery.md §4.2](automated-recovery.md#42-automated-divergence-reconciler))
      are excluded from pre-flight database discovery, gateway database-name
      resolution and replication serving. A diverged copy is never opened for
      writes, served or promoted.
    - **Retention and metrics.** v1 never deletes diverged copies automatically.
      They remain until an operator reconciles and discards them (diverged data
      recovery runbook). `hasqlite_diverged_files` and `hasqlite_diverged_bytes`
      count only finished copies directly under `diverged/` named
      `<db>.diverged-<id>`; temp files, exports and ancestors are not counted.
    - **Operator discard.** `hasqlite diverged discard --id <id>
      --confirm-data-loss --reason <text>` deletes every copy for `<id>` (all
      databases) or, while preservation for `<id>` is pending, removes the
      marker without preserving, leaving any copies already finished in place.
      It refuses an id that matches neither a copy nor the pending marker, and
      refuses to run without `--confirm-data-loss`. Each use appends an audit
      entry with the id, each deleted copy's name and size, the reason, the
      invoking UID and user name, and whether a pending preservation was
      bypassed.
    - **Scope.** Diverged copies exist only on the former writer's volume. If
      that volume is lost, its unreplicated commits are lost. Replicas whose
      snapshots are newer than the promoted one are overwritten on their next
      resync and are not preserved in v1.
16. **Planned switchover**: a stopping writer stops accepting writes, waits a
    bounded time for replicas to complete, for every database in the set, a
    snapshot started after that point, then *releases* the lease (holder
    cleared, generation unchanged, `writer` renewal time updated, database set
    unchanged) so a replica can acquire without waiting a TTL. Stopping writes
    means rejecting new write requests on every database, then draining or
    aborting (`sqlite3_interrupt`) every in-flight write transaction on every
    database and waiting until no write transaction is open and every
    in-flight `COMMIT` has returned. No database-set change is accepted after
    this point. Only then does the writer issue a stop stamp through
    `WriterClock`, so no commit can land after a snapshot that qualifies as
    post-stop. It stamps every later snapshot strictly after the stop stamp,
    waiting for its clock to pass it if needed. Normally that is under one
    second, but after a backward wall-clock step `WriterClock` holds at the
    last stamp until the wall clock catches up, which can take longer than a
    minute. The wait is bounded by the switchover's final-sync timeout and
    spends the shutdown grace period; if it runs out, the switchover is treated
    as a timed-out final sync (no lossless handover, so the former writer
    preserves under rule 15). The release CAS records the stop stamp as
    `writer.stopped_at_secs`. A successor that promotes from post-stop
    snapshots of every database therefore records `promoted_from` with a later
    stamp (the oldest over the set, rule 10), which lets the former writer
    skip the diverged copies (rule 15, *lossless handover check*). If the
    successor promoted from an older snapshot of even one database, for
    example after the final sync timed out, the former writer preserves as
    usual.

## Fencing and access control

17. `enforce_write_fence` gates all mutating SQL on the writer path. Statements
    are classified with `sqlite3_stmt_readonly()` after preparation, never by
    inspecting SQL text. Replica connections are opened read-only with
    `PRAGMA query_only = 1`. An authorizer denies `ATTACH`, `DETACH`,
    `VACUUM INTO` and non-allowlisted pragmas. `SQLITE_DBCONFIG_DEFENSIVE` is
    on, extension loading is off, and `trusted_schema` is off. In clustered
    mode the gateway resolves only names in the current `writer.databases`. It
    rejects any other name with `NOT_FOUND`, a definite rejection, and never
    opens a database with `SQLITE_OPEN_CREATE`, so no unreplicated database can
    be created through SQL.
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
