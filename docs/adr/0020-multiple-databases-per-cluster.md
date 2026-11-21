# ADR-0020: Several replicated databases per cluster under one writer

- Status: Accepted
- Date: 2026-11-18
- Supersedes: ADR-0018
- Related: REQ-MDB-001 to REQ-MDB-023, REQ-CFG-011; safety contract rules 2, 6, 8–10, 15
  and 16; operations §10.2, §10.4; ADR-0005, ADR-0007, ADR-0010, ADR-0011

## Context

ADR-0018 limited clustered v1 to one database so that one ledger described
the whole replica. Applications that keep several SQLite files (for example
one per tenant or per bounded context) then had to run one cluster per file,
each with its own lease, pods and volumes. Standalone mode already serves
several databases, so moving such an application from standalone to a cluster
meant splitting it.

Clustered mode should serve several databases. The promotion gate, the
switchover handover check and diverged-data preservation must still reason
about a well-defined set of snapshots. A replica that lacks one of the
writer's databases, or holds a stale copy of one, must not be promotable.

## Decision

- **One lease, one writer for all databases.** The cluster keeps a single
  lease and generation. The activated writer serves and originates every
  database in the cluster; replicas replicate all of them. Rules 1, 3–6 and 14
  are unchanged. Per-database leases are not adopted (see alternatives).
- **The database set is part of the writer epoch.** The lease's `writer`
  epoch carries `databases`: 1–64 `(name, incarnation)` entries sorted by
  name, each name a path relative to `data.dir`. Each add takes a new
  incarnation from the lease counter `writer.next_incarnation`, so a name
  that is removed and added again is a different database. The lease is the only authority
  for which databases the cluster replicates; node configuration does not list
  them. Genesis or the seed-snapshot procedure records the initial set.
  Activation copies the set, the counter and the tombstones from the
  observed lease unchanged.
- **Set changes go through the writer's renew CAS.** Only the activated
  writer changes the set: `hasqlite db add` and `hasqlite db remove` (audited)
  change it with a renew that also writes the new `writer.databases`. They use
  the renew preconditions of rule 6, so a conflict means demotion. A database
  is served only after the CAS that adds it succeeds. It stops being served
  before the CAS that removes it.
- **Remove deletes the files on every node.** The removing CAS moves the entry
  to the lease's tombstones (`writer.removed`, up to 64, oldest dropped). The
  writer, and each replica when it observes the tombstone, deletes the
  database's `-wal`, `-shm` and main file, then its ledger record. Deletion
  is idempotent and finished after a crash, because the tombstone stays in
  the lease. A node deletes nothing while a divergence preservation is
  pending, so a former writer's unreplicated commits to a removed database
  are preserved first. A node never deletes files that are in neither the
  set nor the tombstones; it reports them as unmanaged.
- **Incarnations guard against reused names.** A ledger record carries its
  database's incarnation. Promotion requires it to match the set entry, and a
  replica deletes local files of an older incarnation before its first pull
  of a new one. An old copy of a removed database therefore can neither pass
  the gate nor be patched into a re-added one.
- **Names.** Each `/`-separated component is non-empty, uses only ASCII
  letters, digits, `.`, `_` or `-`, and does not start with `.`. The whole
  name ends in `.db`, `.sqlite` or `.sqlite3` and is at most 200 bytes with
  each `/` counted as three bytes (its `%2F` encoding), so every generated
  ledger-record and diverged-copy name, including its `.tmp` file, stays
  within the 255-byte file-name limit. The first component is not
  `diverged` or `ancestors`. With the cap of 64, the set fits in the Kubernetes
  Lease annotations (well under 256 KiB).
- **One ledger record per database.** The freshness ledger becomes a directory
  (`freshness.dir`) holding one record per database, named with the same
  encoding as diverged copies (`%` → `%25`, `/` → `%2F`). Invalidation,
  publication and adapter-era attribution (ADR-0007) apply to each record
  independently.
- **Concurrent reads, writes and sync across databases.** The writer serves
  reads and writes on different databases concurrently; each database has its
  own connections and SQLite write lock, and the write fence runs before
  every commit. The origin serves concurrent replication sessions, each with
  its own snapshot deadline. A replica's sync round runs one pull per
  database, at most `sync.max_concurrent_pulls` (default 4, 1–64) at a time.
  Only pulls into the same database file are exclusive.
- **Promotion requires every database.** `validate_promotion` passes only if
  each database in the observed lease's set has a record that passes the
  existing checks. The checks run class by class (ledger shape, writer epoch,
  time bounds); within a class, databases are checked in name order, and the
  first failure is reported. The freshest-first delay uses the largest lag
  over all databases. `writer.promoted_from.synced_at_secs` records the oldest
  snapshot stamp over all databases.
- **Planned switchover covers every database.** The writer stops writes on all
  databases before the stop stamp. A handover is lossless only if every
  database's snapshot started after the stop stamp, which the oldest stamp in
  `promoted_from` captures. The lossless-handover check in rule 15 is otherwise
  unchanged.
- **Preservation covers every database on the volume.** A former writer
  preserves every database that pre-flight discovery finds on its volume,
  including databases no longer in the set. It runs one space check over all
  copies still to be written, and removes the divergence marker only after
  every copy is durable. All copies share one divergence id.
- **No cross-database atomicity.** `ATTACH` stays denied, so a transaction
  touches one database. After an unplanned failover, each database separately
  holds a prefix of its acknowledged writes, but different databases can
  reflect different points in time.

## Consequences

- Applications with several SQLite files run on one cluster with one failover
  domain: all databases move to the new writer together.
- A database that has never been synced, or a stale one, blocks promotion of
  the whole replica. Right after `db add`, no replica is promotable until it
  has synced the new database once. If the writer dies in that window,
  promotion fails closed, as when a writer dies before any replica has synced
  from it.
- With concurrent pulls, a database's lag depends on its own pull and on
  waiting for a pull slot, not on the sum of all databases. Size
  `freshness.max_lag_secs` for the slowest pull, or for a full round when
  there are more databases than pull slots. The config validator still checks
  only `interval_secs + retry_period_secs`.
- Concurrent pulls multiply peak network, disk I/O and origin read
  transactions by up to `max_concurrent_pulls` per replica; the cap bounds
  that against the resource-footprint goal.
- Per-database metrics (`database` label) are bounded by the cap of 64.
- Idle databases still cost one pull per round, which counts against the
  resource-footprint goal.
- `db remove` is permanent data loss on every node, so it requires
  `--confirm-data-loss`. The only surviving copy is a diverged copy on a former
  writer that had not yet resynced. Take a backup first if the data may be
  needed.
- Files from a tombstone that was dropped from the full list, on a node that
  was offline for 64 removals, are not deleted automatically; they are
  reported as unmanaged.
- The `freshness.path` configuration key is replaced by `freshness.dir`.
- A standalone volume with more than 64 databases, or with names that break
  the naming rules, cannot be seeded into a cluster until it is reorganised.

## Alternatives considered

- **One lease and writer per database**: rejected for v1. It would allow
  different databases to have writers on different nodes, but it multiplies
  leases, generations, fencing deadlines, divergence markers and routing
  (one writer Service per database). It also breaks the "one node accepts
  commits" invariant that the simulation and history checker verify.
- **Database set declared in node configuration**: rejected. Nodes could
  disagree during a rolling config change, and a candidate whose
  configuration lacked one of the writer's databases could promote without
  it. The lease is already the authority for the writer epoch.
- **Discover the set from the writer's `data.dir`**: rejected. A file created
  by accident, or a half-created database, would join the set without an
  audited decision.
- **One ledger file with an entry per database**: rejected. Every publication
  would rewrite every other database's record, so a failed write could
  invalidate records unrelated to the pull.
- **Keep removed databases' files on disk**: rejected; operators expect
  removal to reclaim space, and leftover files invite a re-added name to be
  patched onto stale pages.
- **Delete any file not in the set**: rejected; a wrong or reset lease, or a
  file placed by hand, would cause mass deletion. Deletion needs an explicit
  tombstone.
- **Sequential pulls in name order**: rejected; lag would grow with the
  number of databases, and one large database would delay all the others.
- **Treat databases added after the last sync as optional for promotion**:
  rejected. The writer may already have acknowledged writes to the new
  database, and promoting without it would lose them silently.
- **Keep ADR-0018 and run one cluster per database**: still possible, but no
  longer required.
