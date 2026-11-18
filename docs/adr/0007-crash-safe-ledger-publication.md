# ADR-0007: Crash-safe ledger publication and adapter-era attribution

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-SYNC-002, REQ-SYNC-004 to REQ-SYNC-012; safety contract rules 8 and 9; ADR-0005, ADR-0008

## Context

Until the native backend lands (Track N), the transitional adapter writes
pages into the replica file **in place**. A crash mid-apply leaves a torn
database. The adapter also stamps neither origin identity nor snapshot time,
and its replication server serves pulls whether or not its node is the
activated writer. The ledger must never claim a snapshot that is not durably
and completely on disk, nor attribute it to the wrong writer.

## Decision

- **Invalidate before apply**: before the first page is written, the replica
  invalidates the ledger and fsyncs the invalidation.
- **Publish after durability**: a new ledger is published only after the sync
  succeeds and the database file and its directory are fsynced, using temp file
  + rename + directory fsync. The ledger lives on the same filesystem as the
  database (checked at pre-flight with `statfs`/`st_dev`).
- **Adapter-era attribution** (until native origin stamping):
  1. Observe the lease before the pull; abort if `holder_node_id !=
     writer.node_id`.
  2. After the database fsync, observe the lease again; publish only if
     `version` is unchanged and the holder still equals the writer epoch.
  3. Stamp `source_node_id`, `source_generation` and `synced_at_secs :=
     writer.renewed_at_secs` from the *pre-pull* observation, a lower bound on
     the snapshot start, so lag is overstated, never understated.
  4. A role-gating interceptor in front of the adapter's replication server
     rejects pulls (`FAILED_PRECONDITION`) unless the node is the current lease
     holder and activated writer.
- **Exclusive application**: at most one pull per replica path
  (`SyncError::SyncInProgress`).
- With the native backend, the origin stamps node ID, generation and snapshot
  start in the stream header and applies snapshots atomically (staging + rename
  or under SQLite locking), retiring steps 1–3.

## Consequences

- An interrupted sync always leaves the node unpromotable until a full sync
  and `PRAGMA integrity_check` pass.
- Adapter-era lag reads up to one retry period higher than with native
  stamping; size `freshness.max_lag_secs` accordingly.
- Replica reads stay unsupported while application is in place, because
  concurrent readers can see torn pages.
- A lease change during a pull wastes that pull; it is retried next tick.

## Alternatives considered

- **Use sync completion time as `synced_at_secs`**: rejected; it overstates
  freshness relative to the writer's clock.
- **Trust the adapter's serving node without a role gate**: rejected; a
  demoted origin could serve a stale snapshot attributed to the new writer.
