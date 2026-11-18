# ADR-0010: Crash-safe preservation of a former writer's diverged data

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-DEM-001, REQ-DEM-002, REQ-DIV-001 to REQ-DIV-020, REQ-PROMO-010; safety contract rule 15; ADR-0011, ADR-0019

## Context

After a failover, the former writer may hold commits that never reached the
new writer: normal replication lag, or a commit that stalled past the fencing
margin. Its first resync would overwrite them. A crash or partition usually
ends the writer before any demotion code or role-file update runs, so the
trigger must survive those. Commits can exist only in `-wal`, so copying the
main file alone silently drops them. Disk can be full.

## Decision

- **Durable trigger**: activation writes `${data.dir}/.hasqlite.divergence`
  with id `g<generation>` before the writable handle opens. On clustered
  startup preservation is also pending when the lease's writer epoch names this
  node while another holds the lease, when the marker is unreadable while
  database files exist, or when a standalone volume joins a cluster for the
  first time (`standalone-g<g>`). The role file is never the trigger.
- **Blocking**: a pending preservation blocks sync pulls, replica reads and
  promotion.
- **Copy method**: SQLite online backup API from a local read connection with
  no writable handles open; never rename or copy source files.
- **Durable, idempotent write**: `diverged/<db>.diverged-<id>.tmp` → fsync →
  `integrity_check` → rename → directory fsync → audit → remove marker →
  directory fsync. Restarts redo or verify-and-reuse.
- **Unreadable marker**: hard-link it for forensics, then atomically replace it
  with id `unknown-<secs>`, reused across restarts.
- **Fail closed on space or I/O**: require free space for database + WAL plus
  headroom; otherwise stay pending, report
  `hasqlite_divergence_preservation_pending = 1` and alert. The only bypass is
  `hasqlite diverged discard --id <id> --confirm-data-loss --reason <text>`,
  which is audited.
- **Isolation and safety**: `diverged/` (and `ancestors/`) are excluded from
  discovery, gateway name resolution and replication; `0700`/`0600`, opened via
  `openat` + `O_NOFOLLOW`; startup fails if `diverged/` is a symlink, not a
  directory or foreign-owned.
- **Retention**: v1 never deletes copies automatically; reconciliation is
  manual (runbook).
- **Skip when lossless**: a verified planned handover skips the copy
  (ADR-0011).

## Consequences

- No acknowledged commit on the former writer is silently destroyed by resync.
- Each copy can be as large as the database; volumes must be sized for
  retained copies plus preservation headroom, and `hasqlite_diverged_bytes`
  monitored.
- A full disk keeps the former writer out of service until space is freed or
  an operator discards.
- Copies live only on that volume; losing the volume loses them. Replicas
  newer than the promoted snapshot are not preserved in v1.

## Alternatives considered

- **Role-file or demotion-path trigger**: rejected; skipped by crashes.
- **File-by-file copy or rename**: rejected; loses WAL-only commits.
- **Automatic merge into the new writer**: post-v1 (automated recovery Tier 3).
