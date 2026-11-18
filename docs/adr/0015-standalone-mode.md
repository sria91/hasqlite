# ADR-0015: Standalone mode on a canonical, never-replaced file lock

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-STA-001 to REQ-STA-015, REQ-CFG-006; safety contract rules 2, 3, 11; architecture §3 (standalone mode); ADR-0017

## Context

A single-node Docker deployment is a fast-tracked goal (Phase 1b) for local
development, edge and single-host production. Two containers may be pointed at
the same volume by mistake. POSIX `fcntl` locks are released when *any*
descriptor on the file closes; `flock` and `fcntl` do not exclude each other;
an atomic temp-then-rename replaces the inode a holder has locked. Advisory
and SQLite locks are unreliable on NFS, CIFS/SMB and FUSE. Generation history
must survive volume reuse and later migration into a cluster.

## Decision

- `mode = "standalone"` requires `lease.source = "file"` and transitions
  `Starting → Writer(g)` directly, skipping peer freshness checks and running no
  peer replication.
- **One canonical lock path**, `${data.dir}/.hasqlite.lease`, never replaced,
  renamed or deleted, locked with `flock(LOCK_EX | LOCK_NB)` for the process
  lifetime (mutex mode). A second holder fails fast with `EWOULDBLOCK`.
  `fcntl` locks are never used. The record lives in
  `.hasqlite.lease.record`, written by temp + rename + directory fsync while the
  lock is held.
- Clustered single-host file leases use **CAS mode** on the same file type: the
  lock is held only across one read-compare-write. Modes are never mixed on one
  volume.
- The lease record is the **only** source of generation: 1 on a fresh volume,
  `g_prior + 1` on reuse. Database files without a readable record fail
  startup; `hasqlite genesis --generation <g>` is the audited override.
  `PRAGMA user_version` is never used.
- Pre-flight refuses network/FUSE filesystems (`statfs`) and checks write
  permissions on `data.dir`, every database and sidecar, and every parent
  directory, failing on the first denial with path, operation and UID/GID.
- `SIGTERM`/`SIGINT` handlers (PID 1): stop accepting, drain (bounded), WAL
  checkpoint `TRUNCATE` with `PASSIVE` fallback, release the lock, exit 0
  within Docker's 10 s grace.
- Non-root numeric `USER` (default 1000:1000); ports published on host
  loopback.
- Migration to a cluster fences the standalone writer, then seeds the
  Kubernetes Lease with `g_prior` and the writer epoch before any cluster CAS.

## Consequences

- Mutual exclusion on a shared volume holds as long as nobody edits or
  replaces the lock file.
- Standalone requires local storage.
- Standalone may serve several databases. Clustered mode replicates a set of
  up to 64 recorded in the lease (ADR-0020, superseding ADR-0018), so a
  standalone volume with more databases, or with invalid names, must be
  reorganised before it is seeded into a cluster.

## Alternatives considered

- **Configurable lock path**: rejected; two containers could lock different
  inodes.
- **`fcntl` locks**: rejected; released by closing any descriptor.
- **Generation in `PRAGMA user_version`**: rejected; owned by application
  migration tools.
- **Kubernetes Lease in standalone**: rejected; no host-filesystem mutual
  exclusion.
