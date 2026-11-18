# ADR-0003: Writer self-fencing at the renewal deadline

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-FENCE-001 to REQ-FENCE-004, REQ-CFG-004; safety contract rules 1, 5, 14; ADR-0002

## Context

Each node writes its own local disk. Nothing at the storage layer rejects
writes from an old generation, so a partitioned or paused writer could keep
committing after a peer promotes. The only party that can stop those commits
is the writer itself.

## Decision

- The writer accepts a commit only before
  `send time of its last successful renewal + lease.renew_deadline_secs`,
  measured on its local monotonic clock.
- `enforce_write_fence` runs **inside the write transaction immediately before
  `COMMIT`** and checks role, deadline and generation.
- At the deadline the writer calls `sqlite3_interrupt` on in-flight statements,
  sets `/ready` to 503 and demotes.
- `lease.ttl_secs − lease.renew_deadline_secs` is the **fencing margin**,
  validated to be at least 2 s (default 4 s). Because a candidate can only see a
  renewal after the writer sent it, the writer's deadline always falls before
  any candidate's effective TTL elapses.
- `2 × retry_period_secs ≤ renew_deadline_secs`, so one failed renewal attempt
  does not demote the writer.

## Consequences

- Split-brain prevention is cooperative and depends on a timing assumption:
  **no single commit stalls inside `COMMIT` longer than the fencing margin.**
  A longer stall can still produce a commit after a peer promotes. This is an
  explicit residual risk, exercised by pause-injection chaos tests and
  detectable from audit logs; it is not a guarantee.
- A writer that loses access to the lease store stops committing even if its
  database and clients are healthy.
- Closing the stalled-commit window would need an enforceable external or
  storage-level fence or a different commit architecture (open item,
  [operations §10.6](../operations.md#106-unresolved-guarantees-and-recovery-policy)).

## Alternatives considered

- **Check the fence only at statement start**: rejected; a long transaction
  could cross the deadline.
- **Rely on routing (labels/Service) to stop writes**: rejected; routing lags
  leadership (ADR-0016).
- **Larger timeouts**: do not close the window, only move it.
