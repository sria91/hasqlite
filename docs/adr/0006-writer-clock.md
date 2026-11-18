# ADR-0006: Non-decreasing writer clock for all writer-issued stamps

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-FENCE-005, REQ-SWO-003; safety contract rules 4 and 16; ADR-0005, ADR-0011

## Context

The promotion lag check subtracts two writer-issued stamps
(`writer.renewed_at_secs − synced_at_secs`) with saturating subtraction. If the
writer's wall clock steps backwards between a renewal and a later snapshot, a
stale snapshot could look fresh enough to bypass the lag limit. Planned
switchover also needs snapshot stamps that are strictly ordered after a stop
stamp.

## Decision

- The writer issues **every** stamp (renew, activate, release, stop and
  snapshot stamps) through `WriterClock`, which returns
  `max(wall clock, last stamp)`.
- On activation `WriterClock` is seeded from the writer epoch's last renewal,
  so a new writer never stamps earlier than its predecessor's last renewal.

## Consequences

- Stamps never decrease across a backward wall-clock step; a post-step
  snapshot cannot read as fresher than the renewal.
- After a large backward step, `WriterClock` holds at the last stamp until the
  wall clock catches up. A switchover that needs a strictly later stamp may
  have to wait, bounded by the final-sync timeout (ADR-0011).
- Forward jumps are not corrected; the future-skew check (ADR-0005) can deny
  promotion while the condition persists.

## Alternatives considered

- **Raw wall clock**: rejected; backward steps break the lag check.
- **Monotonic clock for stamps**: not comparable across processes or restarts,
  so it cannot be recorded in the lease and compared by other nodes.
