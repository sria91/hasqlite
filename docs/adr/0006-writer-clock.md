# ADR-0006: Non-decreasing writer clock for all writer-issued stamps

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-FENCE-005, REQ-FENCE-006, REQ-PROMO-015, REQ-SWO-003; safety contract rules 4 and 16; ADR-0005, ADR-0011

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
- **Holds fail closed.** A floor alone is not enough: while `WriterClock`
  holds at its last stamp, real time passes that the stamps do not show, so a
  lag check could accept a snapshot within `max_lag_secs` only because the
  stamps were frozen. `WriterClock` reports a *hold* when the wall clock reads
  earlier than the last stamp, or has advanced more than 1 second less than
  the monotonic clock since the last stamp. While held, every lease write
  records `writer.clock_held = true`; the first lease write after the hold
  records its stamp as `writer.clock_resumed_at_secs`. Candidates deny
  promotion while `clock_held` is set and from any snapshot stamped at or
  before `clock_resumed_at_secs` (safety contract rules 4 and 10).

## Consequences

- Stamps never decrease across a backward wall-clock step, and a backward
  step can never make a stale snapshot pass the lag check. It makes replicas
  unpromotable while the clock is held and until each has synced once after
  it resumes.
- A backward step of 1 second or less can go undetected and understate lag by
  at most that much.
- After a large backward step, `WriterClock` holds at the last stamp until the
  wall clock catches up. A switchover that needs a strictly later stamp may
  have to wait, bounded by the final-sync timeout (ADR-0011).
- Forward jumps are not corrected; the future-skew check (ADR-0005) can deny
  promotion while the condition persists.

## Alternatives considered

- **Raw wall clock**: rejected; backward steps break the lag check.
- **Monotonic clock for stamps**: not comparable across processes or restarts,
  so it cannot be recorded in the lease and compared by other nodes.
- **Advance held stamps by monotonic elapsed time**
  (`max(wall, last stamp + monotonic elapsed)`): rejected. It keeps lag honest,
  but after a backward step the stamps stay ahead of the corrected wall clock
  for the rest of the writer's epoch. Replicas' future-skew check would then
  deny promotion for that whole time instead of only during the hold.
- **Deny only while held**: rejected. A snapshot stamped before the step and
  compared with a renewal after the clock resumes still understates lag by
  the length of the hold.
