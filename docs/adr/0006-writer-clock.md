# ADR-0006: Non-decreasing writer clock for all writer-issued stamps

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-FENCE-005, REQ-FENCE-006, REQ-FENCE-007, REQ-PROMO-004, REQ-PROMO-015, REQ-LEASE-014, REQ-SWO-003; safety contract rules 4 and 16; ADR-0005, ADR-0011

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
- **The deficit is measured and corrected.** A floor alone is not enough:
  while `WriterClock` holds at its floor, or the wall clock runs slow, real
  time passes that the stamps do not show. A lag check could then accept a
  snapshot within `max_lag_secs` only because the stamps lagged. `WriterClock`
  keeps a *reference*, set to the first stamp on activation, advanced by the
  monotonic clock and moved forward to the wall clock whenever the wall clock
  reads later. The *deficit* is the reference minus the current stamp. It is
  cumulative, so repeated small backward steps and a slow clock add up as a
  single large step does.
- Every lease write records `writer.clock_deficit_secs` (the deficit rounded
  up, or 0 when it is 1 second or less) in the same CAS as the renewal stamp
  it corrects. Candidates measure lag as
  `writer.renewed_at_secs + writer.clock_deficit_secs − synced_at_secs`.
- **Rebase.** Once the wall clock reads later than the last stamp, the next
  lease write records its stamp as `writer.clock_rebased_at_secs` and a
  deficit of 0, and the writer then resets the reference to that stamp.
  Candidates deny promotion from any snapshot stamped at or before
  `clock_rebased_at_secs`. A rebase is the only way to clear a deficit of
  more than 1 second.
- The lease `format` number covers these fields. A candidate does not
  promote against a lease whose format is newer than it supports (safety
  contract rules 2, 4 and 10).

## Consequences

- Stamps never decrease across a backward wall-clock step, and no clock
  behaviour can make the corrected lag understate the writer's monotonic time
  between a snapshot and a renewal by more than 1 second.
- A writer that crashes during a deficit leaves a lease that candidates
  evaluate conservatively. Nothing is left blocking promotion that only the
  dead writer could clear. If no replica's corrected lag is within the limit,
  recovery follows ADR-0019, as for any writer that died ahead of its
  replicas.
- While the deficit is recorded, snapshots taken before the step stay
  promotable as long as their corrected lag allows. A rebase makes every
  replica unpromotable until it syncs once after the rebase.
- A wall clock that keeps running slow causes a rebase each time its deficit
  passes 1 second.
- After a large backward step, `WriterClock` holds at the last stamp until the
  wall clock catches up. A switchover that needs a strictly later stamp may
  have to wait, bounded by the final-sync timeout (ADR-0011).
- Forward jumps are not corrected; the future-skew check (ADR-0005) can deny
  promotion while the condition persists.
- Rollouts update replicas before the writer (REQ-K8S-004), so the format
  check only bites when a rollout is reversed. An older node then stays
  unpromotable against the newer format until it is upgraded again.

## Alternatives considered

- **Raw wall clock**: rejected; backward steps break the lag check.
- **Monotonic clock for stamps**: not comparable across processes or restarts,
  so it cannot be recorded in the lease and compared by other nodes.
- **Advance held stamps by monotonic elapsed time**
  (`max(wall, last stamp + monotonic elapsed)`): rejected. It keeps lag honest,
  but after a backward step the stamps stay ahead of the corrected wall clock
  for the rest of the writer's epoch. Replicas' future-skew check would then
  deny promotion for that whole time instead of only during the hold.
- **A hold flag** (`clock_held` set on lease writes while the wall clock is
  behind the last stamp, or more than 1 second behind the monotonic clock
  since the last stamp; promotion denied while it is set): rejected. The
  comparison since the last stamp misses repeated small deficits. A crash
  after a flagged write leaves the flag set with no writer to clear it, which
  blocks promotion indefinitely. Older candidates that do not know the flag
  would ignore it.
- **Correction without rebase**: rejected. After a backward step the wall
  clock never catches up with the reference, so every lag would be inflated
  for the rest of the writer's epoch.
- **Rebase as soon as a deficit appears**: rejected. While the stamps are
  still held, it rejects snapshots taken before the step for no gain, and a
  crash during the hold would then leave no promotable replica.
