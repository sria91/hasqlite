# ADR-0011: Planned switchover by lease release with a stop stamp

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-SWO-001 to REQ-SWO-008, REQ-DIV-004, REQ-DIV-005; safety contract rules 9, 10, 15, 16; ADR-0006, ADR-0010

## Context

Rolling updates restart the writer. Waiting a full TTL for failover on each
restart is slow, and without proof that the successor has every commit, the
former writer would have to write a diverged copy every time. Proof must be
durable and checkable by the former writer when it returns.

## Decision

- The stopping writer **stops writes** (reject new writes, drain or interrupt
  in-flight write transactions, wait until no write transaction is open and
  every in-flight `COMMIT` has returned), then issues a **stop stamp** through
  `WriterClock`.
- It stamps every later snapshot strictly after the stop stamp (waiting for
  `WriterClock` if needed). In the adapter era it performs a renew CAS stamped
  after the stop stamp so a pre-pull lease observation can prove a pull started
  post-stop.
- It waits a **bounded time** for replicas to complete a post-stop snapshot,
  then **releases** the lease (holder cleared, generation unchanged, writer
  renewal updated, `writer.stopped_at_secs` recorded), so a replica acquires
  without waiting a TTL.
- The successor's activation records `writer.promoted_from = (source
  generation, synced_at_secs, source stopped_at_secs)`; this is the only durable
  copy of the stop stamp.
- When the former writer returns, it skips the diverged copy only if the
  current activated writer's `promoted_from` names its generation, carries its
  stop stamp and has a snapshot stamp later than it.
- A timed-out final sync (including the clock wait) is never reported as
  lossless; the former writer preserves.
- Kubernetes: PodDisruptionBudget `maxUnavailable: 1`, replicas updated first
  and the writer last, `terminationGracePeriodSeconds` covering drain, final
  sync and checkpoint.

## Consequences

- Planned switchovers are fast and normally leave no diverged copy.
- Requests can be rejected during the gap; there is no durable SQL request
  queue.
- If the successor has already been replaced when the former writer returns,
  the check fails and a copy is written anyway.
- The policy on final-sync or drain timeout (abort versus bounded-loss
  handover) and successor selection are unresolved for v1; post-v1 Tier 4
  proposes options.

## Alternatives considered

- **Let the lease expire**: rejected; costs a TTL per rollout and cannot prove
  losslessness.
- **Compare page hashes at the former writer**: not durable once the successor
  accepts new writes.
