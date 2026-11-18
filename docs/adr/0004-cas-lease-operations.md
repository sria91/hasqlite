# ADR-0004: Compare-and-swap lease operations with separate acquire and activate

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-LEASE-003 to REQ-LEASE-007, REQ-LEASE-012, REQ-LEASE-013, REQ-PROMO-009 to REQ-PROMO-011; safety contract rules 2, 3, 6, 10; ADR-0005

## Context

Several replicas can observe an expired lease at the same moment. A renewal
can land between a candidate's observation and its acquisition attempt. A
superseded writer may try to renew after a peer took over. A candidate can
crash after winning the lease but before it is ready to serve, and replicas
synced from the previous writer must stay promotable in that case.

## Decision

- The lease store exposes **conditional operations only**: `acquire`,
  `activate`, `renew`, `release`. Each is a CAS whose precondition is the
  backend's native revision; each bumps the hasqlite version token atomically.
- `acquire(observed)` succeeds only if the version is unchanged since the
  observation. **The store, not the caller, derives the new generation**
  (`observed + 1`) and sets `holder = self`. The writer epoch is unchanged.
- `activate`, `renew` and `release` require an unchanged version since the
  node's last successful lease write, `holder == self` and an unchanged
  generation. None of them changes the generation.
- **Activation is the only operation that changes the writer epoch.** A node
  opens a writable handle only after both acquire and activate succeed.
- A lost acquire means `StayReplica`. A conflict on activate, renew or release
  means immediate demotion; a writer never overwrites a newer holder.

## Consequences

- Generation is strictly monotonic and never reused, which makes fencing and
  ledger matching unambiguous.
- Exactly one candidate can win a race; losers fail closed.
- An acquired-but-never-activated generation leaves the previous writer epoch
  in place, so peers synced from that writer remain promotable; another
  acquisition may have to wait for the abandoned holder's TTL.
- A writer that activates and then dies before any replica syncs from it
  blocks automatic promotion (fail closed, manual runbook; ADR-0019).

## Alternatives considered

- **Caller-supplied generation**: rejected; allows reuse or skips.
- **Single acquire step that also moves the writer epoch**: rejected; a crash
  between winning and serving would strand every replica on a dead epoch.
- **Blind (unconditional) writes**: rejected; a superseded writer could
  overwrite a newer holder.
