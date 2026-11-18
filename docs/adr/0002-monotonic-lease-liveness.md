# ADR-0002: Lease liveness from a hasqlite-owned version token on the candidate's monotonic clock

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-LEASE-008 to REQ-LEASE-011; safety contract rules 2 and 4; ADR-0003, ADR-0006

## Context

A candidate must decide when the current writer is gone. Comparing the
holder's `renewed_at_secs` with the candidate's wall clock makes the decision
depend on cross-node clock skew and wall-clock jumps. The backend's native
revision (Kubernetes `resourceVersion`) changes whenever *any* actor touches
the Lease object, for example when adding a label or annotation, so it cannot
show that the holder is alive. During a rolling change of lease timing,
candidates and holder can run with different TTLs.

## Decision

- Every lease write made by hasqlite (acquire, activate, renew, release) bumps
  a **hasqlite-owned version token**: a counter in the file record, or the
  annotation `hasqlite.io/version` on the Kubernetes Lease.
- A candidate treats the lease as expired only when it is released or its
  version token has stayed unchanged for the **effective TTL**,
  `max(local lease.ttl_secs, holder's recorded ttl_secs)`, measured on the
  candidate's **local monotonic clock** (`LeaseObservation::is_expired_for`).
- The native revision is used only as the CAS precondition (ADR-0004), never
  as a liveness signal.
- Candidates never compare the lease's `renewed_at_secs` with their own wall
  clock.

## Consequences

- Wall-clock jumps and skew on candidates cannot cause early expiry.
- Unrelated writes to the Lease object neither postpone failover nor count as
  renewals; they can still make a hasqlite CAS fail and retry on the next tick.
- Honouring the holder's recorded TTL keeps the fencing margin (ADR-0003)
  valid while lease timing is rolled out; a longer candidate TTL only delays
  failover until the rollout completes.
- A crashed writer that did not release the lease is replaced only after a full
  effective TTL; failover is not instantaneous.

## Alternatives considered

- **Wall-clock comparison of `renewed_at_secs`**: rejected; unsafe under skew.
- **`resourceVersion` as the liveness signal**: rejected; any actor can bump it.
- **Candidate's own TTL only**: rejected; a candidate with a shorter TTL could
  expire a holder that still commits.
