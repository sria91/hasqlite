# ADR-0014: Client retries only on a definite rejection

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-CLIENT-001 to REQ-CLIENT-003, REQ-API-001 to REQ-API-004; operations §10.2, §10.6

## Context

On failover a write may reach the old writer, commit, and lose its response.
Retrying such a write against the new writer can duplicate it. Durable,
failover-safe deduplication is not designed for v1.

## Decision

- The wire API distinguishes **"rejected before execution"** (including
  `NOT_LEADER` with redirect hint and current generation) from **"outcome
  unknown"**.
- `hasqlite-client` retries automatically only after a definite rejection that
  proves the statement never executed, following the redirect hint.
- An outcome-unknown write is surfaced to the application and never retried
  silently.
- Mutating requests may carry an optional idempotency key; its failover-safe
  semantics are unresolved and it is not a v1 guarantee.
- The protocol carries a version field; version N serves N-1 clients and
  replicas.

## Consequences

- No silent duplicates from client retries.
- Applications must handle outcome-unknown errors themselves.
- Exactly-once execution across failover remains an open item; post-v1 Tier 1
  proposes in-database replicated idempotency.

## Alternatives considered

- **Blind retry on any transport error**: rejected; duplicates writes.
- **Client-side deduplication only**: insufficient; state is lost on failover.
