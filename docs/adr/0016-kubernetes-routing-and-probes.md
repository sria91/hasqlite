# ADR-0016: Kubernetes write routing by pod self-labelling; separate probe semantics

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-PROBE-001 to REQ-PROBE-003, REQ-K8S-001 to REQ-K8S-004; safety contract rules 19 and 20; ADR-0003, ADR-0011

## Context

A standard Kubernetes Service is not writer-aware. If `/ready` passes only on
the writer and is used as the readiness probe, replicas are never "ready" and
StatefulSet rollouts stall. A liveness probe that touches SQLite or the lease
store restarts healthy pods when those are slow.

## Decision

- The node labels **its own pod** `hasqlite.io/role=writer` on activation,
  removes it before demotion completes, reconciles it on startup, and retries
  failed updates with backoff. The writer Service selects that label.
- Probes:
  - `/ready`: 200 only on the activated writer, 503 otherwise (used by HAProxy
    in Compose and local harnesses).
  - `/healthz`: Kubernetes readiness and rollout health.
  - `/live`: Kubernetes liveness; reports only that the controller loop is
    alive and never touches SQLite or the lease store.
- Routing is best-effort; correctness comes from the lease and the pre-commit
  write fence.
- `sync.origin_endpoint` is a stable alias that always routes to the current
  writer, never a pod hostname.

## Consequences

- Labels and endpoints can lag leadership; old connections may reach a former
  writer, whose fence rejects the writes.
- RBAC must allow `patch` on the StatefulSet's own pods.

## Alternatives considered

- **`/ready` as readiness probe**: rejected; stalls rollouts.
- **Lease-aware external controller for routing**: unnecessary; adds a
  component.
