# ADR-0009: One node-agent process, no lease sidecar

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-K8S-002; architecture §2 (pain points) and §3 (operational model); ADR-0003

## Context

The prior art ran a sync sidecar, a lease updater and an `--ha` process as
separate moving parts. A separate lease holder can keep renewing while the
writer process is wedged, which hides the real failure and breaks the
assumption that the lease holder is the process that commits.

## Decision

`hasqlite-node` runs the lease client, controller loop, sync worker, SQL
gateway and probe server **in one process**. The Kubernetes Lease client is
in-process; a separate lease-updater sidecar is out of scope for v1. The node
is also usable as a library (library-first goal), and the `hasqlite` binary is
the batteries-included packaging of it.

## Consequences

- The lease holder and the committing process fail together, which the
  self-fencing argument (ADR-0003) depends on.
- Simpler manifests: one main container per pod, plus an optional TLS proxy.
- The node needs Kubernetes RBAC directly: `get`/`update` on its Lease and
  `patch` on its own StatefulSet pods for role labels.
- A wedged node is detected through the liveness probe on the controller loop
  only (ADR-0016).

## Alternatives considered

- **Lease sidecar**: rejected for the reason above.
- **Separate sync process**: rejected; adds coordination between processes for
  role checks that must be atomic with leadership.
