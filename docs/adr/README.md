# Architecture Decision Records

This directory records the architectural decisions behind hasqlite v1. Each
record states the context, the decision, its consequences and the alternatives
that were rejected. The decisions are extracted from the development plan
([PLAN.md](../PLAN.md) and the documents it links); the
[safety contract](../safety-contract.md) remains the normative source for
invariants, and the [requirements](../requirements.md) restate them as testable
EARS statements.

## Conventions

- File name: `NNNN-short-title.md`, numbered in order of creation. Numbers are
  never reused.
- Status: `Proposed`, `Accepted`, `Superseded by ADR-NNNN` or `Deprecated`.
  A decision is changed by writing a new ADR that supersedes the old one, not
  by rewriting the old one's decision.
- Each ADR links the requirement IDs (`REQ-…`) it gives rise to and the safety
  contract rules it relies on.

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-lease-based-single-writer.md) | Lease-based single writer with asynchronous page replication | Accepted |
| [0002](0002-monotonic-lease-liveness.md) | Lease liveness from a hasqlite-owned version token on the candidate's monotonic clock | Accepted |
| [0003](0003-writer-self-fencing.md) | Writer self-fencing at the renewal deadline | Accepted |
| [0004](0004-cas-lease-operations.md) | Compare-and-swap lease operations with separate acquire and activate | Accepted |
| [0005](0005-promotion-gate.md) | Promotion gated on a freshness ledger bound to the activated writer epoch | Accepted |
| [0006](0006-writer-clock.md) | Non-decreasing writer clock for all writer-issued stamps | Accepted |
| [0007](0007-crash-safe-ledger-publication.md) | Crash-safe ledger publication and adapter-era attribution | Accepted |
| [0008](0008-transitional-replication-adapter.md) | Transitional `rsqlite-rsync` adapter, replaced by a native backend | Accepted |
| [0009](0009-single-node-agent.md) | One node-agent process, no lease sidecar | Accepted |
| [0010](0010-diverged-data-preservation.md) | Crash-safe preservation of a former writer's diverged data | Accepted |
| [0011](0011-planned-switchover.md) | Planned switchover by lease release with a stop stamp | Accepted |
| [0012](0012-hardened-sql-gateway.md) | Hardened SQL gateway classified by SQLite, not by SQL text | Accepted |
| [0013](0013-security-posture.md) | External TLS termination and separate SQL and replication credentials | Accepted |
| [0014](0014-client-retry-semantics.md) | Client retries only on a definite rejection | Accepted |
| [0015](0015-standalone-mode.md) | Standalone mode on a canonical, never-replaced file lock | Accepted |
| [0016](0016-kubernetes-routing-and-probes.md) | Kubernetes write routing by pod self-labelling; separate probe semantics | Accepted |
| [0017](0017-fail-closed-configuration.md) | Fail-closed configuration validation | Accepted |
| [0018](0018-one-database-per-cluster.md) | One replicated database per cluster in v1 | Superseded by ADR-0020 |
| [0019](0019-v1-fail-closed-recovery.md) | v1 fails closed; automated recovery is post-v1 | Accepted |
| [0020](0020-multiple-databases-per-cluster.md) | Several replicated databases per cluster under one writer | Accepted |

## Template

```markdown
# ADR-NNNN: Title

- Status: Proposed
- Date: YYYY-MM-DD
- Related: REQ-…, safety contract rule N, ADR-NNNN

## Context
## Decision
## Consequences
## Alternatives considered
```
