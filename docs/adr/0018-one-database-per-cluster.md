# ADR-0018: One replicated database per cluster in v1

- Status: Superseded by ADR-0020 (2026-11-18)
- Date: 2026-11-17
- Related: REQ-SYNC-014; operations §10.4; ADR-0005, ADR-0007

## Context

The freshness ledger, promotion gate and diverged-data procedure reason about
a single snapshot. Multiple databases would need per-database ledgers and a
promotion rule over all of them.

## Decision

Clustered v1 replicates exactly one database per cluster, described by a
single ledger. Standalone mode may serve several databases.

## Consequences

- Simple, auditable promotion decisions.
- Applications needing several replicated databases run several clusters.
- Standalone multi-database serving does not imply clustered multi-database
  replication.

## Alternatives considered

- **Per-database ledgers in v1**: deferred; widens the promotion and
  divergence state space.
