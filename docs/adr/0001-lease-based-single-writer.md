# ADR-0001: Lease-based single writer with asynchronous page replication

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-LEASE-001, REQ-LEASE-002, REQ-SYNC-*; safety contract rules 1–3, 7–8; ADR-0002, ADR-0003, ADR-0004, ADR-0019

## Context

hasqlite makes SQLite highly available for applications that need one
consistent database, fast failover and an operator-grade deployment story
([PLAN §1](../PLAN.md#1-goals-and-future-goals)). SQLite has no built-in
replication or leader election. The prior art (`rsqlite-rsync`) already
demonstrated two-pass BLAKE3 page-hash sync from a live origin and a
lease-based HA loop. Kubernetes (Lease API) and a single host (file lock) both
offer a coordination primitive that supports compare-and-swap.

## Decision

- The cluster is **active–passive**: exactly one node, the writer, accepts
  commits; every other node is a replica.
- Writer identity is decided by an **external authoritative lease** (Kubernetes
  Lease object or a single-host file lease), not by a quorum of hasqlite nodes.
  The lease carries the holder, a monotonic **generation**, the holder's TTL, a
  hasqlite-owned version token and the last **activated writer epoch**.
- Replicas pull **incremental page-level snapshots** from the writer on a
  schedule (default every 2 s). Replication is asynchronous.
- The informational role file is never authoritative.

## Consequences

- A single writer avoids SQL-level conflict resolution and keeps SQLite
  semantics unchanged for applications.
- Unplanned failover can lose acknowledged writes. The target loss window is
  `freshness.max_lag_secs + lease.renew_deadline_secs`; RPO is not zero
  ([operations §10.2](../operations.md#102-data-safety-and-durability-limitations)).
- Availability depends on the lease backend: if no node can reach it, the
  writer self-fences and nobody can promote.
- Replicas do not scale write throughput.
- The file lease backend is single-host only and is not a cross-host
  coordination mechanism.

## Alternatives considered

- **Raft/Paxos consensus or multi-writer SQL**: out of scope for v1 (future
  goal). Much higher complexity; changes the commit path.
- **Cross-region active/active**: future goal; incompatible with a single
  logical writer.
- **Synchronous replication**: not chosen for v1; it would couple commit
  latency to replica availability. Bounded-loss asynchronous replication is the
  stated product contract.
