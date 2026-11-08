# hasqlite Safety Contract (draft)

This document is normative for implementation and tests. It codifies invariants
already implicit in `rsqlite-rsync` HA mode.

## Ownership

1. **One active writer** per cluster/database scope at any instant.
2. **Lease is authoritative** for writer identity: `holder_node_id`, `generation`,
   `renewed_at_secs`, `ttl_secs`.
3. **Generation increases** on every successful writer transfer (never reuse).

## Replication

4. **Writer is sync origin**; replicas are **exclusive** targets (no concurrent
   writers to replica files).
5. **Origin may be live** (reads/writes); sync produces a consistent snapshot as
   of sync start (rsync snapshot semantics).
6. **Freshness ledger** records last successful sync: `source_node_id`,
   `source_generation`, `synced_at_secs`.
   - **Replica binding**: Freshness must be bound to the replica that received
     the snapshot by requiring replica-specific ledger storage (e.g. replica-private
     state path) or recording the destination identity and validating it before
     allowing promotion.
   - **Snapshot start timestamp**: `synced_at_secs` defines freshness by the
     snapshot start time (when origin snapshot began), not sync completion time.
   - **Publication**: The timestamp and ledger record are published only after
     the sync succeeds.

## Promotion

7. A node may **promote to writer** only if:
   - It successfully executes an atomic lease acquisition / compare-and-swap (CAS)
     operation incrementing the generation counter (`new_gen = expected_gen + 1`)
     with generation fencing, **and**
   - `validate_promotion` passes: freshness bound to the replica, freshness age ≤
     `max_freshness_age_secs`, generation ≥ `min_source_generation`, verified
     `source_node_id`, and clock skew within bounds.
   - If the atomic lease acquisition fails (e.g. concurrent claim lost to a peer),
     promotion is denied and the node remains a replica (fail closed).
8. **First-writer bootstrap**: for clusters with no writer and no successful sync
   ledger, an explicit genesis procedure or verified seed-snapshot procedure
   establishes the initial generation and writer. Normal failover strictly keeps
   `validate_promotion` and its freshness, source identity, replica binding, and
   fencing requirements intact.
9. **Failed promotion** must not open writable SQL paths (fail closed).

## Demotion

10. On lease loss or TTL expiry while writer: **immediate demotion**; stop
    accepting writes; gateway returns `NOT_LEADER` with redirect hint.
11. Former writer **rejoins as replica** and must resync before eligible to
    promote again.

## Fencing

12. `enforce_write_fence` gates all mutating SQL on the writer path.
13. **Authentication** (bearer token) is independent of fencing; both are required
    in production configurations.

## Kubernetes

14. **Rollout probes**: use `/healthz` for pod readiness during rolling updates;
    write traffic must be routed only to the current writer, using a writer-aware
    mechanism such as a controller-managed EndpointSlice or a proxy that checks
    `/ready`. Clarify that `/healthz` readiness alone does not provide writer-only
    routing.
15. **preStop**: mandatory sync attempt before SIGTERM to tighten RPO.

Violations of this contract are **severity-0** bugs.
