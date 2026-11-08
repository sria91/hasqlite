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

## Promotion

7. A node may **promote to writer** only if:
   - It holds the current lease (or acquires it per lease rules), **and**
   - `validate_promotion` passes: freshness age ≤ `max_freshness_age_secs`,
     generation ≥ `min_source_generation`, clock skew within bounds.
8. **Failed promotion** must not open writable SQL paths (fail closed).

## Demotion

9. On lease loss or TTL expiry while writer: **immediate demotion**; stop
   accepting writes; gateway returns `NOT_LEADER` with redirect hint.
10. Former writer **rejoins as replica** and must resync before eligible to
    promote again.

## Fencing

11. `enforce_write_fence` gates all mutating SQL on the writer path.
12. **Authentication** (bearer token) is independent of fencing; both are required
    in production configurations.

## Kubernetes

13. **Rollout probes**: use `/healthz` for pod readiness during rolling updates;
    use `/ready` only when routing write traffic to the current writer.
14. **preStop**: mandatory sync attempt before SIGTERM to tighten RPO.

Violations of this contract are **severity-0** bugs.
