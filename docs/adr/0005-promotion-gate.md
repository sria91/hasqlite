# ADR-0005: Promotion gated on a freshness ledger bound to the activated writer epoch

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-PROMO-001 to REQ-PROMO-014; safety contract rules 9–13; ADR-0004, ADR-0006, ADR-0007

## Context

Promoting a stale or wrong replica silently loses data. Apparent file recency
is not evidence of freshness. Lag must not depend on how long failure
detection took or on clock skew between nodes. When several replicas are
eligible, the freshest should be preferred. A replica cannot catch up after
the writer died, because in v1 it may pull only from the activated writer.

## Decision

- Each replica keeps a **freshness ledger** recording `source_node_id`,
  `source_generation`, `synced_at_secs` (origin-stamped snapshot *start*) and
  a mandatory `destination_node_id`.
- `validate_promotion` passes only if, in this fixed order (first failure
  reported):
  0. **Lease format**: the observed lease's `format` is one this node
     supports (ADR-0006).
  1. **Ledger shape**: present, valid, bound to this node, not self-sourced,
     and an activated writer exists.
  2. **Writer epoch**: source node and generation equal the observed lease's
     activated `writer` epoch.
  3. **Time bounds**: `writer.renewed_at_secs + writer.clock_deficit_secs −
     synced_at_secs` (saturating) ≤ `freshness.max_lag_secs`, all from the
     writer's clock; `synced_at_secs` ≤ local wall time +
     `promotion.max_future_skew_secs`; and `synced_at_secs` is later than
     `writer.clock_rebased_at_secs` when that is set (ADR-0006).
- **Freshest first**: an eligible candidate waits
  `retry_period_secs × lag_secs / max_lag_secs` before acquiring and aborts if
  the lease version changes meanwhile.
- Scheduled sync is **paused while campaigning**, so validated inputs are
  frozen and no second freshness check follows a won CAS.
- **No post-promotion catch-up in v1**: the promoted node opens its writable
  handle on the snapshot it validated.
- Clusters with no activated writer bootstrap only through an explicit genesis
  or verified seed-snapshot procedure.

## Consequences

- Promotion is denied when all replicas are too stale, when the writer died
  before any replica synced from it, or when the writer's clock runs ahead of
  replicas beyond the skew limit; availability is sacrificed to bound loss.
- The error order is part of the `hasqlite_promotion_denied_total` metric
  contract.
- Freshest-first is a preference, not a guarantee; zero-lag ties carry no
  freshness preference.
- A whole-cluster cold restart can promote an eligible replica automatically
  when coordination is available.
- Acknowledged writes absent from the validated snapshot are lost even if
  another replica holds them.

## Alternatives considered

- **Compare snapshot time with the candidate's wall clock for lag**: rejected;
  depends on skew and detection delay.
- **Peer-to-peer catch-up before activation**: deferred to post-v1 until a safe
  peer-transfer protocol exists (contract rule 13).
- **Re-validate after a won CAS**: unnecessary once inputs are frozen.
