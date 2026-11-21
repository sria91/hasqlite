# ADR-0019: v1 fails closed; automated recovery is post-v1

- Status: Accepted
- Date: 2026-11-17
- Related: [automated-recovery.md](../automated-recovery.md); operations §10.3, §10.6; ADR-0005, ADR-0010, ADR-0011

## Context

Some failure states have no safe automatic answer within the v1 design: the
activated writer has failed while every replica is beyond the lag limit; the
activated writer died before any replica synced from it; a replica's snapshot
is torn or corrupt; a former writer holds diverged commits. Two related states
are handled differently: a client write whose outcome is unknown, and a final
sync that timed out during a planned switchover.

## Decision

In v1 the states listed first **fail closed** and require an operator,
guided by runbooks (failover, planned switchover, manual promotion, stale
replica rebuild, diverged data recovery). Stale replicas need an operator only
once the activated writer has failed; while it is still serving, catch-up sync
from it restores eligibility (operations §10.3).

The two related states are not operator cases in this sense:

- **Unknown client outcome**: the client surfaces the error to the
  application and does not retry automatically (ADR-0014); the application
  decides what to do.
- **Timed-out final sync**: the safety guarantees still hold. The switchover
  is not reported as lossless, and the former writer preserves its databases
  before resyncing (safety contract rules 15 and 16). Whether such a handover
  should abort or proceed with bounded loss is unresolved (operations §10.6,
  OPEN-001).

Automated recovery is specified separately for post-v1, in tiers:

1. Staged atomic snapshots, replica auto-quarantine and re-seed, in-database
   replicated idempotency.
2. Emergency stale-replica promotion with peer quorum, dead-epoch roll-forward,
   clock-skew auto-backoff, peer-to-peer post-promotion catch-up.
3. Changeset capture and automated divergence reconciliation.
4. Planned-switchover fallback policies.

Post-v1 mechanisms operate within, or explicitly parameterise, the safety
contract.

## Consequences

- v1 can be unavailable in states where an automated system would recover
  with bounded loss.
- Each recovery mechanism can be added later as an explicit, audited,
  opt-in transition.

## Alternatives considered

- **Automatic stale promotion in v1**: rejected; unbounded or silent loss.
