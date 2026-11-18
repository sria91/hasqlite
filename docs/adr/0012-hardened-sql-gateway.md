# ADR-0012: Hardened SQL gateway classified by SQLite, not by SQL text

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-GW-001 to REQ-GW-007, REQ-FENCE-002; safety contract rule 17; ADR-0003

## Context

The gateway exposes SQL over gRPC. Text inspection misclassifies CTE-wrapped
`INSERT`s, comment-prefixed DML and similar forms. Some SQLite features can
escape the data directory or bypass fencing: `ATTACH`, `VACUUM INTO`,
`PRAGMA writable_schema`, `load_extension`.

## Decision

- Classify statements with `sqlite3_stmt_readonly()` **after preparation**;
  never inspect SQL text.
- Gate every mutating statement with `enforce_write_fence` before `COMMIT`.
- Replica connections are read-only with `PRAGMA query_only = 1`.
- An authorizer denies `ATTACH`, `DETACH`, `VACUUM INTO` and non-allowlisted
  pragmas. `SQLITE_DBCONFIG_DEFENSIVE` on, extension loading off,
  `trusted_schema` off, `sqlite3_limit` caps on SQL length and attached
  databases, a progress handler bounding run time, and `sqlite3_interrupt` on
  demotion or shutdown.
- Database names resolve strictly inside `data.dir` (no traversal, no symlink
  escape; `diverged/` and `ancestors/` excluded).
- Hardening ships in Phase 1b and is not deferrable.

## Consequences

- Applications get a restricted SQLite surface, not every feature.
- Classification is exact for any statement SQLite can prepare.
- A payload suite must run against both writer and replica gateways.

## Alternatives considered

- **SQL text parsing or keyword allowlists**: rejected; bypassable.
- **Trust clients**: rejected; fencing must not depend on client behaviour.
