# ADR-0008: Transitional `rsqlite-rsync` adapter, replaced by a native backend

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-DEP-001 to REQ-DEP-004; architecture §4 (dependency-removal plan); ADR-0007

## Context

`rsqlite-rsync` already implements page-hash sync and transport. Reusing it
lets HA control logic and standalone serving ship before hasqlite has its own
replication. But the upstream crate bundles HA logic with sync, lacks origin
stamping and private-CA TLS, and pulls in dependencies with open advisories.
hasqlite must eventually own its replication, wire protocol and client.

## Decision

- `hasqlite-sync` uses `rsqlite-rsync` v0.5.0 as a Git dependency **pinned by
  commit `rev`**, built with `--locked`.
- The adapter is an **isolation boundary**: no upstream types in control,
  gateway or client crates, and public `hasqlite-sync` errors map upstream
  failures to hasqlite-owned error types.
- A native backend (Track N, weeks 5–12) provides snapshot, page diff,
  transfer, origin stamping, atomic application, snapshot deadlines and TLS with
  `sync.tls_ca_file`. TLS and origin stamping are due by week 8 because Phase 4
  (Kubernetes) depends on them.
- The dependency is removed only when: a native backend provides the required
  behaviour; sync and end-to-end tests exercise it without upstream types; the
  client and wire API are implemented without upstream crates; and
  `cargo tree` and the lockfile contain no `rsqlite-rsync`, with a clean build
  needing no access to the upstream repository. A differential suite compares
  page hashes between adapter and native backends before cutover.

## Consequences

- Earlier delivery of controller, gateway and standalone mode.
- Adapter-era constraints persist until cutover: in-place application, no
  replica reads, lower-bound attribution, public-WebPKI-only TLS.
- Advisories in adapter dependencies are fixed upstream and re-pinned;
  `cargo audit` is never suppressed. `libsqlite3-sys` must move in lockstep
  (`links = "sqlite3"`).
- Drop-in compatibility with the upstream sync CLI is not a goal.

## Alternatives considered

- **Build native replication first**: rejected; it would delay every other
  milestone.
- **Depend on upstream permanently**: rejected; hasqlite must own its protocol
  and safety properties.
