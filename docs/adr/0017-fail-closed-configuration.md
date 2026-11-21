# ADR-0017: Fail-closed configuration validation

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-CFG-001 to REQ-CFG-009; configuration §8; ADR-0003, ADR-0013

## Context

Lease timing, freshness limits and credentials interact: a misspelled key that
silently falls back to a default, or a TTL shorter than the renewal deadline
plus margin, would break safety without any visible error. Identifiers are
embedded in lease records, ledgers, audit lines and metric labels.

## Decision

`ClusterConfig::from_toml` (one TOML file replacing long flag lists)
rejects, at startup:

- unknown keys or sections;
- missing `[freshness]`, `[promotion]`, `[sync]` or `sync.origin_endpoint` in
  clustered mode, and `sync.origin_endpoint` in standalone mode;
- invalid identifiers (`node.id` 1–253 of `[A-Za-z0-9._-]`; DNS-1123 Kubernetes
  names);
- lease timing violating `retry > 0`, `2 × retry ≤ renew_deadline`,
  `ttl ≥ renew_deadline + 2` (checked arithmetic) or `ttl ≤ 3600`;
- `max_lag_secs ≤ interval_secs + retry_period_secs`;
- the credential-source, plaintext-origin and authentication rules from
  ADR-0013 that the file alone determines (exactly one source per credential,
  plaintext origins only on loopback, no unauthenticated clustered gateway);
- a standalone lease path other than the canonical one (ADR-0015).

Rules that need resolved secret values are not `from_toml` checks. The
startup step that resolves both credentials rejects identical SQL and
replication token values with a constant-time comparison (ADR-0013). That
step lands with the Phase 1a credential-resolution path; it does not exist
yet.

## Consequences

- Misconfiguration fails at start, not during a failover.
- Validation logic is unit-tested with table-driven cases.

## Alternatives considered

- **Warn and continue**: rejected; safety-relevant defaults must not apply
  silently.
