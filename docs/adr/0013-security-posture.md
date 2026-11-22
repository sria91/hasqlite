# ADR-0013: External TLS termination and separate SQL and replication credentials

- Status: Accepted
- Date: 2026-11-17
- Related: REQ-SEC-001 to REQ-SEC-011; safety contract rule 18; configuration §7; ADR-0012

## Context

Remote SQL and replication traffic carries data and bearer tokens. Built-in
TLS termination in the gateway is extra scope for v1, while meshes and proxies
are common in target deployments. A leaked SQL token must not grant
replication access, and the reverse. Environment variables leak through
`/proc/*/environ` and pod specs.

## Decision

- The gateway has **no built-in TLS termination** in v1. Remote traffic goes
  through a TLS-terminating sidecar or ingress (for example `:50443` →
  `127.0.0.1:50051`). Plaintext `grpc://` origins are allowed only for literal
  loopback IPs and `localhost` (`*.localhost` rejected).
- Bearer authentication is **mandatory by default** and independent of
  fencing. Unauthenticated gateways are allowed only in standalone mode on a
  loopback bind; clustered mode rejects `allow_unauthenticated` even on
  loopback, because the sidecar forwards remote traffic there.
- SQL and replication use **distinct credentials** with separate scopes,
  compared in constant time. Distinctness is enforced at startup and on every
  rotation: startup rejects identical resolved values. At runtime the node
  holds both credentials as one pair. When either source changes, it reads
  both under one lock and installs the new pair only if the resolved values
  differ. On a match it keeps the previous pair active, rejects the change
  and raises an alert. Serialized reloads that always read both sources mean
  two concurrent rotations to the same value cannot each pass by comparing
  against the other's old value.
- Secrets are read from **files** by preference; environment variables only
  where the deployment requires them. Tokens never appear in logs or errors.
  A valid rotated token that differs from the other credential takes effect
  without a restart.
- Private-CA trust (`sync.tls_ca_file`, optional `sync.tls_server_name`) comes
  with the native transport; no plaintext fallback.

## Consequences

- Operators must deploy and configure a TLS proxy for any remote access.
- The sidecar-to-gateway hop is plaintext on loopback; anything sharing the
  pod network namespace can observe traffic, including tokens.
- Kubernetes delivery depends on native TLS (the adapter trusts only public
  WebPKI roots).

## Alternatives considered

- **Built-in TLS in v1**: deferred (non-goal for v1).
- **One shared token**: rejected; widens blast radius of a leak.
