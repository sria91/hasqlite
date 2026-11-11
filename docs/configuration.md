# hasqlite — Security and Configuration

## 7. Security defaults and deployment posture

The default deployment posture is intentionally conservative:

- Plaintext SQL and replication traffic is permitted only on loopback or behind a
  trusted TLS-terminating proxy. The gateway MUST NOT be publicly exposed without
  a front-end sidecar or ingress that terminates TLS.
- Clustered mode MUST reject `allow_unauthenticated` even when the bind address is
  loopback, because the sidecar or proxy is still part of the remote path.
- Secret material MUST be read from files or mounted secrets rather than process
  environment variables whenever possible. Environment variables are acceptable
  only when the deployment model specifically requires them; they are never the
  preferred credential source in Kubernetes or Docker-based production use.
- The SQL token and replication token are separate credentials with separate
  scopes; they are never interchangeable.

This section is normative for v1 and is intentionally stricter than a permissive
local-dev default.

---

## 8. Configuration contract (v1)

Unified config file (TOML/YAML) replacing long flag lists:

```toml
[node]
id = "pod-0"

[data]
dir = "/var/lib/hasqlite"

[lease]
source = "kubernetes" # or "file" (single host only: file leases need a local filesystem)
ttl_secs = 10            # candidates wait this long after the last observed lease change (local monotonic clock)
renew_deadline_secs = 6  # writer stops committing this long after sending its last successful renewal
retry_period_secs = 2    # renewal / acquisition attempt interval

# If lease.source = "file":
# [lease.file]
# path = "/var/lib/hasqlite-lease/lease"

# If lease.source = "kubernetes":
[lease.kubernetes]
lease_name = "hasqlite-writer"
namespace = "default"

[freshness]
path = "/var/lib/hasqlite/.hasqlite.freshness" # on the PVC, same filesystem as the database
max_lag_secs = 8 # max lag of the last snapshot behind the writer's last lease renewal (both on the writer's clock)

[promotion]
max_future_skew_secs = 2

[sync]
interval_secs = 2
# Required in clustered mode on every node, including the current writer: any node can be demoted and
# must then resync. Use a stable alias that always routes to the current writer, never a pod hostname.
origin_endpoint = "https://hasqlite-writer:50443" # remote origins require TLS; grpc:// is limited to loopback IPs and `localhost`
# The TLS certificate must cover hasqlite-writer. Private-CA trust needs the native transport (Track N):
# tls_ca_file = "/run/secrets/hasqlite_ca.crt"
# A credential is required whenever origin_endpoint is set (exactly one of the two settings below).
# Prefer the file: environment variables are visible in /proc/*/environ and in pod specs.
transport_auth_token_file = "/run/secrets/hasqlite_replication_token"
# transport_auth_token_env = "HASQLITE_REPLICATION_TOKEN"

# Server-side replication credential, distinct from the SQL gateway token (Phase 1a):
# [replication]
# auth_token_file = "/run/secrets/hasqlite_replication_token"

[gateway]
bind = "127.0.0.1:50051" # Loopback bind when co-located with TLS sidecar (:50443); a direct listener must be protected by a TLS ingress and restricted to the trusted proxy
auth_token_file = "/run/secrets/hasqlite_token"
# auth_token_env = "HASQLITE_GRPC_AUTH_TOKEN"
# allow_unauthenticated is rejected in clustered mode
allow_replica_reads = false # unsupported until Track N applies snapshots atomically

[probes]
http_bind = "0.0.0.0:8088"
```

CLI: `hasqlite run --config cluster.toml` plus env overrides.

Validation (`ClusterConfig::from_toml`) fails closed:

- Unknown keys or sections are rejected, so a misspelled setting cannot silently fall back to its default.
- Clustered mode requires `[freshness]`, `[promotion]`, `[sync]`, and `sync.origin_endpoint`, and
  `freshness.max_lag_secs` must exceed `sync.interval_secs` (otherwise steady-state lag would deny every promotion).
- Lease timing: `retry_period_secs > 0`, `retry_period_secs < renew_deadline_secs`,
  `ttl_secs ≥ renew_deadline_secs + 2` (the fencing margin, [§3](architecture.md#3-architecture), computed with checked arithmetic), and
  `ttl_secs ≤ 3600`, which also bounds the writer's renewal deadline.
- Plaintext `grpc://` origins are accepted only for literal loopback IPs and `localhost`; `*.localhost`
  subdomains are rejected because some resolvers forward them upstream, which would expose the token.
- `sync.origin_endpoint` requires a sync credential, and the gateway requires a credential unless
  `allow_unauthenticated = true` is set on a loopback bind in standalone mode. Clustered mode rejects
  `allow_unauthenticated`: the TLS sidecar forwards remote traffic to the loopback listener, which also serves replication.
- Phase 1a adds: the replication credential must differ from the gateway credential; startup must compare the resolved replication and gateway token values and reject a match (distinct environment variable names or file paths alone do not satisfy the separation requirement).

### Standalone single-node profile (Docker / edge)

For running a single standalone node in Docker without replication peers:

```toml
[node]
id = "standalone-0"
mode = "standalone" # Auto-promotes to genesis writer at generation g_prior + 1 (1 on first boot)

[data]
dir = "/var/lib/hasqlite"

[lease]
source = "file" # Standalone mode strictly requires file-backed lease

[lease.file]
# Standalone mode enforces this exact canonical path within data.dir to guarantee mutual exclusion.
# It is the never-replaced lock file; the record lives in .hasqlite.lease.record next to it.
path = "/var/lib/hasqlite/.hasqlite.lease"

# Note: [freshness], [promotion], and [sync] sections are omitted in standalone mode

[gateway]
bind = "0.0.0.0:50051" # Direct listener; use "127.0.0.1:50051" when co-located with TLS sidecar
auth_token_file = "/run/secrets/hasqlite_token" # Mutually exclusive with auth_token_env
# auth_token_env = "HASQLITE_GRPC_AUTH_TOKEN"
# allow_unauthenticated = false  # standalone only, loopback bind only
allow_replica_reads = false

[probes]
http_bind = "0.0.0.0:8088"
```

### Standalone Docker invocation

> [!NOTE]
> **Phase 1b Target**: The Docker invocation below illustrates the planned deployment workflow once the Phase 1b runtime and packaging land. (In the current Phase 0 repository, the binary outputs a bootstrap log and exits).
>
> All required configuration fields (`node.id`, `data.dir`, `lease.source`, `gateway.bind`, `probes.http_bind`) are supplied by mounting the standalone TOML configuration. In Phase 1b, the image entrypoint will also support synthesizing minimal standalone defaults if config mounts are omitted.
>
> **Configurable Non-Root User & Volume Permissions**: The image defaults to UID:GID `1000:1000` (named `hasqlite:hasqlite` in the debian-slim variant). Custom UID/GID values can be configured at build time using `--build-arg UID=<uid> --build-arg GID=<gid>`; `--build-arg USER=<user> --build-arg GROUP=<group>` set the names where the base image has passwd tooling. Note that build arguments configure the image's internal user identity and default directory permissions, but do not change ownership or permissions on host bind mounts. When mounting a host directory to `/var/lib/hasqlite`, users must ensure the host directory and any existing database/lease files grant write access to the runtime identity (e.g., `chown -R <uid>:<gid> /path/to/host/data`, or assigning group ownership `chgrp -R <gid> /path/to/host/data` paired with `chmod -R 0770 /path/to/host/data`). When running under rootless Docker or user namespace remapping (`userns-remap`), container UIDs/GIDs are translated to subordinate host ID ranges defined in `/etc/subuid` and `/etc/subgid` (or mapped to the rootless daemon user); operators must determine the translated host UID/GID and apply ownership or permissions using those mapped host IDs rather than container-internal values. The image configures numeric `USER ${UID}:${GID}` so runtime `-u` flags are unnecessary unless explicitly overriding the container user at runtime.
>
> **Secret File Permissions**: When using `auth_token_file` or `transport_auth_token_file` with mounted secrets (e.g. `/run/secrets/hasqlite_token`), ensure each node that reads the secret can access it as the configured runtime UID/GID (or mapped host ID under rootless/userns-remap) without exposing credentials to other local users. For Kubernetes pods running as UID 1000, configure a restricted `fsGroup` and use mode `0440` so only the node UID and that group can read the token; do not use world-readable permissions (`0444`).
>
> **Plaintext Gateway & Network Security**: The gRPC SQL Gateway communicates in plaintext in v1 and does not provide built-in TLS termination. The host port mapping binds strictly to host loopback (`127.0.0.1:50051:50051`) to avoid publishing the unencrypted gateway on public interfaces. Any remote or multi-host access MUST traverse an external TLS termination reverse proxy or ingress sidecar (such as Envoy, Traefik, or Nginx).

```bash
# Optional: build with custom non-root user/group and UID/GID (defaults to hasqlite:hasqlite and 1000:1000 if omitted)
# docker build \
#   --build-arg USER=customuser \
#   --build-arg GROUP=customgroup \
#   --build-arg UID=1001 \
#   --build-arg GID=1001 \
#   -t hasqlite:${HASQLITE_VERSION:-0.1.0} \
#   -f deploy/docker/Dockerfile .

docker run -d \
  --name hasqlite \
  -p 127.0.0.1:50051:50051 \
  -p 127.0.0.1:8088:8088 \
  -v /path/to/standalone.toml:/etc/hasqlite/cluster.toml:ro \
  -v hasqlite-data:/var/lib/hasqlite \
  -v /path/to/token.txt:/run/secrets/hasqlite_token:ro \
  hasqlite:${HASQLITE_VERSION:-0.1.0} \
  run --config /etc/hasqlite/cluster.toml
```

---
