# hasqlite

**High Availability SQLite** — a Rust-native control plane and node runtime for
active–passive SQLite clusters: lease-based single-writer fencing, incremental
page-level replication, and an optional SQL gateway for application traffic.

This repository is a **focused HA product** informed by
[`rsqlite-rsync`](https://github.com/sria91/rsqlite-rsync). The current
replication implementation uses `rsqlite-rsync` as a transitional dependency;
the long-term goal is for hasqlite to implement and own replication, wire
protocol, and client functionality without requiring that crate.

## Status

**Planning / bootstrap.** See [docs/PLAN.md](docs/PLAN.md) for the full
development and test strategy. Application code lands in phased milestones
described there.

## Dependency transition

| Concern | Current implementation | Long-term hasqlite goal |
|--------|------------------------|-------------------------|
| Page sync protocol & transports | `hasqlite-sync` uses `rsqlite-rsync` | Native hasqlite implementation |
| HA control loop, promotion, fencing | First-class hasqlite crates | First-class hasqlite crates |
| SQL Gateway & client | Being developed alongside replication | Hasqlite-owned API and client |
| Sync CLI | None yet; transitional replication is a `hasqlite-sync` library integration | Native hasqlite CLI (planned) |
| Standalone single-node in Docker | Fast-tracked deliverable (Phase 1b) | Dedicated container image & config profile |
| K8s manifests & runbooks | Being developed in this repository | Opinionated, versioned under `deploy/` |

The dependency will remain until hasqlite's native replication path and tests
cover the required behavior. The migration and dependency-removal criteria are
tracked in [docs/PLAN.md](docs/PLAN.md).

## Quick links

- [Comprehensive plan](docs/PLAN.md)
- [Safety contract (draft)](docs/safety-contract.md)
- [Prior-art HA guide](https://github.com/sria91/rsqlite-rsync/blob/master/docs/ha-guide.md)

## License

MIT OR Apache-2.0
