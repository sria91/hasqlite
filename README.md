# hasqlite

**High Availability SQLite** — a Rust-native control plane and node runtime for
active–passive SQLite clusters: lease-based single-writer fencing, incremental
page-level replication, and an optional SQL gateway for application traffic.

This repository is a **focused HA product** built on learnings from
[`rsqlite-rsync`](https://github.com/sria91/rsqlite-rsync): bandwidth-efficient rsync-style page sync,
file/Kubernetes lease coordination, freshness-gated promotion, and gRPC SQL
access with leader discovery.

## Status

**Planning / bootstrap.** See [docs/PLAN.md](docs/PLAN.md) for the full
development and test strategy. Application code lands in phased milestones
described there.

## Relationship to rsqlite-rsync

| Concern | rsqlite-rsync (today) | hasqlite (target) |
|--------|------------------------|-------------------|
| Page sync protocol & transports | First-class | Reuse via path/git dependency |
| HA control loop, promotion, fencing | Module in monolith | First-class crates + `hasqlite` binary |
| SQL Gateway & client | Embedded in `--ha` | Node runtime concern; shared proto with rsync |
| CLI sync tool | `rsqlite-rsync` binary | Optional thin wrapper or direct library calls |
| K8s manifests & runbooks | Examples in rsync repo | Opinionated, versioned under `deploy/` |

Long term, durable HA types and orchestration logic should **live here**; sync
and wire protocol remain shared with (or extracted from) rsqlite-rsync to avoid
duplication.

## Quick links

- [Comprehensive plan](docs/PLAN.md)
- [Safety contract (draft)](docs/safety-contract.md)
- [rsqlite-rsync HA guide](https://github.com/sria91/rsqlite-rsync/blob/master/docs/ha-guide.md)

## License

MIT OR Apache-2.0 (aligned with rsqlite-rsync)
