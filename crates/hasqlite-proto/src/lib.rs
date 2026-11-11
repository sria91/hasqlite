//! hasqlite-owned wire API definitions (Phase 1a): SQL gateway and replication services.
//!
//! Registered in the workspace ahead of implementation so lint, docstring, test and coverage gates
//! apply from the first line of protocol code. Protocol definitions land with Phase 1a.
#![forbid(unsafe_code)]

/// Wire protocol version this crate implements; version N serves N-1 clients and replicas.
pub const PROTOCOL_VERSION: u32 = 1;
