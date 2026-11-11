//! Rust client SDK: automatic `NOT_LEADER` failover that never silently retries a write whose outcome
//! is unknown. Implementation follows the Phase 1a wire API.
//!
//! Registered in the workspace ahead of implementation so lint, docstring, test and coverage gates
//! apply from the first line of client code.
#![forbid(unsafe_code)]
