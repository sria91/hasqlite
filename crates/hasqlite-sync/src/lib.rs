//! Sync orchestration policies (Phase 2).

/// Sync scheduler policy identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncTrigger {
    Periodic,
    PreShutdown,
    PostPromoteCatchUp,
    OnLeaseChange,
}
