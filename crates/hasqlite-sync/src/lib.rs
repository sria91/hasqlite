//! Sync orchestration policies (Phase 2).

/// Sync scheduler policy identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncTrigger {
    Periodic,
    PreShutdown,
    PostPromoteCatchUp,
    OnLeaseChange,
}

/// Sync execution errors encountered during scheduled or triggered replication.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum SyncError {
    #[error("sync origin unreachable: {endpoint}")]
    OriginUnreachable { endpoint: String },
    #[error("sync timed out after {timeout_secs}s")]
    Timeout { timeout_secs: u64 },
    #[error("authentication failed with origin endpoint")]
    AuthFailed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_triggers_equality() {
        assert_eq!(SyncTrigger::Periodic, SyncTrigger::Periodic);
        assert_ne!(SyncTrigger::PreShutdown, SyncTrigger::Periodic);
    }

    #[test]
    fn sync_error_display() {
        let err = SyncError::OriginUnreachable {
            endpoint: "grpc://10.0.0.1:50051".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "sync origin unreachable: grpc://10.0.0.1:50051"
        );
    }
}
