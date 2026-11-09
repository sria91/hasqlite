//! Sync orchestration policies (Phase 2).

/// Sync scheduler policy identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyncTrigger {
    /// Scheduled periodic background replication.
    Periodic,
    /// Pre-shutdown catch-up replication to minimize RPO.
    PreShutdown,
    /// Post-promotion catch-up synchronization.
    PostPromoteCatchUp,
    /// Triggered replication on observed writer lease transition.
    OnLeaseChange,
}

/// Sync execution errors encountered during scheduled or triggered replication.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum SyncError {
    /// Remote origin endpoint is unreachable or refused connection.
    #[error("sync origin unreachable: {endpoint}")]
    OriginUnreachable {
        /// The origin endpoint that failed to connect.
        endpoint: String,
    },
    /// Replication RPC exceeded the allotted timeout.
    #[error("sync timed out after {timeout_secs}s")]
    Timeout {
        /// Timeout duration in seconds.
        timeout_secs: u64,
    },
    /// Authentication failed when connecting to the origin gRPC service.
    #[error("authentication failed with origin endpoint")]
    AuthFailed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_triggers_equality_and_hashing() {
        use std::collections::HashSet;

        assert_eq!(SyncTrigger::Periodic, SyncTrigger::Periodic);
        assert_ne!(SyncTrigger::PreShutdown, SyncTrigger::Periodic);

        let mut triggers = HashSet::new();
        triggers.insert(SyncTrigger::Periodic);
        triggers.insert(SyncTrigger::PreShutdown);
        triggers.insert(SyncTrigger::PostPromoteCatchUp);
        triggers.insert(SyncTrigger::OnLeaseChange);
        assert_eq!(triggers.len(), 4);
        assert!(triggers.contains(&SyncTrigger::Periodic));
    }

    #[test]
    fn sync_error_display() {
        let err1 = SyncError::OriginUnreachable {
            endpoint: "grpc://10.0.0.1:50051".to_string(),
        };
        assert_eq!(
            err1.to_string(),
            "sync origin unreachable: grpc://10.0.0.1:50051"
        );

        let err2 = SyncError::Timeout { timeout_secs: 15 };
        assert_eq!(err2.to_string(), "sync timed out after 15s");

        let err3 = SyncError::AuthFailed;
        assert_eq!(
            err3.to_string(),
            "authentication failed with origin endpoint"
        );
    }
}
