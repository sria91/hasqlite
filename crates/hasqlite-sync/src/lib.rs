//! Sync orchestration policies (Phase 2).

use hasqlite_core::SyncSection;
use std::path::Path;

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
#[derive(thiserror::Error, Debug)]
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
    /// Sync configuration is invalid or does not specify an origin endpoint.
    #[error("invalid sync configuration: {0}")]
    InvalidConfiguration(String),
    /// The configured authentication environment variable is unavailable.
    #[error("sync authentication environment variable '{name}' is unavailable")]
    AuthTokenEnvironmentVariable {
        /// Environment variable name configured for the sync token.
        name: String,
    },
    /// Reading the configured token file failed.
    #[error("failed to read sync authentication token file '{path}': {message}")]
    AuthTokenFile {
        /// Configured token file path.
        path: String,
        /// File read error, without including the token contents.
        message: String,
    },
    /// A configured authentication source contained no token.
    #[error("sync authentication token must not be empty")]
    EmptyAuthToken,
    /// The underlying replication pull failed.
    #[error("replication pull failed: {0}")]
    Replication(#[source] rsqlite_rsync::error::SyncError),
}

/// Pulls a database replica from the configured origin.
///
/// The configured environment or file credential is loaded and passed to the
/// replication client, which attaches it to the authenticated gRPC request.
pub async fn pull_replica(
    sync: &SyncSection,
    replica_path: &Path,
    database: &str,
) -> Result<(), SyncError> {
    sync.validate()
        .map_err(|error| SyncError::InvalidConfiguration(error.to_string()))?;
    let endpoint = sync.origin_endpoint.as_deref().ok_or_else(|| {
        SyncError::InvalidConfiguration("sync.origin_endpoint is required for replication".into())
    })?;
    let auth_token = load_sync_auth_token(sync).await?;

    rsqlite_rsync::grpc_pull_sync(replica_path, endpoint, database, auth_token.as_deref())
        .await
        .map_err(SyncError::Replication)
}

async fn load_sync_auth_token(sync: &SyncSection) -> Result<Option<String>, SyncError> {
    let token = match (
        sync.transport_auth_token_env.as_deref(),
        sync.transport_auth_token_file.as_deref(),
    ) {
        (Some(name), None) => {
            Some(
                std::env::var(name).map_err(|_| SyncError::AuthTokenEnvironmentVariable {
                    name: name.to_string(),
                })?,
            )
        }
        (None, Some(path)) => Some(tokio::fs::read_to_string(path).await.map_err(|error| {
            SyncError::AuthTokenFile {
                path: path.display().to_string(),
                message: error.to_string(),
            }
        })?),
        (None, None) => None,
        (Some(_), Some(_)) => {
            return Err(SyncError::InvalidConfiguration(
                "sync authentication environment variable and file are mutually exclusive".into(),
            ));
        }
    };

    token
        .map(|token| {
            let token = token.trim().to_string();
            if token.is_empty() {
                Err(SyncError::EmptyAuthToken)
            } else {
                Ok(token)
            }
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn sync_config() -> SyncSection {
        SyncSection {
            interval_secs: 1,
            origin_endpoint: Some("https://writer:50443".into()),
            transport_auth_token_env: None,
            transport_auth_token_file: None,
        }
    }

    #[tokio::test]
    async fn sync_auth_token_is_loaded_from_file() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let token_path = directory.path().join("sync-token");
        std::fs::write(&token_path, "test-replication-token\n").expect("write token");
        let mut sync = sync_config();
        sync.transport_auth_token_file = Some(token_path);

        assert_eq!(
            load_sync_auth_token(&sync)
                .await
                .expect("load token")
                .as_deref(),
            Some("test-replication-token")
        );
    }

    #[tokio::test]
    async fn sync_auth_token_file_errors_do_not_expose_contents() {
        let mut sync = sync_config();
        sync.transport_auth_token_file = Some(PathBuf::from("/missing/sync-token"));

        let error = load_sync_auth_token(&sync).await.unwrap_err();
        assert!(matches!(error, SyncError::AuthTokenFile { .. }));
        assert!(!error.to_string().contains("test-replication-token"));
    }

    #[tokio::test]
    async fn sync_auth_token_file_rejects_empty_token() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let token_path = directory.path().join("sync-token");
        std::fs::write(&token_path, " \n").expect("write token");
        let mut sync = sync_config();
        sync.transport_auth_token_file = Some(token_path);

        assert!(matches!(
            load_sync_auth_token(&sync).await.unwrap_err(),
            SyncError::EmptyAuthToken
        ));
    }

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
