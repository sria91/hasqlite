//! Sync orchestration policies (Phase 2).
//!
//! Replication currently delegates to `rsqlite-rsync`; this integration is
//! transitional and is replaced when the native backend meets the cutover
//! criteria in `docs/PLAN.md`.

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
    /// The origin rejected the configured credential.
    #[error("authentication failed with origin endpoint: {message}")]
    AuthFailed {
        /// Transport error reported for the rejection (never contains the token).
        message: String,
    },
    /// Sync configuration is invalid or does not specify an origin endpoint.
    #[error("invalid sync configuration: {0}")]
    InvalidConfiguration(String),
    /// The configured authentication environment variable is not set.
    #[error("sync authentication environment variable '{name}' is not set")]
    AuthTokenEnvNotSet {
        /// Environment variable name that was not found in the process environment.
        name: String,
    },
    /// The configured authentication environment variable contains non-UTF-8 data.
    #[error("sync authentication environment variable '{name}' contains invalid Unicode")]
    AuthTokenEnvNotUnicode {
        /// Environment variable name whose value could not be decoded.
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
    /// The underlying replication pull failed; the cause is available via `Error::source`.
    #[error("replication pull failed")]
    Replication(#[source] ReplicationError),
}

/// Hasqlite-owned error categories for replication backend failures.
#[derive(thiserror::Error, Debug)]
pub enum ReplicationError {
    /// A replication network request failed.
    #[error("replication network error: {0}")]
    Network(String),
    /// The replication backend failed for a non-network reason.
    #[error("replication operation failed: {0}")]
    Operation(String),
}

/// Maps an adapter error into the hasqlite-owned replication error categories.
fn map_replication_error(error: rsqlite_rsync::error::SyncError) -> ReplicationError {
    match error {
        rsqlite_rsync::error::SyncError::Network(message) => ReplicationError::Network(message),
        error => ReplicationError::Operation(error.to_string()),
    }
}

/// Description of the gRPC `Unauthenticated` status, which rsqlite-rsync embeds in its `Network` error when the origin rejects the token.
///
/// rsqlite-rsync has no typed auth variant and stringifies the tonic status, so this match is pinned by the
/// `pull_replica_reports_rejected_token_as_auth_failure` integration test.
const UNAUTHENTICATED_DESCRIPTION: &str = "does not have valid authentication credentials";

/// Maps an adapter pull error, surfacing origin credential rejection as [`SyncError::AuthFailed`].
fn map_pull_error(error: rsqlite_rsync::error::SyncError) -> SyncError {
    match error {
        rsqlite_rsync::error::SyncError::Network(message)
            if message.contains(UNAUTHENTICATED_DESCRIPTION) =>
        {
            SyncError::AuthFailed { message }
        }
        error => SyncError::Replication(map_replication_error(error)),
    }
}

/// Pulls a database replica from the configured origin.
///
/// The configured environment or file credential is re-read on every call, so a rotated token takes
/// effect on the next pull, and is passed to the replication client, which attaches it to the
/// authenticated gRPC request.
///
/// The transitional backend writes pages into `replica_path` in place, so an interrupted pull leaves a
/// torn file. Callers must invalidate the freshness ledger before calling and publish a new one only
/// after this returns `Ok`; on success the replica and its directory entry have been fsynced.
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
        .map_err(map_pull_error)?;
    fsync_replica(replica_path).await
}

/// Flushes the replica file and, on Unix, its parent directory so a published ledger never outlives the data.
async fn fsync_replica(replica_path: &Path) -> Result<(), SyncError> {
    let fsync_error = |path: &Path, error: std::io::Error| {
        SyncError::Replication(ReplicationError::Operation(format!(
            "failed to fsync '{}': {error}",
            path.display()
        )))
    };
    // Windows `FlushFileBuffers` requires write access, so a read-only handle cannot be synced there.
    tokio::fs::OpenOptions::new()
        .write(true)
        .open(replica_path)
        .await
        .map_err(|error| fsync_error(replica_path, error))?
        .sync_all()
        .await
        .map_err(|error| fsync_error(replica_path, error))?;

    #[cfg(unix)]
    if let Some(parent) = replica_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::File::open(parent)
            .await
            .map_err(|error| fsync_error(parent, error))?
            .sync_all()
            .await
            .map_err(|error| fsync_error(parent, error))?;
    }
    Ok(())
}

/// Loads and trims the configured sync token from its environment variable or file, if one is configured.
async fn load_sync_auth_token(sync: &SyncSection) -> Result<Option<String>, SyncError> {
    let token = match (
        sync.transport_auth_token_env.as_deref(),
        sync.transport_auth_token_file.as_deref(),
    ) {
        (Some(name), None) => Some(std::env::var(name).map_err(|e| match e {
            std::env::VarError::NotPresent => SyncError::AuthTokenEnvNotSet {
                name: name.to_string(),
            },
            std::env::VarError::NotUnicode(_) => SyncError::AuthTokenEnvNotUnicode {
                name: name.to_string(),
            },
        })?),
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

    /// Sync section with a remote origin and no credential source.
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
        let directory = tempfile::tempdir().expect("create temporary directory");
        let token_path = directory.path().join("sync-token");
        // The invalid UTF-8 byte makes `read_to_string` fail after the token bytes are already on disk.
        std::fs::write(&token_path, b"test-replication-token\xff").expect("write token");
        let mut sync = sync_config();
        sync.transport_auth_token_file = Some(token_path);

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

    // Each env-var test uses its own variable name so parallel tests never observe each other's mutations.
    #[tokio::test(flavor = "current_thread")]
    async fn sync_auth_token_is_loaded_from_env() {
        const NAME: &str = "HASQLITE_TEST_SYNC_TOKEN_LOADED";
        // SAFETY: nothing in this test binary reads the environment through non-Rust code (e.g. libc
        // `getenv`) concurrently; Rust-side reads are serialized by std's environment lock.
        unsafe { std::env::set_var(NAME, " test-replication-token \n") };
        let mut sync = sync_config();
        sync.transport_auth_token_env = Some(NAME.into());

        assert_eq!(
            load_sync_auth_token(&sync)
                .await
                .expect("load token")
                .as_deref(),
            Some("test-replication-token")
        );
    }

    #[tokio::test]
    async fn sync_auth_token_env_missing_is_reported_by_name() {
        const NAME: &str = "HASQLITE_TEST_SYNC_TOKEN_MISSING";
        let mut sync = sync_config();
        sync.transport_auth_token_env = Some(NAME.into());

        assert!(matches!(
            load_sync_auth_token(&sync).await.unwrap_err(),
            SyncError::AuthTokenEnvNotSet { name } if name == NAME
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_auth_token_env_rejects_blank_value() {
        const NAME: &str = "HASQLITE_TEST_SYNC_TOKEN_BLANK";
        // SAFETY: see `sync_auth_token_is_loaded_from_env`.
        unsafe { std::env::set_var(NAME, "  ") };
        let mut sync = sync_config();
        sync.transport_auth_token_env = Some(NAME.into());

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
        let err = SyncError::AuthFailed {
            message: "rejected".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "authentication failed with origin endpoint: rejected"
        );
    }

    #[test]
    fn unauthenticated_network_error_maps_to_auth_failed() {
        let error = map_pull_error(rsqlite_rsync::error::SyncError::Network(
            "failed to establish gRPC stream: code: 'The request does not have valid authentication credentials', message: \"missing or invalid authorization credentials\""
                .to_string(),
        ));
        assert!(matches!(
            error,
            SyncError::AuthFailed { message } if message.contains("missing or invalid authorization credentials")
        ));
    }

    #[test]
    fn other_network_errors_stay_replication_network_errors() {
        let error = map_pull_error(rsqlite_rsync::error::SyncError::Network(
            "failed to connect to gRPC server: connection refused".to_string(),
        ));
        assert!(matches!(
            error,
            SyncError::Replication(ReplicationError::Network(_))
        ));
    }

    #[test]
    fn replication_error_is_exposed_only_as_source() {
        let error = map_pull_error(rsqlite_rsync::error::SyncError::Network(
            "connection refused".to_string(),
        ));
        assert_eq!(error.to_string(), "replication pull failed");
        let source = std::error::Error::source(&error).expect("retain the replication error");
        assert!(source.is::<ReplicationError>());
        assert_eq!(
            source.to_string(),
            "replication network error: connection refused"
        );
    }
}
