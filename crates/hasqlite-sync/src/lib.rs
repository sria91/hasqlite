//! Sync orchestration policies (Phase 2).
//!
//! Replication currently delegates to `rsqlite-rsync`; this integration is
//! transitional and is replaced when the native backend meets the cutover
//! criteria in `docs/PLAN.md`.
//!
//! The crash-safe ledger protocol (ADR-0007, safety contract rule 9) is enforced by types:
//!
//! 1. [`LedgerStore::begin_pull`] claims exclusive use of the replica file and invalidates the database's
//!    ledger record durably, returning a [`PullPermit`].
//! 2. [`pull_replica`] consumes the permit, applies the pull and fsyncs the replica, returning a
//!    [`PulledReplica`].
//! 3. [`PulledReplica::publish`] writes the new ledger record with temp file + rename + directory fsync.
//!
//! A pull therefore cannot start without an invalidated ledger, and a record cannot be published for a
//! pull that has not completed and been made durable.
#![forbid(unsafe_code)]

use hasqlite_core::{DatabaseName, SyncSection};
use hasqlite_lease::{DatabaseSet, FreshnessLedger};
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use tokio::io::AsyncReadExt;
use zeroize::Zeroizing;

/// Maximum size of a sync token file; larger files are rejected rather than read into memory.
pub const MAX_AUTH_TOKEN_FILE_BYTES: u64 = 4_096;
/// Suffix appended to a replica path to name its cross-process pull lock file.
pub const PULL_LOCK_SUFFIX: &str = ".pull-lock";

/// Sync scheduler policy identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyncTrigger {
    /// Scheduled periodic background replication.
    Periodic,
    /// Pre-shutdown catch-up replication to minimize RPO.
    PreShutdown,
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
    /// Another pull into the same replica path is still running, in this process or another one (safety
    /// contract rule 7: a replica file has exactly one writer, the sync engine).
    #[error("a replication pull into '{path}' is already in progress")]
    SyncInProgress {
        /// Replica path that is being written by the other pull.
        path: String,
    },
    /// Reading, invalidating or publishing a freshness ledger record failed; the record stays invalidated.
    #[error("freshness ledger operation on '{path}' failed: {message}")]
    Ledger {
        /// Ledger file or directory involved.
        path: String,
        /// Underlying error.
        message: String,
    },
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

/// Builds a [`SyncError::Ledger`] for `path`.
fn ledger_error(path: &Path, error: impl std::fmt::Display) -> SyncError {
    SyncError::Ledger {
        path: path.display().to_string(),
        message: error.to_string(),
    }
}

/// The freshness ledger directory (`freshness.dir`): one record per database, named
/// [`DatabaseName::encoded_file_name`] (ADR-0020, REQ-MDB-010).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerStore {
    dir: PathBuf,
}

impl LedgerStore {
    /// Uses `dir` as the ledger directory; pre-flight is responsible for its ownership, mode and filesystem.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// Returns the path of `database`'s ledger record.
    pub fn record_path(&self, database: &DatabaseName) -> PathBuf {
        self.dir.join(database.encoded_file_name())
    }

    /// Reads `database`'s ledger record.
    ///
    /// A missing record is `Ok(None)`. A record that cannot be parsed, or that names a different database,
    /// is also `Ok(None)`: it is treated as invalidated, which denies promotion (fail closed). Other read
    /// errors are returned.
    pub async fn read(
        &self,
        database: &DatabaseName,
    ) -> Result<Option<FreshnessLedger>, SyncError> {
        let path = self.record_path(database);
        let contents = match tokio::fs::read_to_string(&path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(ledger_error(&path, error)),
        };
        Ok(toml::from_str::<FreshnessLedger>(&contents)
            .ok()
            .filter(|record| &record.database == database))
    }

    /// Reads the record of every database in `set`, omitting databases without a valid record, in the
    /// shape `validate_promotion` expects.
    pub async fn read_set(
        &self,
        set: &DatabaseSet,
    ) -> Result<BTreeMap<DatabaseName, FreshnessLedger>, SyncError> {
        let mut records = BTreeMap::new();
        for entry in set.entries() {
            if let Some(record) = self.read(&entry.name).await? {
                records.insert(entry.name.clone(), record);
            }
        }
        Ok(records)
    }

    /// Claims exclusive use of `replica_path` for a pull of `database` and durably invalidates that
    /// database's ledger record (REQ-SYNC-002, REQ-SYNC-005).
    ///
    /// A pull already in progress in this process is refused before any I/O. A pull held by another
    /// process is detected through an advisory lock on `<replica_path>.pull-lock`; the lock file is never
    /// deleted, since unlinking a lock file races with processes that have it open. Both cases return
    /// [`SyncError::SyncInProgress`] without touching the ledger or the replica.
    pub async fn begin_pull(
        &self,
        replica_path: &Path,
        database: DatabaseName,
    ) -> Result<PullPermit, SyncError> {
        let guard = ActivePullGuard::acquire(replica_path)?;
        let lock = lock_replica(replica_path).await?;
        self.invalidate(&database).await?;
        Ok(PullPermit {
            replica_path: replica_path.to_path_buf(),
            database,
            store: self.clone(),
            _lock: lock,
            _guard: guard,
        })
    }

    /// Removes `database`'s record (a missing record is already invalid) and fsyncs the directory.
    async fn invalidate(&self, database: &DatabaseName) -> Result<(), SyncError> {
        let path = self.record_path(database);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(ledger_error(&path, error)),
        }
        fsync_dir(&self.dir).await
    }
}

/// Exclusive, ledger-invalidated claim on a replica path, required to start a pull.
#[derive(Debug)]
pub struct PullPermit {
    replica_path: PathBuf,
    database: DatabaseName,
    store: LedgerStore,
    _lock: std::fs::File,
    _guard: ActivePullGuard,
}

impl PullPermit {
    /// Replica file the pull writes into.
    pub fn replica_path(&self) -> &Path {
        &self.replica_path
    }

    /// Database being pulled.
    pub fn database(&self) -> &DatabaseName {
        &self.database
    }
}

/// A completed pull whose replica file and directory have been fsynced; it still holds the exclusive claim.
#[derive(Debug)]
pub struct PulledReplica {
    permit: PullPermit,
}

impl PulledReplica {
    /// Database that was pulled.
    pub fn database(&self) -> &DatabaseName {
        &self.permit.database
    }

    /// Publishes `record` as the database's new ledger record with temp file + fsync + rename + directory
    /// fsync (REQ-SYNC-006), then releases the claim. On error the record stays invalidated.
    pub async fn publish(self, record: &FreshnessLedger) -> Result<(), SyncError> {
        let store = &self.permit.store;
        let path = store.record_path(&self.permit.database);
        if record.database != self.permit.database {
            return Err(ledger_error(
                &path,
                format!(
                    "record describes database '{}', not the pulled '{}'",
                    record.database, self.permit.database
                ),
            ));
        }
        let contents = toml::to_string(record).map_err(|error| ledger_error(&path, error))?;
        let mut temp_name = path.file_name().unwrap_or_default().to_os_string();
        temp_name.push(".tmp");
        let temp = path.with_file_name(temp_name);
        let write = async {
            let mut file = tokio::fs::File::create(&temp).await?;
            tokio::io::AsyncWriteExt::write_all(&mut file, contents.as_bytes()).await?;
            file.sync_all().await?;
            tokio::fs::rename(&temp, &path).await
        };
        if let Err(error) = write.await {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(ledger_error(&temp, error));
        }
        fsync_dir(&store.dir).await
    }
}

/// Pulls a database replica from the configured origin into the permit's replica path.
///
/// The configured environment or file credential is re-read on every call, so a rotated token takes
/// effect on the next pull, and is passed to the replication client, which attaches it to the
/// authenticated gRPC request.
///
/// The transitional backend writes pages into the replica file in place, so an interrupted pull leaves a
/// torn file; the permit guarantees the ledger record was invalidated first. On success the replica and its
/// directory entry have been fsynced and the returned [`PulledReplica`] may publish a new record.
pub async fn pull_replica(
    sync: &SyncSection,
    permit: PullPermit,
) -> Result<PulledReplica, SyncError> {
    sync.validate()
        .map_err(|error| SyncError::InvalidConfiguration(error.to_string()))?;
    let endpoint = sync.origin_endpoint.as_deref().ok_or_else(|| {
        SyncError::InvalidConfiguration("sync.origin_endpoint is required for replication".into())
    })?;
    let auth_token = load_sync_auth_token(sync).await?;

    rsqlite_rsync::grpc_pull_sync(
        &permit.replica_path,
        endpoint,
        permit.database.as_str(),
        auth_token.as_deref().map(String::as_str),
    )
    .await
    .map_err(map_pull_error)?;
    fsync_replica(&permit.replica_path).await?;
    Ok(PulledReplica { permit })
}

/// Process-wide set of replica paths currently being written by a pull.
fn active_pulls() -> &'static Mutex<HashSet<PathBuf>> {
    static ACTIVE: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    ACTIVE.get_or_init(|| Mutex::new(HashSet::new()))
}

/// RAII registration of an in-progress pull into one replica path; dropping it releases the path.
///
/// This is the I/O-free fast path for REQ-SYNC-002. It compares absolute paths lexically, so two spellings
/// of one file (via `..` or symlinks) may both register; the advisory file lock taken next catches those.
#[derive(Debug)]
struct ActivePullGuard {
    path: PathBuf,
}

impl ActivePullGuard {
    /// Registers `replica_path` (made absolute, without resolving `..` or symlinks) or fails if it is
    /// already registered.
    fn acquire(replica_path: &Path) -> Result<Self, SyncError> {
        let path = std::path::absolute(replica_path).unwrap_or_else(|_| replica_path.to_path_buf());
        let mut active = active_pulls()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !active.insert(path.clone()) {
            return Err(SyncError::SyncInProgress {
                path: path.display().to_string(),
            });
        }
        Ok(Self { path })
    }
}

impl Drop for ActivePullGuard {
    fn drop(&mut self) {
        active_pulls()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.path);
    }
}

/// Returns the lock file path guarding pulls into `replica_path`.
pub fn pull_lock_path(replica_path: &Path) -> PathBuf {
    let mut name = replica_path.as_os_str().to_os_string();
    name.push(PULL_LOCK_SUFFIX);
    PathBuf::from(name)
}

/// Takes a non-blocking exclusive advisory lock on `replica_path`'s lock file, held until the file is dropped.
///
/// The lock is on the lock file's inode, so every spelling of the path, and every process, contends for it.
async fn lock_replica(replica_path: &Path) -> Result<std::fs::File, SyncError> {
    let lock_path = pull_lock_path(replica_path);
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .await
        .map_err(|error| {
            SyncError::Replication(ReplicationError::Operation(format!(
                "failed to open pull lock '{}': {error}",
                lock_path.display()
            )))
        })?
        .into_std()
        .await;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(SyncError::SyncInProgress {
            path: replica_path.display().to_string(),
        }),
        Err(std::fs::TryLockError::Error(error)) => {
            Err(SyncError::Replication(ReplicationError::Operation(
                format!("failed to lock '{}': {error}", lock_path.display()),
            )))
        }
    }
}

/// Fsyncs a directory so renames and unlinks inside it are durable (a no-op off Unix).
async fn fsync_dir(dir: &Path) -> Result<(), SyncError> {
    #[cfg(unix)]
    tokio::fs::File::open(dir)
        .await
        .map_err(|error| ledger_error(dir, error))?
        .sync_all()
        .await
        .map_err(|error| ledger_error(dir, error))?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
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

/// A sync credential, wiped from memory when dropped.
type AuthToken = Zeroizing<String>;

/// Loads and trims the configured sync token from its environment variable or file, if one is configured.
async fn load_sync_auth_token(sync: &SyncSection) -> Result<Option<AuthToken>, SyncError> {
    load_sync_auth_token_with(sync, |name: &str| std::env::var(name)).await
}

/// Reads a token file of at most [`MAX_AUTH_TOKEN_FILE_BYTES`], warning if other users can read it.
async fn read_token_file(path: &Path) -> Result<AuthToken, SyncError> {
    let file_error = |message: String| SyncError::AuthTokenFile {
        path: path.display().to_string(),
        message,
    };
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|error| file_error(error.to_string()))?;
    #[cfg(unix)]
    if let Ok(metadata) = file.metadata().await {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o004 != 0 {
            tracing::warn!(
                path = %path.display(),
                "sync authentication token file is world-readable; restrict it to the node user"
            );
        }
    }
    let mut bytes = Zeroizing::new(Vec::new());
    file.take(MAX_AUTH_TOKEN_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| file_error(error.to_string()))?;
    if bytes.len() as u64 > MAX_AUTH_TOKEN_FILE_BYTES {
        return Err(file_error(format!(
            "file is larger than {MAX_AUTH_TOKEN_FILE_BYTES} bytes"
        )));
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| file_error("stream did not contain valid UTF-8".to_string()))?;
    Ok(Zeroizing::new(text.to_string()))
}

/// [`load_sync_auth_token`] with an injectable environment reader, so tests never mutate the process
/// environment (which is unsound while other threads may read it).
async fn load_sync_auth_token_with<E>(
    sync: &SyncSection,
    read_env: E,
) -> Result<Option<AuthToken>, SyncError>
where
    E: Fn(&str) -> Result<String, std::env::VarError>,
{
    let token = match (
        sync.transport_auth_token_env.as_deref(),
        sync.transport_auth_token_file.as_deref(),
    ) {
        (Some(name), None) => Some(Zeroizing::new(read_env(name).map_err(|e| match e {
            std::env::VarError::NotPresent => SyncError::AuthTokenEnvNotSet {
                name: name.to_string(),
            },
            std::env::VarError::NotUnicode(_) => SyncError::AuthTokenEnvNotUnicode {
                name: name.to_string(),
            },
        })?)),
        (None, Some(path)) => Some(read_token_file(path).await?),
        (None, None) => None,
        (Some(_), Some(_)) => {
            return Err(SyncError::InvalidConfiguration(
                "sync authentication environment variable and file are mutually exclusive".into(),
            ));
        }
    };

    token
        .map(|token| {
            let trimmed = token.trim();
            if trimmed.is_empty() {
                Err(SyncError::EmptyAuthToken)
            } else {
                Ok(Zeroizing::new(trimmed.to_string()))
            }
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hasqlite_core::{DEFAULT_MAX_CONCURRENT_PULLS, NodeId};

    /// Sync section with a remote origin and no credential source.
    fn sync_config() -> SyncSection {
        SyncSection {
            interval_secs: 1,
            max_concurrent_pulls: DEFAULT_MAX_CONCURRENT_PULLS,
            origin_endpoint: Some("https://writer:50443".into()),
            transport_auth_token_env: None,
            transport_auth_token_file: None,
        }
    }

    /// Parses a database name.
    fn db(name: &str) -> DatabaseName {
        DatabaseName::parse(name).unwrap()
    }

    /// A valid ledger record for `database`.
    fn record(database: &str) -> FreshnessLedger {
        FreshnessLedger {
            database: db(database),
            incarnation: 1,
            source_node_id: NodeId::new("node-1"),
            source_generation: 5,
            synced_at_secs: 1_000,
            destination_node_id: NodeId::new("node-2"),
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
                .as_deref()
                .map(String::as_str),
            Some("test-replication-token")
        );
    }

    #[tokio::test]
    async fn sync_auth_token_file_errors_do_not_expose_contents() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let token_path = directory.path().join("sync-token");
        // The invalid UTF-8 byte makes decoding fail after the token bytes are already on disk.
        std::fs::write(&token_path, b"test-replication-token\xff").expect("write token");
        let mut sync = sync_config();
        sync.transport_auth_token_file = Some(token_path);

        let error = load_sync_auth_token(&sync).await.unwrap_err();
        assert!(matches!(error, SyncError::AuthTokenFile { .. }));
        assert!(!error.to_string().contains("test-replication-token"));
    }

    #[tokio::test]
    async fn sync_auth_token_file_rejects_oversized_file() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let token_path = directory.path().join("sync-token");
        std::fs::write(
            &token_path,
            vec![b'a'; MAX_AUTH_TOKEN_FILE_BYTES as usize + 1],
        )
        .expect("write token");
        let mut sync = sync_config();
        sync.transport_auth_token_file = Some(token_path.clone());

        assert!(matches!(
            load_sync_auth_token(&sync).await.unwrap_err(),
            SyncError::AuthTokenFile { message, .. } if message.contains("larger than")
        ));

        std::fs::write(&token_path, vec![b'a'; MAX_AUTH_TOKEN_FILE_BYTES as usize])
            .expect("write token");
        assert!(load_sync_auth_token(&sync).await.is_ok());
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

    /// Fake environment holding exactly one variable.
    fn env_with(
        name: &'static str,
        value: &'static str,
    ) -> impl Fn(&str) -> Result<String, std::env::VarError> {
        move |requested: &str| {
            if requested == name {
                Ok(value.to_string())
            } else {
                Err(std::env::VarError::NotPresent)
            }
        }
    }

    #[tokio::test]
    async fn sync_auth_token_is_loaded_from_env() {
        const NAME: &str = "HASQLITE_TEST_SYNC_TOKEN_LOADED";
        let mut sync = sync_config();
        sync.transport_auth_token_env = Some(NAME.into());

        assert_eq!(
            load_sync_auth_token_with(&sync, env_with(NAME, " test-replication-token \n"))
                .await
                .expect("load token")
                .as_deref()
                .map(String::as_str),
            Some("test-replication-token")
        );
    }

    #[tokio::test]
    async fn sync_auth_token_env_rejects_non_unicode_value() {
        const NAME: &str = "HASQLITE_TEST_SYNC_TOKEN_BINARY";
        let mut sync = sync_config();
        sync.transport_auth_token_env = Some(NAME.into());
        let read_env = |_: &str| {
            Err(std::env::VarError::NotUnicode(std::ffi::OsString::from(
                "\u{fffd}",
            )))
        };

        assert!(matches!(
            load_sync_auth_token_with(&sync, read_env).await.unwrap_err(),
            SyncError::AuthTokenEnvNotUnicode { name } if name == NAME
        ));
    }

    #[tokio::test]
    async fn sync_auth_token_env_missing_is_reported_by_name() {
        // Uses the real process environment: the variable is never set anywhere in this test binary.
        const NAME: &str = "HASQLITE_TEST_SYNC_TOKEN_MISSING";
        let mut sync = sync_config();
        sync.transport_auth_token_env = Some(NAME.into());

        assert!(matches!(
            load_sync_auth_token(&sync).await.unwrap_err(),
            SyncError::AuthTokenEnvNotSet { name } if name == NAME
        ));
    }

    #[tokio::test]
    async fn sync_auth_token_env_rejects_blank_value() {
        const NAME: &str = "HASQLITE_TEST_SYNC_TOKEN_BLANK";
        let mut sync = sync_config();
        sync.transport_auth_token_env = Some(NAME.into());

        assert!(matches!(
            load_sync_auth_token_with(&sync, env_with(NAME, "  "))
                .await
                .unwrap_err(),
            SyncError::EmptyAuthToken
        ));
    }

    #[tokio::test]
    async fn sync_auth_token_rejects_both_sources_at_load_time() {
        let mut sync = sync_config();
        sync.transport_auth_token_env = Some("HASQLITE_TEST_SYNC_TOKEN_BOTH".into());
        sync.transport_auth_token_file = Some(PathBuf::from("/nonexistent/token"));

        assert!(matches!(
            load_sync_auth_token(&sync).await.unwrap_err(),
            SyncError::InvalidConfiguration(message) if message.contains("mutually exclusive")
        ));
    }

    #[tokio::test]
    async fn fsync_replica_reports_missing_file_and_never_creates_it() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let replica_path = directory.path().join("missing.db");

        let error = fsync_replica(&replica_path).await.unwrap_err();
        assert!(matches!(
            &error,
            SyncError::Replication(ReplicationError::Operation(message)) if message.contains("failed to fsync")
        ));
        assert!(!replica_path.exists());
    }

    #[tokio::test]
    async fn fsync_replica_syncs_existing_file_and_directory() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let replica_path = directory.path().join("replica.db");
        std::fs::write(&replica_path, b"pages").expect("write replica");

        fsync_replica(&replica_path).await.expect("fsync replica");
    }

    #[test]
    fn active_pull_guard_is_exclusive_per_path_and_released_on_drop() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let replica_path = directory.path().join("replica.db");
        let other_path = directory.path().join("other.db");

        let first = ActivePullGuard::acquire(&replica_path).expect("first pull registers");
        assert!(matches!(
            ActivePullGuard::acquire(&replica_path).unwrap_err(),
            SyncError::SyncInProgress { path } if path.ends_with("replica.db")
        ));
        // A different replica path is independent.
        let _other = ActivePullGuard::acquire(&other_path).expect("other path registers");
        drop(first);
        let _again = ActivePullGuard::acquire(&replica_path).expect("path is free after drop");
    }

    #[tokio::test]
    async fn begin_pull_refuses_in_process_duplicate_before_any_io() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let store = LedgerStore::new(directory.path().join("ledger"));
        let replica_path = directory.path().join("replica.db");

        let _in_progress = ActivePullGuard::acquire(&replica_path).expect("simulate running pull");
        let error = store
            .begin_pull(&replica_path, db("app.db"))
            .await
            .unwrap_err();
        assert!(
            matches!(error, SyncError::SyncInProgress { .. }),
            "got {error:?}"
        );
        // Rejected before any I/O: neither the lock file nor the replica was created.
        assert!(!pull_lock_path(&replica_path).exists());
        assert!(!replica_path.exists());
    }

    #[tokio::test]
    async fn begin_pull_refuses_a_path_locked_by_another_process() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let ledger_dir = directory.path().join("ledger");
        std::fs::create_dir(&ledger_dir).unwrap();
        let store = LedgerStore::new(&ledger_dir);
        let replica_path = directory.path().join("replica.db");
        // flock locks belong to an open file description, so a second open in this process behaves like
        // another process holding the lock while the in-process guard is free.
        let held_elsewhere = lock_replica(&replica_path).await.expect("take lock");
        let error = store
            .begin_pull(&replica_path, db("app.db"))
            .await
            .unwrap_err();
        assert!(
            matches!(error, SyncError::SyncInProgress { .. }),
            "got {error:?}"
        );
        // Another spelling of the same path contends for the same lock file inode.
        let alias = ledger_dir.join("..").join("replica.db");
        assert!(matches!(
            store.begin_pull(&alias, db("app.db")).await.unwrap_err(),
            SyncError::SyncInProgress { .. }
        ));
        drop(held_elsewhere);
        assert!(store.begin_pull(&replica_path, db("app.db")).await.is_ok());
    }

    #[tokio::test]
    async fn begin_pull_invalidates_the_record_and_publish_restores_it() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let ledger_dir = directory.path().join("ledger");
        std::fs::create_dir(&ledger_dir).unwrap();
        let store = LedgerStore::new(&ledger_dir);
        let replica_path = directory.path().join("tenants%2Facme.db");
        std::fs::write(&replica_path, b"pages").unwrap();
        let database = db("tenants/acme.db");
        let mut published = record("tenants/acme.db");
        std::fs::write(
            store.record_path(&database),
            toml::to_string(&published).unwrap(),
        )
        .unwrap();
        assert_eq!(
            store.read(&database).await.unwrap(),
            Some(published.clone())
        );

        let permit = store
            .begin_pull(&replica_path, database.clone())
            .await
            .unwrap();
        assert_eq!(permit.database(), &database);
        assert_eq!(permit.replica_path(), replica_path);
        assert_eq!(store.read(&database).await.unwrap(), None, "invalidated");

        // Simulate the pull completing; publishing writes atomically and leaves no temp file behind.
        fsync_replica(&replica_path).await.unwrap();
        let pulled = PulledReplica { permit };
        published.synced_at_secs = 2_000;
        pulled.publish(&published).await.unwrap();
        assert_eq!(store.read(&database).await.unwrap(), Some(published));
        let names: Vec<_> = std::fs::read_dir(&ledger_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names, ["tenants%2Facme.db"]);
    }

    #[tokio::test]
    async fn publish_refuses_a_record_for_another_database() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let ledger_dir = directory.path().join("ledger");
        std::fs::create_dir(&ledger_dir).unwrap();
        let store = LedgerStore::new(&ledger_dir);
        let replica_path = directory.path().join("app.db");
        let pulled = PulledReplica {
            permit: store.begin_pull(&replica_path, db("app.db")).await.unwrap(),
        };
        assert!(matches!(
            pulled.publish(&record("other.db")).await.unwrap_err(),
            SyncError::Ledger { .. }
        ));
        assert_eq!(store.read(&db("app.db")).await.unwrap(), None);
    }

    #[tokio::test]
    async fn read_treats_corrupt_or_misfiled_records_as_absent() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let store = LedgerStore::new(directory.path());
        let database = db("app.db");
        std::fs::write(store.record_path(&database), "not = [valid").unwrap();
        assert_eq!(store.read(&database).await.unwrap(), None);
        std::fs::write(
            store.record_path(&database),
            toml::to_string(&record("other.db")).unwrap(),
        )
        .unwrap();
        assert_eq!(store.read(&database).await.unwrap(), None);

        let set = DatabaseSet::new(vec![
            hasqlite_lease::DatabaseEntry {
                name: db("app.db"),
                incarnation: 1,
            },
            hasqlite_lease::DatabaseEntry {
                name: db("b.db"),
                incarnation: 1,
            },
        ])
        .unwrap();
        std::fs::write(
            store.record_path(&db("b.db")),
            toml::to_string(&record("b.db")).unwrap(),
        )
        .unwrap();
        let records = store.read_set(&set).await.unwrap();
        assert_eq!(records.keys().cloned().collect::<Vec<_>>(), [db("b.db")]);
    }

    #[test]
    fn sync_triggers_equality_and_hashing() {
        use std::collections::HashSet;

        assert_eq!(SyncTrigger::Periodic, SyncTrigger::Periodic);
        assert_ne!(SyncTrigger::PreShutdown, SyncTrigger::Periodic);

        let mut triggers = HashSet::new();
        triggers.insert(SyncTrigger::Periodic);
        triggers.insert(SyncTrigger::PreShutdown);
        triggers.insert(SyncTrigger::OnLeaseChange);
        assert_eq!(triggers.len(), 3);
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
