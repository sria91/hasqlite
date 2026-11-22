//! Sync orchestration policies (Phase 2).
//!
//! Replication currently delegates to `rsqlite-rsync`; this integration is
//! transitional and is replaced when the native backend meets the cutover
//! criteria in `docs/PLAN.md`.
//!
//! The crash-safe ledger protocol (ADR-0007, safety contract rule 9) is enforced by types:
//!
//! 1. [`LedgerStore::begin_pull`] claims exclusive use of the database's replica file under `data.dir` and
//!    invalidates the database's ledger record durably, returning a [`PullPermit`].
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
use zeroize::Zeroizing;

/// Maximum size of a sync token file; larger files are rejected rather than read into memory.
pub const MAX_AUTH_TOKEN_FILE_BYTES: u64 = 4_096;
/// Reserved directory under `data.dir`, outside the database namespace (database names never start with
/// `.`), that holds the cross-process pull lock files of every database on the volume.
pub const PULL_LOCK_DIR: &str = ".hasqlite.pull-locks";
/// Suffix of a pull lock file name (see [`pull_lock_file_name`]).
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
    /// is also `Ok(None)`: it is treated as invalidated, which denies promotion (fail closed). So is a
    /// record whose publication failed after its rename and whose rollback could not be confirmed. Other
    /// read errors are returned.
    pub async fn read(
        &self,
        database: &DatabaseName,
    ) -> Result<Option<FreshnessLedger>, SyncError> {
        let path = self.record_path(database);
        if unconfirmed_records().contains(&path) {
            return Ok(None);
        }
        let contents = match tokio::fs::read_to_string(&path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(ledger_error(&path, error)),
        };
        Ok(toml::from_str::<FreshnessLedger>(&contents)
            .ok()
            .filter(|record| &record.database == database))
    }

    /// Reads the record of every database in `set`, in the shape `validate_promotion` expects.
    ///
    /// Databases without a valid record (missing, unparseable or misfiled, see [`Self::read`]) are omitted.
    /// Any other read failure aborts with [`SyncError::Ledger`]; the caller must not campaign on it.
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

    /// Claims exclusive use of `database`'s replica file under `data_dir` and durably invalidates that
    /// database's ledger record (REQ-SYNC-002, REQ-SYNC-005).
    ///
    /// The replica file is always `<data_dir>/<database>` with `data_dir` canonicalized, so the record
    /// invalidated is the one for the file the pull writes. A name whose existing components under
    /// `data_dir` include a symlink is rejected, because its write target would not be determined by the
    /// name alone.
    ///
    /// A pull already in progress in this process is refused before any I/O. A pull held by another
    /// process is detected through an advisory lock on [`PULL_LOCK_DIR`]`/<bounded name>`
    /// (see [`pull_lock_file_name`]), keyed by the database name, so every spelling of `data_dir` contends
    /// for the same lock. Lock files are never deleted, since unlinking a lock file races with processes
    /// that have it open. Both cases return [`SyncError::SyncInProgress`] without touching the ledger or
    /// the replica.
    ///
    /// The lock and invalidation run on a blocking task that owns the claim, so cancelling this future
    /// cannot release the claim while an issued unlink may still complete.
    pub async fn begin_pull(
        &self,
        data_dir: &Path,
        database: DatabaseName,
    ) -> Result<PullPermit, SyncError> {
        let guard = ActivePullGuard::acquire(&data_dir.join(database.as_str()))?;
        let store = self.clone();
        let data_dir = data_dir.to_path_buf();
        run_blocking(move || {
            let data_dir = std::fs::canonicalize(&data_dir).map_err(|error| {
                replication_operation(format!(
                    "failed to resolve data directory '{}': {error}",
                    data_dir.display()
                ))
            })?;
            let replica_path = resolve_replica_path(&data_dir, &database)?;
            let lock = lock_replica(&data_dir, &database)?;
            store.invalidate(&database)?;
            Ok(PullPermit {
                replica_path,
                database,
                store,
                _lock: lock,
                _guard: guard,
            })
        })
        .await
    }

    /// Removes `database`'s record (a missing record is already invalid) and fsyncs the directory.
    fn invalidate(&self, database: &DatabaseName) -> Result<(), SyncError> {
        let path = self.record_path(database);
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(ledger_error(&path, error)),
        }
        fsync_dir(&self.dir)?;
        unconfirmed_records().remove(&path);
        Ok(())
    }
}

/// Runs blocking I/O that owns everything it needs, so dropping the awaiting future cannot cut it short.
async fn run_blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, SyncError> + Send + 'static,
) -> Result<T, SyncError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| replication_operation(format!("ledger I/O task failed: {error}")))?
}

/// Builds a [`ReplicationError::Operation`] sync error.
fn replication_operation(message: String) -> SyncError {
    SyncError::Replication(ReplicationError::Operation(message))
}

/// Returns `<data_dir>/<database>`, rejecting a name whose existing components are symlinks.
fn resolve_replica_path(data_dir: &Path, database: &DatabaseName) -> Result<PathBuf, SyncError> {
    let mut path = data_dir.to_path_buf();
    for component in database.as_str().split('/') {
        path.push(component);
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(replication_operation(format!(
                    "replica path '{}' contains a symlink; refusing an aliased write target",
                    path.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(replication_operation(format!(
                    "failed to inspect '{}': {error}",
                    path.display()
                )));
            }
        }
    }
    Ok(data_dir.join(database.as_str()))
}

/// Ledger record paths whose publication failed after the rename and whose removal could not be
/// confirmed; [`LedgerStore::read`] treats them as invalidated until the next successful invalidation.
fn unconfirmed_records() -> std::sync::MutexGuard<'static, HashSet<PathBuf>> {
    static UNCONFIRMED: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    UNCONFIRMED
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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
    /// Replica file the pull writes into: `<canonical data_dir>/<database>`.
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
    ///
    /// The whole transaction runs on a blocking task that owns the claim, so cancelling this future cannot
    /// release it while the rename may still install the record. If the directory fsync after the rename
    /// fails, the record is removed again and the removal synced; if that cannot be confirmed, the record
    /// is treated as invalidated by [`LedgerStore::read`] in this process.
    pub async fn publish(self, record: &FreshnessLedger) -> Result<(), SyncError> {
        let record = record.clone();
        run_blocking(move || self.publish_blocking(&record)).await
    }

    /// Blocking body of [`Self::publish`]; the claim is released when `self` drops at the end.
    fn publish_blocking(self, record: &FreshnessLedger) -> Result<(), SyncError> {
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
        let write = || -> std::io::Result<()> {
            let mut file = std::fs::File::create(&temp)?;
            std::io::Write::write_all(&mut file, contents.as_bytes())?;
            file.sync_all()?;
            std::fs::rename(&temp, &path)
        };
        if let Err(error) = write() {
            let _ = std::fs::remove_file(&temp);
            return Err(ledger_error(&temp, error));
        }
        if let Err(error) = fsync_dir(&store.dir) {
            let rolled_back = match std::fs::remove_file(&path) {
                Ok(()) => true,
                Err(remove) => remove.kind() == std::io::ErrorKind::NotFound,
            } && fsync_dir(&store.dir).is_ok();
            if !rolled_back {
                unconfirmed_records().insert(path);
            }
            return Err(error);
        }
        Ok(())
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
/// of one data directory (via `..` or symlinks) may both register; the advisory file lock taken next
/// catches those.
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

/// Maximum length of one path component on the supported filesystems.
const MAX_FILE_NAME_BYTES: usize = 255;

/// Returns the lock file name for `database` inside [`PULL_LOCK_DIR`].
///
/// It is the database's [`DatabaseName::encoded_file_name`] plus [`PULL_LOCK_SUFFIX`] when that fits in
/// one path component. Otherwise the encoded name is truncated and followed by `-` and a 64-bit FNV-1a
/// hash of the full encoded name, so the name is deterministic, at most 255 bytes, and distinct for
/// distinct databases (a hash collision would only cause a spurious `SyncInProgress`).
pub fn pull_lock_file_name(database: &DatabaseName) -> String {
    let encoded = database.encoded_file_name();
    if encoded.len() + PULL_LOCK_SUFFIX.len() <= MAX_FILE_NAME_BYTES {
        return format!("{encoded}{PULL_LOCK_SUFFIX}");
    }
    let hash = encoded
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    // Encoded names are ASCII, so any byte offset is a character boundary.
    let keep = MAX_FILE_NAME_BYTES - PULL_LOCK_SUFFIX.len() - 17;
    format!("{}-{hash:016x}{PULL_LOCK_SUFFIX}", &encoded[..keep])
}

/// Returns the lock file path guarding pulls of `database` into `data_dir`.
pub fn pull_lock_path(data_dir: &Path, database: &DatabaseName) -> PathBuf {
    data_dir
        .join(PULL_LOCK_DIR)
        .join(pull_lock_file_name(database))
}

/// Takes a non-blocking exclusive advisory lock on `database`'s lock file in `data_dir`, creating the
/// reserved lock directory (mode `0700`) if needed; the lock is held until the file is dropped.
///
/// The lock is on the lock file's inode, so every process, and every spelling of the path, contends for it.
fn lock_replica(data_dir: &Path, database: &DatabaseName) -> Result<std::fs::File, SyncError> {
    let lock_dir = data_dir.join(PULL_LOCK_DIR);
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    let is_dir = builder
        .create(&lock_dir)
        .and_then(|()| std::fs::symlink_metadata(&lock_dir))
        .map_err(|error| {
            replication_operation(format!(
                "failed to create pull lock directory '{}': {error}",
                lock_dir.display()
            ))
        })?
        .is_dir();
    if !is_dir {
        return Err(replication_operation(format!(
            "pull lock directory '{}' is not a directory",
            lock_dir.display()
        )));
    }
    let lock_path = pull_lock_path(data_dir, database);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .map_err(|error| {
            replication_operation(format!(
                "failed to open pull lock '{}': {error}",
                lock_path.display()
            ))
        })?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(SyncError::SyncInProgress {
            path: data_dir.join(database.as_str()).display().to_string(),
        }),
        Err(std::fs::TryLockError::Error(error)) => Err(replication_operation(format!(
            "failed to lock '{}': {error}",
            lock_path.display()
        ))),
    }
}

/// Fsyncs a directory so renames and unlinks inside it are durable (a no-op off Unix).
fn fsync_dir(dir: &Path) -> Result<(), SyncError> {
    #[cfg(unix)]
    std::fs::File::open(dir)
        .and_then(|file| file.sync_all())
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
///
/// The file is read with blocking reads straight into one fixed, zeroized buffer one byte larger than
/// the limit, so no growing or intermediate buffer is left holding token bytes.
async fn read_token_file(path: &Path) -> Result<AuthToken, SyncError> {
    let owned = path.to_path_buf();
    tokio::task::spawn_blocking(move || read_token_file_blocking(&owned))
        .await
        .map_err(|error| SyncError::AuthTokenFile {
            path: path.display().to_string(),
            message: format!("token read task failed: {error}"),
        })?
}

/// Blocking body of [`read_token_file`].
fn read_token_file_blocking(path: &Path) -> Result<AuthToken, SyncError> {
    use std::io::Read;
    let file_error = |message: String| SyncError::AuthTokenFile {
        path: path.display().to_string(),
        message,
    };
    let mut file = std::fs::File::open(path).map_err(|error| file_error(error.to_string()))?;
    #[cfg(unix)]
    if let Ok(metadata) = file.metadata() {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o004 != 0 {
            tracing::warn!(
                path = %path.display(),
                "sync authentication token file is world-readable; restrict it to the node user"
            );
        }
    }
    let mut buffer = Zeroizing::new([0u8; MAX_AUTH_TOKEN_FILE_BYTES as usize + 1]);
    let mut len = 0;
    while len < buffer.len() {
        match file.read(&mut buffer[len..]) {
            Ok(0) => break,
            Ok(read) => len += read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(file_error(error.to_string())),
        }
    }
    if len as u64 > MAX_AUTH_TOKEN_FILE_BYTES {
        return Err(file_error(format!(
            "file is larger than {MAX_AUTH_TOKEN_FILE_BYTES} bytes"
        )));
    }
    let text = std::str::from_utf8(&buffer[..len])
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
        let replica_path = directory.path().join("app.db");

        let _in_progress = ActivePullGuard::acquire(&replica_path).expect("simulate running pull");
        let error = store
            .begin_pull(directory.path(), db("app.db"))
            .await
            .unwrap_err();
        assert!(
            matches!(error, SyncError::SyncInProgress { .. }),
            "got {error:?}"
        );
        // Rejected before any I/O: neither the lock directory nor the replica was created.
        assert!(!directory.path().join(PULL_LOCK_DIR).exists());
        assert!(!replica_path.exists());
    }

    #[tokio::test]
    async fn begin_pull_refuses_a_database_locked_by_another_process() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let ledger_dir = directory.path().join("ledger");
        std::fs::create_dir(&ledger_dir).unwrap();
        let store = LedgerStore::new(&ledger_dir);
        let data_dir = std::fs::canonicalize(directory.path()).unwrap();
        // flock locks belong to an open file description, so a second open in this process behaves like
        // another process holding the lock while the in-process guard is free.
        let held_elsewhere = lock_replica(&data_dir, &db("app.db")).expect("take lock");
        let error = store
            .begin_pull(directory.path(), db("app.db"))
            .await
            .unwrap_err();
        assert!(
            matches!(error, SyncError::SyncInProgress { .. }),
            "got {error:?}"
        );
        // Other spellings of the data directory resolve to the same lock.
        let alias = ledger_dir.join("..");
        assert!(matches!(
            store.begin_pull(&alias, db("app.db")).await.unwrap_err(),
            SyncError::SyncInProgress { .. }
        ));
        #[cfg(unix)]
        {
            let link = tempfile::tempdir().unwrap();
            let link = link.path().join("data-link");
            std::os::unix::fs::symlink(directory.path(), &link).unwrap();
            assert!(matches!(
                store.begin_pull(&link, db("app.db")).await.unwrap_err(),
                SyncError::SyncInProgress { .. }
            ));
        }
        // A different database is independent.
        let other = store.begin_pull(directory.path(), db("other.db")).await;
        assert!(other.is_ok());
        drop(held_elsewhere);
        let permit = store
            .begin_pull(directory.path(), db("app.db"))
            .await
            .unwrap();
        assert_eq!(permit.replica_path(), data_dir.join("app.db"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn begin_pull_rejects_a_symlinked_replica_path() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let elsewhere = tempfile::tempdir().expect("create temporary directory");
        std::os::unix::fs::symlink(elsewhere.path(), directory.path().join("tenants")).unwrap();
        let store = LedgerStore::new(directory.path());
        let error = store
            .begin_pull(directory.path(), db("tenants/acme.db"))
            .await
            .unwrap_err();
        assert!(
            matches!(&error, SyncError::Replication(ReplicationError::Operation(message)) if message.contains("symlink")),
            "got {error:?}"
        );
    }

    #[test]
    fn pull_lock_file_names_are_bounded_and_distinct() {
        assert_eq!(
            pull_lock_file_name(&db("tenants/acme.db")),
            "tenants%2Facme.db.pull-lock"
        );
        let long = |last: char| {
            let component = format!("{}{last}.db", "a".repeat(120));
            db(&format!("{component}/{component}"))
        };
        let (first, second) = (
            pull_lock_file_name(&long('x')),
            pull_lock_file_name(&long('y')),
        );
        assert_eq!(first.len(), MAX_FILE_NAME_BYTES);
        assert!(first.ends_with(PULL_LOCK_SUFFIX));
        assert_ne!(first, second);
        assert_eq!(first, pull_lock_file_name(&long('x')), "deterministic");
    }

    #[tokio::test]
    async fn begin_pull_invalidates_the_record_and_publish_restores_it() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let ledger_dir = directory.path().join("ledger");
        std::fs::create_dir(&ledger_dir).unwrap();
        let store = LedgerStore::new(&ledger_dir);
        std::fs::create_dir(directory.path().join("tenants")).unwrap();
        let replica_path = std::fs::canonicalize(directory.path())
            .unwrap()
            .join("tenants/acme.db");
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
            .begin_pull(directory.path(), database.clone())
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
        // The claim was released when publication finished.
        assert!(store.begin_pull(directory.path(), database).await.is_ok());
    }

    #[tokio::test]
    async fn unconfirmed_record_reads_as_invalidated_until_next_invalidation() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let store = LedgerStore::new(directory.path());
        let database = db("unconfirmed.db");
        std::fs::write(
            store.record_path(&database),
            toml::to_string(&record("unconfirmed.db")).unwrap(),
        )
        .unwrap();
        unconfirmed_records().insert(store.record_path(&database));
        assert_eq!(store.read(&database).await.unwrap(), None);
        let _permit = store
            .begin_pull(directory.path(), database.clone())
            .await
            .unwrap();
        assert!(!unconfirmed_records().contains(&store.record_path(&database)));
    }

    #[tokio::test]
    async fn publish_refuses_a_record_for_another_database() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let ledger_dir = directory.path().join("ledger");
        std::fs::create_dir(&ledger_dir).unwrap();
        let store = LedgerStore::new(&ledger_dir);
        let pulled = PulledReplica {
            permit: store
                .begin_pull(directory.path(), db("app.db"))
                .await
                .unwrap(),
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
