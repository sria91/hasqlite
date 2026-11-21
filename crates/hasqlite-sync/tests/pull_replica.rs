use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use hasqlite_core::{DEFAULT_MAX_CONCURRENT_PULLS, DatabaseName, NodeId, SyncSection};
use hasqlite_lease::FreshnessLedger;
use hasqlite_sync::{LedgerStore, PulledReplica, SyncError, pull_replica};
use libsqlite3_sys as ffi;
use rsqlite_rsync::db::Connection;
use rsqlite_rsync::gateway::{AuthConfig, DatabaseEngine, ReplicationServer};
use rsqlite_rsync::proto::rsqlite::v1::replication_service_server::ReplicationServiceServer;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;

const TEST_TOKEN: &str = "test-replication-token";

struct TestServer {
    address: SocketAddr,
    data_dir: PathBuf,
    _directory: TempDir,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl TestServer {
    /// Starts an authenticated replication server on an ephemeral loopback port.
    async fn start() -> Self {
        let directory = tempfile::tempdir().expect("create test directory");
        let data_dir = directory.path().join("databases");
        fs::create_dir_all(&data_dir).expect("create database directory");

        // Serving the already-bound listener avoids a bind-then-rebind race on the port.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test port");
        let address = listener.local_addr().expect("read test address");

        let engine = DatabaseEngine::new(&data_dir).expect("create database engine");
        let replication = ReplicationServer::new(
            engine,
            AuthConfig::required(TEST_TOKEN.to_string()),
            rsqlite_rsync::SyncTuning::default(),
            None,
        );
        let service = ReplicationServiceServer::new(replication);
        let (shutdown, shutdown_rx) = oneshot::channel();

        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("run test replication server");
        });

        Self {
            address,
            data_dir,
            _directory: directory,
            shutdown: Some(shutdown),
            task: Some(task),
        }
    }

    /// Shuts the server down and re-raises any panic from its task, which would otherwise go unobserved.
    async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.await.expect("test replication server task failed");
        }
    }

    /// Returns the loopback `grpc://` endpoint of the server.
    fn endpoint(&self) -> String {
        format!("grpc://{}", self.address)
    }

    /// Returns the path of `database` in the server's data directory.
    fn database_path(&self, database: &str) -> PathBuf {
        self.data_dir.join(database)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

/// Creates a database at `path` with one table and one row.
fn seed_database(path: &Path) {
    let connection = Connection::open(path, ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE)
        .expect("open test database");
    connection
        .exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT);")
        .expect("create test table");
    connection
        .exec("INSERT INTO items (value) VALUES ('replicated');")
        .expect("insert test row");
}

/// Sync section pointing at `endpoint` and authenticating with `token_file`.
fn sync_config(endpoint: String, token_file: PathBuf) -> SyncSection {
    SyncSection {
        interval_secs: 1,
        max_concurrent_pulls: DEFAULT_MAX_CONCURRENT_PULLS,
        origin_endpoint: Some(endpoint),
        transport_auth_token_env: None,
        transport_auth_token_file: Some(token_file),
    }
}

/// Logical database name used across pull-replica integration tests.
const DATABASE: &str = "source.db";

/// Parsed [`DATABASE`].
fn database() -> DatabaseName {
    DatabaseName::parse(DATABASE).expect("valid database name")
}

/// Common scaffolding for `pull_replica` integration tests: a running server with a seeded
/// database, a temporary directory, and an empty token file, replica path and ledger directory inside it.
struct PullTestSetup {
    server: TestServer,
    _directory: tempfile::TempDir,
    /// Path where the token should be written before each pull attempt.
    pub token_file: PathBuf,
    /// Destination path for the pulled replica.
    pub replica_path: PathBuf,
    /// Freshness ledger directory.
    pub ledger: LedgerStore,
}

impl PullTestSetup {
    async fn start() -> Self {
        let server = TestServer::start().await;
        seed_database(&server.database_path(DATABASE));
        let directory = tempfile::tempdir().expect("create replica directory");
        let token_file = directory.path().join("sync-token");
        let replica_path = directory.path().join("replica.db");
        let ledger_dir = directory.path().join("ledger");
        fs::create_dir(&ledger_dir).expect("create ledger directory");
        Self {
            server,
            _directory: directory,
            token_file,
            replica_path,
            ledger: LedgerStore::new(ledger_dir),
        }
    }

    /// Returns a `SyncSection` pointing at the test server using `self.token_file`.
    fn sync_config(&self) -> SyncSection {
        sync_config(self.server.endpoint(), self.token_file.clone())
    }

    /// Runs the protocol up to (not including) publication: claim + invalidate, then pull.
    async fn pull(&self, sync: &SyncSection) -> Result<PulledReplica, SyncError> {
        let permit = self
            .ledger
            .begin_pull(&self.replica_path, database())
            .await?;
        pull_replica(sync, permit).await
    }
}

/// A ledger record for a pull of [`DATABASE`].
fn record(synced_at_secs: u64) -> FreshnessLedger {
    FreshnessLedger {
        database: database(),
        incarnation: 1,
        source_node_id: NodeId::new("node-1"),
        source_generation: 5,
        synced_at_secs,
        destination_node_id: NodeId::new("node-2"),
    }
}

#[tokio::test]
async fn pull_replica_loads_file_token_and_copies_database() {
    let setup = PullTestSetup::start().await;
    fs::write(&setup.token_file, format!("{TEST_TOKEN}\n")).expect("write token file");

    setup
        .pull(&setup.sync_config())
        .await
        .expect("pull replica with configured token");

    assert_eq!(
        fs::read(&setup.replica_path).expect("read replica"),
        fs::read(setup.server.database_path(DATABASE)).expect("read source database"),
        "replica database should match the authenticated source"
    );
    setup.server.stop().await;
}

#[tokio::test]
async fn pull_then_publish_records_a_readable_ledger() {
    let setup = PullTestSetup::start().await;
    fs::write(&setup.token_file, TEST_TOKEN).expect("write token file");
    fs::write(
        setup.ledger.record_path(&database()),
        toml::to_string(&record(1_000)).expect("encode"),
    )
    .expect("seed previous record");

    let pulled = setup
        .pull(&setup.sync_config())
        .await
        .expect("pull replica");
    assert_eq!(pulled.database(), &database());
    assert_eq!(
        setup.ledger.read(&database()).await.expect("read ledger"),
        None,
        "the previous record is invalidated while the pull is unpublished"
    );
    pulled.publish(&record(2_000)).await.expect("publish");
    assert_eq!(
        setup.ledger.read(&database()).await.expect("read ledger"),
        Some(record(2_000))
    );
    setup.server.stop().await;
}

#[tokio::test]
async fn failed_pull_leaves_the_ledger_invalidated() {
    let setup = PullTestSetup::start().await;
    fs::write(&setup.token_file, "wrong-token").expect("write token file");
    fs::write(
        setup.ledger.record_path(&database()),
        toml::to_string(&record(1_000)).expect("encode"),
    )
    .expect("seed previous record");

    setup
        .pull(&setup.sync_config())
        .await
        .expect_err("wrong token should fail authentication");
    assert_eq!(
        setup.ledger.read(&database()).await.expect("read ledger"),
        None
    );
    setup.server.stop().await;
}

#[tokio::test]
async fn pull_replica_reports_rejected_token_as_auth_failure() {
    let setup = PullTestSetup::start().await;
    fs::write(&setup.token_file, "wrong-token").expect("write token file");

    let error = setup
        .pull(&setup.sync_config())
        .await
        .expect_err("wrong token should fail authentication");

    assert!(
        matches!(error, SyncError::AuthFailed { .. }),
        "expected AuthFailed, got {error:?}"
    );
    // The rejected credential must not leak through the error chain (`AuthFailed` docs promise this).
    let mut rendered = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(inner) = source {
        rendered.push_str(&inner.to_string());
        source = inner.source();
    }
    assert!(
        !rendered.contains("wrong-token") && !format!("{error:?}").contains("wrong-token"),
        "error output leaked the token: {rendered}"
    );
    setup.server.stop().await;
}

#[tokio::test]
async fn pull_replica_with_rejected_token_leaves_existing_replica_untouched() {
    let setup = PullTestSetup::start().await;
    fs::write(&setup.token_file, "wrong-token").expect("write token file");
    let previous_contents = b"previous replica contents";
    fs::write(&setup.replica_path, previous_contents).expect("seed stale replica");

    setup
        .pull(&setup.sync_config())
        .await
        .expect_err("wrong token should fail authentication");

    assert_eq!(
        fs::read(&setup.replica_path).expect("read replica"),
        previous_contents,
        "a rejected pull must not modify the replica file"
    );
    setup.server.stop().await;
}

#[tokio::test]
async fn pull_replica_uses_rotated_token_file_without_restart() {
    let setup = PullTestSetup::start().await;
    fs::write(&setup.token_file, "stale-token").expect("write stale token");
    let sync = setup.sync_config();

    let error = setup
        .pull(&sync)
        .await
        .expect_err("stale token should fail authentication");
    assert!(
        matches!(error, SyncError::AuthFailed { .. }),
        "expected AuthFailed, got {error:?}"
    );

    fs::write(&setup.token_file, TEST_TOKEN).expect("rotate token");
    setup
        .pull(&sync)
        .await
        .expect("pull replica with rotated token");
    assert_eq!(
        fs::read(&setup.replica_path).expect("read replica"),
        fs::read(setup.server.database_path(DATABASE)).expect("read source database"),
    );
    setup.server.stop().await;
}

#[tokio::test]
async fn pull_replica_rejects_origin_without_credential() {
    let sync = SyncSection {
        interval_secs: 1,
        max_concurrent_pulls: DEFAULT_MAX_CONCURRENT_PULLS,
        origin_endpoint: Some("grpc://127.0.0.1:50051".to_string()),
        transport_auth_token_env: None,
        transport_auth_token_file: None,
    };
    let directory = tempfile::tempdir().expect("create replica directory");
    let permit = LedgerStore::new(directory.path())
        .begin_pull(&directory.path().join("replica.db"), database())
        .await
        .expect("claim replica");

    let error = pull_replica(&sync, permit)
        .await
        .expect_err("origin without a credential must not be pulled unauthenticated");

    assert!(matches!(
        error,
        SyncError::InvalidConfiguration(message) if message.contains("requires a credential")
    ));
}

#[tokio::test]
async fn pull_replica_requires_origin_endpoint() {
    let sync = SyncSection {
        interval_secs: 1,
        max_concurrent_pulls: DEFAULT_MAX_CONCURRENT_PULLS,
        origin_endpoint: None,
        transport_auth_token_env: None,
        transport_auth_token_file: None,
    };
    let directory = tempfile::tempdir().expect("create replica directory");
    let permit = LedgerStore::new(directory.path())
        .begin_pull(&directory.path().join("replica.db"), database())
        .await
        .expect("claim replica");

    let error = pull_replica(&sync, permit)
        .await
        .expect_err("pull without an origin must fail");

    assert!(matches!(
        error,
        SyncError::InvalidConfiguration(message) if message.contains("sync.origin_endpoint is required")
    ));
}
