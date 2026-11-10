use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use hasqlite_core::SyncSection;
use hasqlite_sync::{SyncError, pull_replica};
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

    fn endpoint(&self) -> String {
        format!("grpc://{}", self.address)
    }

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

fn sync_config(endpoint: String, token_file: PathBuf) -> SyncSection {
    SyncSection {
        interval_secs: 1,
        origin_endpoint: Some(endpoint),
        transport_auth_token_env: None,
        transport_auth_token_file: Some(token_file),
    }
}

#[tokio::test]
async fn pull_replica_loads_file_token_and_copies_database() {
    let server = TestServer::start().await;
    let database = "source.db";
    seed_database(&server.database_path(database));

    let directory = tempfile::tempdir().expect("create replica directory");
    let token_file = directory.path().join("sync-token");
    fs::write(&token_file, format!("{TEST_TOKEN}\n")).expect("write token file");
    let replica_path = directory.path().join("replica.db");

    pull_replica(
        &sync_config(server.endpoint(), token_file),
        &replica_path,
        database,
    )
    .await
    .expect("pull replica with configured token");

    assert_eq!(
        fs::read(&replica_path).expect("read replica"),
        fs::read(server.database_path(database)).expect("read source database"),
        "replica database should match the authenticated source"
    );
    server.stop().await;
}

#[tokio::test]
async fn pull_replica_reports_rejected_token_as_auth_failure() {
    let server = TestServer::start().await;
    let database = "source.db";
    seed_database(&server.database_path(database));

    let directory = tempfile::tempdir().expect("create replica directory");
    let token_file = directory.path().join("wrong-token");
    fs::write(&token_file, "wrong-token").expect("write token file");
    let replica_path = directory.path().join("replica.db");

    let error = pull_replica(
        &sync_config(server.endpoint(), token_file),
        &replica_path,
        database,
    )
    .await
    .expect_err("wrong token should fail authentication");

    assert!(
        matches!(error, SyncError::AuthFailed { .. }),
        "expected AuthFailed, got {error:?}"
    );
    server.stop().await;
}

#[tokio::test]
async fn pull_replica_rejects_origin_without_credential() {
    let sync = SyncSection {
        interval_secs: 1,
        origin_endpoint: Some("grpc://127.0.0.1:50051".to_string()),
        transport_auth_token_env: None,
        transport_auth_token_file: None,
    };
    let directory = tempfile::tempdir().expect("create replica directory");

    let error = pull_replica(&sync, &directory.path().join("replica.db"), "source.db")
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
        origin_endpoint: None,
        transport_auth_token_env: None,
        transport_auth_token_file: None,
    };
    let directory = tempfile::tempdir().expect("create replica directory");

    let error = pull_replica(&sync, &directory.path().join("replica.db"), "source.db")
        .await
        .expect_err("pull without an origin must fail");

    assert!(matches!(
        error,
        SyncError::InvalidConfiguration(message) if message.contains("sync.origin_endpoint is required")
    ));
}
