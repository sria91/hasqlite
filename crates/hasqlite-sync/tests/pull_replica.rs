use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use hasqlite_core::SyncSection;
use hasqlite_sync::{SyncError, pull_replica};
use libsqlite3_sys as ffi;
use rsqlite_rsync::db::Connection;
use rsqlite_rsync::gateway::{AuthConfig, DatabaseEngine, ReplicationServer};
use rsqlite_rsync::proto::rsqlite::v1::replication_service_server::ReplicationServiceServer;
use tempfile::TempDir;
use tokio::sync::oneshot;

const TEST_TOKEN: &str = "test-replication-token";

struct TestServer {
    address: SocketAddr,
    data_dir: PathBuf,
    _directory: TempDir,
    shutdown: Option<oneshot::Sender<()>>,
}

impl TestServer {
    async fn start() -> Self {
        let directory = tempfile::tempdir().expect("create test directory");
        let data_dir = directory.path().join("databases");
        fs::create_dir_all(&data_dir).expect("create database directory");

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test port");
        let address = listener.local_addr().expect("read test address");
        drop(listener);

        let engine = DatabaseEngine::new(&data_dir).expect("create database engine");
        let replication = ReplicationServer::new(
            engine,
            AuthConfig::required(TEST_TOKEN.to_string()),
            rsqlite_rsync::SyncTuning::default(),
            None,
        );
        let service = ReplicationServiceServer::new(replication);
        let (shutdown, shutdown_rx) = oneshot::channel();

        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_shutdown(address, async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("run test replication server");
        });

        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if std::net::TcpStream::connect(address).is_ok() {
                return Self {
                    address,
                    data_dir,
                    _directory: directory,
                    shutdown: Some(shutdown),
                };
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("test replication server did not start on {address}");
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
}

#[tokio::test]
async fn pull_replica_preserves_replication_error_type() {
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

    assert!(matches!(
        error,
        SyncError::Replication(rsqlite_rsync::error::SyncError::Network(_))
    ));
    assert!(
        std::error::Error::source(&error).is_some(),
        "retain the underlying replication error as a source"
    );
}
