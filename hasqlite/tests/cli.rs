//! End-to-end checks of the fail-closed startup path through the real binary.

use std::process::Command;

/// Valid standalone configuration rooted at `dir`.
fn standalone_config(dir: &std::path::Path) -> String {
    format!(
        r#"
[node]
id = "edge-0"
mode = "standalone"

[data]
dir = "{dir}"

[lease]
source = "file"

[lease.file]
path = "{dir}/.hasqlite.lease"

[gateway]
bind = "127.0.0.1:50051"
allow_unauthenticated = true

[probes]
http_bind = "127.0.0.1:8088"
"#,
        dir = dir.display()
    )
}

/// Runs the binary with `args` and returns its exit code and stderr.
fn hasqlite(args: &[&str]) -> (Option<i32>, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_hasqlite"))
        .args(args)
        .env("RUST_LOG", "info")
        .output()
        .expect("run hasqlite");
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn check_accepts_a_valid_configuration() {
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("hasqlite.toml");
    std::fs::write(&config, standalone_config(directory.path())).unwrap();

    let (code, stderr) = hasqlite(&["check", "--config", config.to_str().unwrap()]);
    assert_eq!(code, Some(0), "stderr: {stderr}");
    assert!(stderr.contains("configuration valid"), "stderr: {stderr}");
}

#[test]
fn check_reports_every_problem_and_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("hasqlite.toml");
    let invalid = standalone_config(directory.path())
        .replace("127.0.0.1:50051", "0.0.0.0:50051")
        .replace("127.0.0.1:8088", "not-an-address");
    std::fs::write(&config, invalid).unwrap();

    let (code, stderr) = hasqlite(&["check", "--config", config.to_str().unwrap()]);
    assert_eq!(code, Some(78), "stderr: {stderr}");
    assert!(stderr.contains("gateway.bind"), "stderr: {stderr}");
    assert!(stderr.contains("probes.http_bind"), "stderr: {stderr}");
    assert!(stderr.contains("refusing to start"), "stderr: {stderr}");
}

#[test]
fn run_refuses_to_pretend_the_node_is_running() {
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("hasqlite.toml");
    std::fs::write(&config, standalone_config(directory.path())).unwrap();

    let (code, stderr) = hasqlite(&["run", "--config", config.to_str().unwrap()]);
    assert_eq!(code, Some(69), "stderr: {stderr}");
    assert!(stderr.contains("not implemented"), "stderr: {stderr}");
}

#[test]
fn missing_configuration_file_fails_closed() {
    let (code, _) = hasqlite(&["check", "--config", "/nonexistent/hasqlite.toml"]);
    assert_eq!(code, Some(78));
}

#[test]
fn invalid_command_line_is_a_usage_error() {
    let (code, stderr) = hasqlite(&["run"]);
    assert_eq!(code, Some(2));
    assert!(stderr.contains("usage: hasqlite"));
}
