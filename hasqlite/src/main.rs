//! Entry point for the hasqlite node daemon CLI.
//!
//! `hasqlite check --config <file>` loads and validates a configuration and exits; `hasqlite run --config
//! <file>` does the same before starting the node, so the fail-closed startup path (ADR-0017) is exercised
//! end to end from the first release.
#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;

use hasqlite_core::{ClusterConfig, HasqliteError};
use tracing_subscriber::EnvFilter;

/// Exit status for an invalid command line.
const EXIT_USAGE: u8 = 2;
/// Exit status for a configuration that cannot be read, parsed or validated.
const EXIT_CONFIG: u8 = 78; // EX_CONFIG from sysexits.h
/// Exit status when the requested function is not available in this build.
const EXIT_UNAVAILABLE: u8 = 69; // EX_UNAVAILABLE from sysexits.h

/// Usage text printed for `--help` and for invalid command lines.
const USAGE: &str = "usage: hasqlite <check|run> --config <file>";

/// A parsed command line.
#[derive(Debug, PartialEq, Eq)]
enum Command {
    /// Validate the configuration and exit.
    Check {
        /// Configuration file path.
        config: PathBuf,
    },
    /// Validate the configuration and start the node.
    Run {
        /// Configuration file path.
        config: PathBuf,
    },
    /// Print usage.
    Help,
}

/// Parses the arguments after the program name.
fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<Command, String> {
    let mut args = args.into_iter();
    let subcommand = match args.next() {
        None => return Err("missing command".to_string()),
        Some(arg) if arg == "-h" || arg == "--help" || arg == "help" => return Ok(Command::Help),
        Some(arg) => arg,
    };
    let mut config = None;
    while let Some(arg) = args.next() {
        if arg == "--config" || arg == "-c" {
            let value = args
                .next()
                .ok_or_else(|| "--config requires a file path".to_string())?;
            if config.replace(PathBuf::from(value)).is_some() {
                return Err("--config given more than once".to_string());
            }
        } else if let Some(value) = arg.to_str().and_then(|a| a.strip_prefix("--config=")) {
            if config.replace(PathBuf::from(value)).is_some() {
                return Err("--config given more than once".to_string());
            }
        } else {
            return Err(format!("unexpected argument '{}'", arg.to_string_lossy()));
        }
    }
    let config = config.ok_or_else(|| "--config <file> is required".to_string())?;
    match subcommand.to_str() {
        Some("check") => Ok(Command::Check { config }),
        Some("run") => Ok(Command::Run { config }),
        _ => Err(format!(
            "unknown command '{}'",
            subcommand.to_string_lossy()
        )),
    }
}

/// Reads and validates the configuration at `path`, logging every problem found.
fn load_config(path: &std::path::Path) -> Result<ClusterConfig, ExitCode> {
    let contents = std::fs::read_to_string(path).map_err(|error| {
        tracing::error!(path = %path.display(), %error, "cannot read configuration");
        ExitCode::from(EXIT_CONFIG)
    })?;
    ClusterConfig::from_toml(&contents).map_err(|error| {
        match &error {
            HasqliteError::Validation(errors) => {
                for issue in errors.issues() {
                    tracing::error!(path = %path.display(), field = issue.field, "{}", issue.message);
                }
            }
            HasqliteError::Config(message) => {
                tracing::error!(path = %path.display(), "{message}");
            }
        }
        tracing::error!(path = %path.display(), "configuration rejected; refusing to start");
        ExitCode::from(EXIT_CONFIG)
    })
}

/// Initializes logging from `RUST_LOG` (default `info`) and dispatches the command.
fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let command = match parse_args(std::env::args_os().skip(1)) {
        Ok(command) => command,
        Err(message) => {
            eprintln!("hasqlite: {message}\n{USAGE}");
            return ExitCode::from(EXIT_USAGE);
        }
    };
    match command {
        Command::Help => {
            println!("{USAGE}");
            ExitCode::SUCCESS
        }
        Command::Check { config } => match load_config(&config) {
            Ok(loaded) => {
                tracing::info!(
                    path = %config.display(),
                    node = %loaded.node.id,
                    mode = ?loaded.node.mode,
                    "configuration valid"
                );
                ExitCode::SUCCESS
            }
            Err(code) => code,
        },
        Command::Run { config } => match load_config(&config) {
            Ok(loaded) => {
                tracing::info!(
                    version = hasqlite_node::NODE_API_VERSION,
                    node = %loaded.node.id,
                    mode = ?loaded.node.mode,
                    "hasqlite starting"
                );
                tracing::error!(
                    "the node runtime is not implemented yet (see docs/PLAN.md); refusing to report a running node"
                );
                ExitCode::from(EXIT_UNAVAILABLE)
            }
            Err(code) => code,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parses string arguments.
    fn parse(args: &[&str]) -> Result<Command, String> {
        parse_args(args.iter().map(OsString::from))
    }

    #[test]
    fn parses_commands_with_config() {
        assert_eq!(
            parse(&["check", "--config", "a.toml"]),
            Ok(Command::Check {
                config: "a.toml".into()
            })
        );
        assert_eq!(
            parse(&["run", "-c", "a.toml"]),
            Ok(Command::Run {
                config: "a.toml".into()
            })
        );
        assert_eq!(
            parse(&["run", "--config=a.toml"]),
            Ok(Command::Run {
                config: "a.toml".into()
            })
        );
        assert_eq!(parse(&["--help"]), Ok(Command::Help));
    }

    #[test]
    fn rejects_invalid_command_lines() {
        for (args, expected) in [
            (&[][..], "missing command"),
            (&["run"][..], "--config <file> is required"),
            (&["run", "--config"][..], "requires a file path"),
            (&["serve", "--config", "a.toml"][..], "unknown command"),
            (
                &["run", "--config", "a", "--config", "b"][..],
                "more than once",
            ),
            (&["run", "--verbose"][..], "unexpected argument"),
        ] {
            let error = parse(args).unwrap_err();
            assert!(error.contains(expected), "{args:?}: got '{error}'");
        }
    }
}
