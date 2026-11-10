//! Shared configuration and identifiers for the hasqlite workspace.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Stable identity for a cluster member (hostname, pod name, etc.).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

impl NodeId {
    /// Creates a new `NodeId` from the provided string identifier.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the string slice of the `NodeId`.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the `NodeId` and returns the inner `String`.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for NodeId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Operational mode for the node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum NodeMode {
    /// Active member of an HA cluster with lease fencing and replication.
    #[default]
    Clustered,
    /// Standalone single node with auto-granted genesis lease and dormant replication.
    Standalone,
}

/// Canonical file name for standalone advisory lease locks within the database volume.
pub const STANDALONE_CANONICAL_LEASE_FILENAME: &str = ".hasqlite.lease";

/// Default lease TTL: candidates wait this long after the last observed lease change before acquiring.
pub const DEFAULT_LEASE_TTL_SECS: u64 = 10;
/// Default writer renewal deadline: commits stop this long after the last successful renewal was sent.
pub const DEFAULT_LEASE_RENEW_DEADLINE_SECS: u64 = 6;
/// Default interval between lease renewal or acquisition attempts.
pub const DEFAULT_LEASE_RETRY_PERIOD_SECS: u64 = 2;
/// Minimum gap between `lease.ttl_secs` and `lease.renew_deadline_secs`.
///
/// A writer stalled inside a commit for longer than this gap can still commit after a peer promotes,
/// because no storage-level fence rejects stale generations; the margin is the tolerated stall.
pub const MIN_LEASE_FENCING_MARGIN_SECS: u64 = 2;
/// Maximum `lease.ttl_secs`; it also bounds `lease.renew_deadline_secs`, keeping writer deadlines representable.
pub const MAX_LEASE_TTL_SECS: u64 = 3_600;

/// Returns true if a path is empty or contains only whitespace characters.
fn is_empty_or_whitespace(path: &std::path::Path) -> bool {
    path.as_os_str().is_empty() || path.to_str().map(|s| s.trim().is_empty()).unwrap_or(false)
}

/// Checks a credential source pair: at most one of the env var or token file may be set, and neither may be blank.
fn validate_credential_source(
    env_field: &str,
    env: Option<&str>,
    file_field: &str,
    file: Option<&std::path::Path>,
) -> Result<(), HasqliteError> {
    if env.is_some() && file.is_some() {
        return Err(HasqliteError::Validation(format!(
            "cannot specify both {env_field} and {file_field} (choose one credential source)"
        )));
    }
    if let Some(env) = env
        && env.trim().is_empty()
    {
        return Err(HasqliteError::Validation(format!(
            "{env_field} must not be empty when specified"
        )));
    }
    if let Some(path) = file
        && is_empty_or_whitespace(path)
    {
        return Err(HasqliteError::Validation(format!(
            "{file_field} must not be empty when specified"
        )));
    }
    Ok(())
}

/// Returns true if `endpoint` is `https://host:port`, or `grpc://host:port` with a loopback host.
fn is_valid_sync_origin_endpoint(endpoint: &str) -> bool {
    let Some((scheme, authority)) = endpoint.split_once("://") else {
        return false;
    };
    if !matches!(scheme, "grpc" | "https")
        || authority.is_empty()
        || authority.contains(['/', '?', '#', '@'])
    {
        return false;
    }

    let (host, port) = if let Some(bracketed_host) = authority.strip_prefix('[') {
        let Some((host, suffix)) = bracketed_host.split_once(']') else {
            return false;
        };
        let Some(port) = suffix.strip_prefix(':') else {
            return false;
        };
        if host.parse::<std::net::Ipv6Addr>().is_err() {
            return false;
        }
        (host, port)
    } else {
        let Some((host, port)) = authority.rsplit_once(':') else {
            return false;
        };
        if host.is_empty() || host.contains(':') {
            return false;
        }
        (host, port)
    };

    let host = host.strip_suffix('.').unwrap_or(host);
    if host.is_empty() {
        return false;
    }

    let ip = host.parse::<std::net::IpAddr>().ok();
    let valid_hostname = host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        });
    let valid_host = ip.is_some() || valid_hostname;
    // `*.localhost` subdomains are excluded: some resolvers forward them upstream, which would send the token in cleartext.
    let loopback = ip.is_some_and(|ip| ip.is_loopback()) || host.eq_ignore_ascii_case("localhost");

    valid_host
        && !port.is_empty()
        && port.bytes().all(|byte| byte.is_ascii_digit())
        && port.parse::<u16>().is_ok_and(|port| port != 0)
        && (scheme == "https" || (scheme == "grpc" && loopback))
}

/// Supported coordination backend types for writer leases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LeaseSourceType {
    /// Kubernetes Lease API coordination.
    Kubernetes,
    /// File-backed lease with advisory POSIX locking (`flock`).
    File,
}

/// Top-level cluster configuration (see `docs/PLAN.md` §7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    /// Identity and execution mode of the node.
    pub node: NodeSection,
    /// Storage directory paths for SQLite databases and lockfiles.
    pub data: DataSection,
    /// Authoritative writer lease coordination settings.
    pub lease: LeaseSection,
    /// Freshness ledger metadata path and retention limits (required in clustered mode).
    #[serde(default)]
    pub freshness: Option<FreshnessSection>,
    /// Promotion eligibility thresholds (required in clustered mode).
    #[serde(default)]
    pub promotion: Option<PromotionSection>,
    /// Incremental sync schedule and origin connection parameters (required in clustered mode).
    #[serde(default)]
    pub sync: Option<SyncSection>,
    /// Application gRPC SQL Gateway binding and authentication settings.
    pub gateway: GatewaySection,
    /// HTTP health probe listener configuration.
    pub probes: ProbesSection,
}

impl ClusterConfig {
    /// Parses a `ClusterConfig` from a TOML string and runs semantic validation.
    pub fn from_toml(toml_str: &str) -> Result<Self, HasqliteError> {
        let config: Self =
            toml::from_str(toml_str).map_err(|e| HasqliteError::Config(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// Returns the canonical standalone file lease path within the database data directory.
    pub fn canonical_standalone_lease_path(&self) -> PathBuf {
        self.data.dir.join(STANDALONE_CANONICAL_LEASE_FILENAME)
    }

    /// Validates semantic correctness, required backend sections, positive intervals, path presence, and socket address formats.
    pub fn validate(&self) -> Result<(), HasqliteError> {
        if self.node.id.as_str().trim().is_empty() {
            return Err(HasqliteError::Validation(
                "node.id must not be empty".to_string(),
            ));
        }
        if is_empty_or_whitespace(&self.data.dir) {
            return Err(HasqliteError::Validation(
                "data.dir must not be empty".to_string(),
            ));
        }

        // Checked before `validate_lease` so a standalone node misconfigured with another backend
        // is told to switch to a file lease rather than to complete the other backend's section.
        if self.node.mode == NodeMode::Standalone && self.lease.source != LeaseSourceType::File {
            return Err(HasqliteError::Validation(
                "node.mode is 'standalone' but lease.source is not 'file' (standalone mode requires file-backed lease for flock mutual exclusion)".to_string(),
            ));
        }
        Self::validate_lease(&self.lease)?;

        match self.node.mode {
            NodeMode::Clustered => {
                let freshness = self.freshness.as_ref().ok_or_else(|| {
                    HasqliteError::Validation(
                        "freshness section is required in clustered mode".to_string(),
                    )
                })?;
                Self::validate_freshness(freshness)?;

                let sync = self.sync.as_ref().ok_or_else(|| {
                    HasqliteError::Validation(
                        "sync section is required in clustered mode".to_string(),
                    )
                })?;
                sync.validate()?;
                // Any clustered node can be demoted to replica and must then resync from the writer alias.
                if sync.origin_endpoint.is_none() {
                    return Err(HasqliteError::Validation(
                        "sync.origin_endpoint is required in clustered mode".to_string(),
                    ));
                }
                // Steady-state lag reaches about one sync interval, so a smaller bound would deny every promotion.
                if freshness.max_lag_secs <= sync.interval_secs {
                    return Err(HasqliteError::Validation(format!(
                        "freshness.max_lag_secs ({}) must exceed sync.interval_secs ({})",
                        freshness.max_lag_secs, sync.interval_secs
                    )));
                }

                let promotion = self.promotion.as_ref().ok_or_else(|| {
                    HasqliteError::Validation(
                        "promotion section is required in clustered mode".to_string(),
                    )
                })?;
                Self::validate_promotion_section(promotion)?;
            }
            NodeMode::Standalone => {
                let Some(file) = &self.lease.file else {
                    return Err(HasqliteError::Validation(
                        "lease.source is 'file' but lease.file section is missing".to_string(),
                    ));
                };
                let canonical_lock = self.canonical_standalone_lease_path();
                if file.path != canonical_lock {
                    return Err(HasqliteError::Validation(format!(
                        "standalone mode requires canonical lease lock path '{}', found '{}'",
                        canonical_lock.display(),
                        file.path.display()
                    )));
                }
                if let Some(ref freshness) = self.freshness {
                    Self::validate_freshness(freshness)?;
                }
                if let Some(ref sync) = self.sync {
                    sync.validate()?;
                }
                if let Some(ref promotion) = self.promotion {
                    Self::validate_promotion_section(promotion)?;
                }
            }
        }

        validate_credential_source(
            "gateway.auth_token_env",
            self.gateway.auth_token_env.as_deref(),
            "gateway.auth_token_file",
            self.gateway.auth_token_file.as_deref(),
        )?;
        if (self.gateway.auth_token_env.is_some() || self.gateway.auth_token_file.is_some())
            && self.gateway.allow_unauthenticated
        {
            return Err(HasqliteError::Validation(
                "cannot specify gateway credentials together with gateway.allow_unauthenticated = true".to_string(),
            ));
        }
        // A loopback bind does not imply local-only access: the clustered topology fronts the gateway with
        // a TLS sidecar, which would expose unauthenticated SQL and replication to the network.
        if self.gateway.allow_unauthenticated && self.node.mode == NodeMode::Clustered {
            return Err(HasqliteError::Validation(
                "gateway.allow_unauthenticated is only permitted in standalone mode".to_string(),
            ));
        }
        if self.gateway.auth_token_env.is_none()
            && self.gateway.auth_token_file.is_none()
            && !self.gateway.allow_unauthenticated
        {
            return Err(HasqliteError::Validation(
                "gateway requires authentication credentials (set gateway.auth_token_env or gateway.auth_token_file, or explicitly enable gateway.allow_unauthenticated = true)".to_string(),
            ));
        }
        let gateway_bind = self
            .gateway
            .bind
            .parse::<std::net::SocketAddr>()
            .map_err(|_| {
                HasqliteError::Validation(format!(
                    "gateway.bind '{}' is not a valid socket address (expected IP:PORT)",
                    self.gateway.bind
                ))
            })?;
        if self.gateway.allow_unauthenticated && !gateway_bind.ip().is_loopback() {
            return Err(HasqliteError::Validation(
                "gateway.allow_unauthenticated requires gateway.bind to use a loopback address"
                    .to_string(),
            ));
        }
        if self
            .probes
            .http_bind
            .parse::<std::net::SocketAddr>()
            .is_err()
        {
            return Err(HasqliteError::Validation(format!(
                "probes.http_bind '{}' is not a valid socket address (expected IP:PORT)",
                self.probes.http_bind
            )));
        }
        Ok(())
    }

    /// Validates lease timing bounds and the section required by the selected lease backend.
    fn validate_lease(lease: &LeaseSection) -> Result<(), HasqliteError> {
        if lease.retry_period_secs == 0 {
            return Err(HasqliteError::Validation(
                "lease.retry_period_secs must be greater than 0".to_string(),
            ));
        }
        if lease.retry_period_secs >= lease.renew_deadline_secs {
            return Err(HasqliteError::Validation(format!(
                "lease.retry_period_secs ({}) must be less than lease.renew_deadline_secs ({})",
                lease.retry_period_secs, lease.renew_deadline_secs
            )));
        }
        if lease.ttl_secs > MAX_LEASE_TTL_SECS {
            return Err(HasqliteError::Validation(format!(
                "lease.ttl_secs must not exceed {MAX_LEASE_TTL_SECS} seconds"
            )));
        }
        let min_ttl_secs = lease
            .renew_deadline_secs
            .checked_add(MIN_LEASE_FENCING_MARGIN_SECS)
            .ok_or_else(|| {
                HasqliteError::Validation(format!(
                    "lease.renew_deadline_secs ({}) is too large",
                    lease.renew_deadline_secs
                ))
            })?;
        if lease.ttl_secs < min_ttl_secs {
            return Err(HasqliteError::Validation(format!(
                "lease.ttl_secs ({}) must exceed lease.renew_deadline_secs ({}) by at least {MIN_LEASE_FENCING_MARGIN_SECS} seconds",
                lease.ttl_secs, lease.renew_deadline_secs
            )));
        }
        match lease.source {
            LeaseSourceType::Kubernetes => {
                let k8s = lease.kubernetes.as_ref().ok_or_else(|| {
                    HasqliteError::Validation(
                        "lease.source is 'kubernetes' but lease.kubernetes section is missing"
                            .to_string(),
                    )
                })?;
                if k8s.lease_name.trim().is_empty() {
                    return Err(HasqliteError::Validation(
                        "lease.kubernetes.lease_name must not be empty".to_string(),
                    ));
                }
                if k8s.namespace.trim().is_empty() {
                    return Err(HasqliteError::Validation(
                        "lease.kubernetes.namespace must not be empty".to_string(),
                    ));
                }
            }
            LeaseSourceType::File => {
                let file = lease.file.as_ref().ok_or_else(|| {
                    HasqliteError::Validation(
                        "lease.source is 'file' but lease.file section is missing".to_string(),
                    )
                })?;
                if is_empty_or_whitespace(&file.path) {
                    return Err(HasqliteError::Validation(
                        "lease.file.path must not be empty".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Validates the freshness ledger path and lag bound.
    fn validate_freshness(freshness: &FreshnessSection) -> Result<(), HasqliteError> {
        if is_empty_or_whitespace(&freshness.path) {
            return Err(HasqliteError::Validation(
                "freshness.path must not be empty".to_string(),
            ));
        }
        if freshness.max_lag_secs == 0 {
            return Err(HasqliteError::Validation(
                "freshness.max_lag_secs must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }

    /// Validates promotion thresholds.
    fn validate_promotion_section(promotion: &PromotionSection) -> Result<(), HasqliteError> {
        if promotion.max_future_skew_secs > 300 {
            return Err(HasqliteError::Validation(
                "promotion.max_future_skew_secs must not exceed 300 seconds".to_string(),
            ));
        }
        Ok(())
    }
}

/// Node identity and operating mode section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSection {
    /// Unique identifier for this cluster or standalone node.
    pub id: NodeId,
    /// Operating mode (clustered vs standalone).
    #[serde(default)]
    pub mode: NodeMode,
}

/// Data storage directories section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataSection {
    /// Base directory for SQLite databases and volume lockfiles.
    pub dir: PathBuf,
}

/// Writer lease backend coordination section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseSection {
    /// Coordination source type (`kubernetes` or `file`).
    pub source: LeaseSourceType,
    /// Configuration for file-backed lease coordination.
    #[serde(default)]
    pub file: Option<FileLeaseSection>,
    /// Configuration for Kubernetes Lease API coordination.
    #[serde(default)]
    pub kubernetes: Option<KubernetesLeaseSection>,
    /// Seconds a candidate waits, on its local monotonic clock, after the last observed lease change.
    #[serde(default = "default_lease_ttl_secs")]
    pub ttl_secs: u64,
    /// Seconds after sending its last successful renewal at which the writer stops accepting commits.
    #[serde(default = "default_lease_renew_deadline_secs")]
    pub renew_deadline_secs: u64,
    /// Seconds between lease renewal or acquisition attempts.
    #[serde(default = "default_lease_retry_period_secs")]
    pub retry_period_secs: u64,
}

/// Serde default for `lease.ttl_secs`.
fn default_lease_ttl_secs() -> u64 {
    DEFAULT_LEASE_TTL_SECS
}

/// Serde default for `lease.renew_deadline_secs`.
fn default_lease_renew_deadline_secs() -> u64 {
    DEFAULT_LEASE_RENEW_DEADLINE_SECS
}

/// Serde default for `lease.retry_period_secs`.
fn default_lease_retry_period_secs() -> u64 {
    DEFAULT_LEASE_RETRY_PERIOD_SECS
}

/// File-backed lease configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileLeaseSection {
    /// Path to the lease lockfile.
    pub path: PathBuf,
}

/// Kubernetes Lease API configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KubernetesLeaseSection {
    /// Name of the Kubernetes `Lease` resource.
    pub lease_name: String,
    /// Kubernetes namespace where the `Lease` resource is created.
    pub namespace: String,
}

/// Replica freshness tracking configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FreshnessSection {
    /// Path to the freshness ledger state file; keep it on the same filesystem as the database.
    pub path: PathBuf,
    /// Maximum allowed lag, in seconds, of the last snapshot behind the writer's last lease renewal.
    pub max_lag_secs: u64,
}

/// Writer promotion criteria and thresholds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromotionSection {
    /// Maximum allowable future clock skew in seconds.
    pub max_future_skew_secs: u64,
}

/// Page-level incremental synchronization configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncSection {
    /// Interval in seconds between periodic background sync passes.
    pub interval_secs: u64,
    /// Origin endpoint to pull replication pages from (`https://host:port`, or `grpc://host:port` for loopback only).
    /// Required in clustered mode, where it should name a stable alias that always routes to the current writer.
    #[serde(default)]
    pub origin_endpoint: Option<String>,
    /// Environment variable containing the replication authentication token (mutually exclusive with `transport_auth_token_file`).
    #[serde(default)]
    pub transport_auth_token_env: Option<String>,
    /// File path containing the replication authentication token (mutually exclusive with `transport_auth_token_env`).
    #[serde(default)]
    pub transport_auth_token_file: Option<PathBuf>,
}

impl SyncSection {
    /// Validates sync intervals, origin security, and credential source settings.
    pub fn validate(&self) -> Result<(), HasqliteError> {
        if self.interval_secs == 0 {
            return Err(HasqliteError::Validation(
                "sync.interval_secs must be greater than 0".to_string(),
            ));
        }
        if let Some(ref endpoint) = self.origin_endpoint
            && !is_valid_sync_origin_endpoint(endpoint)
        {
            return Err(HasqliteError::Validation(
                "sync.origin_endpoint must use HTTPS for remote origins; grpc:// is limited to loopback addresses, with a valid host and non-zero port".to_string(),
            ));
        }
        if self.origin_endpoint.is_some()
            && self.transport_auth_token_env.is_none()
            && self.transport_auth_token_file.is_none()
        {
            return Err(HasqliteError::Validation(
                "sync.origin_endpoint requires a credential (set sync.transport_auth_token_env or sync.transport_auth_token_file)".to_string(),
            ));
        }
        validate_credential_source(
            "sync.transport_auth_token_env",
            self.transport_auth_token_env.as_deref(),
            "sync.transport_auth_token_file",
            self.transport_auth_token_file.as_deref(),
        )
    }
}

/// SQL gateway listener and authentication configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewaySection {
    /// Socket address (`IP:PORT`) on which the gRPC gateway listens.
    pub bind: String,
    /// Environment variable containing the bearer auth token (mutually exclusive with `auth_token_file`).
    #[serde(default)]
    pub auth_token_env: Option<String>,
    /// File path containing the bearer auth token (mutually exclusive with `auth_token_env`).
    #[serde(default)]
    pub auth_token_file: Option<PathBuf>,
    /// Whether to permit read-only SQL queries on replicas.
    #[serde(default)]
    pub allow_replica_reads: bool,
    /// Whether to permit unauthenticated gateway access (insecure; loopback binds only, disabled by default).
    #[serde(default)]
    pub allow_unauthenticated: bool,
}

/// Health and readiness HTTP probes section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbesSection {
    /// Socket address (`IP:PORT`) on which HTTP probes listen.
    pub http_bind: String,
}

/// Root error type for configuration parsing and semantic validation.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum HasqliteError {
    /// Deserialization or syntax error in configuration.
    #[error("configuration error: {0}")]
    Config(String),
    /// Semantic validation error in configuration constraints.
    #[error("validation error: {0}")]
    Validation(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILE_LEASE_CLUSTER_TOML: &str = r#"
[node]
id = "node-alpha"

[data]
dir = "/tmp/data"

[lease]
source = "file"

[lease.file]
path = "/tmp/hasqlite.lease"

[freshness]
path = "/tmp/freshness.txt"
max_lag_secs = 5

[promotion]
max_future_skew_secs = 3

[sync]
interval_secs = 1
origin_endpoint = "https://hasqlite-writer:50443"
transport_auth_token_env = "HASQLITE_GRPC_AUTH_TOKEN"

[gateway]
bind = "127.0.0.1:50051"
auth_token_file = "/run/secrets/hasqlite_token"

[probes]
http_bind = "127.0.0.1:8088"
"#;

    /// Valid clustered configuration using a Kubernetes lease, mutated by individual tests.
    fn valid_test_config() -> ClusterConfig {
        ClusterConfig {
            node: NodeSection {
                id: NodeId::from("pod-0"),
                mode: NodeMode::Clustered,
            },
            data: DataSection {
                dir: PathBuf::from("/var/lib/hasqlite"),
            },
            lease: LeaseSection {
                source: LeaseSourceType::Kubernetes,
                file: None,
                kubernetes: Some(KubernetesLeaseSection {
                    lease_name: "hasqlite-writer".to_string(),
                    namespace: "default".to_string(),
                }),
                ttl_secs: DEFAULT_LEASE_TTL_SECS,
                renew_deadline_secs: DEFAULT_LEASE_RENEW_DEADLINE_SECS,
                retry_period_secs: DEFAULT_LEASE_RETRY_PERIOD_SECS,
            },
            freshness: Some(FreshnessSection {
                path: PathBuf::from("/var/lib/hasqlite/.hasqlite.freshness"),
                max_lag_secs: 10,
            }),
            promotion: Some(PromotionSection {
                max_future_skew_secs: 2,
            }),
            sync: Some(SyncSection {
                interval_secs: 2,
                origin_endpoint: Some("https://writer:50443".to_string()),
                transport_auth_token_env: Some("HASQLITE_GRPC_AUTH_TOKEN".to_string()),
                transport_auth_token_file: None,
            }),
            gateway: GatewaySection {
                bind: "0.0.0.0:50051".to_string(),
                auth_token_env: Some("HASQLITE_GRPC_AUTH_TOKEN".to_string()),
                auth_token_file: None,
                allow_replica_reads: false,
                allow_unauthenticated: false,
            },
            probes: ProbesSection {
                http_bind: "0.0.0.0:8088".to_string(),
            },
        }
    }

    /// Valid standalone configuration with the canonical file lease.
    fn valid_standalone_config() -> ClusterConfig {
        let mut config = valid_test_config();
        config.node.mode = NodeMode::Standalone;
        config.lease.source = LeaseSourceType::File;
        config.lease.kubernetes = None;
        config.lease.file = Some(FileLeaseSection {
            path: PathBuf::from("/var/lib/hasqlite/.hasqlite.lease"),
        });
        config
    }

    #[test]
    fn cluster_config_models_all_documented_sections() {
        let config = valid_test_config();
        assert_eq!(config.node.id, NodeId::from("pod-0"));
        assert_eq!(config.node.id.as_str(), "pod-0");
        assert_eq!(config.node.mode, NodeMode::Clustered);
        assert_eq!(config.data.dir, PathBuf::from("/var/lib/hasqlite"));
        assert_eq!(config.lease.source, LeaseSourceType::Kubernetes);
        assert_eq!(config.freshness.as_ref().unwrap().max_lag_secs, 10);
        assert_eq!(config.promotion.as_ref().unwrap().max_future_skew_secs, 2);
        assert_eq!(config.sync.as_ref().unwrap().interval_secs, 2);
        assert_eq!(config.gateway.bind, "0.0.0.0:50051");
        assert_eq!(config.probes.http_bind, "0.0.0.0:8088");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn toml_roundtrip_and_defaults() {
        let toml_str = FILE_LEASE_CLUSTER_TOML;

        let parsed: ClusterConfig = toml::from_str(toml_str).expect("should parse valid toml");
        assert_eq!(parsed.node.id, NodeId::from("node-alpha"));
        assert_eq!(parsed.node.mode, NodeMode::Clustered);
        assert_eq!(parsed.lease.source, LeaseSourceType::File);
        assert!(!parsed.gateway.allow_replica_reads);
        assert!(!parsed.gateway.allow_unauthenticated);
        assert_eq!(parsed.gateway.auth_token_env, None);
        assert_eq!(
            parsed.gateway.auth_token_file,
            Some(PathBuf::from("/run/secrets/hasqlite_token"))
        );
        assert_eq!(parsed.lease.ttl_secs, DEFAULT_LEASE_TTL_SECS);
        assert_eq!(
            parsed.lease.renew_deadline_secs,
            DEFAULT_LEASE_RENEW_DEADLINE_SECS
        );
        assert_eq!(
            parsed.lease.retry_period_secs,
            DEFAULT_LEASE_RETRY_PERIOD_SECS
        );
        assert_eq!(
            parsed.sync.as_ref().unwrap().origin_endpoint.as_deref(),
            Some("https://hasqlite-writer:50443")
        );
        assert_eq!(
            parsed.sync.as_ref().unwrap().transport_auth_token_file,
            None
        );
        assert!(parsed.validate().is_ok());

        let default_gw: GatewaySection =
            toml::from_str("bind = \"127.0.0.1:50051\"").expect("default gateway section");
        assert!(!default_gw.allow_unauthenticated);
        assert!(!default_gw.allow_replica_reads);

        let serialized = toml::to_string(&parsed).expect("should serialize to toml");
        let reloaded: ClusterConfig =
            toml::from_str(&serialized).expect("should reload serialized toml");
        assert_eq!(parsed, reloaded);
    }

    #[test]
    fn standalone_config_omitting_sync_and_promotion_is_valid() {
        let toml_str = r#"
[node]
id = "standalone-0"
mode = "standalone"

[data]
dir = "/var/lib/hasqlite"

[lease]
source = "file"

[lease.file]
path = "/var/lib/hasqlite/.hasqlite.lease"

[gateway]
bind = "0.0.0.0:50051"
auth_token_file = "/run/secrets/hasqlite_token"

[probes]
http_bind = "0.0.0.0:8088"
"#;

        let parsed: ClusterConfig = toml::from_str(toml_str).expect("should parse valid toml");
        assert_eq!(parsed.node.id, NodeId::from("standalone-0"));
        assert_eq!(parsed.node.mode, NodeMode::Standalone);
        assert_eq!(parsed.lease.source, LeaseSourceType::File);
        assert!(parsed.freshness.is_none());
        assert!(parsed.promotion.is_none());
        assert!(parsed.sync.is_none());
        assert_eq!(
            parsed.gateway.auth_token_file,
            Some(PathBuf::from("/run/secrets/hasqlite_token"))
        );
        assert!(!parsed.gateway.allow_unauthenticated);
        assert!(parsed.validate().is_ok());
    }

    /// Applies `mutate` to a valid clustered config and asserts validation fails with `expected_substr`.
    fn assert_validation_err<F>(mutate: F, expected_substr: &str)
    where
        F: FnOnce(&mut ClusterConfig),
    {
        let mut config = valid_test_config();
        mutate(&mut config);
        let err = config.validate().unwrap_err();
        match err {
            HasqliteError::Validation(msg) => {
                assert!(
                    msg.contains(expected_substr),
                    "expected error containing '{expected_substr}', got '{msg}'"
                );
            }
            other => panic!("expected Validation error, got '{other:?}'"),
        }
    }

    #[test]
    fn clustered_mode_requires_freshness_sync_promotion() {
        assert_validation_err(
            |c| c.freshness = None,
            "freshness section is required in clustered mode",
        );
        assert_validation_err(
            |c| c.sync = None,
            "sync section is required in clustered mode",
        );
        assert_validation_err(
            |c| c.promotion = None,
            "promotion section is required in clustered mode",
        );
    }

    #[test]
    fn standalone_mode_rejects_kubernetes_lease() {
        assert_validation_err(
            |c| {
                c.node.mode = NodeMode::Standalone;
                c.lease.source = LeaseSourceType::Kubernetes;
            },
            "node.mode is 'standalone' but lease.source is not 'file'",
        );
    }

    #[test]
    fn standalone_mode_reports_lease_source_before_backend_section() {
        assert_validation_err(
            |c| {
                c.node.mode = NodeMode::Standalone;
                c.lease.source = LeaseSourceType::Kubernetes;
                c.lease.kubernetes = None;
            },
            "node.mode is 'standalone' but lease.source is not 'file'",
        );
    }

    #[test]
    fn unknown_top_level_section_is_rejected() {
        let toml_str = FILE_LEASE_CLUSTER_TOML.replace("[sync]", "[sycn]");
        let err = ClusterConfig::from_toml(&toml_str).unwrap_err();
        assert!(
            matches!(&err, HasqliteError::Config(message) if message.contains("unknown field")),
            "expected unknown-field config error, got {err:?}"
        );
    }

    #[test]
    fn standalone_mode_rejects_non_canonical_lease_path() {
        assert_validation_err(
            |c| {
                c.node.mode = NodeMode::Standalone;
                c.lease.source = LeaseSourceType::File;
                c.lease.file = Some(FileLeaseSection {
                    path: PathBuf::from("/var/lib/hasqlite/other.lease"),
                });
            },
            "standalone mode requires canonical lease lock path",
        );
    }

    #[test]
    fn gateway_rejects_both_auth_token_env_and_file() {
        assert_validation_err(
            |c| {
                c.gateway.auth_token_env = Some("HASQLITE_AUTH_TOKEN".to_string());
                c.gateway.auth_token_file = Some(PathBuf::from("/etc/hasqlite/token.txt"));
            },
            "cannot specify both gateway.auth_token_env and gateway.auth_token_file",
        );
    }

    #[test]
    fn gateway_rejects_credentials_with_allow_unauthenticated() {
        assert_validation_err(
            |c| {
                c.gateway.auth_token_env = Some("HASQLITE_AUTH_TOKEN".to_string());
                c.gateway.allow_unauthenticated = true;
            },
            "cannot specify gateway credentials together with gateway.allow_unauthenticated = true",
        );
        assert_validation_err(
            |c| {
                c.gateway.auth_token_env = None;
                c.gateway.auth_token_file = Some(PathBuf::from("/etc/hasqlite/token.txt"));
                c.gateway.allow_unauthenticated = true;
            },
            "cannot specify gateway credentials together with gateway.allow_unauthenticated = true",
        );
    }

    #[test]
    fn gateway_rejects_unauthenticated_without_flag() {
        assert_validation_err(
            |c| {
                c.gateway.auth_token_env = None;
                c.gateway.auth_token_file = None;
                c.gateway.allow_unauthenticated = false;
            },
            "gateway requires authentication credentials",
        );
    }

    #[test]
    fn gateway_rejects_unauthenticated_public_bind() {
        let mut config = valid_standalone_config();
        config.gateway.auth_token_env = None;
        config.gateway.allow_unauthenticated = true;
        config.gateway.bind = "0.0.0.0:50051".to_string();
        assert!(matches!(
            config.validate().unwrap_err(),
            HasqliteError::Validation(message)
                if message.contains("gateway.allow_unauthenticated requires gateway.bind to use a loopback address")
        ));
    }

    #[test]
    fn gateway_allows_unauthenticated_loopback_bind_in_standalone_mode() {
        let mut config = valid_standalone_config();
        config.gateway.auth_token_env = None;
        config.gateway.allow_unauthenticated = true;
        config.gateway.bind = "127.0.0.1:50051".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn clustered_mode_rejects_unauthenticated_gateway_even_on_loopback() {
        assert_validation_err(
            |c| {
                c.gateway.auth_token_env = None;
                c.gateway.allow_unauthenticated = true;
                c.gateway.bind = "127.0.0.1:50051".to_string();
            },
            "gateway.allow_unauthenticated is only permitted in standalone mode",
        );
    }

    #[test]
    fn lease_timing_must_leave_fencing_margin() {
        assert_validation_err(
            |c| {
                c.lease.ttl_secs = 7;
                c.lease.renew_deadline_secs = 6;
            },
            "lease.ttl_secs (7) must exceed lease.renew_deadline_secs (6) by at least 2 seconds",
        );
        let mut config = valid_test_config();
        config.lease.ttl_secs = 8;
        config.lease.renew_deadline_secs = 6;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn lease_timing_rejects_unbounded_values() {
        assert_validation_err(
            |c| c.lease.ttl_secs = MAX_LEASE_TTL_SECS + 1,
            "lease.ttl_secs must not exceed 3600 seconds",
        );
        assert_validation_err(
            |c| c.lease.renew_deadline_secs = u64::MAX,
            &format!("lease.renew_deadline_secs ({}) is too large", u64::MAX),
        );
        assert_validation_err(
            |c| c.lease.renew_deadline_secs = u64::MAX - MIN_LEASE_FENCING_MARGIN_SECS,
            "lease.ttl_secs (10) must exceed lease.renew_deadline_secs",
        );
        let mut config = valid_test_config();
        config.lease.ttl_secs = MAX_LEASE_TTL_SECS;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn lease_retry_period_must_be_positive_and_below_renew_deadline() {
        assert_validation_err(
            |c| c.lease.retry_period_secs = 0,
            "lease.retry_period_secs must be greater than 0",
        );
        assert_validation_err(
            |c| c.lease.retry_period_secs = c.lease.renew_deadline_secs,
            "lease.retry_period_secs (6) must be less than lease.renew_deadline_secs (6)",
        );
    }

    #[test]
    fn clustered_mode_requires_lag_bound_above_sync_interval() {
        assert_validation_err(
            |c| c.freshness.as_mut().unwrap().max_lag_secs = c.sync.as_ref().unwrap().interval_secs,
            "freshness.max_lag_secs (2) must exceed sync.interval_secs (2)",
        );
    }

    #[test]
    fn lease_timing_overrides_parse_from_toml() {
        let toml_str = FILE_LEASE_CLUSTER_TOML.replace(
            "[lease]\nsource = \"file\"",
            "[lease]\nsource = \"file\"\nttl_secs = 20\nrenew_deadline_secs = 12\nretry_period_secs = 3",
        );
        let config = ClusterConfig::from_toml(&toml_str).expect("parse lease timing overrides");
        assert_eq!(config.lease.ttl_secs, 20);
        assert_eq!(config.lease.renew_deadline_secs, 12);
        assert_eq!(config.lease.retry_period_secs, 3);
    }

    #[test]
    fn validation_rejects_empty_node_id() {
        assert_validation_err(
            |c| c.node.id = NodeId::from("   "),
            "node.id must not be empty",
        );
    }

    #[test]
    fn validation_rejects_zero_sync_interval() {
        assert_validation_err(
            |c| c.sync.as_mut().unwrap().interval_secs = 0,
            "sync.interval_secs must be greater than 0",
        );
    }

    #[test]
    fn validation_rejects_zero_freshness_max_age() {
        assert_validation_err(
            |c| c.freshness.as_mut().unwrap().max_lag_secs = 0,
            "freshness.max_lag_secs must be greater than 0",
        );
    }

    #[test]
    fn validation_rejects_missing_kubernetes_section() {
        assert_validation_err(
            |c| {
                c.lease.source = LeaseSourceType::Kubernetes;
                c.lease.kubernetes = None;
            },
            "lease.source is 'kubernetes' but lease.kubernetes section is missing",
        );
    }

    #[test]
    fn validation_rejects_missing_file_section() {
        assert_validation_err(
            |c| {
                c.lease.source = LeaseSourceType::File;
                c.lease.file = None;
            },
            "lease.source is 'file' but lease.file section is missing",
        );
    }

    #[test]
    fn validation_rejects_invalid_socket_address() {
        assert_validation_err(
            |c| c.gateway.bind = "invalid-address".to_string(),
            "gateway.bind 'invalid-address' is not a valid socket address",
        );
        assert_validation_err(
            |c| c.probes.http_bind = "999.999.999.999:80".to_string(),
            "probes.http_bind '999.999.999.999:80' is not a valid socket address",
        );
    }

    #[test]
    fn validation_rejects_empty_and_whitespace_paths() {
        for invalid_path in ["", "   ", " \t "] {
            assert_validation_err(
                |c| c.data.dir = PathBuf::from(invalid_path),
                "data.dir must not be empty",
            );
            assert_validation_err(
                |c| c.freshness.as_mut().unwrap().path = PathBuf::from(invalid_path),
                "freshness.path must not be empty",
            );
            assert_validation_err(
                |c| {
                    c.lease.source = LeaseSourceType::File;
                    c.lease.kubernetes = None;
                    c.lease.file = Some(FileLeaseSection {
                        path: PathBuf::from(invalid_path),
                    });
                },
                "lease.file.path must not be empty",
            );
        }
    }

    #[test]
    fn validation_rejects_excessive_clock_skew() {
        assert_validation_err(
            |c| c.promotion.as_mut().unwrap().max_future_skew_secs = 301,
            "promotion.max_future_skew_secs must not exceed 300 seconds",
        );
    }

    #[test]
    fn validation_rejects_empty_strings_in_optional_fields() {
        assert_validation_err(
            |c| c.sync.as_mut().unwrap().origin_endpoint = Some("   ".to_string()),
            "sync.origin_endpoint must use HTTPS for remote origins",
        );
        assert_validation_err(
            |c| {
                c.sync.as_mut().unwrap().origin_endpoint =
                    Some("grpc://writer endpoint:50051".to_string())
            },
            "sync.origin_endpoint must use HTTPS for remote origins",
        );
        assert_validation_err(
            |c| c.sync.as_mut().unwrap().transport_auth_token_env = Some("".to_string()),
            "sync.transport_auth_token_env must not be empty",
        );
        assert_validation_err(
            |c| c.gateway.auth_token_env = Some("   ".to_string()),
            "gateway.auth_token_env must not be empty",
        );
        assert_validation_err(
            |c| {
                c.gateway.auth_token_env = None;
                c.gateway.auth_token_file = Some(PathBuf::from("   "));
            },
            "gateway.auth_token_file must not be empty",
        );
        assert_validation_err(
            |c| c.lease.kubernetes.as_mut().unwrap().lease_name = "   ".to_string(),
            "lease.kubernetes.lease_name must not be empty",
        );
        assert_validation_err(
            |c| c.lease.kubernetes.as_mut().unwrap().namespace = "".to_string(),
            "lease.kubernetes.namespace must not be empty",
        );
    }

    #[test]
    fn sync_accepts_supported_origin_endpoint_formats() {
        for endpoint in [
            "https://writer:50443",
            "https://writer.example.com.:50443",
            "grpc://127.0.0.1:50051",
            "grpc://127.0.0.2:50051",
            "grpc://localhost:50051",
            "grpc://localhost.:50051",
            "grpc://[::1]:50051",
        ] {
            let mut config = valid_test_config();
            config.sync.as_mut().unwrap().origin_endpoint = Some(endpoint.to_string());
            assert!(
                config.validate().is_ok(),
                "expected endpoint '{endpoint}' to be valid"
            );
        }
    }

    #[test]
    fn sync_rejects_malformed_origin_endpoints() {
        for endpoint in [
            "",
            "http://writer:50051",
            "grpc://writer:50051",
            "grpc://writer.example.com.:50051",
            "grpc://writer.localhost:50051",
            "grpc://",
            "grpc://writer",
            "grpc://writer:abc",
            "https://writer:+443",
            "grpc://writer:0",
            "grpc://writer:65536",
            "grpc://bad_host:50051",
            "grpc://user@writer:50051",
            "grpc://writer:50051/path",
            "grpc://[not-ipv6]:50051",
        ] {
            assert_validation_err(
                |config| config.sync.as_mut().unwrap().origin_endpoint = Some(endpoint.to_string()),
                "sync.origin_endpoint must use HTTPS for remote origins",
            );
        }
    }

    #[test]
    fn sync_rejects_both_auth_token_env_and_file() {
        assert_validation_err(
            |c| {
                c.sync.as_mut().unwrap().transport_auth_token_env = Some("ENV_TOKEN".to_string());
                c.sync.as_mut().unwrap().transport_auth_token_file =
                    Some(PathBuf::from("/etc/token"));
            },
            "cannot specify both sync.transport_auth_token_env and sync.transport_auth_token_file",
        );
    }

    #[test]
    fn sync_rejects_whitespace_auth_token_file() {
        assert_validation_err(
            |c| {
                c.sync.as_mut().unwrap().transport_auth_token_env = None;
                c.sync.as_mut().unwrap().transport_auth_token_file = Some(PathBuf::from("   "));
            },
            "sync.transport_auth_token_file must not be empty when specified",
        );
    }

    #[test]
    fn sync_origin_requires_credential_source() {
        assert_validation_err(
            |c| c.sync.as_mut().unwrap().transport_auth_token_env = None,
            "sync.origin_endpoint requires a credential",
        );
    }

    #[test]
    fn clustered_mode_requires_sync_origin() {
        assert_validation_err(
            |c| c.sync.as_mut().unwrap().origin_endpoint = None,
            "sync.origin_endpoint is required in clustered mode",
        );
    }

    #[test]
    fn standalone_mode_allows_missing_origin_and_credential() {
        let mut config = valid_standalone_config();
        config.sync.as_mut().unwrap().origin_endpoint = None;
        config.sync.as_mut().unwrap().transport_auth_token_env = None;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn standalone_mode_requires_lease_file_section() {
        assert_validation_err(
            |c| {
                c.node.mode = NodeMode::Standalone;
                c.lease.source = LeaseSourceType::File;
                c.lease.kubernetes = None;
                c.lease.file = None;
            },
            "lease.source is 'file' but lease.file section is missing",
        );
    }

    #[test]
    fn unknown_config_keys_are_rejected() {
        let err = toml::from_str::<GatewaySection>(
            "bind = \"127.0.0.1:50051\"\nallow_unauthorized = true",
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn from_toml_parses_and_validates() {
        let toml_str = FILE_LEASE_CLUSTER_TOML;
        let config = ClusterConfig::from_toml(toml_str).expect("should parse valid toml");
        assert_eq!(config.node.id.as_str(), "node-alpha");

        let invalid_toml = "invalid toml syntax [[";
        let err = ClusterConfig::from_toml(invalid_toml).unwrap_err();
        assert!(matches!(err, HasqliteError::Config(_)));

        let semantically_invalid_toml = toml_str.replace("interval_secs = 1", "interval_secs = 0");
        let err = ClusterConfig::from_toml(&semantically_invalid_toml).unwrap_err();
        assert!(matches!(
            err,
            HasqliteError::Validation(message)
                if message == "sync.interval_secs must be greater than 0"
        ));
    }

    #[test]
    fn canonical_standalone_lease_path_matches_expected() {
        let config = valid_test_config();
        assert_eq!(
            config.canonical_standalone_lease_path(),
            PathBuf::from("/var/lib/hasqlite/.hasqlite.lease")
        );
    }

    #[test]
    fn node_id_conversions_and_deref() {
        let id = NodeId::new("node-alpha");
        assert_eq!(id.as_str(), "node-alpha");
        assert_eq!(&*id, "node-alpha");
        assert_eq!(format!("{id}"), "node-alpha");
        assert_eq!(id.into_inner(), "node-alpha");
    }
}
