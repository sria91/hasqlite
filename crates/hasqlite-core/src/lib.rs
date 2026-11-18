//! Shared configuration and identifiers for the hasqlite workspace.
#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Stable identity for a cluster member (hostname, pod name, etc.).
///
/// Deserialization validates the identifier (see [`NodeId::parse`]), so IDs read from lease records,
/// ledgers or configuration can never carry separators or control characters into records and logs.
/// [`NodeId::new`] does not validate; use it only for trusted, in-process values and call
/// [`NodeId::is_valid`] (as `ClusterConfig::validate` does) where that matters.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct NodeId(String);

impl NodeId {
    /// Creates a new `NodeId` from the provided string identifier without validating it.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Creates a `NodeId`, rejecting anything other than 1–253 ASCII letters, digits, `.`, `_` or `-`.
    pub fn parse(id: impl Into<String>) -> Result<Self, InvalidNodeId> {
        let id = id.into();
        if is_valid_identifier(&id) {
            Ok(Self(id))
        } else {
            Err(InvalidNodeId(id))
        }
    }

    /// Returns true if this ID satisfies the rules enforced by [`NodeId::parse`].
    pub fn is_valid(&self) -> bool {
        is_valid_identifier(&self.0)
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

impl TryFrom<String> for NodeId {
    type Error = InvalidNodeId;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::parse(s)
    }
}

impl From<NodeId> for String {
    fn from(id: NodeId) -> Self {
        id.0
    }
}

impl std::str::FromStr for NodeId {
    type Err = InvalidNodeId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A string that is not a valid [`NodeId`]; the offending value is escaped when displayed.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error(
    "invalid node id \"{}\": must be 1-{MAX_IDENTIFIER_LEN} ASCII letters, digits, '.', '_' or '-'",
    .0.escape_debug()
)]
pub struct InvalidNodeId(String);

/// Maximum length of a database name in bytes, and of each `/`-separated component.
pub const MAX_DATABASE_NAME_LEN: usize = 255;
/// Maximum number of databases in a cluster's database set, and of tombstones (ADR-0020).
pub const MAX_DATABASES: usize = 64;
/// File extensions a database name must end with.
const DATABASE_NAME_EXTENSIONS: [&str; 3] = [".db", ".sqlite", ".sqlite3"];
/// First path components reserved for hasqlite-managed directories inside `data.dir`.
const RESERVED_DATABASE_ROOTS: [&str; 2] = ["diverged", "ancestors"];

/// Name of a replicated database: a path relative to `data.dir` (ADR-0020, REQ-MDB-002).
///
/// Each `/`-separated component is 1–255 ASCII letters, digits, `.`, `_` or `-` and does not start with
/// `.`; the whole name is at most 255 bytes, ends in `.db`, `.sqlite` or `.sqlite3`, and does not start
/// with the reserved component `diverged` or `ancestors`. Construction and deserialization both validate.
/// Ordering is byte-wise, which is the order the database set is sorted and checked in.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct DatabaseName(String);

impl DatabaseName {
    /// Validates and wraps a database name.
    pub fn parse(name: impl Into<String>) -> Result<Self, InvalidDatabaseName> {
        let name = name.into();
        match database_name_violation(&name) {
            None => Ok(Self(name)),
            Some(reason) => Err(InvalidDatabaseName { name, reason }),
        }
    }

    /// Returns the name as a relative path string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the single-component file name used for this database's ledger record and diverged copies:
    /// `%` is encoded as `%25` and `/` as `%2F`, so distinct names never collide.
    pub fn encoded_file_name(&self) -> String {
        self.0.replace('%', "%25").replace('/', "%2F")
    }
}

impl std::fmt::Display for DatabaseName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for DatabaseName {
    type Error = InvalidDatabaseName;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        Self::parse(name)
    }
}

impl From<DatabaseName> for String {
    fn from(name: DatabaseName) -> Self {
        name.0
    }
}

impl std::str::FromStr for DatabaseName {
    type Err = InvalidDatabaseName;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl AsRef<str> for DatabaseName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A string that is not a valid [`DatabaseName`]; the offending value is escaped when displayed.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("invalid database name \"{}\": {reason}", .name.escape_debug())]
pub struct InvalidDatabaseName {
    /// The rejected name.
    pub name: String,
    /// Which rule it broke.
    pub reason: &'static str,
}

/// Returns the first database naming rule `name` breaks, or `None` if it is valid.
fn database_name_violation(name: &str) -> Option<&'static str> {
    if name.len() > MAX_DATABASE_NAME_LEN {
        return Some("longer than 255 bytes");
    }
    if !DATABASE_NAME_EXTENSIONS
        .iter()
        .any(|ext| name.ends_with(ext))
    {
        return Some("must end in .db, .sqlite or .sqlite3");
    }
    for component in name.split('/') {
        if component.is_empty() {
            return Some("contains an empty path component");
        }
        if component.starts_with('.') {
            return Some("a path component starts with '.'");
        }
        if !component
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        {
            return Some("path components may contain only ASCII letters, digits, '.', '_' or '-'");
        }
    }
    let root = name.split('/').next().unwrap_or_default();
    if RESERVED_DATABASE_ROOTS.contains(&root) {
        return Some("starts with a reserved directory ('diverged' or 'ancestors')");
    }
    None
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
/// Maximum `promotion.max_future_skew_secs`.
pub const MAX_FUTURE_SKEW_SECS: u64 = 300;
/// Default `sync.max_concurrent_pulls`.
pub const DEFAULT_MAX_CONCURRENT_PULLS: u32 = 4;
/// Maximum `sync.max_concurrent_pulls` (one pull per database, and a cluster holds at most 64 databases).
pub const MAX_CONCURRENT_PULLS: u32 = MAX_DATABASES as u32;
/// Maximum length of `node.id` and of Kubernetes resource names (DNS-1123 subdomain limit).
pub const MAX_IDENTIFIER_LEN: usize = 253;

/// Returns true if `id` is a safe identifier: 1–253 ASCII letters, digits, `.`, `_` or `-`.
///
/// Node IDs are embedded in lease records, ledger files, audit log lines and metrics labels, so
/// whitespace, separators and control characters are rejected to rule out record and log injection.
fn is_valid_identifier(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_IDENTIFIER_LEN
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Returns true if `label` is a DNS label: 1–63 bytes of characters accepted by `allowed`, with no
/// leading or trailing `-`.
fn is_dns_label(label: &str, allowed: impl Fn(u8) -> bool) -> bool {
    !label.is_empty()
        && label.len() <= 63
        && !label.starts_with('-')
        && !label.ends_with('-')
        && label.bytes().all(|b| b == b'-' || allowed(b))
}

/// Returns true if `name` is a dot-separated sequence of DNS labels of at most 253 bytes.
fn is_dns_name(name: &str, allowed: impl Fn(u8) -> bool + Copy) -> bool {
    !name.is_empty()
        && name.len() <= MAX_IDENTIFIER_LEN
        && name.split('.').all(|label| is_dns_label(label, allowed))
}

/// Returns true if `name` is a DNS-1123 subdomain as required for Kubernetes resource names and namespaces.
fn is_dns1123_subdomain(name: &str) -> bool {
    is_dns_name(name, |b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

/// Returns true if a path is empty or contains only whitespace characters.
fn is_empty_or_whitespace(path: &Path) -> bool {
    path.as_os_str().is_empty() || path.to_str().map(|s| s.trim().is_empty()).unwrap_or(false)
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
    let valid_hostname = is_dns_name(host, |b| b.is_ascii_alphanumeric());
    // Reject addresses that look like IPv4 literals (all dot-separated labels are purely
    // numeric) but failed to parse as a valid IP, e.g. `grpc://999.999.999.999:50051`.
    let looks_like_ip_literal = host
        .split('.')
        .all(|label| !label.is_empty() && label.bytes().all(|b| b.is_ascii_digit()));
    let valid_host = ip.is_some() || (valid_hostname && !looks_like_ip_literal);
    // `*.localhost` subdomains are excluded: some resolvers forward them upstream, which would send the token in cleartext.
    let loopback = ip.is_some_and(|ip| ip.is_loopback()) || host.eq_ignore_ascii_case("localhost");

    valid_host
        && !port.is_empty()
        && port.bytes().all(|byte| byte.is_ascii_digit())
        && port.parse::<u16>().is_ok_and(|port| port != 0)
        && (scheme == "https" || (scheme == "grpc" && loopback))
}

/// Accumulates every validation problem found, so an operator can fix a configuration in one pass.
#[derive(Debug, Default)]
struct Issues(Vec<ValidationIssue>);

impl Issues {
    /// Records a problem with `field`.
    fn push(&mut self, field: &'static str, message: impl Into<String>) {
        self.0.push(ValidationIssue {
            field,
            message: message.into(),
        });
    }

    /// Converts the collected problems into a result.
    fn into_result(self) -> Result<(), HasqliteError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(HasqliteError::Validation(ValidationErrors(self.0)))
        }
    }

    /// Checks a credential source pair: at most one of the env var or token file may be set, and neither may be blank.
    fn credential_source(
        &mut self,
        env_field: &'static str,
        env: Option<&str>,
        file_field: &'static str,
        file: Option<&Path>,
    ) {
        if env.is_some() && file.is_some() {
            self.push(
                env_field,
                format!(
                    "cannot specify both {env_field} and {file_field} (choose one credential source)"
                ),
            );
        }
        if env.is_some_and(|env| env.trim().is_empty()) {
            self.push(
                env_field,
                format!("{env_field} must not be empty when specified"),
            );
        }
        if file.is_some_and(is_empty_or_whitespace) {
            self.push(
                file_field,
                format!("{file_field} must not be empty when specified"),
            );
        }
    }

    /// Parses `value` as a [`std::net::SocketAddr`], recording a problem on failure.
    fn socket_addr(&mut self, field: &'static str, value: &str) -> Option<std::net::SocketAddr> {
        match value.parse::<std::net::SocketAddr>() {
            Ok(addr) => Some(addr),
            Err(e) => {
                self.push(
                    field,
                    format!(
                        "{field} '{value}' is not a valid socket address (expected IP:PORT): {e}"
                    ),
                );
                None
            }
        }
    }
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

/// Top-level cluster configuration (see `docs/configuration.md` §8).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    /// Identity and execution mode of the node.
    pub node: NodeSection,
    /// Storage directory paths for SQLite databases and lockfiles.
    pub data: DataSection,
    /// Authoritative writer lease coordination settings.
    pub lease: LeaseSection,
    /// Freshness ledger directory and lag bound (required in clustered mode).
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

    /// Validates semantic correctness, required backend sections, positive intervals, path presence, and
    /// socket address formats.
    ///
    /// Every problem found is reported, in the order the sections appear in the configuration. Checks that
    /// depend on another value (for example the lag bound, which needs a valid sync interval) are skipped
    /// while that value is itself invalid, so each mistake is reported once.
    pub fn validate(&self) -> Result<(), HasqliteError> {
        let mut issues = Issues::default();

        if self.node.id.as_str().trim().is_empty() {
            issues.push("node.id", "node.id must not be empty");
        } else if !self.node.id.is_valid() {
            issues.push(
                "node.id",
                format!(
                    "node.id must be 1-{MAX_IDENTIFIER_LEN} ASCII letters, digits, '.', '_' or '-' (it is embedded in lease records, ledgers and logs)"
                ),
            );
        }
        if is_empty_or_whitespace(&self.data.dir) {
            issues.push("data.dir", "data.dir must not be empty");
        }

        // A standalone node misconfigured with another backend is told to switch to a file lease rather
        // than to complete the other backend's section.
        let wrong_standalone_source =
            self.node.mode == NodeMode::Standalone && self.lease.source != LeaseSourceType::File;
        if wrong_standalone_source {
            issues.push(
                "lease.source",
                "node.mode is 'standalone' but lease.source is not 'file' (standalone mode requires file-backed lease for flock mutual exclusion)",
            );
        }
        Self::validate_lease(
            &mut issues,
            &self.lease,
            self.node.mode,
            &self.data.dir,
            !wrong_standalone_source,
        );

        match self.node.mode {
            NodeMode::Clustered => self.validate_clustered_sections(&mut issues),
            NodeMode::Standalone => {
                if let Some(ref freshness) = self.freshness {
                    Self::validate_freshness(&mut issues, freshness);
                }
                if let Some(ref sync) = self.sync {
                    sync.collect_issues(&mut issues);
                    // Standalone mode never runs peer replication, so a configured origin is a misconfiguration
                    // (most likely a clustered profile reused with the wrong mode).
                    if sync.origin_endpoint.is_some() {
                        issues.push(
                            "sync.origin_endpoint",
                            "sync.origin_endpoint must not be set in standalone mode (standalone nodes never pull from a peer)",
                        );
                    }
                }
                if let Some(ref promotion) = self.promotion {
                    Self::validate_promotion_section(&mut issues, promotion);
                }
            }
        }

        self.validate_gateway(&mut issues);
        issues.socket_addr("probes.http_bind", &self.probes.http_bind);
        issues.into_result()
    }

    /// Validates the sections clustered mode requires: freshness, sync (with an origin) and promotion.
    fn validate_clustered_sections(&self, issues: &mut Issues) {
        match self.freshness {
            Some(ref freshness) => Self::validate_freshness(issues, freshness),
            None => issues.push(
                "freshness",
                "freshness section is required in clustered mode",
            ),
        }
        match self.sync {
            Some(ref sync) => {
                sync.collect_issues(issues);
                // Any clustered node can be demoted to replica and must then resync from the writer alias.
                if sync.origin_endpoint.is_none() {
                    issues.push(
                        "sync.origin_endpoint",
                        "sync.origin_endpoint is required in clustered mode",
                    );
                }
            }
            None => issues.push("sync", "sync section is required in clustered mode"),
        }
        // Lag is measured from the snapshot start to the writer's last renewal. Between two snapshot
        // starts the writer renews every retry period, so steady-state lag reaches about
        // `interval + retry_period` plus the sync duration; a smaller bound would deny routine promotions.
        if let (Some(freshness), Some(sync)) = (&self.freshness, &self.sync)
            && freshness.max_lag_secs != 0
        {
            let min_lag_bound = sync
                .interval_secs
                .saturating_add(self.lease.retry_period_secs);
            if freshness.max_lag_secs <= min_lag_bound {
                issues.push(
                    "freshness.max_lag_secs",
                    format!(
                        "freshness.max_lag_secs ({}) must exceed sync.interval_secs ({}) + lease.retry_period_secs ({}); allow for the sync duration as well",
                        freshness.max_lag_secs, sync.interval_secs, self.lease.retry_period_secs
                    ),
                );
            }
        }
        match self.promotion {
            Some(ref promotion) => Self::validate_promotion_section(issues, promotion),
            None => issues.push(
                "promotion",
                "promotion section is required in clustered mode",
            ),
        }
    }

    /// Validates gateway credentials, the unauthenticated escape hatch and the bind address.
    fn validate_gateway(&self, issues: &mut Issues) {
        let gateway = &self.gateway;
        issues.credential_source(
            "gateway.auth_token_env",
            gateway.auth_token_env.as_deref(),
            "gateway.auth_token_file",
            gateway.auth_token_file.as_deref(),
        );
        let has_credentials = gateway.auth_token_env.is_some() || gateway.auth_token_file.is_some();
        if has_credentials && gateway.allow_unauthenticated {
            issues.push(
                "gateway.allow_unauthenticated",
                "cannot specify gateway credentials together with gateway.allow_unauthenticated = true",
            );
        }
        // A loopback bind does not imply local-only access: the clustered topology fronts the gateway with
        // a TLS sidecar, which would expose unauthenticated SQL and replication to the network.
        if gateway.allow_unauthenticated && self.node.mode == NodeMode::Clustered {
            issues.push(
                "gateway.allow_unauthenticated",
                "gateway.allow_unauthenticated is only permitted in standalone mode",
            );
        }
        if !has_credentials && !gateway.allow_unauthenticated {
            issues.push(
                "gateway",
                "gateway requires authentication credentials (set gateway.auth_token_env or gateway.auth_token_file, or explicitly enable gateway.allow_unauthenticated = true)",
            );
        }
        if let Some(bind) = issues.socket_addr("gateway.bind", &gateway.bind)
            && gateway.allow_unauthenticated
            && !bind.ip().is_loopback()
        {
            issues.push(
                "gateway.bind",
                "gateway.allow_unauthenticated requires gateway.bind to use a loopback address",
            );
        }
    }

    /// Validates lease timing bounds and, when `check_backend` is set, the section required by the selected
    /// lease backend (in standalone mode also the canonical lock path inside `data_dir`).
    fn validate_lease(
        issues: &mut Issues,
        lease: &LeaseSection,
        mode: NodeMode,
        data_dir: &Path,
        check_backend: bool,
    ) {
        if lease.retry_period_secs == 0 {
            issues.push(
                "lease.retry_period_secs",
                "lease.retry_period_secs must be greater than 0",
            );
        }
        if lease.renew_deadline_secs == 0 {
            issues.push(
                "lease.renew_deadline_secs",
                "lease.renew_deadline_secs must be greater than 0",
            );
        }
        // At least two renewal attempts must fit before the deadline so one failed attempt does not demote.
        if lease.retry_period_secs != 0
            && lease.renew_deadline_secs != 0
            && lease.retry_period_secs.saturating_mul(2) > lease.renew_deadline_secs
        {
            issues.push(
                "lease.retry_period_secs",
                format!(
                    "lease.retry_period_secs ({}) must be at most half of lease.renew_deadline_secs ({}) so one failed renewal attempt does not demote the writer",
                    lease.retry_period_secs, lease.renew_deadline_secs
                ),
            );
        }
        if lease.ttl_secs > MAX_LEASE_TTL_SECS {
            issues.push(
                "lease.ttl_secs",
                format!("lease.ttl_secs must not exceed {MAX_LEASE_TTL_SECS} seconds"),
            );
        }
        match lease
            .renew_deadline_secs
            .checked_add(MIN_LEASE_FENCING_MARGIN_SECS)
        {
            None => issues.push(
                "lease.renew_deadline_secs",
                format!(
                    "lease.renew_deadline_secs ({}) is too large",
                    lease.renew_deadline_secs
                ),
            ),
            Some(min_ttl_secs) if lease.ttl_secs < min_ttl_secs => issues.push(
                "lease.ttl_secs",
                format!(
                    "lease.ttl_secs ({}) must exceed lease.renew_deadline_secs ({}) by at least {MIN_LEASE_FENCING_MARGIN_SECS} seconds",
                    lease.ttl_secs, lease.renew_deadline_secs
                ),
            ),
            Some(_) => {}
        }
        if !check_backend {
            return;
        }
        match lease.source {
            LeaseSourceType::Kubernetes => {
                let Some(k8s) = lease.kubernetes.as_ref() else {
                    issues.push(
                        "lease.kubernetes",
                        "lease.source is 'kubernetes' but lease.kubernetes section is missing",
                    );
                    return;
                };
                for (field, value) in [
                    ("lease.kubernetes.lease_name", &k8s.lease_name),
                    ("lease.kubernetes.namespace", &k8s.namespace),
                ] {
                    if value.trim().is_empty() {
                        issues.push(field, format!("{field} must not be empty"));
                    } else if !is_dns1123_subdomain(value) {
                        issues.push(
                            field,
                            format!(
                                "{field} must be a DNS-1123 subdomain (lowercase letters, digits, '-' and '.')"
                            ),
                        );
                    }
                }
            }
            LeaseSourceType::File => {
                let Some(file) = lease.file.as_ref() else {
                    issues.push(
                        "lease.file",
                        "lease.source is 'file' but lease.file section is missing",
                    );
                    return;
                };
                if is_empty_or_whitespace(&file.path) {
                    issues.push("lease.file.path", "lease.file.path must not be empty");
                } else if mode == NodeMode::Standalone {
                    let canonical_lock = data_dir.join(STANDALONE_CANONICAL_LEASE_FILENAME);
                    if file.path != canonical_lock {
                        issues.push(
                            "lease.file.path",
                            format!(
                                "standalone mode requires canonical lease lock path '{}', found '{}'",
                                canonical_lock.display(),
                                file.path.display()
                            ),
                        );
                    }
                }
            }
        }
    }

    /// Validates the freshness ledger directory and lag bound.
    fn validate_freshness(issues: &mut Issues, freshness: &FreshnessSection) {
        if is_empty_or_whitespace(&freshness.dir) {
            issues.push("freshness.dir", "freshness.dir must not be empty");
        }
        if freshness.max_lag_secs == 0 {
            issues.push(
                "freshness.max_lag_secs",
                "freshness.max_lag_secs must be greater than 0",
            );
        }
    }

    /// Validates promotion thresholds.
    fn validate_promotion_section(issues: &mut Issues, promotion: &PromotionSection) {
        if promotion.max_future_skew_secs > MAX_FUTURE_SKEW_SECS {
            issues.push(
                "promotion.max_future_skew_secs",
                format!(
                    "promotion.max_future_skew_secs must not exceed {MAX_FUTURE_SKEW_SECS} seconds"
                ),
            );
        }
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

/// Serde default for `sync.max_concurrent_pulls`.
fn default_max_concurrent_pulls() -> u32 {
    DEFAULT_MAX_CONCURRENT_PULLS
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
///
/// The single-file `path` key of earlier versions is rejected as an unknown field (REQ-CFG-010).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FreshnessSection {
    /// Directory holding one ledger record per replicated database; keep it on the same filesystem as the
    /// databases, owned by the node UID with mode `0700` (checked at pre-flight).
    pub dir: PathBuf,
    /// Maximum allowed lag, in seconds, of each database's last snapshot behind the writer's last lease renewal.
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
    /// Maximum number of pulls of different databases run at the same time (1–64, default 4).
    #[serde(default = "default_max_concurrent_pulls")]
    pub max_concurrent_pulls: u32,
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
    /// Validates sync intervals, pull concurrency, origin security, and credential source settings.
    pub fn validate(&self) -> Result<(), HasqliteError> {
        let mut issues = Issues::default();
        self.collect_issues(&mut issues);
        issues.into_result()
    }

    /// Records every problem with this section.
    fn collect_issues(&self, issues: &mut Issues) {
        if self.interval_secs == 0 {
            issues.push(
                "sync.interval_secs",
                "sync.interval_secs must be greater than 0",
            );
        }
        if !(1..=MAX_CONCURRENT_PULLS).contains(&self.max_concurrent_pulls) {
            issues.push(
                "sync.max_concurrent_pulls",
                format!(
                    "sync.max_concurrent_pulls ({}) must be between 1 and {MAX_CONCURRENT_PULLS}",
                    self.max_concurrent_pulls
                ),
            );
        }
        if let Some(ref endpoint) = self.origin_endpoint
            && !is_valid_sync_origin_endpoint(endpoint)
        {
            issues.push(
                "sync.origin_endpoint",
                "sync.origin_endpoint must use HTTPS for remote origins; grpc:// is limited to loopback addresses, with a valid host and non-zero port",
            );
        }
        if self.origin_endpoint.is_some()
            && self.transport_auth_token_env.is_none()
            && self.transport_auth_token_file.is_none()
        {
            issues.push(
                "sync.origin_endpoint",
                "sync.origin_endpoint requires a credential (set sync.transport_auth_token_env or sync.transport_auth_token_file)",
            );
        }
        issues.credential_source(
            "sync.transport_auth_token_env",
            self.transport_auth_token_env.as_deref(),
            "sync.transport_auth_token_file",
            self.transport_auth_token_file.as_deref(),
        );
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

/// One semantic problem with a configuration value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationIssue {
    /// Dotted path of the offending setting or section, e.g. `lease.ttl_secs`.
    pub field: &'static str,
    /// Human-readable explanation, naming the setting and the rule it breaks.
    pub message: String,
}

impl std::fmt::Display for ValidationIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// Every problem `ClusterConfig::validate` found, in configuration order; never empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationErrors(Vec<ValidationIssue>);

impl ValidationErrors {
    /// Returns the individual problems.
    pub fn issues(&self) -> &[ValidationIssue] {
        &self.0
    }

    /// Returns true if any problem concerns `field`.
    pub fn has_field(&self, field: &str) -> bool {
        self.0.iter().any(|issue| issue.field == field)
    }
}

impl std::fmt::Display for ValidationErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.as_slice() {
            [single] => write!(f, "{single}"),
            issues => {
                write!(f, "{} problems: ", issues.len())?;
                for (index, issue) in issues.iter().enumerate() {
                    if index > 0 {
                        f.write_str("; ")?;
                    }
                    write!(f, "{issue}")?;
                }
                Ok(())
            }
        }
    }
}

/// Root error type for configuration parsing and semantic validation.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum HasqliteError {
    /// Deserialization or syntax error in configuration.
    #[error("configuration error: {0}")]
    Config(String),
    /// Semantic validation errors in configuration constraints.
    #[error("validation error: {0}")]
    Validation(ValidationErrors),
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
dir = "/tmp/freshness"
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
                id: NodeId::new("pod-0"),
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
                dir: PathBuf::from("/var/lib/hasqlite/.hasqlite.freshness"),
                max_lag_secs: 10,
            }),
            promotion: Some(PromotionSection {
                max_future_skew_secs: 2,
            }),
            sync: Some(SyncSection {
                interval_secs: 2,
                max_concurrent_pulls: DEFAULT_MAX_CONCURRENT_PULLS,
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

    /// Valid standalone configuration with the canonical file lease and no `[sync]` section.
    fn valid_standalone_config() -> ClusterConfig {
        let mut config = valid_test_config();
        config.node.mode = NodeMode::Standalone;
        config.sync = None;
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
        assert_eq!(config.node.id, NodeId::new("pod-0"));
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
        assert_eq!(parsed.node.id, NodeId::new("node-alpha"));
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
        assert_eq!(parsed.node.id, NodeId::new("standalone-0"));
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
            HasqliteError::Validation(errors) => {
                assert!(
                    errors
                        .issues()
                        .iter()
                        .any(|issue| issue.message.contains(expected_substr)),
                    "expected an error containing '{expected_substr}', got '{errors}'"
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
    fn standalone_mode_reports_lease_source_instead_of_backend_section() {
        let mut config = valid_standalone_config();
        config.lease.source = LeaseSourceType::Kubernetes;
        config.lease.file = None;
        let HasqliteError::Validation(errors) = config.validate().unwrap_err() else {
            panic!("expected validation errors");
        };
        assert_eq!(errors.issues().len(), 1, "got {errors}");
        assert_eq!(errors.issues()[0].field, "lease.source");
    }

    #[test]
    fn validation_reports_every_independent_problem_in_config_order() {
        let mut config = valid_test_config();
        config.node.id = NodeId::new("bad id");
        config.lease.ttl_secs = MAX_LEASE_TTL_SECS + 1;
        config.sync.as_mut().unwrap().interval_secs = 0;
        config.probes.http_bind = "nowhere".to_string();
        let HasqliteError::Validation(errors) = config.validate().unwrap_err() else {
            panic!("expected validation errors");
        };
        let fields: Vec<_> = errors.issues().iter().map(|issue| issue.field).collect();
        assert_eq!(
            fields,
            [
                "node.id",
                "lease.ttl_secs",
                "sync.interval_secs",
                "probes.http_bind"
            ]
        );
        assert!(
            errors
                .to_string()
                .starts_with("4 problems: node.id must be")
        );
    }

    #[test]
    fn dependent_checks_are_skipped_while_their_inputs_are_invalid() {
        // A zero lag bound is reported once, not also as "must exceed interval + retry".
        let mut config = valid_test_config();
        config.freshness.as_mut().unwrap().max_lag_secs = 0;
        let HasqliteError::Validation(errors) = config.validate().unwrap_err() else {
            panic!("expected validation errors");
        };
        assert_eq!(errors.issues().len(), 1, "got {errors}");
        // A zero retry period is not also reported as violating the half-deadline rule.
        let mut config = valid_test_config();
        config.lease.renew_deadline_secs = 0;
        let HasqliteError::Validation(errors) = config.validate().unwrap_err() else {
            panic!("expected validation errors");
        };
        assert!(
            !errors
                .issues()
                .iter()
                .any(|i| i.message.contains("at most half"))
        );
    }

    #[test]
    fn freshness_path_is_rejected_in_favour_of_dir() {
        let toml_str = FILE_LEASE_CLUSTER_TOML
            .replace("dir = \"/tmp/freshness\"", "path = \"/tmp/freshness.txt\"");
        let err = ClusterConfig::from_toml(&toml_str).unwrap_err();
        assert!(
            matches!(&err, HasqliteError::Config(message) if message.contains("unknown field `path`")),
            "got {err:?}"
        );
    }

    #[test]
    fn sync_max_concurrent_pulls_defaults_and_bounds() {
        let config = ClusterConfig::from_toml(FILE_LEASE_CLUSTER_TOML).expect("valid config");
        assert_eq!(
            config.sync.unwrap().max_concurrent_pulls,
            DEFAULT_MAX_CONCURRENT_PULLS
        );
        for invalid in [0, MAX_CONCURRENT_PULLS + 1] {
            assert_validation_err(
                |c| c.sync.as_mut().unwrap().max_concurrent_pulls = invalid,
                "sync.max_concurrent_pulls",
            );
        }
        for valid in [1, MAX_CONCURRENT_PULLS] {
            let mut config = valid_test_config();
            config.sync.as_mut().unwrap().max_concurrent_pulls = valid;
            assert!(config.validate().is_ok());
        }
    }

    #[test]
    fn node_id_parse_and_deserialize_reject_unsafe_ids() {
        assert!(NodeId::parse("pod-0").is_ok());
        let err = NodeId::parse("node\n1").unwrap_err();
        assert!(
            err.to_string().contains("\"node\\n1\""),
            "control characters are escaped: {err}"
        );
        assert!("node 1".parse::<NodeId>().is_err());
        assert!(!NodeId::new("node 1").is_valid());

        #[derive(Deserialize)]
        struct Holder {
            #[allow(dead_code)]
            id: NodeId,
        }
        let err = toml::from_str::<Holder>("id = \"evil\\nline\"")
            .err()
            .expect("rejected");
        assert!(err.to_string().contains("invalid node id"), "got {err}");

        let toml_str =
            FILE_LEASE_CLUSTER_TOML.replace("id = \"node-alpha\"", "id = \"node alpha\"");
        assert!(matches!(
            ClusterConfig::from_toml(&toml_str).unwrap_err(),
            HasqliteError::Config(message) if message.contains("invalid node id")
        ));
    }

    #[test]
    fn database_names_follow_adr_0020_rules() {
        for valid in [
            "app.db",
            "tenants/acme.db",
            "a/b/c.sqlite",
            "x.sqlite3",
            "diverged.db",
            "my_db-1.v2.db",
        ] {
            assert!(
                DatabaseName::parse(valid).is_ok(),
                "expected '{valid}' to be valid"
            );
        }
        let long_component = format!("{}.db", "a".repeat(253));
        for (invalid, reason) in [
            ("app", "must end in"),
            ("app.txt", "must end in"),
            ("", "must end in"),
            (".db", "starts with '.'"),
            ("tenants/.hidden.db", "starts with '.'"),
            ("tenants//acme.db", "empty path component"),
            ("/abs.db", "empty path component"),
            ("../escape.db", "starts with '.'"),
            ("tenants/ac me.db", "may contain only"),
            ("tenants\\acme.db", "may contain only"),
            ("ten%ants.db", "may contain only"),
            ("diverged/app.db", "reserved"),
            ("ancestors/app.db", "reserved"),
            (long_component.as_str(), "longer than 255"),
        ] {
            let err = DatabaseName::parse(invalid).unwrap_err();
            assert!(
                err.reason.contains(reason),
                "'{invalid}': expected reason containing '{reason}', got '{}'",
                err.reason
            );
        }
        assert!(DatabaseName::parse(format!("{}.db", "a".repeat(252))).is_ok());
    }

    #[test]
    fn database_name_deserialization_validates_and_orders_bytewise() {
        #[derive(Deserialize)]
        struct Holder {
            name: DatabaseName,
        }
        let holder: Holder = toml::from_str("name = \"tenants/acme.db\"").expect("valid");
        assert_eq!(holder.name.as_str(), "tenants/acme.db");
        assert!(toml::from_str::<Holder>("name = \"../etc/passwd.db\"").is_err());

        let mut names = ["b.db", "a/z.db", "a.db"].map(|n| DatabaseName::parse(n).unwrap());
        names.sort();
        assert_eq!(names.map(|n| n.to_string()), ["a.db", "a/z.db", "b.db"]);
    }

    #[test]
    fn database_name_encodes_to_a_single_file_name() {
        let name = DatabaseName::parse("tenants/acme.db").unwrap();
        assert_eq!(name.encoded_file_name(), "tenants%2Facme.db");
        assert_eq!(
            DatabaseName::parse("app.db").unwrap().encoded_file_name(),
            "app.db"
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
    fn gateway_rejects_auth_token_env_with_allow_unauthenticated() {
        assert_validation_err(
            |c| {
                c.gateway.auth_token_env = Some("HASQLITE_AUTH_TOKEN".to_string());
                c.gateway.allow_unauthenticated = true;
            },
            "cannot specify gateway credentials together with gateway.allow_unauthenticated = true",
        );
    }

    #[test]
    fn gateway_rejects_auth_token_file_with_allow_unauthenticated() {
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
            HasqliteError::Validation(errors)
                if errors.to_string().contains("gateway.allow_unauthenticated requires gateway.bind to use a loopback address")
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
            "lease.retry_period_secs (6) must be at most half of lease.renew_deadline_secs (6)",
        );
        // One failed attempt at 4s would leave no second attempt before the 6s deadline.
        assert_validation_err(
            |c| c.lease.retry_period_secs = 4,
            "lease.retry_period_secs (4) must be at most half of lease.renew_deadline_secs (6)",
        );
        let mut config = valid_test_config();
        config.lease.retry_period_secs = 3;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn lease_timing_rejects_zero_renew_deadline() {
        assert_validation_err(
            |c| c.lease.renew_deadline_secs = 0,
            "lease.renew_deadline_secs must be greater than 0",
        );
    }

    #[test]
    fn clustered_mode_requires_lag_bound_above_sync_interval_plus_retry_period() {
        assert_validation_err(
            |c| c.freshness.as_mut().unwrap().max_lag_secs = c.sync.as_ref().unwrap().interval_secs,
            "freshness.max_lag_secs (2) must exceed sync.interval_secs (2) + lease.retry_period_secs (2)",
        );
        // interval 2 + retry 2 = 4: equal to the bound is still rejected, one more second passes.
        assert_validation_err(
            |c| c.freshness.as_mut().unwrap().max_lag_secs = 4,
            "freshness.max_lag_secs (4) must exceed sync.interval_secs (2) + lease.retry_period_secs (2)",
        );
        let mut config = valid_test_config();
        config.freshness.as_mut().unwrap().max_lag_secs = 5;
        assert!(config.validate().is_ok());
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
            |c| c.node.id = NodeId::new("   "),
            "node.id must not be empty",
        );
    }

    #[test]
    fn validation_rejects_unsafe_node_ids() {
        for id in [
            "node 1",
            "node=1",
            "node\n1",
            "node/1",
            "nöde",
            "node:1",
            &"n".repeat(MAX_IDENTIFIER_LEN + 1),
        ] {
            assert_validation_err(
                |c| c.node.id = NodeId::new(id),
                "node.id must be 1-253 ASCII letters, digits, '.', '_' or '-'",
            );
        }
        for id in ["pod-0", "hasqlite-0.hasqlite.default", "edge_node-7", "a"] {
            let mut config = valid_test_config();
            config.node.id = NodeId::new(id);
            assert!(
                config.validate().is_ok(),
                "expected node id '{id}' to be valid"
            );
        }
    }

    #[test]
    fn validation_rejects_non_dns1123_kubernetes_names() {
        for name in [
            "Hasqlite-Writer",
            "writer_lease",
            "-writer",
            "writer.",
            "a..b",
        ] {
            assert_validation_err(
                |c| c.lease.kubernetes.as_mut().unwrap().lease_name = name.to_string(),
                "lease.kubernetes.lease_name must be a DNS-1123 subdomain",
            );
            assert_validation_err(
                |c| c.lease.kubernetes.as_mut().unwrap().namespace = name.to_string(),
                "lease.kubernetes.namespace must be a DNS-1123 subdomain",
            );
        }
        let mut config = valid_test_config();
        config.lease.kubernetes.as_mut().unwrap().lease_name = "hasqlite-writer.v1".to_string();
        config.lease.kubernetes.as_mut().unwrap().namespace = "data-plane".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn standalone_mode_rejects_sync_origin_endpoint() {
        let mut config = valid_standalone_config();
        config.sync = valid_test_config().sync;
        assert!(matches!(
            config.validate().unwrap_err(),
            HasqliteError::Validation(errors)
                if errors.to_string().contains("sync.origin_endpoint must not be set in standalone mode")
        ));
        config.sync.as_mut().unwrap().origin_endpoint = None;
        config.sync.as_mut().unwrap().transport_auth_token_env = None;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn gateway_allows_unauthenticated_ipv6_loopback_bind_in_standalone_mode() {
        let mut config = valid_standalone_config();
        config.gateway.auth_token_env = None;
        config.gateway.allow_unauthenticated = true;
        config.gateway.bind = "[::1]:50051".to_string();
        assert!(config.validate().is_ok());
        config.gateway.bind = "[::]:50051".to_string();
        assert!(matches!(
            config.validate().unwrap_err(),
            HasqliteError::Validation(errors)
                if errors.to_string().contains("requires gateway.bind to use a loopback address")
        ));
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
                |c| c.freshness.as_mut().unwrap().dir = PathBuf::from(invalid_path),
                "freshness.dir must not be empty",
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
            "grpc://999.999.999.999:50051",
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
    fn standalone_mode_allows_sync_section_without_origin_and_credential() {
        let mut config = valid_standalone_config();
        config.sync = Some(SyncSection {
            interval_secs: 2,
            max_concurrent_pulls: DEFAULT_MAX_CONCURRENT_PULLS,
            origin_endpoint: None,
            transport_auth_token_env: None,
            transport_auth_token_file: None,
        });
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
            HasqliteError::Validation(errors)
                if errors.issues().len() == 1 && errors.has_field("sync.interval_secs")
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
    fn node_id_conversions() {
        let id = NodeId::new("node-alpha");
        assert_eq!(id.as_str(), "node-alpha");
        assert_eq!(id.as_ref(), "node-alpha");
        assert_eq!(NodeId::parse(String::from("node-alpha")).unwrap(), id);
        assert_eq!(String::from(id.clone()), "node-alpha");
        assert_eq!(format!("{id}"), "node-alpha");
        assert_eq!(id.into_inner(), "node-alpha");
    }
}
