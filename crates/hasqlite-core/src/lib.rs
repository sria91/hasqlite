//! Shared configuration and identifiers for the hasqlite workspace.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Stable identity for a cluster member (hostname, pod name, etc.).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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

/// Supported coordination backend types for writer leases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LeaseSourceType {
    Kubernetes,
    File,
}

/// Top-level cluster configuration (see `docs/PLAN.md` §7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub node: NodeSection,
    pub data: DataSection,
    pub lease: LeaseSection,
    pub freshness: FreshnessSection,
    pub promotion: PromotionSection,
    pub sync: SyncSection,
    pub gateway: GatewaySection,
    pub probes: ProbesSection,
}

impl ClusterConfig {
    /// Validates semantic correctness, required backend sections, positive intervals, path presence, and socket address formats.
    pub fn validate(&self) -> Result<(), HasqliteError> {
        if self.node.id.as_str().trim().is_empty() {
            return Err(HasqliteError::Validation(
                "node.id must not be empty".to_string(),
            ));
        }
        if self.data.dir.as_os_str().is_empty() {
            return Err(HasqliteError::Validation(
                "data.dir must not be empty".to_string(),
            ));
        }
        if self.freshness.path.as_os_str().is_empty() {
            return Err(HasqliteError::Validation(
                "freshness.path must not be empty".to_string(),
            ));
        }
        if self.sync.interval_secs == 0 {
            return Err(HasqliteError::Validation(
                "sync.interval_secs must be greater than 0".to_string(),
            ));
        }
        if self.freshness.max_age_secs == 0 {
            return Err(HasqliteError::Validation(
                "freshness.max_age_secs must be greater than 0".to_string(),
            ));
        }
        if self.promotion.max_future_skew_secs > 300 {
            return Err(HasqliteError::Validation(
                "promotion.max_future_skew_secs must not exceed 300 seconds".to_string(),
            ));
        }
        if let Some(ref ep) = self.sync.origin_endpoint
            && ep.trim().is_empty()
        {
            return Err(HasqliteError::Validation(
                "sync.origin_endpoint must not be empty when specified".to_string(),
            ));
        }
        if let Some(ref env) = self.sync.transport_auth_token_env
            && env.trim().is_empty()
        {
            return Err(HasqliteError::Validation(
                "sync.transport_auth_token_env must not be empty when specified".to_string(),
            ));
        }
        if let Some(ref env) = self.gateway.auth_token_env
            && env.trim().is_empty()
        {
            return Err(HasqliteError::Validation(
                "gateway.auth_token_env must not be empty when specified".to_string(),
            ));
        }
        match self.lease.source {
            LeaseSourceType::Kubernetes => {
                let k8s = self.lease.kubernetes.as_ref().ok_or_else(|| {
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
                let file = self.lease.file.as_ref().ok_or_else(|| {
                    HasqliteError::Validation(
                        "lease.source is 'file' but lease.file section is missing".to_string(),
                    )
                })?;
                if file.path.as_os_str().is_empty() {
                    return Err(HasqliteError::Validation(
                        "lease.file.path must not be empty".to_string(),
                    ));
                }
            }
        }
        if self.gateway.bind.parse::<std::net::SocketAddr>().is_err() {
            return Err(HasqliteError::Validation(format!(
                "gateway.bind '{}' is not a valid socket address (expected IP:PORT)",
                self.gateway.bind
            )));
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSection {
    pub id: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataSection {
    pub dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseSection {
    pub source: LeaseSourceType,
    #[serde(default)]
    pub file: Option<FileLeaseSection>,
    #[serde(default)]
    pub kubernetes: Option<KubernetesLeaseSection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileLeaseSection {
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KubernetesLeaseSection {
    pub lease_name: String,
    pub namespace: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreshnessSection {
    pub path: PathBuf,
    pub max_age_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotionSection {
    pub min_source_generation: u64,
    pub max_future_skew_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncSection {
    pub interval_secs: u64,
    #[serde(default)]
    pub origin_endpoint: Option<String>,
    #[serde(default)]
    pub transport_auth_token_env: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewaySection {
    pub bind: String,
    #[serde(default)]
    pub auth_token_env: Option<String>,
    #[serde(default)]
    pub allow_replica_reads: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbesSection {
    pub http_bind: String,
}

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum HasqliteError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("validation error: {0}")]
    Validation(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_test_config() -> ClusterConfig {
        ClusterConfig {
            node: NodeSection {
                id: NodeId::from("pod-0"),
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
            },
            freshness: FreshnessSection {
                path: PathBuf::from("/var/run/freshness.txt"),
                max_age_secs: 10,
            },
            promotion: PromotionSection {
                min_source_generation: 0,
                max_future_skew_secs: 2,
            },
            sync: SyncSection {
                interval_secs: 2,
                origin_endpoint: Some("grpc://writer:50051".to_string()),
                transport_auth_token_env: Some("HASQLITE_GRPC_AUTH_TOKEN".to_string()),
            },
            gateway: GatewaySection {
                bind: "0.0.0.0:50051".to_string(),
                auth_token_env: Some("HASQLITE_GRPC_AUTH_TOKEN".to_string()),
                allow_replica_reads: false,
            },
            probes: ProbesSection {
                http_bind: "0.0.0.0:8088".to_string(),
            },
        }
    }

    #[test]
    fn cluster_config_models_all_documented_sections() {
        let config = valid_test_config();
        assert_eq!(config.node.id, NodeId::from("pod-0"));
        assert_eq!(config.node.id.as_str(), "pod-0");
        assert_eq!(config.data.dir, PathBuf::from("/var/lib/hasqlite"));
        assert_eq!(config.lease.source, LeaseSourceType::Kubernetes);
        assert_eq!(config.freshness.max_age_secs, 10);
        assert_eq!(config.promotion.max_future_skew_secs, 2);
        assert_eq!(config.sync.interval_secs, 2);
        assert_eq!(config.gateway.bind, "0.0.0.0:50051");
        assert_eq!(config.probes.http_bind, "0.0.0.0:8088");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn toml_roundtrip_and_defaults() {
        let toml_str = r#"
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
max_age_secs = 5

[promotion]
min_source_generation = 1
max_future_skew_secs = 3

[sync]
interval_secs = 1

[gateway]
bind = "127.0.0.1:50051"

[probes]
http_bind = "127.0.0.1:8088"
"#;

        let parsed: ClusterConfig = toml::from_str(toml_str).expect("should parse valid toml");
        assert_eq!(parsed.node.id, NodeId::from("node-alpha"));
        assert_eq!(parsed.lease.source, LeaseSourceType::File);
        assert!(!parsed.gateway.allow_replica_reads);
        assert_eq!(parsed.gateway.auth_token_env, None);
        assert_eq!(parsed.sync.origin_endpoint, None);
        assert!(parsed.validate().is_ok());

        let serialized = toml::to_string(&parsed).expect("should serialize to toml");
        let reloaded: ClusterConfig =
            toml::from_str(&serialized).expect("should reload serialized toml");
        assert_eq!(parsed, reloaded);
    }

    #[test]
    fn validation_rejects_empty_node_id() {
        let mut config = valid_test_config();
        config.node.id = NodeId::from("   ");
        let err = config.validate().unwrap_err();
        assert_eq!(
            err,
            HasqliteError::Validation("node.id must not be empty".to_string())
        );
    }

    #[test]
    fn validation_rejects_zero_sync_interval() {
        let mut config = valid_test_config();
        config.sync.interval_secs = 0;
        let err = config.validate().unwrap_err();
        assert_eq!(
            err,
            HasqliteError::Validation("sync.interval_secs must be greater than 0".to_string())
        );
    }

    #[test]
    fn validation_rejects_zero_freshness_max_age() {
        let mut config = valid_test_config();
        config.freshness.max_age_secs = 0;
        let err = config.validate().unwrap_err();
        assert_eq!(
            err,
            HasqliteError::Validation("freshness.max_age_secs must be greater than 0".to_string())
        );
    }

    #[test]
    fn validation_rejects_missing_kubernetes_section() {
        let mut config = valid_test_config();
        config.lease.source = LeaseSourceType::Kubernetes;
        config.lease.kubernetes = None;
        let err = config.validate().unwrap_err();
        assert_eq!(
            err,
            HasqliteError::Validation(
                "lease.source is 'kubernetes' but lease.kubernetes section is missing".to_string()
            )
        );
    }

    #[test]
    fn validation_rejects_missing_file_section() {
        let mut config = valid_test_config();
        config.lease.source = LeaseSourceType::File;
        config.lease.file = None;
        let err = config.validate().unwrap_err();
        assert_eq!(
            err,
            HasqliteError::Validation(
                "lease.source is 'file' but lease.file section is missing".to_string()
            )
        );
    }

    #[test]
    fn validation_rejects_invalid_socket_address() {
        let mut config = valid_test_config();
        config.gateway.bind = "invalid-address".to_string();
        let err = config.validate().unwrap_err();
        assert!(matches!(err, HasqliteError::Validation(msg) if msg.contains("gateway.bind")));

        let mut config2 = valid_test_config();
        config2.probes.http_bind = "999.999.999.999:80".to_string();
        let err2 = config2.validate().unwrap_err();
        assert!(matches!(err2, HasqliteError::Validation(msg) if msg.contains("probes.http_bind")));
    }

    #[test]
    fn validation_rejects_empty_paths() {
        let mut config = valid_test_config();
        config.data.dir = PathBuf::from("");
        let err = config.validate().unwrap_err();
        assert_eq!(
            err,
            HasqliteError::Validation("data.dir must not be empty".to_string())
        );

        let mut config2 = valid_test_config();
        config2.freshness.path = PathBuf::from("");
        let err2 = config2.validate().unwrap_err();
        assert_eq!(
            err2,
            HasqliteError::Validation("freshness.path must not be empty".to_string())
        );
    }

    #[test]
    fn validation_rejects_excessive_clock_skew() {
        let mut config = valid_test_config();
        config.promotion.max_future_skew_secs = 301;
        let err = config.validate().unwrap_err();
        assert_eq!(
            err,
            HasqliteError::Validation(
                "promotion.max_future_skew_secs must not exceed 300 seconds".to_string()
            )
        );
    }

    #[test]
    fn validation_rejects_empty_strings_in_optional_fields() {
        let mut config = valid_test_config();
        config.sync.origin_endpoint = Some("   ".to_string());
        let err = config.validate().unwrap_err();
        assert_eq!(
            err,
            HasqliteError::Validation(
                "sync.origin_endpoint must not be empty when specified".to_string()
            )
        );

        let mut config2 = valid_test_config();
        config2.sync.transport_auth_token_env = Some("".to_string());
        let err2 = config2.validate().unwrap_err();
        assert_eq!(
            err2,
            HasqliteError::Validation(
                "sync.transport_auth_token_env must not be empty when specified".to_string()
            )
        );

        let mut config3 = valid_test_config();
        config3.gateway.auth_token_env = Some("   ".to_string());
        let err3 = config3.validate().unwrap_err();
        assert_eq!(
            err3,
            HasqliteError::Validation(
                "gateway.auth_token_env must not be empty when specified".to_string()
            )
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
