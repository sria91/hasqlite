//! Shared configuration and identifiers for the hasqlite workspace.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Stable identity for a cluster member (hostname, pod name, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSection {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataSection {
    pub dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseSection {
    pub source: String,
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

#[derive(thiserror::Error, Debug)]
pub enum HasqliteError {
    #[error("configuration error: {0}")]
    Config(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_config_models_all_documented_sections() {
        let config = ClusterConfig {
            node: NodeSection {
                id: "pod-0".to_string(),
            },
            data: DataSection {
                dir: PathBuf::from("/var/lib/hasqlite"),
            },
            lease: LeaseSection {
                source: "kubernetes".to_string(),
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
        };

        assert_eq!(config.node.id, "pod-0");
        assert_eq!(config.data.dir, PathBuf::from("/var/lib/hasqlite"));
        assert_eq!(config.lease.source, "kubernetes");
        assert_eq!(config.freshness.max_age_secs, 10);
        assert_eq!(config.promotion.max_future_skew_secs, 2);
        assert_eq!(config.sync.interval_secs, 2);
        assert_eq!(config.gateway.bind, "0.0.0.0:50051");
        assert_eq!(config.probes.http_bind, "0.0.0.0:8088");
    }
}
