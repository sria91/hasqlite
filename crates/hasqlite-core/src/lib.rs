//! Shared configuration and identifiers for the hasqlite workspace.

use serde::{Deserialize, Serialize};

/// Stable identity for a cluster member (hostname, pod name, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

/// Top-level cluster configuration (see `docs/PLAN.md` §7).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub node: NodeSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSection {
    pub id: String,
}

#[derive(thiserror::Error, Debug)]
pub enum HasqliteError {
    #[error("configuration error: {0}")]
    Config(String),
}
