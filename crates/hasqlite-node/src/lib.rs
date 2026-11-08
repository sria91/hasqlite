//! Node supervisor: lease loop + sync worker + gateway (Phase 3).

/// Runtime version string for support bundles.
pub const NODE_API_VERSION: &str = "0.0.0-bootstrap";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_api_version_is_valid_semver_prefix() {
        assert!(NODE_API_VERSION.starts_with("0.0.0"));
    }
}
