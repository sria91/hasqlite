//! Probe endpoints: `/live`, `/healthz`, `/ready` (Phase 3).

/// Liveness probe endpoint path for basic process liveness checks.
pub const PATH_LIVE: &str = "/live";
/// Healthz probe endpoint path for rolling deployment traffic checks.
pub const PATH_HEALTHZ: &str = "/healthz";
/// Readiness probe endpoint path indicating active writer leadership.
pub const PATH_READY: &str = "/ready";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_paths_match_specification() {
        assert_eq!(PATH_LIVE, "/live");
        assert_eq!(PATH_HEALTHZ, "/healthz");
        assert_eq!(PATH_READY, "/ready");
    }
}
