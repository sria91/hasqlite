//! Probe endpoints: `/live`, `/healthz`, `/ready` (Phase 3).

/// Probe path constants aligned with rsqlite-rsync HA HTTP server.
pub const PATH_LIVE: &str = "/live";
pub const PATH_HEALTHZ: &str = "/healthz";
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
