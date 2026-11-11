//! Hardened SQL gateway engine (Phase 1b): `sqlite3_stmt_readonly` classification, authorizer,
//! `SQLITE_DBCONFIG_DEFENSIVE`, statement limits and the pre-`COMMIT` write fence hook.
//!
//! Registered in the workspace ahead of implementation so lint, docstring, test and coverage gates
//! apply from the first line of gateway code.
#![forbid(unsafe_code)]
