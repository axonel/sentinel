//! Plexis Server
//!
//! API-first HTTP server and operational control interface for Plexis.

pub mod cli;
pub mod git;
pub mod github;
pub mod routes;
pub mod state;
pub mod terminal;
pub mod workflow_executor;
pub mod workspace_lock;

pub use routes::{create_router, spawn_mission_runner, MissionRunnerGuard};
pub use state::{AppState, RunnerHandle};
pub use workspace_lock::WorkspaceLockManager;
