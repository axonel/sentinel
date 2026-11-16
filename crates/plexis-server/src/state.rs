use plexis_providers::capabilities::{standard_capability_matrix, ProviderCapabilities};
use plexis_runtime::agent_host::LocalAgentHost;
use plexis_runtime::backend::BackendRegistry;
use plexis_storage::SqliteStore;
use plexis_tools::ToolRegistry;
use std::sync::Arc;

use crate::github::{DefaultGitHubClient, GitHubIntegration};
use crate::terminal::TerminalBuffer;
use crate::workflow_executor::ServerWorkflowExecutor;

/// Container for shared runtime and persistence resources.
/// Execution runner coordination handle for an active background task.
#[derive(Clone, Debug)]
pub struct RunnerHandle {
    pub runner_id: u64,
    pub restart_generation: u64,
    pub notify: Arc<tokio::sync::Notify>,
    pub restart_requested: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<SqliteStore>,
    pub tool_registry: Arc<ToolRegistry>,
    pub auth_token: Option<String>,
    pub terminal_buffer: Arc<TerminalBuffer>,
    pub github: Arc<dyn GitHubIntegration>,
    pub capability_matrix: Vec<ProviderCapabilities>,
    pub agent_host: Arc<LocalAgentHost>,
    pub backend_registry: Arc<BackendRegistry>,
    pub mission_engine: Arc<plexis_runtime::MissionEngine<SqliteStore>>,
    pub workspace_locks: Arc<crate::workspace_lock::WorkspaceLockManager>,
    pub active_mission_runners:
        Arc<std::sync::Mutex<std::collections::HashMap<plexis_core::ids::MissionId, RunnerHandle>>>,
    pub bind_host: String,
    pub is_loopback: bool,
}

impl AppState {
    pub fn new(store: SqliteStore) -> Self {
        let auth_token = std::env::var("SENTINEL_AUTH_TOKEN")
            .or_else(|_| std::env::var("PLEXIS_AUTH_TOKEN"))
            .ok()
            .filter(|s| !s.trim().is_empty());
        let store_arc = Arc::new(store);
        let tool_registry = Arc::new(ToolRegistry::standard_suite());
        let terminal_buffer = Arc::new(TerminalBuffer::new());
        let agent_host = Arc::new(LocalAgentHost::with_default_binary());
        let workflow_executor = Arc::new(ServerWorkflowExecutor::new(
            store_arc.clone(),
            tool_registry.clone(),
            terminal_buffer.clone(),
            agent_host.clone(),
        ));
        let mission_engine = Arc::new(
            plexis_runtime::MissionEngine::new(store_arc.clone())
                .with_workflow_executor(workflow_executor),
        );
        Self {
            store: store_arc,
            tool_registry,
            auth_token,
            terminal_buffer,
            github: Arc::new(DefaultGitHubClient::new()),
            capability_matrix: standard_capability_matrix(),
            agent_host,
            backend_registry: Arc::new(BackendRegistry::with_defaults()),
            mission_engine,
            workspace_locks: Arc::new(crate::workspace_lock::WorkspaceLockManager::new()),
            active_mission_runners: Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            bind_host: "127.0.0.1".to_string(),
            is_loopback: true,
        }
    }

    pub fn with_store(store: Arc<SqliteStore>) -> Self {
        let auth_token = std::env::var("SENTINEL_AUTH_TOKEN")
            .or_else(|_| std::env::var("PLEXIS_AUTH_TOKEN"))
            .ok()
            .filter(|s| !s.trim().is_empty());
        let tool_registry = Arc::new(ToolRegistry::standard_suite());
        let terminal_buffer = Arc::new(TerminalBuffer::new());
        let agent_host = Arc::new(LocalAgentHost::with_default_binary());
        let workflow_executor = Arc::new(ServerWorkflowExecutor::new(
            store.clone(),
            tool_registry.clone(),
            terminal_buffer.clone(),
            agent_host.clone(),
        ));
        let mission_engine = Arc::new(
            plexis_runtime::MissionEngine::new(store.clone())
                .with_workflow_executor(workflow_executor),
        );
        Self {
            store,
            tool_registry,
            auth_token,
            terminal_buffer,
            github: Arc::new(DefaultGitHubClient::new()),
            capability_matrix: standard_capability_matrix(),
            agent_host,
            backend_registry: Arc::new(BackendRegistry::with_defaults()),
            mission_engine,
            workspace_locks: Arc::new(crate::workspace_lock::WorkspaceLockManager::new()),
            active_mission_runners: Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            bind_host: "127.0.0.1".to_string(),
            is_loopback: true,
        }
    }

    pub fn is_mission_runner_active(&self, mission_id: &plexis_core::ids::MissionId) -> bool {
        let map = match self.active_mission_runners.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        map.contains_key(mission_id)
    }

    pub fn get_mission_runner_handle(
        &self,
        mission_id: &plexis_core::ids::MissionId,
    ) -> Option<RunnerHandle> {
        let map = match self.active_mission_runners.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        map.get(mission_id).cloned()
    }

    pub fn remove_runner_from_map_if_owner(
        map: &mut std::collections::HashMap<plexis_core::ids::MissionId, RunnerHandle>,
        mission_id: &plexis_core::ids::MissionId,
        runner_id: u64,
    ) -> bool {
        if map.get(mission_id).map(|h| h.runner_id) == Some(runner_id) {
            map.remove(mission_id);
            true
        } else {
            false
        }
    }

    pub fn remove_mission_runner_if_owner(
        &self,
        mission_id: &plexis_core::ids::MissionId,
        runner_id: u64,
    ) -> bool {
        let mut map = match self.active_mission_runners.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        Self::remove_runner_from_map_if_owner(&mut map, mission_id, runner_id)
    }

    pub fn with_auth_token(mut self, token: Option<String>) -> Self {
        self.auth_token = token;
        self
    }

    pub fn with_bind_host(mut self, host: impl Into<String>, is_loopback: bool) -> Self {
        self.bind_host = host.into();
        self.is_loopback = is_loopback;
        self
    }

    /// Executes deep startup and recovery reconciliation comparing durable SQLite state
    /// with physical Git repositories, unassigning orphaned tasks, resolving intermediate
    /// integration missions, and returning an audit report.
    pub async fn reconcile_startup(
        &self,
    ) -> Result<plexis_runtime::reconciler::ReconciliationReport, plexis_runtime::RuntimeError>
    {
        let reconciler = plexis_runtime::reconciler::Reconciler::new(
            self.store.clone(),
            self.store.clone(),
            self.store.clone(),
        )
        .with_workflow_store(self.store.clone())
        .with_command_store(self.store.clone())
        .with_execution_store(self.store.clone())
        .with_mission_store(self.store.clone())
        .with_workspace_store(self.store.clone());

        reconciler.reconcile_startup().await
    }

    /// Evaluates candidate token using timing-attack-safe comparison,
    /// supporting token rotation via comma-separated active tokens.
    pub fn is_authorized_token(&self, candidate: &str) -> bool {
        let Some(ref token_spec) = self.auth_token else {
            return true;
        };
        for valid_token in token_spec
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            if constant_time_eq(candidate, valid_token) {
                return true;
            }
        }
        false
    }
}

/// Constant-time string equality check preventing timing attacks.
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    if a_bytes.len() != b_bytes.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a_bytes.iter().zip(b_bytes.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
