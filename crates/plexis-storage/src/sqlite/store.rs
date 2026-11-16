//! SQLite implementation of the Plexis storage repositories.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::sync::Arc;
use tokio::sync::Mutex;

use plexis_core::ids::{
    AgentId, ApprovalId, CheckpointId, CommandId, EventId, ExecutionId, LeaseId, MemoryId,
    MessageId, MissionId, PlanId, RecoveryId, SessionId, TaskId, VerificationId, WorkflowId,
    WorkspaceId,
};
use plexis_core::state::{
    AgentState, CommandState, ExecutionState, MissionState, TaskState, WorkflowState,
};
use plexis_core::{
    Agent, AgentMessage, ApprovalRecord, ApprovalState, Command, CommandTarget, CommandType, Event,
    Execution, ExecutionProfile, Lease, MemoryProvenance, MemoryRecord, MemoryScope, MemoryState,
    MessageType, Mission, MissionBudget, MissionBudgetConsumed, MissionCheckpoint, MissionCycle,
    MissionHealth, MissionOutcome, PlanStatus, PlanningRecord, RecoveryRecord, RecoveryResult,
    Session, StoppingCondition, Task, TaskGraph, Verification, VerificationVerdict, Workflow,
    Workspace,
};

use crate::error::StorageError;
use crate::sqlite::migrations::run_migrations;
use crate::traits::{
    AgentStore, ApprovalStore, CommandStore, EventStore, ExecutionStore, LeaseStore, MemoryStore,
    MessageStore, MissionStore, PlanStore, RecoveryStore, RetentionPruneReport, RetentionStore,
    SessionStore, TaskStore, VerificationStore, WorkflowStore, WorkspaceStore,
};

/// Primary SQLite-backed storage manager for Plexis.
#[derive(Clone)]
pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
    event_broadcaster: tokio::sync::broadcast::Sender<Event>,
}

impl SqliteStore {
    /// Opens or creates a SQLite database file at `path` and runs migrations.
    pub fn open(path: &str) -> Result<Self, StorageError> {
        let mut conn = Connection::open(path)?;
        Self::configure_and_migrate(&mut conn)?;
        let (tx, _) = tokio::sync::broadcast::channel(1000);
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            event_broadcaster: tx,
        })
    }

    /// Opens an in-memory SQLite database, ideal for testing and ephemeral workloads.
    pub fn open_in_memory() -> Result<Self, StorageError> {
        let mut conn = Connection::open_in_memory()?;
        Self::configure_and_migrate(&mut conn)?;
        let (tx, _) = tokio::sync::broadcast::channel(1000);
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            event_broadcaster: tx,
        })
    }

    /// Subscribes to live events as they are persisted.
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<Event> {
        self.event_broadcaster.subscribe()
    }

    fn configure_and_migrate(conn: &mut Connection) -> Result<(), StorageError> {
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;",
        )?;
        run_migrations(conn)?;
        Ok(())
    }
}

#[async_trait]
impl WorkflowStore for SqliteStore {
    async fn create_workflow(&self, wf: &Workflow) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO workflows (id, workspace_id, title, objective, state, metadata, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                wf.id.to_string(),
                wf.workspace_id.map(|id| id.to_string()),
                wf.title,
                wf.objective,
                wf.state.as_str(),
                serde_json::to_string(&wf.metadata)?,
                wf.created_at.to_rfc3339(),
                wf.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    async fn get_workflow(&self, id: &WorkflowId) -> Result<Option<Workflow>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, workspace_id, title, objective, state, metadata, created_at, updated_at
             FROM workflows WHERE id = ?1",
        )?;

        let result = stmt
            .query_row(params![id.to_string()], |row| {
                let id_str: String = row.get(0)?;
                let ws_str: Option<String> = row.get(1)?;
                let title: String = row.get(2)?;
                let objective: String = row.get(3)?;
                let state_str: String = row.get(4)?;
                let meta_str: String = row.get(5)?;
                let created_str: String = row.get(6)?;
                let updated_str: String = row.get(7)?;

                Ok((
                    id_str,
                    ws_str,
                    title,
                    objective,
                    state_str,
                    meta_str,
                    created_str,
                    updated_str,
                ))
            })
            .optional()?;

        match result {
            Some((
                id_str,
                ws_str,
                title,
                objective,
                state_str,
                meta_str,
                created_str,
                updated_str,
            )) => {
                let wf_id: WorkflowId = id_str.parse()?;
                let workspace_id = match ws_str {
                    Some(s) => Some(s.parse()?),
                    None => None,
                };
                let state: WorkflowState = state_str.parse()?;
                let metadata: serde_json::Value = serde_json::from_str(&meta_str)?;
                let created_at = DateTime::parse_from_rfc3339(&created_str)
                    .map_err(|e| StorageError::Migration(e.to_string()))?
                    .with_timezone(&Utc);
                let updated_at = DateTime::parse_from_rfc3339(&updated_str)
                    .map_err(|e| StorageError::Migration(e.to_string()))?
                    .with_timezone(&Utc);

                Ok(Some(Workflow {
                    id: wf_id,
                    workspace_id,
                    title,
                    objective,
                    state,
                    metadata,
                    created_at,
                    updated_at,
                }))
            }
            None => Ok(None),
        }
    }

    async fn update_workflow(&self, wf: &Workflow) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        let rows = conn.execute(
            "UPDATE workflows
             SET workspace_id = ?1, title = ?2, objective = ?3, state = ?4, metadata = ?5, updated_at = ?6
             WHERE id = ?7",
            params![
                wf.workspace_id.map(|id| id.to_string()),
                wf.title,
                wf.objective,
                wf.state.as_str(),
                serde_json::to_string(&wf.metadata)?,
                wf.updated_at.to_rfc3339(),
                wf.id.to_string(),
            ],
        )?;

        if rows == 0 {
            return Err(StorageError::NotFound {
                entity_type: "Workflow",
                id: wf.id.to_string(),
            });
        }
        Ok(())
    }

    async fn list_workflows(&self) -> Result<Vec<Workflow>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, workspace_id, title, objective, state, metadata, created_at, updated_at
             FROM workflows ORDER BY created_at DESC",
        )?;

        let rows = stmt.query_map([], |row| {
            let id_str: String = row.get(0)?;
            let ws_str: Option<String> = row.get(1)?;
            let title: String = row.get(2)?;
            let objective: String = row.get(3)?;
            let state_str: String = row.get(4)?;
            let meta_str: String = row.get(5)?;
            let created_str: String = row.get(6)?;
            let updated_str: String = row.get(7)?;

            Ok((
                id_str,
                ws_str,
                title,
                objective,
                state_str,
                meta_str,
                created_str,
                updated_str,
            ))
        })?;

        let mut workflows = Vec::new();
        for r in rows {
            let (id_str, ws_str, title, objective, state_str, meta_str, created_str, updated_str) =
                r?;
            let wf_id: WorkflowId = id_str.parse()?;
            let workspace_id = match ws_str {
                Some(s) => Some(s.parse()?),
                None => None,
            };
            let state: WorkflowState = state_str.parse()?;
            let metadata: serde_json::Value = serde_json::from_str(&meta_str)?;
            let created_at = DateTime::parse_from_rfc3339(&created_str)
                .map_err(|e| StorageError::Migration(e.to_string()))?
                .with_timezone(&Utc);
            let updated_at = DateTime::parse_from_rfc3339(&updated_str)
                .map_err(|e| StorageError::Migration(e.to_string()))?
                .with_timezone(&Utc);

            workflows.push(Workflow {
                id: wf_id,
                workspace_id,
                title,
                objective,
                state,
                metadata,
                created_at,
                updated_at,
            });
        }

        Ok(workflows)
    }

    async fn list_workflows_by_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<Workflow>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, workspace_id, title, objective, state, metadata, created_at, updated_at
             FROM workflows WHERE workspace_id = ?1 ORDER BY created_at DESC",
        )?;

        let rows = stmt.query_map(params![workspace_id.to_string()], |row| {
            let id_str: String = row.get(0)?;
            let ws_str: Option<String> = row.get(1)?;
            let title: String = row.get(2)?;
            let objective: String = row.get(3)?;
            let state_str: String = row.get(4)?;
            let meta_str: String = row.get(5)?;
            let created_str: String = row.get(6)?;
            let updated_str: String = row.get(7)?;

            Ok((
                id_str,
                ws_str,
                title,
                objective,
                state_str,
                meta_str,
                created_str,
                updated_str,
            ))
        })?;

        let mut workflows = Vec::new();
        for r in rows {
            let (id_str, ws_str, title, objective, state_str, meta_str, created_str, updated_str) =
                r?;
            let wf_id: WorkflowId = id_str.parse()?;
            let workspace_id = match ws_str {
                Some(s) => Some(s.parse()?),
                None => None,
            };
            let state: WorkflowState = state_str.parse()?;
            let metadata: serde_json::Value = serde_json::from_str(&meta_str)?;
            let created_at = DateTime::parse_from_rfc3339(&created_str)
                .map_err(|e| StorageError::Migration(e.to_string()))?
                .with_timezone(&Utc);
            let updated_at = DateTime::parse_from_rfc3339(&updated_str)
                .map_err(|e| StorageError::Migration(e.to_string()))?
                .with_timezone(&Utc);

            workflows.push(Workflow {
                id: wf_id,
                workspace_id,
                title,
                objective,
                state,
                metadata,
                created_at,
                updated_at,
            });
        }

        Ok(workflows)
    }
}

#[async_trait]
impl TaskStore for SqliteStore {
    async fn create_task(&self, task: &Task) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO tasks (
                id, workflow_id, workspace_id, objective, description, parent_id, state,
                priority, criteria, assigned_agent_id, attempts, max_attempts,
                current_lease_generation, metadata, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                task.id.to_string(),
                task.workflow_id.to_string(),
                task.workspace_id.map(|id| id.to_string()),
                task.objective,
                task.description,
                task.parent_id.map(|id| id.to_string()),
                task.state.as_str(),
                task.priority,
                serde_json::to_string(&task.criteria)?,
                task.assigned_agent_id.map(|id| id.to_string()),
                task.attempts,
                task.max_attempts,
                0i64, // initial lease generation
                serde_json::to_string(&task.metadata)?,
                task.created_at.to_rfc3339(),
                task.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    async fn get_task(&self, id: &TaskId) -> Result<Option<Task>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, workflow_id, workspace_id, objective, description, parent_id, state,
                    priority, criteria, assigned_agent_id, attempts, max_attempts,
                    metadata, created_at, updated_at
             FROM tasks WHERE id = ?1",
        )?;

        let row = stmt
            .query_row(params![id.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, i32>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, u32>(10)?,
                    row.get::<_, u32>(11)?,
                    row.get::<_, String>(12)?,
                    row.get::<_, String>(13)?,
                    row.get::<_, String>(14)?,
                ))
            })
            .optional()?;

        match row {
            Some(t) => Ok(Some(parse_task_tuple(t)?)),
            None => Ok(None),
        }
    }

    async fn update_task(&self, task: &Task) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        let rows = conn.execute(
            "UPDATE tasks SET
                workspace_id = ?1, objective = ?2, description = ?3, parent_id = ?4, state = ?5,
                priority = ?6, criteria = ?7, assigned_agent_id = ?8, attempts = ?9,
                max_attempts = ?10, metadata = ?11, updated_at = ?12
             WHERE id = ?13",
            params![
                task.workspace_id.map(|id| id.to_string()),
                task.objective,
                task.description,
                task.parent_id.map(|id| id.to_string()),
                task.state.as_str(),
                task.priority,
                serde_json::to_string(&task.criteria)?,
                task.assigned_agent_id.map(|id| id.to_string()),
                task.attempts,
                task.max_attempts,
                serde_json::to_string(&task.metadata)?,
                task.updated_at.to_rfc3339(),
                task.id.to_string(),
            ],
        )?;

        if rows == 0 {
            return Err(StorageError::NotFound {
                entity_type: "Task",
                id: task.id.to_string(),
            });
        }
        Ok(())
    }

    async fn list_tasks_by_workflow(
        &self,
        workflow_id: &WorkflowId,
    ) -> Result<Vec<Task>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, workflow_id, workspace_id, objective, description, parent_id, state,
                    priority, criteria, assigned_agent_id, attempts, max_attempts,
                    metadata, created_at, updated_at
             FROM tasks WHERE workflow_id = ?1 ORDER BY priority DESC, created_at ASC",
        )?;

        let rows = stmt.query_map(params![workflow_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, i32>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, u32>(10)?,
                row.get::<_, u32>(11)?,
                row.get::<_, String>(12)?,
                row.get::<_, String>(13)?,
                row.get::<_, String>(14)?,
            ))
        })?;

        let mut tasks = Vec::new();
        for r in rows {
            tasks.push(parse_task_tuple(r?)?);
        }

        Ok(tasks)
    }

    async fn list_tasks_by_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<Task>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, workflow_id, workspace_id, objective, description, parent_id, state,
                    priority, criteria, assigned_agent_id, attempts, max_attempts,
                    metadata, created_at, updated_at
             FROM tasks WHERE workspace_id = ?1 ORDER BY priority DESC, created_at ASC",
        )?;

        let rows = stmt.query_map(params![workspace_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, i32>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, u32>(10)?,
                row.get::<_, u32>(11)?,
                row.get::<_, String>(12)?,
                row.get::<_, String>(13)?,
                row.get::<_, String>(14)?,
            ))
        })?;

        let mut tasks = Vec::new();
        for r in rows {
            tasks.push(parse_task_tuple(r?)?);
        }

        Ok(tasks)
    }

    async fn add_dependency(
        &self,
        task_id: &TaskId,
        depends_on_id: &TaskId,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT OR IGNORE INTO task_dependencies (task_id, depends_on_id) VALUES (?1, ?2)",
            params![task_id.to_string(), depends_on_id.to_string()],
        )?;
        Ok(())
    }

    async fn remove_dependency(
        &self,
        task_id: &TaskId,
        depends_on_id: &TaskId,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "DELETE FROM task_dependencies WHERE task_id = ?1 AND depends_on_id = ?2",
            params![task_id.to_string(), depends_on_id.to_string()],
        )?;
        Ok(())
    }

    async fn get_dependencies(&self, task_id: &TaskId) -> Result<Vec<TaskId>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt =
            conn.prepare("SELECT depends_on_id FROM task_dependencies WHERE task_id = ?1")?;

        let rows = stmt.query_map(params![task_id.to_string()], |row| {
            let s: String = row.get(0)?;
            Ok(s)
        })?;

        let mut ids = Vec::new();
        for r in rows {
            let s = r?;
            ids.push(s.parse()?);
        }

        Ok(ids)
    }

    async fn get_dependents(&self, task_id: &TaskId) -> Result<Vec<TaskId>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt =
            conn.prepare("SELECT task_id FROM task_dependencies WHERE depends_on_id = ?1")?;

        let rows = stmt.query_map(params![task_id.to_string()], |row| {
            let s: String = row.get(0)?;
            Ok(s)
        })?;

        let mut ids = Vec::new();
        for r in rows {
            let s = r?;
            ids.push(s.parse()?);
        }

        Ok(ids)
    }

    async fn load_task_graph(&self, workflow_id: &WorkflowId) -> Result<TaskGraph, StorageError> {
        let tasks = self.list_tasks_by_workflow(workflow_id).await?;
        let conn = self.conn.lock().await;

        let mut stmt = conn.prepare(
            "SELECT td.task_id, td.depends_on_id
             FROM task_dependencies td
             JOIN tasks t ON td.task_id = t.id
             WHERE t.workflow_id = ?1",
        )?;

        let edge_rows = stmt.query_map(params![workflow_id.to_string()], |row| {
            let t: String = row.get(0)?;
            let d: String = row.get(1)?;
            Ok((t, d))
        })?;

        let mut graph = TaskGraph::new();
        for task in tasks {
            graph.add_task(task);
        }

        for edge in edge_rows {
            let (task_str, dep_str) = edge?;
            let t_id: TaskId = task_str.parse()?;
            let d_id: TaskId = dep_str.parse()?;
            graph
                .add_dependency(t_id, d_id)
                .map_err(|e| StorageError::Migration(e.to_string()))?;
        }

        Ok(graph)
    }

    async fn get_current_lease_generation(&self, task_id: &TaskId) -> Result<u64, StorageError> {
        let conn = self.conn.lock().await;
        let gen: u64 = conn
            .query_row(
                "SELECT current_lease_generation FROM tasks WHERE id = ?1",
                params![task_id.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0);
        Ok(gen)
    }

    async fn increment_lease_generation(&self, task_id: &TaskId) -> Result<u64, StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "UPDATE tasks SET current_lease_generation = current_lease_generation + 1 WHERE id = ?1",
            params![task_id.to_string()],
        )?;
        let gen: u64 = conn.query_row(
            "SELECT current_lease_generation FROM tasks WHERE id = ?1",
            params![task_id.to_string()],
            |row| row.get(0),
        )?;
        Ok(gen)
    }

    async fn decompose_task_transactional(
        &self,
        parent_id: &TaskId,
        children: &[Task],
        child_dependencies: &[(TaskId, TaskId)],
        terminal_child_ids: &[TaskId],
    ) -> Result<(), StorageError> {
        let mut conn = self.conn.lock().await;
        let tx = conn.transaction()?;

        // Verify parent exists
        let parent_exists: bool = tx
            .query_row(
                "SELECT 1 FROM tasks WHERE id = ?1",
                params![parent_id.to_string()],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);

        if !parent_exists {
            return Err(StorageError::NotFound {
                entity_type: "Task",
                id: parent_id.to_string(),
            });
        }

        // 1. Fetch parent's current upstream dependencies
        let parent_upstream_deps = {
            let mut p_deps_stmt =
                tx.prepare("SELECT depends_on_id FROM task_dependencies WHERE task_id = ?1")?;
            let rows = p_deps_stmt.query_map(params![parent_id.to_string()], |row| row.get(0))?;
            rows.collect::<Result<Vec<String>, _>>()?
        };

        // 2. Fetch parent's current downstream dependents
        let parent_downstream_deps = {
            let mut p_depd_stmt =
                tx.prepare("SELECT task_id FROM task_dependencies WHERE depends_on_id = ?1")?;
            let rows = p_depd_stmt.query_map(params![parent_id.to_string()], |row| row.get(0))?;
            rows.collect::<Result<Vec<String>, _>>()?
        };

        // 3. Insert all child tasks
        for child in children {
            tx.execute(
                "INSERT INTO tasks (
                    id, workflow_id, workspace_id, objective, description, parent_id, state,
                    priority, criteria, assigned_agent_id, attempts, max_attempts,
                    current_lease_generation, metadata, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                params![
                    child.id.to_string(),
                    child.workflow_id.to_string(),
                    child.workspace_id.map(|id| id.to_string()),
                    child.objective,
                    child.description,
                    Some(parent_id.to_string()),
                    child.state.as_str(),
                    child.priority,
                    serde_json::to_string(&child.criteria)?,
                    child.assigned_agent_id.map(|id| id.to_string()),
                    child.attempts,
                    child.max_attempts,
                    0i64,
                    serde_json::to_string(&child.metadata)?,
                    child.created_at.to_rfc3339(),
                    child.updated_at.to_rfc3339(),
                ],
            )?;
        }

        // 4. Insert internal child-to-child dependencies
        for (task_id, depends_on) in child_dependencies {
            tx.execute(
                "INSERT OR IGNORE INTO task_dependencies (task_id, depends_on_id) VALUES (?1, ?2)",
                params![task_id.to_string(), depends_on.to_string()],
            )?;
        }

        // 5. Initial children (those without internal incoming dependencies) inherit parent's upstream dependencies
        let internal_dependents: std::collections::HashSet<TaskId> =
            child_dependencies.iter().map(|(t, _)| *t).collect();
        for child in children {
            if !internal_dependents.contains(&child.id) {
                for p_dep in &parent_upstream_deps {
                    tx.execute(
                        "INSERT OR IGNORE INTO task_dependencies (task_id, depends_on_id) VALUES (?1, ?2)",
                        params![child.id.to_string(), p_dep],
                    )?;
                }
            }
        }

        // 6. Downstream dependents of parent now depend on terminal children
        for downstream in &parent_downstream_deps {
            tx.execute(
                "DELETE FROM task_dependencies WHERE task_id = ?1 AND depends_on_id = ?2",
                params![downstream, parent_id.to_string()],
            )?;
            for term_id in terminal_child_ids {
                tx.execute(
                    "INSERT OR IGNORE INTO task_dependencies (task_id, depends_on_id) VALUES (?1, ?2)",
                    params![downstream, term_id.to_string()],
                )?;
            }
        }

        // 7. Update parent task state to Discarded so it no longer executes
        tx.execute(
            "UPDATE tasks SET state = 'discarded', updated_at = ?2 WHERE id = ?1",
            params![parent_id.to_string(), Utc::now().to_rfc3339()],
        )?;

        tx.commit()?;
        Ok(())
    }
}

type TaskTuple = (
    String,
    String,
    Option<String>,
    String,
    Option<String>,
    Option<String>,
    String,
    i32,
    String,
    Option<String>,
    u32,
    u32,
    String,
    String,
    String,
);

fn parse_task_tuple(t: TaskTuple) -> Result<Task, StorageError> {
    let (
        id_str,
        wf_str,
        ws_str,
        objective,
        description,
        parent_str,
        state_str,
        priority,
        criteria_str,
        agent_str,
        attempts,
        max_attempts,
        meta_str,
        created_str,
        updated_str,
    ) = t;

    let id: TaskId = id_str.parse()?;
    let workflow_id: WorkflowId = wf_str.parse()?;
    let workspace_id = match ws_str {
        Some(s) => Some(s.parse()?),
        None => None,
    };
    let parent_id = match parent_str {
        Some(s) => Some(s.parse()?),
        None => None,
    };
    let state: TaskState = state_str.parse()?;
    let criteria: Vec<String> = serde_json::from_str(&criteria_str)?;
    let assigned_agent_id = match agent_str {
        Some(s) => Some(s.parse()?),
        None => None,
    };
    let metadata: serde_json::Value = serde_json::from_str(&meta_str)?;
    let created_at = DateTime::parse_from_rfc3339(&created_str)
        .map_err(|e| StorageError::Migration(e.to_string()))?
        .with_timezone(&Utc);
    let updated_at = DateTime::parse_from_rfc3339(&updated_str)
        .map_err(|e| StorageError::Migration(e.to_string()))?
        .with_timezone(&Utc);

    Ok(Task {
        id,
        workflow_id,
        workspace_id,
        objective,
        description,
        parent_id,
        state,
        priority,
        criteria,
        assigned_agent_id,
        attempts,
        max_attempts,
        metadata,
        created_at,
        updated_at,
    })
}

#[async_trait]
impl AgentStore for SqliteStore {
    async fn create_agent(&self, agent: &Agent) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO agents (
                id, display_name, role, provider, model, parameters,
                capabilities, permissions, state, current_execution_id,
                configuration, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                agent.id.to_string(),
                agent.display_name,
                agent.role,
                agent.provider_profile.provider,
                agent.provider_profile.model,
                serde_json::to_string(&agent.provider_profile.parameters)?,
                serde_json::to_string(&agent.capabilities)?,
                serde_json::to_string(&agent.permissions)?,
                agent.state.as_str(),
                agent.current_execution_id.map(|id| id.to_string()),
                serde_json::to_string(&agent.configuration)?,
                agent.created_at.to_rfc3339(),
                agent.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    async fn get_agent(&self, id: &AgentId) -> Result<Option<Agent>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, display_name, role, provider, model, parameters,
                    capabilities, permissions, state, current_execution_id,
                    configuration, created_at, updated_at
             FROM agents WHERE id = ?1",
        )?;

        let row = stmt
            .query_row(params![id.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, String>(12)?,
                ))
            })
            .optional()?;

        match row {
            Some(r) => {
                let id: AgentId = r.0.parse()?;
                let display_name = r.1;
                let role = r.2;
                let provider_profile = ExecutionProfile {
                    provider: r.3,
                    model: r.4,
                    parameters: serde_json::from_str(&r.5)?,
                };
                let capabilities: Vec<String> = serde_json::from_str(&r.6)?;
                let permissions: Vec<String> = serde_json::from_str(&r.7)?;
                let state: AgentState = r.8.parse()?;
                let current_execution_id = match r.9 {
                    Some(s) => Some(s.parse()?),
                    None => None,
                };
                let configuration: serde_json::Value = serde_json::from_str(&r.10)?;
                let created_at = DateTime::parse_from_rfc3339(&r.11)
                    .map_err(|e| StorageError::Migration(e.to_string()))?
                    .with_timezone(&Utc);
                let updated_at = DateTime::parse_from_rfc3339(&r.12)
                    .map_err(|e| StorageError::Migration(e.to_string()))?
                    .with_timezone(&Utc);

                Ok(Some(Agent {
                    id,
                    display_name,
                    role,
                    provider_profile,
                    capabilities,
                    permissions,
                    state,
                    current_execution_id,
                    configuration,
                    created_at,
                    updated_at,
                }))
            }
            None => Ok(None),
        }
    }

    async fn update_agent(&self, agent: &Agent) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        let rows = conn.execute(
            "UPDATE agents SET
                display_name = ?1, role = ?2, provider = ?3, model = ?4,
                parameters = ?5, capabilities = ?6, permissions = ?7,
                state = ?8, current_execution_id = ?9, configuration = ?10,
                updated_at = ?11
             WHERE id = ?12",
            params![
                agent.display_name,
                agent.role,
                agent.provider_profile.provider,
                agent.provider_profile.model,
                serde_json::to_string(&agent.provider_profile.parameters)?,
                serde_json::to_string(&agent.capabilities)?,
                serde_json::to_string(&agent.permissions)?,
                agent.state.as_str(),
                agent.current_execution_id.map(|id| id.to_string()),
                serde_json::to_string(&agent.configuration)?,
                agent.updated_at.to_rfc3339(),
                agent.id.to_string(),
            ],
        )?;

        if rows == 0 {
            return Err(StorageError::NotFound {
                entity_type: "Agent",
                id: agent.id.to_string(),
            });
        }
        Ok(())
    }

    async fn list_agents(&self) -> Result<Vec<Agent>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, display_name, role, provider, model, parameters,
                    capabilities, permissions, state, current_execution_id,
                    configuration, created_at, updated_at
             FROM agents ORDER BY created_at ASC",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, String>(10)?,
                row.get::<_, String>(11)?,
                row.get::<_, String>(12)?,
            ))
        })?;

        let mut agents = Vec::new();
        for r in rows {
            let r = r?;
            let id: AgentId = r.0.parse()?;
            let display_name = r.1;
            let role = r.2;
            let provider_profile = ExecutionProfile {
                provider: r.3,
                model: r.4,
                parameters: serde_json::from_str(&r.5)?,
            };
            let capabilities: Vec<String> = serde_json::from_str(&r.6)?;
            let permissions: Vec<String> = serde_json::from_str(&r.7)?;
            let state: AgentState = r.8.parse()?;
            let current_execution_id = match r.9 {
                Some(s) => Some(s.parse()?),
                None => None,
            };
            let configuration: serde_json::Value = serde_json::from_str(&r.10)?;
            let created_at = DateTime::parse_from_rfc3339(&r.11)
                .map_err(|e| StorageError::Migration(e.to_string()))?
                .with_timezone(&Utc);
            let updated_at = DateTime::parse_from_rfc3339(&r.12)
                .map_err(|e| StorageError::Migration(e.to_string()))?
                .with_timezone(&Utc);

            agents.push(Agent {
                id,
                display_name,
                role,
                provider_profile,
                capabilities,
                permissions,
                state,
                current_execution_id,
                configuration,
                created_at,
                updated_at,
            });
        }

        Ok(agents)
    }
}

#[async_trait]
impl SessionStore for SqliteStore {
    async fn create_session(&self, session: &Session) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO sessions (
                id, agent_id, provider_session_id, working_directory,
                metadata, created_at, updated_at, closed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                session.id.to_string(),
                session.agent_id.to_string(),
                session.provider_session_id,
                session.working_directory,
                serde_json::to_string(&session.metadata)?,
                session.created_at.to_rfc3339(),
                session.updated_at.to_rfc3339(),
                session.closed_at.map(|t| t.to_rfc3339()),
            ],
        )?;
        Ok(())
    }

    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, agent_id, provider_session_id, working_directory,
                    metadata, created_at, updated_at, closed_at
             FROM sessions WHERE id = ?1",
        )?;

        let row = stmt
            .query_row(params![id.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<String>>(7)?,
                ))
            })
            .optional()?;

        match row {
            Some((
                id_str,
                agent_str,
                prov_id,
                work_dir,
                meta_str,
                created_str,
                updated_str,
                closed_str,
            )) => {
                let id: SessionId = id_str.parse()?;
                let agent_id: AgentId = agent_str.parse()?;
                let metadata: serde_json::Value = serde_json::from_str(&meta_str)?;
                let created_at = DateTime::parse_from_rfc3339(&created_str)
                    .map_err(|e| StorageError::Migration(e.to_string()))?
                    .with_timezone(&Utc);
                let updated_at = DateTime::parse_from_rfc3339(&updated_str)
                    .map_err(|e| StorageError::Migration(e.to_string()))?
                    .with_timezone(&Utc);
                let closed_at = match closed_str {
                    Some(s) => Some(
                        DateTime::parse_from_rfc3339(&s)
                            .map_err(|e| StorageError::Migration(e.to_string()))?
                            .with_timezone(&Utc),
                    ),
                    None => None,
                };

                let state = if closed_at.is_some() {
                    plexis_core::SessionState::Closed
                } else {
                    plexis_core::SessionState::Active
                };

                Ok(Some(Session {
                    id,
                    agent_id,
                    state,
                    provider_session_id: prov_id,
                    working_directory: work_dir,
                    metadata,
                    created_at,
                    updated_at,
                    closed_at,
                }))
            }
            None => Ok(None),
        }
    }

    async fn update_session(&self, session: &Session) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        let rows = conn.execute(
            "UPDATE sessions SET
                provider_session_id = ?1, working_directory = ?2,
                metadata = ?3, updated_at = ?4, closed_at = ?5
             WHERE id = ?6",
            params![
                session.provider_session_id,
                session.working_directory,
                serde_json::to_string(&session.metadata)?,
                session.updated_at.to_rfc3339(),
                session.closed_at.map(|t| t.to_rfc3339()),
                session.id.to_string(),
            ],
        )?;

        if rows == 0 {
            return Err(StorageError::NotFound {
                entity_type: "Session",
                id: session.id.to_string(),
            });
        }
        Ok(())
    }

    async fn list_sessions_by_agent(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<Session>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, agent_id, provider_session_id, working_directory,
                    metadata, created_at, updated_at, closed_at
             FROM sessions WHERE agent_id = ?1 ORDER BY created_at DESC",
        )?;

        let rows = stmt.query_map(params![agent_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, Option<String>>(7)?,
            ))
        })?;

        let mut sessions = Vec::new();
        for r in rows {
            let (
                id_str,
                agent_str,
                prov_id,
                work_dir,
                meta_str,
                created_str,
                updated_str,
                closed_str,
            ) = r?;
            let id: SessionId = id_str.parse()?;
            let agent_id: AgentId = agent_str.parse()?;
            let metadata: serde_json::Value = serde_json::from_str(&meta_str)?;
            let created_at = DateTime::parse_from_rfc3339(&created_str)
                .map_err(|e| StorageError::Migration(e.to_string()))?
                .with_timezone(&Utc);
            let updated_at = DateTime::parse_from_rfc3339(&updated_str)
                .map_err(|e| StorageError::Migration(e.to_string()))?
                .with_timezone(&Utc);
            let closed_at = match closed_str {
                Some(s) => Some(
                    DateTime::parse_from_rfc3339(&s)
                        .map_err(|e| StorageError::Migration(e.to_string()))?
                        .with_timezone(&Utc),
                ),
                None => None,
            };

            let state = if closed_at.is_some() {
                plexis_core::SessionState::Closed
            } else {
                plexis_core::SessionState::Active
            };

            sessions.push(Session {
                id,
                agent_id,
                state,
                provider_session_id: prov_id,
                working_directory: work_dir,
                metadata,
                created_at,
                updated_at,
                closed_at,
            });
        }

        Ok(sessions)
    }
}

#[async_trait]
impl ExecutionStore for SqliteStore {
    async fn create_execution(&self, exec: &Execution) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO executions (
                id, task_id, agent_id, session_id, lease_id, state,
                attempt, started_at, completed_at, error_message,
                metadata, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                exec.id.to_string(),
                exec.task_id.to_string(),
                exec.agent_id.to_string(),
                exec.session_id.map(|id| id.to_string()),
                exec.lease_id.map(|id| id.to_string()),
                exec.state.as_str(),
                exec.attempt,
                exec.started_at.map(|t| t.to_rfc3339()),
                exec.completed_at.map(|t| t.to_rfc3339()),
                exec.error_message,
                serde_json::to_string(&exec.metadata)?,
                exec.created_at.to_rfc3339(),
                exec.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    async fn get_execution(&self, id: &ExecutionId) -> Result<Option<Execution>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, task_id, agent_id, session_id, lease_id, state,
                    attempt, started_at, completed_at, error_message,
                    metadata, created_at, updated_at
             FROM executions WHERE id = ?1",
        )?;

        let row = stmt
            .query_row(params![id.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, u32>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, String>(12)?,
                ))
            })
            .optional()?;

        match row {
            Some(r) => Ok(Some(parse_execution_tuple(r)?)),
            None => Ok(None),
        }
    }

    async fn update_execution(&self, exec: &Execution) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        let rows = conn.execute(
            "UPDATE executions SET
                state = ?1, started_at = ?2, completed_at = ?3,
                error_message = ?4, metadata = ?5, updated_at = ?6
             WHERE id = ?7",
            params![
                exec.state.as_str(),
                exec.started_at.map(|t| t.to_rfc3339()),
                exec.completed_at.map(|t| t.to_rfc3339()),
                exec.error_message,
                serde_json::to_string(&exec.metadata)?,
                exec.updated_at.to_rfc3339(),
                exec.id.to_string(),
            ],
        )?;

        if rows == 0 {
            return Err(StorageError::NotFound {
                entity_type: "Execution",
                id: exec.id.to_string(),
            });
        }
        Ok(())
    }

    async fn list_executions_by_task(
        &self,
        task_id: &TaskId,
    ) -> Result<Vec<Execution>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, task_id, agent_id, session_id, lease_id, state,
                    attempt, started_at, completed_at, error_message,
                    metadata, created_at, updated_at
             FROM executions WHERE task_id = ?1 ORDER BY attempt ASC",
        )?;

        let rows = stmt.query_map(params![task_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, u32>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, String>(10)?,
                row.get::<_, String>(11)?,
                row.get::<_, String>(12)?,
            ))
        })?;

        let mut execs = Vec::new();
        for r in rows {
            execs.push(parse_execution_tuple(r?)?);
        }
        Ok(execs)
    }

    async fn list_executions_by_agent(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<Execution>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, task_id, agent_id, session_id, lease_id, state,
                    attempt, started_at, completed_at, error_message,
                    metadata, created_at, updated_at
             FROM executions WHERE agent_id = ?1 ORDER BY created_at DESC",
        )?;

        let rows = stmt.query_map(params![agent_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, u32>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, String>(10)?,
                row.get::<_, String>(11)?,
                row.get::<_, String>(12)?,
            ))
        })?;

        let mut execs = Vec::new();
        for r in rows {
            execs.push(parse_execution_tuple(r?)?);
        }
        Ok(execs)
    }
}

type ExecutionTuple = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    u32,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    String,
    String,
);

fn parse_execution_tuple(r: ExecutionTuple) -> Result<Execution, StorageError> {
    let id: ExecutionId = r.0.parse()?;
    let task_id: TaskId = r.1.parse()?;
    let agent_id: AgentId = r.2.parse()?;
    let session_id = match r.3 {
        Some(s) => Some(s.parse()?),
        None => None,
    };
    let lease_id = match r.4 {
        Some(s) => Some(s.parse()?),
        None => None,
    };
    let state: ExecutionState = r.5.parse()?;
    let attempt = r.6;
    let started_at = match r.7 {
        Some(s) => Some(
            DateTime::parse_from_rfc3339(&s)
                .map_err(|e| StorageError::Migration(e.to_string()))?
                .with_timezone(&Utc),
        ),
        None => None,
    };
    let completed_at = match r.8 {
        Some(s) => Some(
            DateTime::parse_from_rfc3339(&s)
                .map_err(|e| StorageError::Migration(e.to_string()))?
                .with_timezone(&Utc),
        ),
        None => None,
    };
    let error_message = r.9;
    let metadata: serde_json::Value = serde_json::from_str(&r.10)?;
    let created_at = DateTime::parse_from_rfc3339(&r.11)
        .map_err(|e| StorageError::Migration(e.to_string()))?
        .with_timezone(&Utc);
    let updated_at = DateTime::parse_from_rfc3339(&r.12)
        .map_err(|e| StorageError::Migration(e.to_string()))?
        .with_timezone(&Utc);

    Ok(Execution {
        id,
        task_id,
        agent_id,
        session_id,
        lease_id,
        state,
        attempt,
        started_at,
        completed_at,
        error_message,
        metadata,
        created_at,
        updated_at,
    })
}

#[async_trait]
impl CommandStore for SqliteStore {
    async fn enqueue_command(&self, cmd: &Command) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;

        let (target_type, target_id) = match &cmd.target {
            CommandTarget::Agent(id) => ("agent", Some(id.to_string())),
            CommandTarget::Task(id) => ("task", Some(id.to_string())),
            CommandTarget::Workflow(id) => ("workflow", Some(id.to_string())),
            CommandTarget::System => ("system", None),
        };

        let result = conn.execute(
            "INSERT INTO commands (
                id, target_type, target_id, command_type, payload,
                state, idempotency_key, attempts, max_attempts,
                created_at, dispatched_at, completed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                cmd.id.to_string(),
                target_type,
                target_id,
                serde_json::to_string(&cmd.command_type)?,
                serde_json::to_string(&cmd.payload)?,
                cmd.state.as_str(),
                cmd.idempotency_key,
                cmd.attempts,
                cmd.max_attempts,
                cmd.created_at.to_rfc3339(),
                cmd.dispatched_at.map(|d| d.to_rfc3339()),
                cmd.completed_at.map(|c| c.to_rfc3339()),
            ],
        );

        match result {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Err(StorageError::IdempotencyConflict(
                    cmd.idempotency_key.clone(),
                ))
            }
            Err(e) => Err(StorageError::Sqlite(e)),
        }
    }

    async fn get_command(&self, id: &CommandId) -> Result<Option<Command>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, target_type, target_id, command_type, payload,
                    state, idempotency_key, attempts, max_attempts,
                    created_at, dispatched_at, completed_at
             FROM commands WHERE id = ?1",
        )?;

        let row = stmt
            .query_row(params![id.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, u32>(7)?,
                    row.get::<_, u32>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, Option<String>>(11)?,
                ))
            })
            .optional()?;

        match row {
            Some(r) => Ok(Some(parse_command_tuple(r)?)),
            None => Ok(None),
        }
    }

    async fn get_by_idempotency_key(&self, key: &str) -> Result<Option<Command>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, target_type, target_id, command_type, payload,
                    state, idempotency_key, attempts, max_attempts,
                    created_at, dispatched_at, completed_at
             FROM commands WHERE idempotency_key = ?1",
        )?;

        let row = stmt
            .query_row(params![key], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, u32>(7)?,
                    row.get::<_, u32>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, Option<String>>(11)?,
                ))
            })
            .optional()?;

        match row {
            Some(r) => Ok(Some(parse_command_tuple(r)?)),
            None => Ok(None),
        }
    }

    async fn claim_next_queued_command(&self) -> Result<Option<Command>, StorageError> {
        let mut conn = self.conn.lock().await;
        let tx = conn.transaction()?;

        let queued_id: Option<String> = tx
            .query_row(
                "SELECT id FROM commands
                 WHERE state IN ('queued', 'retrying')
                 ORDER BY created_at ASC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?;

        let Some(cmd_id) = queued_id else {
            return Ok(None);
        };

        let now = Utc::now();
        tx.execute(
            "UPDATE commands SET
                state = 'dispatched',
                attempts = attempts + 1,
                dispatched_at = ?1
             WHERE id = ?2",
            params![now.to_rfc3339(), cmd_id],
        )?;

        let cmd = {
            let mut stmt = tx.prepare(
                "SELECT id, target_type, target_id, command_type, payload,
                        state, idempotency_key, attempts, max_attempts,
                        created_at, dispatched_at, completed_at
                 FROM commands WHERE id = ?1",
            )?;

            let row = stmt.query_row(params![cmd_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, u32>(7)?,
                    row.get::<_, u32>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, Option<String>>(11)?,
                ))
            })?;

            parse_command_tuple(row)?
        };

        tx.commit()?;

        Ok(Some(cmd))
    }

    async fn claim_command_by_id(&self, id: &CommandId) -> Result<Option<Command>, StorageError> {
        let mut conn = self.conn.lock().await;
        let tx = conn.transaction()?;

        let queued_id: Option<String> = tx
            .query_row(
                "SELECT id FROM commands
                 WHERE id = ?1 AND state IN ('queued', 'retrying')",
                params![id.to_string()],
                |row| row.get(0),
            )
            .optional()?;

        let Some(cmd_id) = queued_id else {
            return Ok(None);
        };

        let now = Utc::now();
        tx.execute(
            "UPDATE commands SET
                state = 'dispatched',
                attempts = attempts + 1,
                dispatched_at = ?1
             WHERE id = ?2",
            params![now.to_rfc3339(), cmd_id],
        )?;

        let cmd = {
            let mut stmt = tx.prepare(
                "SELECT id, target_type, target_id, command_type, payload,
                        state, idempotency_key, attempts, max_attempts,
                        created_at, dispatched_at, completed_at
                 FROM commands WHERE id = ?1",
            )?;

            let row = stmt.query_row(params![cmd_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, u32>(7)?,
                    row.get::<_, u32>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, Option<String>>(11)?,
                ))
            })?;

            parse_command_tuple(row)?
        };

        tx.commit()?;

        Ok(Some(cmd))
    }

    async fn list_commands_by_state(
        &self,
        state: CommandState,
    ) -> Result<Vec<Command>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, target_type, target_id, command_type, payload, state,
                    idempotency_key, attempts, max_attempts, created_at, dispatched_at, completed_at
             FROM commands WHERE state = ?1 ORDER BY created_at ASC",
        )?;

        let rows = stmt.query_map(params![state.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, u32>(7)?,
                row.get::<_, u32>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, Option<String>>(10)?,
                row.get::<_, Option<String>>(11)?,
            ))
        })?;

        let mut commands = Vec::new();
        for r in rows {
            commands.push(parse_command_tuple(r?)?);
        }
        Ok(commands)
    }

    async fn update_command(&self, cmd: &Command) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "UPDATE commands SET
                state = ?1, attempts = ?2, dispatched_at = ?3, completed_at = ?4
             WHERE id = ?5",
            params![
                cmd.state.as_str(),
                cmd.attempts,
                cmd.dispatched_at.map(|d| d.to_rfc3339()),
                cmd.completed_at.map(|c| c.to_rfc3339()),
                cmd.id.to_string(),
            ],
        )?;
        Ok(())
    }
}

type CommandTuple = (
    String,
    String,
    Option<String>,
    String,
    String,
    String,
    String,
    u32,
    u32,
    String,
    Option<String>,
    Option<String>,
);

fn parse_command_tuple(r: CommandTuple) -> Result<Command, StorageError> {
    let id: CommandId = r.0.parse()?;
    let target = match r.1.as_str() {
        "agent" => CommandTarget::Agent(r.2.unwrap_or_default().parse()?),
        "task" => CommandTarget::Task(r.2.unwrap_or_default().parse()?),
        "workflow" => CommandTarget::Workflow(r.2.unwrap_or_default().parse()?),
        _ => CommandTarget::System,
    };
    let command_type: CommandType = serde_json::from_str(&r.3)?;
    let payload: serde_json::Value = serde_json::from_str(&r.4)?;
    let state: CommandState = r.5.parse()?;
    let idempotency_key = r.6;
    let attempts = r.7;
    let max_attempts = r.8;
    let created_at = DateTime::parse_from_rfc3339(&r.9)
        .map_err(|e| StorageError::Migration(e.to_string()))?
        .with_timezone(&Utc);
    let dispatched_at = match r.10 {
        Some(s) => Some(
            DateTime::parse_from_rfc3339(&s)
                .map_err(|e| StorageError::Migration(e.to_string()))?
                .with_timezone(&Utc),
        ),
        None => None,
    };
    let completed_at = match r.11 {
        Some(s) => Some(
            DateTime::parse_from_rfc3339(&s)
                .map_err(|e| StorageError::Migration(e.to_string()))?
                .with_timezone(&Utc),
        ),
        None => None,
    };

    Ok(Command {
        id,
        target,
        command_type,
        payload,
        state,
        idempotency_key,
        attempts,
        max_attempts,
        created_at,
        dispatched_at,
        completed_at,
    })
}

#[async_trait]
impl LeaseStore for SqliteStore {
    async fn acquire_lease(&self, lease: &Lease) -> Result<Lease, StorageError> {
        let mut conn = self.conn.lock().await;
        let tx = conn.transaction()?;

        // Check if existing lease on task is still active
        let existing = tx
            .query_row(
                "SELECT id, expires_at, generation FROM leases WHERE task_id = ?1",
                params![lease.task_id.to_string()],
                |row| {
                    let id: String = row.get(0)?;
                    let exp: String = row.get(1)?;
                    let gen: u64 = row.get(2)?;
                    Ok((id, exp, gen))
                },
            )
            .optional()?;

        let now = Utc::now();
        if let Some((old_id, exp_str, _old_gen)) = existing {
            let exp = DateTime::parse_from_rfc3339(&exp_str)
                .map_err(|e| StorageError::Migration(e.to_string()))?
                .with_timezone(&Utc);

            if exp > now {
                return Err(StorageError::LeaseConflict(lease.task_id.to_string()));
            } else {
                // Delete stale lease
                tx.execute("DELETE FROM leases WHERE id = ?1", params![old_id])?;
            }
        }

        // Monotonically increment task's lease generation
        tx.execute(
            "UPDATE tasks SET current_lease_generation = current_lease_generation + 1 WHERE id = ?1",
            params![lease.task_id.to_string()],
        )?;

        let granted_generation: u64 = tx.query_row(
            "SELECT current_lease_generation FROM tasks WHERE id = ?1",
            params![lease.task_id.to_string()],
            |row| row.get(0),
        )?;

        tx.execute(
            "INSERT INTO leases (id, task_id, agent_id, generation, acquired_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                lease.id.to_string(),
                lease.task_id.to_string(),
                lease.agent_id.to_string(),
                granted_generation,
                lease.acquired_at.to_rfc3339(),
                lease.expires_at.to_rfc3339(),
            ],
        )?;

        tx.commit()?;

        let mut granted_lease = lease.clone();
        granted_lease.generation = granted_generation;
        Ok(granted_lease)
    }

    async fn get_lease_by_task(&self, task_id: &TaskId) -> Result<Option<Lease>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, task_id, agent_id, generation, acquired_at, expires_at
             FROM leases WHERE task_id = ?1",
        )?;

        let row = stmt
            .query_row(params![task_id.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })
            .optional()?;

        match row {
            Some((id_str, task_str, agent_str, generation, acq_str, exp_str)) => {
                let id: LeaseId = id_str.parse()?;
                let task_id: TaskId = task_str.parse()?;
                let agent_id: AgentId = agent_str.parse()?;
                let acquired_at = DateTime::parse_from_rfc3339(&acq_str)
                    .map_err(|e| StorageError::Migration(e.to_string()))?
                    .with_timezone(&Utc);
                let expires_at = DateTime::parse_from_rfc3339(&exp_str)
                    .map_err(|e| StorageError::Migration(e.to_string()))?
                    .with_timezone(&Utc);

                Ok(Some(Lease {
                    id,
                    task_id,
                    agent_id,
                    generation,
                    acquired_at,
                    expires_at,
                }))
            }
            None => Ok(None),
        }
    }

    async fn renew_lease(
        &self,
        lease_id: &LeaseId,
        new_expiry: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        let rows = conn.execute(
            "UPDATE leases SET expires_at = ?1 WHERE id = ?2",
            params![new_expiry.to_rfc3339(), lease_id.to_string()],
        )?;

        if rows == 0 {
            return Err(StorageError::NotFound {
                entity_type: "Lease",
                id: lease_id.to_string(),
            });
        }
        Ok(())
    }

    async fn release_lease(&self, lease_id: &LeaseId) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "DELETE FROM leases WHERE id = ?1",
            params![lease_id.to_string()],
        )?;
        Ok(())
    }

    async fn reclaim_expired_leases(&self) -> Result<Vec<TaskId>, StorageError> {
        let mut conn = self.conn.lock().await;
        let now = Utc::now().to_rfc3339();
        let tx = conn.transaction()?;

        let expired_rows = {
            let mut stmt = tx.prepare("SELECT task_id FROM leases WHERE expires_at <= ?1")?;
            let rows = stmt.query_map(params![now], |row| {
                let s: String = row.get(0)?;
                Ok(s)
            })?;
            let mut expired_tasks = Vec::new();
            for r in rows {
                let s = r?;
                expired_tasks.push(s.parse()?);
            }
            expired_tasks
        };

        tx.execute("DELETE FROM leases WHERE expires_at <= ?1", params![now])?;
        tx.commit()?;

        Ok(expired_rows)
    }
}

#[async_trait]
impl EventStore for SqliteStore {
    async fn append_event(&self, event: &Event) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO events (
                id, aggregate_type, aggregate_id, event_type, payload,
                actor, causation_id, correlation_id, timestamp
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                event.id.to_string(),
                event.aggregate_type,
                event.aggregate_id,
                event.event_type,
                serde_json::to_string(&event.payload)?,
                event.actor,
                event.causation_id,
                event.correlation_id,
                event.timestamp.to_rfc3339(),
            ],
        )?;

        let rowid = conn.last_insert_rowid() as u64;
        let mut broadcast_evt = event.clone();
        broadcast_evt.sequence = Some(rowid);
        let _ = self.event_broadcaster.send(broadcast_evt);
        Ok(())
    }

    async fn list_events_by_aggregate(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Vec<Event>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT rowid, id, aggregate_type, aggregate_id, event_type, payload,
                    actor, causation_id, correlation_id, timestamp
             FROM events
             WHERE aggregate_type = ?1 AND aggregate_id = ?2
             ORDER BY rowid ASC",
        )?;

        let rows = stmt.query_map(params![aggregate_type, aggregate_id], parse_event_row)?;
        let mut events = Vec::new();
        for r in rows {
            events.push(r?);
        }
        Ok(events)
    }

    async fn list_recent_events(&self, limit: usize) -> Result<Vec<Event>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT rowid, id, aggregate_type, aggregate_id, event_type, payload,
                    actor, causation_id, correlation_id, timestamp
             FROM events
             ORDER BY rowid DESC
             LIMIT ?1",
        )?;

        let rows = stmt.query_map(params![limit as i64], parse_event_row)?;
        let mut events = Vec::new();
        for r in rows {
            events.push(r?);
        }
        Ok(events)
    }

    async fn list_events_after(
        &self,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<Event>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT rowid, id, aggregate_type, aggregate_id, event_type, payload,
                    actor, causation_id, correlation_id, timestamp
             FROM events
             WHERE rowid > ?1
             ORDER BY rowid ASC
             LIMIT ?2",
        )?;

        let rows = stmt.query_map(
            params![after_sequence as i64, limit as i64],
            parse_event_row,
        )?;
        let mut events = Vec::new();
        for r in rows {
            events.push(r?);
        }
        Ok(events)
    }

    async fn get_latest_event_sequence(&self) -> Result<u64, StorageError> {
        let conn = self.conn.lock().await;
        let seq: i64 = conn.query_row("SELECT COALESCE(MAX(rowid), 0) FROM events", [], |row| {
            row.get(0)
        })?;
        Ok(seq as u64)
    }
}

fn parse_event_row(row: &rusqlite::Row<'_>) -> Result<Event, rusqlite::Error> {
    let rowid: i64 = row.get(0)?;
    let id_str: String = row.get(1)?;
    let agg_type: String = row.get(2)?;
    let agg_id: String = row.get(3)?;
    let evt_type: String = row.get(4)?;
    let pay_str: String = row.get(5)?;
    let actor: Option<String> = row.get(6)?;
    let caus: Option<String> = row.get(7)?;
    let corr: Option<String> = row.get(8)?;
    let ts_str: String = row.get(9)?;

    let id: EventId = id_str
        .parse()
        .map_err(|e: plexis_core::ids::IdParseError| {
            rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(e))
        })?;
    let payload: serde_json::Value =
        serde_json::from_str(&pay_str).unwrap_or(serde_json::Value::Null);
    let timestamp = DateTime::parse_from_rfc3339(&ts_str)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(9, rusqlite::types::Type::Text, Box::new(e))
        })?;

    Ok(Event {
        id,
        aggregate_type: agg_type,
        aggregate_id: agg_id,
        event_type: evt_type,
        payload,
        actor,
        causation_id: caus,
        correlation_id: corr,
        timestamp,
        sequence: Some(rowid as u64),
    })
}

#[async_trait]
impl VerificationStore for SqliteStore {
    async fn create_verification(&self, verification: &Verification) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        let id_str = verification.id.to_string();
        let task_id_str = verification.task_id.to_string();
        let verdict_str = match verification.verdict {
            VerificationVerdict::Passed => "passed",
            VerificationVerdict::Failed => "failed",
            VerificationVerdict::Inconclusive => "inconclusive",
        };
        let evidence_str = serde_json::to_string(&verification.evidence)?;
        let created_at_str = verification.created_at.to_rfc3339();

        conn.execute(
            "INSERT INTO verifications (id, task_id, verifier_kind, verdict, evidence, failure_reason, duration_ms, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                id_str,
                task_id_str,
                verification.verifier_kind,
                verdict_str,
                evidence_str,
                verification.failure_reason,
                verification.duration_ms as i64,
                created_at_str,
            ],
        )?;

        Ok(())
    }

    async fn get_verification(
        &self,
        id: &VerificationId,
    ) -> Result<Option<Verification>, StorageError> {
        let conn = self.conn.lock().await;
        let id_str = id.to_string();

        let mut stmt = conn.prepare(
            "SELECT id, task_id, verifier_kind, verdict, evidence, failure_reason, duration_ms, created_at
             FROM verifications WHERE id = ?1",
        )?;

        let row = stmt
            .query_row(params![id_str], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, Option<String>>(5)?,
                    r.get::<_, i64>(6)?,
                    r.get::<_, String>(7)?,
                ))
            })
            .optional()?;

        match row {
            Some((id_s, task_s, kind, verdict_s, ev_s, fail_s, dur, cr_s)) => {
                let id: VerificationId = id_s.parse()?;
                let task_id: TaskId = task_s.parse()?;
                let verdict = match verdict_s.as_str() {
                    "passed" => VerificationVerdict::Passed,
                    "failed" => VerificationVerdict::Failed,
                    _ => VerificationVerdict::Inconclusive,
                };
                let evidence: serde_json::Value = serde_json::from_str(&ev_s)?;
                let created_at = DateTime::parse_from_rfc3339(&cr_s)
                    .map_err(|e| StorageError::Migration(e.to_string()))?
                    .with_timezone(&Utc);

                Ok(Some(Verification {
                    id,
                    task_id,
                    verifier_kind: kind,
                    verdict,
                    evidence,
                    failure_reason: fail_s,
                    duration_ms: dur as u64,
                    created_at,
                }))
            }
            None => Ok(None),
        }
    }

    async fn list_verifications_by_task(
        &self,
        task_id: &TaskId,
    ) -> Result<Vec<Verification>, StorageError> {
        let conn = self.conn.lock().await;
        let task_str = task_id.to_string();

        let mut stmt = conn.prepare(
            "SELECT id, task_id, verifier_kind, verdict, evidence, failure_reason, duration_ms, created_at
             FROM verifications WHERE task_id = ?1 ORDER BY created_at ASC",
        )?;

        let rows = stmt.query_map(params![task_str], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, Option<String>>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, String>(7)?,
            ))
        })?;

        let mut results = Vec::new();
        for r in rows {
            let (id_s, task_s, kind, verdict_s, ev_s, fail_s, dur, cr_s) = r?;
            let id: VerificationId = id_s.parse()?;
            let task_id: TaskId = task_s.parse()?;
            let verdict = match verdict_s.as_str() {
                "passed" => VerificationVerdict::Passed,
                "failed" => VerificationVerdict::Failed,
                _ => VerificationVerdict::Inconclusive,
            };
            let evidence: serde_json::Value = serde_json::from_str(&ev_s)?;
            let created_at = DateTime::parse_from_rfc3339(&cr_s)
                .map_err(|e| StorageError::Migration(e.to_string()))?
                .with_timezone(&Utc);

            results.push(Verification {
                id,
                task_id,
                verifier_kind: kind,
                verdict,
                evidence,
                failure_reason: fail_s,
                duration_ms: dur as u64,
                created_at,
            });
        }

        Ok(results)
    }

    async fn list_verifications_by_workflow(
        &self,
        workflow_id: &WorkflowId,
    ) -> Result<Vec<Verification>, StorageError> {
        let conn = self.conn.lock().await;
        let wf_str = workflow_id.to_string();

        let mut stmt = conn.prepare(
            "SELECT v.id, v.task_id, v.verifier_kind, v.verdict, v.evidence, v.failure_reason, v.duration_ms, v.created_at
             FROM verifications v
             JOIN tasks t ON v.task_id = t.id
             WHERE t.workflow_id = ?1
             ORDER BY v.created_at ASC",
        )?;

        let rows = stmt.query_map(params![wf_str], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, Option<String>>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, String>(7)?,
            ))
        })?;

        let mut results = Vec::new();
        for r in rows {
            let (id_s, task_s, kind, verdict_s, ev_s, fail_s, dur, cr_s) = r?;
            let id: VerificationId = id_s.parse()?;
            let task_id: TaskId = task_s.parse()?;
            let verdict = match verdict_s.as_str() {
                "passed" => VerificationVerdict::Passed,
                "failed" => VerificationVerdict::Failed,
                _ => VerificationVerdict::Inconclusive,
            };
            let evidence: serde_json::Value = serde_json::from_str(&ev_s)?;
            let created_at = DateTime::parse_from_rfc3339(&cr_s)
                .map_err(|e| StorageError::Migration(e.to_string()))?
                .with_timezone(&Utc);

            results.push(Verification {
                id,
                task_id,
                verifier_kind: kind,
                verdict,
                evidence,
                failure_reason: fail_s,
                duration_ms: dur as u64,
                created_at,
            });
        }

        Ok(results)
    }
}

#[async_trait]
impl MessageStore for SqliteStore {
    async fn send_message(&self, message: &AgentMessage) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        let type_json = serde_json::to_string(&message.message_type)?;
        let type_str = type_json.trim_matches('"');
        conn.execute(
            "INSERT INTO messages (
                id, from_agent, to_agent, workflow_id, task_id, message_type,
                content, payload, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                message.id.to_string(),
                message.from_agent.to_string(),
                message.to_agent.to_string(),
                message.workflow_id.to_string(),
                message.task_id.map(|t| t.to_string()),
                type_str,
                message.content,
                serde_json::to_string(&message.payload)?,
                message.created_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    async fn get_message(&self, id: &MessageId) -> Result<Option<AgentMessage>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, from_agent, to_agent, workflow_id, task_id, message_type,
                    content, payload, created_at
             FROM messages WHERE id = ?1",
        )?;

        let row = stmt
            .query_row(params![id.to_string()], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, String>(8)?,
                ))
            })
            .optional()?;

        match row {
            Some(t) => Ok(Some(parse_message_tuple(t)?)),
            None => Ok(None),
        }
    }

    async fn list_messages_by_workflow(
        &self,
        workflow_id: &WorkflowId,
    ) -> Result<Vec<AgentMessage>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, from_agent, to_agent, workflow_id, task_id, message_type,
                    content, payload, created_at
             FROM messages WHERE workflow_id = ?1 ORDER BY created_at ASC",
        )?;

        let rows = stmt.query_map(params![workflow_id.to_string()], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, String>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, String>(8)?,
            ))
        })?;

        let mut results = Vec::new();
        for r in rows {
            results.push(parse_message_tuple(r?)?);
        }
        Ok(results)
    }

    async fn list_messages_by_task(
        &self,
        task_id: &TaskId,
    ) -> Result<Vec<AgentMessage>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, from_agent, to_agent, workflow_id, task_id, message_type,
                    content, payload, created_at
             FROM messages WHERE task_id = ?1 ORDER BY created_at ASC",
        )?;

        let rows = stmt.query_map(params![task_id.to_string()], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, String>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, String>(8)?,
            ))
        })?;

        let mut results = Vec::new();
        for r in rows {
            results.push(parse_message_tuple(r?)?);
        }
        Ok(results)
    }

    async fn list_messages_for_agent(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<AgentMessage>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, from_agent, to_agent, workflow_id, task_id, message_type,
                    content, payload, created_at
             FROM messages WHERE to_agent = ?1 ORDER BY created_at ASC",
        )?;

        let rows = stmt.query_map(params![agent_id.to_string()], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, String>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, String>(8)?,
            ))
        })?;

        let mut results = Vec::new();
        for r in rows {
            results.push(parse_message_tuple(r?)?);
        }
        Ok(results)
    }
}

type MessageTuple = (
    String,
    String,
    String,
    String,
    Option<String>,
    String,
    String,
    String,
    String,
);

fn parse_message_tuple(t: MessageTuple) -> Result<AgentMessage, StorageError> {
    let (id_s, from_s, to_s, wf_s, task_s, type_s, content, payload_s, cr_s) = t;
    let id: MessageId = id_s.parse()?;
    let from_agent: AgentId = from_s.parse()?;
    let to_agent: AgentId = to_s.parse()?;
    let workflow_id: WorkflowId = wf_s.parse()?;
    let task_id = match task_s {
        Some(s) => Some(s.parse()?),
        None => None,
    };
    let message_type: MessageType = serde_json::from_str(&format!("\"{}\"", type_s))?;
    let payload: serde_json::Value = serde_json::from_str(&payload_s)?;
    let created_at = DateTime::parse_from_rfc3339(&cr_s)
        .map_err(|e| StorageError::Migration(e.to_string()))?
        .with_timezone(&Utc);

    Ok(AgentMessage {
        id,
        from_agent,
        to_agent,
        workflow_id,
        task_id,
        message_type,
        content,
        payload,
        created_at,
    })
}

#[async_trait]
impl ApprovalStore for SqliteStore {
    async fn create_approval(&self, approval: &ApprovalRecord) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO approvals (
                id, task_id, workflow_id, requested_by, action_description, state,
                reason, created_at, decided_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                approval.id.to_string(),
                approval.task_id.to_string(),
                approval.workflow_id.to_string(),
                approval.requested_by.map(|a| a.to_string()),
                approval.action_description,
                approval.state.as_str(),
                approval.reason,
                approval.created_at.to_rfc3339(),
                approval.decided_at.map(|d| d.to_rfc3339()),
            ],
        )?;
        Ok(())
    }

    async fn get_approval(&self, id: &ApprovalId) -> Result<Option<ApprovalRecord>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, task_id, workflow_id, requested_by, action_description, state,
                    reason, created_at, decided_at
             FROM approvals WHERE id = ?1",
        )?;

        let row = stmt
            .query_row(params![id.to_string()], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<String>>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, Option<String>>(6)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, Option<String>>(8)?,
                ))
            })
            .optional()?;

        match row {
            Some(t) => Ok(Some(parse_approval_tuple(t)?)),
            None => Ok(None),
        }
    }

    async fn update_approval(&self, approval: &ApprovalRecord) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        let rows = conn.execute(
            "UPDATE approvals SET state = ?1, reason = ?2, decided_at = ?3 WHERE id = ?4",
            params![
                approval.state.as_str(),
                approval.reason,
                approval.decided_at.map(|d| d.to_rfc3339()),
                approval.id.to_string(),
            ],
        )?;
        if rows == 0 {
            return Err(StorageError::NotFound {
                entity_type: "ApprovalRecord",
                id: approval.id.to_string(),
            });
        }
        Ok(())
    }

    async fn list_approvals_by_workflow(
        &self,
        workflow_id: &WorkflowId,
    ) -> Result<Vec<ApprovalRecord>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, task_id, workflow_id, requested_by, action_description, state,
                    reason, created_at, decided_at
             FROM approvals WHERE workflow_id = ?1 ORDER BY created_at DESC",
        )?;

        let rows = stmt.query_map(params![workflow_id.to_string()], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, Option<String>>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, Option<String>>(8)?,
            ))
        })?;

        let mut results = Vec::new();
        for r in rows {
            results.push(parse_approval_tuple(r?)?);
        }
        Ok(results)
    }

    async fn list_approvals_by_task(
        &self,
        task_id: &TaskId,
    ) -> Result<Vec<ApprovalRecord>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, task_id, workflow_id, requested_by, action_description, state,
                    reason, created_at, decided_at
             FROM approvals WHERE task_id = ?1 ORDER BY created_at DESC",
        )?;

        let rows = stmt.query_map(params![task_id.to_string()], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, Option<String>>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, Option<String>>(8)?,
            ))
        })?;

        let mut results = Vec::new();
        for r in rows {
            results.push(parse_approval_tuple(r?)?);
        }
        Ok(results)
    }

    async fn list_pending_approvals(&self) -> Result<Vec<ApprovalRecord>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, task_id, workflow_id, requested_by, action_description, state,
                    reason, created_at, decided_at
             FROM approvals WHERE state = 'pending' ORDER BY created_at ASC",
        )?;

        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, Option<String>>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, Option<String>>(8)?,
            ))
        })?;

        let mut results = Vec::new();
        for r in rows {
            results.push(parse_approval_tuple(r?)?);
        }
        Ok(results)
    }

    async fn list_all_approvals(&self) -> Result<Vec<ApprovalRecord>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, task_id, workflow_id, requested_by, action_description, state,
                    reason, created_at, decided_at
             FROM approvals ORDER BY created_at DESC",
        )?;

        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, Option<String>>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, Option<String>>(8)?,
            ))
        })?;

        let mut results = Vec::new();
        for r in rows {
            results.push(parse_approval_tuple(r?)?);
        }
        Ok(results)
    }
}

type ApprovalTuple = (
    String,
    String,
    String,
    Option<String>,
    String,
    String,
    Option<String>,
    String,
    Option<String>,
);

fn parse_approval_tuple(t: ApprovalTuple) -> Result<ApprovalRecord, StorageError> {
    let (id_s, task_s, wf_s, req_s, desc, state_s, reason, cr_s, dec_s) = t;
    let id: ApprovalId = id_s.parse()?;
    let task_id: TaskId = task_s.parse()?;
    let workflow_id: WorkflowId = wf_s.parse()?;
    let requested_by = match req_s {
        Some(s) => Some(s.parse()?),
        None => None,
    };
    let state = match state_s.as_str() {
        "approved" => ApprovalState::Approved,
        "rejected" => ApprovalState::Rejected,
        _ => ApprovalState::Pending,
    };
    let created_at = DateTime::parse_from_rfc3339(&cr_s)
        .map_err(|e| StorageError::Migration(e.to_string()))?
        .with_timezone(&Utc);
    let decided_at = match dec_s {
        Some(s) => Some(
            DateTime::parse_from_rfc3339(&s)
                .map_err(|e| StorageError::Migration(e.to_string()))?
                .with_timezone(&Utc),
        ),
        None => None,
    };

    Ok(ApprovalRecord {
        id,
        task_id,
        workflow_id,
        requested_by,
        action_description: desc,
        state,
        reason,
        created_at,
        decided_at,
    })
}

#[async_trait]
impl PlanStore for SqliteStore {
    async fn save_plan_record(&self, record: &PlanningRecord) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO planning_records (
                id, workflow_id, parent_task_id, objective, provider, model,
                proposal, status, validation_errors, applied_mutations, latency_ms,
                prompt_tokens, completion_tokens, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                record.id.to_string(),
                record.workflow_id.to_string(),
                record.parent_task_id.map(|t| t.to_string()),
                record.objective,
                record.provider,
                record.model,
                serde_json::to_string(&record.proposal)?,
                record.status.as_str(),
                serde_json::to_string(&record.validation_errors)?,
                serde_json::to_string(&record.applied_mutations)?,
                record.latency_ms as i64,
                record.prompt_tokens,
                record.completion_tokens,
                record.created_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    async fn get_plan_record(&self, id: &PlanId) -> Result<Option<PlanningRecord>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, workflow_id, parent_task_id, objective, provider, model,
                    proposal, status, validation_errors, applied_mutations, latency_ms,
                    prompt_tokens, completion_tokens, created_at
             FROM planning_records WHERE id = ?1",
        )?;

        let row = stmt
            .query_row(params![id.to_string()], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, String>(8)?,
                    r.get::<_, String>(9)?,
                    r.get::<_, i64>(10)?,
                    r.get::<_, Option<u32>>(11)?,
                    r.get::<_, Option<u32>>(12)?,
                    r.get::<_, String>(13)?,
                ))
            })
            .optional()?;

        match row {
            Some(t) => Ok(Some(parse_plan_tuple(t)?)),
            None => Ok(None),
        }
    }

    async fn list_plan_records_by_workflow(
        &self,
        workflow_id: &WorkflowId,
    ) -> Result<Vec<PlanningRecord>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, workflow_id, parent_task_id, objective, provider, model,
                    proposal, status, validation_errors, applied_mutations, latency_ms,
                    prompt_tokens, completion_tokens, created_at
             FROM planning_records WHERE workflow_id = ?1 ORDER BY created_at DESC",
        )?;

        let rows = stmt.query_map(params![workflow_id.to_string()], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, String>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, String>(8)?,
                r.get::<_, String>(9)?,
                r.get::<_, i64>(10)?,
                r.get::<_, Option<u32>>(11)?,
                r.get::<_, Option<u32>>(12)?,
                r.get::<_, String>(13)?,
            ))
        })?;

        let mut results = Vec::new();
        for r in rows {
            results.push(parse_plan_tuple(r?)?);
        }
        Ok(results)
    }
}

type PlanTuple = (
    String,
    String,
    Option<String>,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    i64,
    Option<u32>,
    Option<u32>,
    String,
);

fn parse_plan_tuple(t: PlanTuple) -> Result<PlanningRecord, StorageError> {
    let (
        id_s,
        wf_s,
        parent_s,
        obj,
        prov,
        modl,
        prop_s,
        stat_s,
        errs_s,
        muts_s,
        lat,
        p_tok,
        c_tok,
        cr_s,
    ) = t;

    let id: PlanId = id_s.parse()?;
    let workflow_id: WorkflowId = wf_s.parse()?;
    let parent_task_id = match parent_s {
        Some(s) => Some(s.parse()?),
        None => None,
    };
    let proposal: serde_json::Value = serde_json::from_str(&prop_s)?;
    let status = match stat_s.as_str() {
        "validated" => PlanStatus::Validated,
        "applied" => PlanStatus::Applied,
        "rejected" => PlanStatus::Rejected,
        _ => PlanStatus::Proposed,
    };
    let validation_errors: Vec<String> = serde_json::from_str(&errs_s)?;
    let applied_mutations: serde_json::Value = serde_json::from_str(&muts_s)?;
    let created_at = DateTime::parse_from_rfc3339(&cr_s)
        .map_err(|e| StorageError::Migration(e.to_string()))?
        .with_timezone(&Utc);

    Ok(PlanningRecord {
        id,
        workflow_id,
        parent_task_id,
        objective: obj,
        provider: prov,
        model: modl,
        proposal,
        status,
        validation_errors,
        applied_mutations,
        latency_ms: lat as u64,
        prompt_tokens: p_tok,
        completion_tokens: c_tok,
        created_at,
    })
}

#[async_trait]
impl MemoryStore for SqliteStore {
    async fn save_memory(&self, memory: &MemoryRecord) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO memory_records (
                id, scope, scope_id, source, content, embedding_json,
                importance, metadata_json, provenance_json, state,
                superseded_by, created_at, updated_at, accessed_at, access_count
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                memory.id.to_string(),
                memory.scope.as_str(),
                memory.scope_id,
                memory.source,
                memory.content,
                memory
                    .embedding
                    .as_ref()
                    .map(|v| serde_json::to_string(v).unwrap_or_default()),
                memory.importance,
                serde_json::to_string(&memory.metadata)?,
                serde_json::to_string(&memory.provenance)?,
                memory.state.as_str(),
                memory.superseded_by.map(|id| id.to_string()),
                memory.created_at.to_rfc3339(),
                memory.updated_at.to_rfc3339(),
                memory.accessed_at.map(|t| t.to_rfc3339()),
                memory.access_count,
            ],
        )?;
        Ok(())
    }

    async fn get_memory(&self, id: &MemoryId) -> Result<Option<MemoryRecord>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, scope, scope_id, source, content, embedding_json,
                    importance, metadata_json, provenance_json, state,
                    superseded_by, created_at, updated_at, accessed_at, access_count
             FROM memory_records WHERE id = ?1",
        )?;

        let res = stmt
            .query_row(params![id.to_string()], parse_memory_row)
            .optional()?;
        Ok(res)
    }

    async fn update_memory(&self, memory: &MemoryRecord) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        let affected = conn.execute(
            "UPDATE memory_records SET
                scope = ?1, scope_id = ?2, source = ?3, content = ?4,
                embedding_json = ?5, importance = ?6, metadata_json = ?7,
                provenance_json = ?8, state = ?9, superseded_by = ?10,
                updated_at = ?11, accessed_at = ?12, access_count = ?13
             WHERE id = ?14",
            params![
                memory.scope.as_str(),
                memory.scope_id,
                memory.source,
                memory.content,
                memory
                    .embedding
                    .as_ref()
                    .map(|v| serde_json::to_string(v).unwrap_or_default()),
                memory.importance,
                serde_json::to_string(&memory.metadata)?,
                serde_json::to_string(&memory.provenance)?,
                memory.state.as_str(),
                memory.superseded_by.map(|id| id.to_string()),
                memory.updated_at.to_rfc3339(),
                memory.accessed_at.map(|t| t.to_rfc3339()),
                memory.access_count,
                memory.id.to_string(),
            ],
        )?;

        if affected == 0 {
            return Err(StorageError::NotFound {
                entity_type: "MemoryRecord",
                id: memory.id.to_string(),
            });
        }
        Ok(())
    }

    async fn list_memories_by_scope(
        &self,
        scope: MemoryScope,
        scope_id: Option<&str>,
    ) -> Result<Vec<MemoryRecord>, StorageError> {
        let conn = self.conn.lock().await;
        let query = if scope_id.is_some() {
            "SELECT id, scope, scope_id, source, content, embedding_json,
                    importance, metadata_json, provenance_json, state,
                    superseded_by, created_at, updated_at, accessed_at, access_count
             FROM memory_records
             WHERE scope = ?1 AND scope_id = ?2 AND state != 'deleted'
             ORDER BY created_at DESC"
        } else {
            "SELECT id, scope, scope_id, source, content, embedding_json,
                    importance, metadata_json, provenance_json, state,
                    superseded_by, created_at, updated_at, accessed_at, access_count
             FROM memory_records
             WHERE scope = ?1 AND state != 'deleted'
             ORDER BY created_at DESC"
        };

        let mut stmt = conn.prepare(query)?;
        let rows = if let Some(sid) = scope_id {
            stmt.query_map(params![scope.as_str(), sid], parse_memory_row)?
        } else {
            stmt.query_map(params![scope.as_str()], parse_memory_row)?
        };

        let mut list = Vec::new();
        for r in rows {
            list.push(r?);
        }
        Ok(list)
    }

    async fn list_active_memories(
        &self,
        scopes: Option<&[MemoryScope]>,
        scope_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MemoryRecord>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, scope, scope_id, source, content, embedding_json,
                    importance, metadata_json, provenance_json, state,
                    superseded_by, created_at, updated_at, accessed_at, access_count
             FROM memory_records
             WHERE state = 'active'
             ORDER BY created_at DESC
             LIMIT ?1",
        )?;

        let rows = stmt.query_map(params![limit as i64], parse_memory_row)?;
        let mut list = Vec::new();
        for r in rows {
            let mem = r?;
            if let Some(sc_filter) = scopes {
                if !sc_filter.contains(&mem.scope) {
                    continue;
                }
            }
            if let Some(expected_sid) = scope_id {
                if mem.scope_id.as_deref() != Some(expected_sid) {
                    continue;
                }
            }
            list.push(mem);
        }
        Ok(list)
    }
}

fn parse_memory_row(row: &rusqlite::Row<'_>) -> Result<MemoryRecord, rusqlite::Error> {
    let id_str: String = row.get(0)?;
    let scope_str: String = row.get(1)?;
    let scope_id: Option<String> = row.get(2)?;
    let source: String = row.get(3)?;
    let content: String = row.get(4)?;
    let emb_str: Option<String> = row.get(5)?;
    let importance: f64 = row.get(6)?;
    let meta_str: String = row.get(7)?;
    let prov_str: String = row.get(8)?;
    let state_str: String = row.get(9)?;
    let sup_str: Option<String> = row.get(10)?;
    let created_str: String = row.get(11)?;
    let updated_str: String = row.get(12)?;
    let accessed_str: Option<String> = row.get(13)?;
    let access_count: i64 = row.get(14)?;

    let id: MemoryId = id_str
        .parse()
        .map_err(|e: plexis_core::ids::IdParseError| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })?;
    let scope: MemoryScope = scope_str.parse().map_err(|e: String| {
        rusqlite::Error::FromSqlConversionFailure(
            1,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        )
    })?;
    let embedding: Option<Vec<f32>> = emb_str.and_then(|s| serde_json::from_str(&s).ok());
    let metadata: serde_json::Value =
        serde_json::from_str(&meta_str).unwrap_or(serde_json::Value::Null);
    let provenance: MemoryProvenance = serde_json::from_str(&prov_str).unwrap_or_default();
    let state: MemoryState = state_str.parse().map_err(|e: String| {
        rusqlite::Error::FromSqlConversionFailure(
            9,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        )
    })?;
    let superseded_by = sup_str.and_then(|s| s.parse().ok());
    let created_at = DateTime::parse_from_rfc3339(&created_str)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(11, rusqlite::types::Type::Text, Box::new(e))
        })?;
    let updated_at = DateTime::parse_from_rfc3339(&updated_str)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(12, rusqlite::types::Type::Text, Box::new(e))
        })?;
    let accessed_at = accessed_str.and_then(|s| {
        DateTime::parse_from_rfc3339(&s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    });

    Ok(MemoryRecord {
        id,
        scope,
        scope_id,
        source,
        content,
        embedding,
        importance: importance as f32,
        metadata,
        provenance,
        state,
        superseded_by,
        created_at,
        updated_at,
        accessed_at,
        access_count: access_count as u32,
    })
}

#[async_trait]
impl RecoveryStore for SqliteStore {
    async fn save_recovery_record(&self, record: &RecoveryRecord) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO recovery_records (
                id, task_id, workflow_id, execution_id, attempt,
                strategy, strategy_version, failure_reason, diagnosis_json,
                recovery_action, action_reason, result, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                record.id.to_string(),
                record.task_id.to_string(),
                record.workflow_id.to_string(),
                record.execution_id.map(|id| id.to_string()),
                record.attempt,
                record.strategy,
                record.strategy_version,
                record.failure_reason,
                serde_json::to_string(&record.diagnosis)?,
                record.recovery_action,
                record.action_reason,
                record.result.as_str(),
                record.created_at.to_rfc3339(),
                record.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    async fn get_recovery_record(
        &self,
        id: &RecoveryId,
    ) -> Result<Option<RecoveryRecord>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, task_id, workflow_id, execution_id, attempt,
                    strategy, strategy_version, failure_reason, diagnosis_json,
                    recovery_action, action_reason, result, created_at, updated_at
             FROM recovery_records WHERE id = ?1",
        )?;

        let res = stmt
            .query_row(params![id.to_string()], parse_recovery_row)
            .optional()?;
        Ok(res)
    }

    async fn update_recovery_record(&self, record: &RecoveryRecord) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        let affected = conn.execute(
            "UPDATE recovery_records SET
                execution_id = ?1, attempt = ?2, strategy = ?3,
                strategy_version = ?4, failure_reason = ?5, diagnosis_json = ?6,
                recovery_action = ?7, action_reason = ?8, result = ?9, updated_at = ?10
             WHERE id = ?11",
            params![
                record.execution_id.map(|id| id.to_string()),
                record.attempt,
                record.strategy,
                record.strategy_version,
                record.failure_reason,
                serde_json::to_string(&record.diagnosis)?,
                record.recovery_action,
                record.action_reason,
                record.result.as_str(),
                record.updated_at.to_rfc3339(),
                record.id.to_string(),
            ],
        )?;

        if affected == 0 {
            return Err(StorageError::NotFound {
                entity_type: "RecoveryRecord",
                id: record.id.to_string(),
            });
        }
        Ok(())
    }

    async fn list_recovery_records_by_task(
        &self,
        task_id: &TaskId,
    ) -> Result<Vec<RecoveryRecord>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, task_id, workflow_id, execution_id, attempt,
                    strategy, strategy_version, failure_reason, diagnosis_json,
                    recovery_action, action_reason, result, created_at, updated_at
             FROM recovery_records
             WHERE task_id = ?1
             ORDER BY attempt ASC, created_at ASC",
        )?;

        let rows = stmt.query_map(params![task_id.to_string()], parse_recovery_row)?;
        let mut list = Vec::new();
        for r in rows {
            list.push(r?);
        }
        Ok(list)
    }

    async fn list_recovery_records_by_workflow(
        &self,
        workflow_id: &WorkflowId,
    ) -> Result<Vec<RecoveryRecord>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, task_id, workflow_id, execution_id, attempt,
                    strategy, strategy_version, failure_reason, diagnosis_json,
                    recovery_action, action_reason, result, created_at, updated_at
             FROM recovery_records
             WHERE workflow_id = ?1
             ORDER BY created_at ASC",
        )?;

        let rows = stmt.query_map(params![workflow_id.to_string()], parse_recovery_row)?;
        let mut list = Vec::new();
        for r in rows {
            list.push(r?);
        }
        Ok(list)
    }
}

fn parse_recovery_row(row: &rusqlite::Row<'_>) -> Result<RecoveryRecord, rusqlite::Error> {
    let id_str: String = row.get(0)?;
    let task_str: String = row.get(1)?;
    let wf_str: String = row.get(2)?;
    let exec_str: Option<String> = row.get(3)?;
    let attempt: u32 = row.get(4)?;
    let strategy: String = row.get(5)?;
    let strategy_version: u32 = row.get(6)?;
    let failure_reason: String = row.get(7)?;
    let diag_str: String = row.get(8)?;
    let recovery_action: String = row.get(9)?;
    let action_reason: Option<String> = row.get(10)?;
    let result_str: String = row.get(11)?;
    let created_str: String = row.get(12)?;
    let updated_str: String = row.get(13)?;

    let id: RecoveryId = id_str
        .parse()
        .map_err(|e: plexis_core::ids::IdParseError| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })?;
    let task_id: TaskId = task_str
        .parse()
        .map_err(|e: plexis_core::ids::IdParseError| {
            rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(e))
        })?;
    let workflow_id: WorkflowId = wf_str
        .parse()
        .map_err(|e: plexis_core::ids::IdParseError| {
            rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(e))
        })?;
    let execution_id = exec_str.and_then(|s| s.parse().ok());
    let diagnosis: serde_json::Value =
        serde_json::from_str(&diag_str).unwrap_or(serde_json::Value::Null);
    let result: RecoveryResult = result_str.parse().unwrap_or_default();
    let created_at = DateTime::parse_from_rfc3339(&created_str)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(12, rusqlite::types::Type::Text, Box::new(e))
        })?;
    let updated_at = DateTime::parse_from_rfc3339(&updated_str)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(13, rusqlite::types::Type::Text, Box::new(e))
        })?;

    Ok(RecoveryRecord {
        id,
        task_id,
        workflow_id,
        execution_id,
        attempt,
        strategy,
        strategy_version,
        failure_reason,
        diagnosis,
        recovery_action,
        action_reason,
        result,
        created_at,
        updated_at,
    })
}

#[async_trait]
impl WorkspaceStore for SqliteStore {
    async fn create_workspace(&self, ws: &Workspace) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO workspaces (id, name, canonical_path, policy, limits, vcs, metadata, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                ws.id.to_string(),
                ws.name,
                ws.canonical_path.to_string_lossy().to_string(),
                serde_json::to_string(&ws.policy)?,
                serde_json::to_string(&ws.limits)?,
                serde_json::to_string(&ws.vcs)?,
                serde_json::to_string(&ws.metadata)?,
                ws.created_at.to_rfc3339(),
                ws.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    async fn get_workspace(&self, id: &WorkspaceId) -> Result<Option<Workspace>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, name, canonical_path, policy, limits, vcs, metadata, created_at, updated_at
             FROM workspaces WHERE id = ?1",
        )?;

        let row = stmt
            .query_row(params![id.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                ))
            })
            .optional()?;

        match row {
            Some(tuple) => Ok(Some(parse_workspace_tuple(tuple)?)),
            None => Ok(None),
        }
    }

    async fn get_workspace_by_path(
        &self,
        canonical_path: &std::path::Path,
    ) -> Result<Option<Workspace>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, name, canonical_path, policy, limits, vcs, metadata, created_at, updated_at
             FROM workspaces WHERE canonical_path = ?1",
        )?;

        let path_str = canonical_path.to_string_lossy().to_string();
        let row = stmt
            .query_row(params![path_str], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                ))
            })
            .optional()?;

        match row {
            Some(tuple) => Ok(Some(parse_workspace_tuple(tuple)?)),
            None => Ok(None),
        }
    }

    async fn update_workspace(&self, ws: &Workspace) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        let rows = conn.execute(
            "UPDATE workspaces
             SET name = ?1, canonical_path = ?2, policy = ?3, limits = ?4, vcs = ?5, metadata = ?6, updated_at = ?7
             WHERE id = ?8",
            params![
                ws.name,
                ws.canonical_path.to_string_lossy().to_string(),
                serde_json::to_string(&ws.policy)?,
                serde_json::to_string(&ws.limits)?,
                serde_json::to_string(&ws.vcs)?,
                serde_json::to_string(&ws.metadata)?,
                ws.updated_at.to_rfc3339(),
                ws.id.to_string(),
            ],
        )?;

        if rows == 0 {
            return Err(StorageError::NotFound {
                entity_type: "Workspace",
                id: ws.id.to_string(),
            });
        }
        Ok(())
    }

    async fn list_workspaces(&self) -> Result<Vec<Workspace>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, name, canonical_path, policy, limits, vcs, metadata, created_at, updated_at
             FROM workspaces ORDER BY created_at DESC",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
            ))
        })?;

        let mut list = Vec::new();
        for r in rows {
            list.push(parse_workspace_tuple(r?)?);
        }
        Ok(list)
    }

    async fn delete_workspace(&self, id: &WorkspaceId) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "DELETE FROM workspaces WHERE id = ?1",
            params![id.to_string()],
        )?;
        Ok(())
    }
}

type WorkspaceTuple = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
);

fn parse_workspace_tuple(t: WorkspaceTuple) -> Result<Workspace, StorageError> {
    let (
        id_str,
        name,
        path_str,
        policy_str,
        limits_str,
        vcs_str,
        meta_str,
        created_str,
        updated_str,
    ) = t;

    let id: WorkspaceId = id_str.parse()?;
    let canonical_path = std::path::PathBuf::from(path_str);
    let policy = serde_json::from_str(&policy_str)?;
    let limits = serde_json::from_str(&limits_str)?;
    let vcs = serde_json::from_str(&vcs_str)?;
    let metadata = serde_json::from_str(&meta_str)?;
    let created_at = DateTime::parse_from_rfc3339(&created_str)
        .map_err(|e| StorageError::Migration(e.to_string()))?
        .with_timezone(&Utc);
    let updated_at = DateTime::parse_from_rfc3339(&updated_str)
        .map_err(|e| StorageError::Migration(e.to_string()))?
        .with_timezone(&Utc);

    Ok(Workspace {
        id,
        name,
        canonical_path,
        policy,
        limits,
        vcs,
        metadata,
        created_at,
        updated_at,
    })
}

#[async_trait]
impl RetentionStore for SqliteStore {
    async fn prune_historical_records(
        &self,
        older_than: DateTime<Utc>,
    ) -> Result<RetentionPruneReport, StorageError> {
        let conn = self.conn.lock().await;
        let older_str = older_than.to_rfc3339();

        let pruned_events = conn.execute(
            "DELETE FROM events WHERE timestamp < ?1",
            params![older_str],
        )? as u64;

        let pruned_messages = conn.execute(
            "DELETE FROM messages WHERE created_at < ?1",
            params![older_str],
        )? as u64;

        let pruned_commands = conn.execute(
            "DELETE FROM commands WHERE completed_at < ?1 AND state IN ('completed', 'failed')",
            params![older_str],
        )? as u64;

        Ok(RetentionPruneReport {
            pruned_events,
            pruned_messages,
            pruned_commands,
        })
    }
}

#[allow(clippy::type_complexity)]
fn parse_mission_tuple(
    t: (
        String,
        Option<String>,
        Option<String>,
        String,
        String,
        String,
        u32,
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        String,
        String,
    ),
) -> Result<Mission, StorageError> {
    let id: MissionId = t.0.parse()?;
    let workspace_id = match t.1 {
        Some(s) if !s.trim().is_empty() => Some(s.parse()?),
        _ => None,
    };
    let active_workflow_id = match t.2 {
        Some(s) if !s.trim().is_empty() => Some(s.parse()?),
        _ => None,
    };
    let state: MissionState = t.5.parse()?;
    let budget: MissionBudget = serde_json::from_str(&t.7)?;
    let budget_consumed: MissionBudgetConsumed = serde_json::from_str(&t.8)?;
    let health_status: MissionHealth = serde_json::from_str(&t.9).unwrap_or(match t.9.as_str() {
        "stagnant" => MissionHealth::Stagnant,
        "stalled" => MissionHealth::Stalled,
        "degraded" => MissionHealth::Degraded,
        "escalated" => MissionHealth::Escalated,
        _ => MissionHealth::Healthy,
    });
    let stopping_condition: StoppingCondition = serde_json::from_str(&t.10)?;
    let final_outcome: Option<MissionOutcome> = match t.12 {
        Some(s) if !s.trim().is_empty() => Some(serde_json::from_str(&s)?),
        _ => None,
    };
    let metadata: serde_json::Value = serde_json::from_str(&t.14).unwrap_or_default();
    let created_at = DateTime::parse_from_rfc3339(&t.15)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    let updated_at = DateTime::parse_from_rfc3339(&t.16)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());

    Ok(Mission {
        id,
        workspace_id,
        active_workflow_id,
        title: t.3,
        objective: t.4,
        state,
        cycle_index: t.6,
        budget,
        budget_consumed,
        health_status,
        stopping_condition,
        latest_verified_commit: t.11,
        final_outcome,
        escalation_reason: t.13,
        metadata,
        created_at,
        updated_at,
    })
}

#[allow(clippy::type_complexity)]
fn parse_checkpoint_tuple(
    t: (
        String,
        String,
        u32,
        String,
        String,
        String,
        String,
        String,
        String,
        Option<String>,
        String,
        String,
    ),
) -> Result<MissionCheckpoint, StorageError> {
    let id: CheckpointId = t.0.parse()?;
    let mission_id: MissionId = t.1.parse()?;
    let workflow_id: WorkflowId = t.3.parse()?;
    let task_states_summary: serde_json::Value = serde_json::from_str(&t.4).unwrap_or_default();
    let active_executions: Vec<ExecutionId> = serde_json::from_str(&t.5).unwrap_or_default();
    let completed_tasks: Vec<TaskId> = serde_json::from_str(&t.6).unwrap_or_default();
    let unresolved_tasks: Vec<TaskId> = serde_json::from_str(&t.7).unwrap_or_default();
    let budget_consumed: MissionBudgetConsumed = serde_json::from_str(&t.8)?;
    let planner_context: serde_json::Value = serde_json::from_str(&t.10).unwrap_or_default();
    let created_at = DateTime::parse_from_rfc3339(&t.11)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());

    Ok(MissionCheckpoint {
        id,
        mission_id,
        cycle_index: t.2,
        workflow_id,
        task_states_summary,
        active_executions,
        completed_tasks,
        unresolved_tasks,
        budget_consumed,
        latest_verified_commit: t.9,
        planner_context,
        created_at,
    })
}

fn parse_cycle_tuple(
    t: (
        String,
        String,
        u32,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        u32,
    ),
) -> Result<MissionCycle, StorageError> {
    let mission_id: MissionId = t.1.parse()?;
    let workflow_id: WorkflowId = t.3.parse()?;
    let started_at = DateTime::parse_from_rfc3339(&t.5)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    let completed_at = t.6.and_then(|s| {
        DateTime::parse_from_rfc3339(&s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    });

    Ok(MissionCycle {
        id: t.0,
        mission_id,
        cycle_index: t.2,
        workflow_id,
        phase: t.4,
        started_at,
        completed_at,
        outcome: t.7,
        discovered_tasks_count: t.8,
    })
}

#[async_trait]
impl MissionStore for SqliteStore {
    async fn create_mission(&self, m: &Mission) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO missions (
                id, workspace_id, active_workflow_id, title, objective, state,
                cycle_index, budget, budget_consumed, health_status, stopping_condition,
                latest_verified_commit, final_outcome, escalation_reason, metadata,
                created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![
                m.id.to_string(),
                m.workspace_id.map(|id| id.to_string()),
                m.active_workflow_id.map(|id| id.to_string()),
                m.title,
                m.objective,
                m.state.as_str(),
                m.cycle_index,
                serde_json::to_string(&m.budget)?,
                serde_json::to_string(&m.budget_consumed)?,
                serde_json::to_string(&m.health_status)?,
                serde_json::to_string(&m.stopping_condition)?,
                m.latest_verified_commit,
                m.final_outcome
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
                m.escalation_reason,
                serde_json::to_string(&m.metadata)?,
                m.created_at.to_rfc3339(),
                m.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    async fn get_mission(&self, id: &MissionId) -> Result<Option<Mission>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, workspace_id, active_workflow_id, title, objective, state,
                    cycle_index, budget, budget_consumed, health_status, stopping_condition,
                    latest_verified_commit, final_outcome, escalation_reason, metadata,
                    created_at, updated_at
             FROM missions WHERE id = ?1",
        )?;

        let row = stmt
            .query_row(params![id.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, u32>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, Option<String>>(11)?,
                    row.get::<_, Option<String>>(12)?,
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, String>(14)?,
                    row.get::<_, String>(15)?,
                    row.get::<_, String>(16)?,
                ))
            })
            .optional()?;

        match row {
            Some(t) => Ok(Some(parse_mission_tuple(t)?)),
            None => Ok(None),
        }
    }

    async fn update_mission(&self, m: &Mission) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "UPDATE missions SET
                workspace_id = ?1,
                active_workflow_id = ?2,
                title = ?3,
                objective = ?4,
                state = ?5,
                cycle_index = ?6,
                budget = ?7,
                budget_consumed = ?8,
                health_status = ?9,
                stopping_condition = ?10,
                latest_verified_commit = ?11,
                final_outcome = ?12,
                escalation_reason = ?13,
                metadata = ?14,
                updated_at = ?15
             WHERE id = ?16",
            params![
                m.workspace_id.map(|id| id.to_string()),
                m.active_workflow_id.map(|id| id.to_string()),
                m.title,
                m.objective,
                m.state.as_str(),
                m.cycle_index,
                serde_json::to_string(&m.budget)?,
                serde_json::to_string(&m.budget_consumed)?,
                serde_json::to_string(&m.health_status)?,
                serde_json::to_string(&m.stopping_condition)?,
                m.latest_verified_commit,
                m.final_outcome
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
                m.escalation_reason,
                serde_json::to_string(&m.metadata)?,
                m.updated_at.to_rfc3339(),
                m.id.to_string(),
            ],
        )?;
        Ok(())
    }

    async fn update_mission_conditional(
        &self,
        m: &Mission,
        expected_state: MissionState,
    ) -> Result<bool, StorageError> {
        let conn = self.conn.lock().await;
        let rows = conn.execute(
            "UPDATE missions SET
                workspace_id = ?1,
                active_workflow_id = ?2,
                title = ?3,
                objective = ?4,
                state = ?5,
                cycle_index = ?6,
                budget = ?7,
                budget_consumed = ?8,
                health_status = ?9,
                stopping_condition = ?10,
                latest_verified_commit = ?11,
                final_outcome = ?12,
                escalation_reason = ?13,
                metadata = ?14,
                updated_at = ?15
             WHERE id = ?16 AND state = ?17",
            params![
                m.workspace_id.map(|id| id.to_string()),
                m.active_workflow_id.map(|id| id.to_string()),
                m.title,
                m.objective,
                m.state.as_str(),
                m.cycle_index,
                serde_json::to_string(&m.budget)?,
                serde_json::to_string(&m.budget_consumed)?,
                serde_json::to_string(&m.health_status)?,
                serde_json::to_string(&m.stopping_condition)?,
                m.latest_verified_commit,
                m.final_outcome
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
                m.escalation_reason,
                serde_json::to_string(&m.metadata)?,
                m.updated_at.to_rfc3339(),
                m.id.to_string(),
                expected_state.as_str(),
            ],
        )?;
        Ok(rows > 0)
    }

    async fn list_missions(&self) -> Result<Vec<Mission>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, workspace_id, active_workflow_id, title, objective, state,
                    cycle_index, budget, budget_consumed, health_status, stopping_condition,
                    latest_verified_commit, final_outcome, escalation_reason, metadata,
                    created_at, updated_at
             FROM missions ORDER BY created_at DESC",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, u32>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, String>(10)?,
                row.get::<_, Option<String>>(11)?,
                row.get::<_, Option<String>>(12)?,
                row.get::<_, Option<String>>(13)?,
                row.get::<_, String>(14)?,
                row.get::<_, String>(15)?,
                row.get::<_, String>(16)?,
            ))
        })?;

        let mut list = Vec::new();
        for r in rows {
            list.push(parse_mission_tuple(r?)?);
        }
        Ok(list)
    }

    async fn list_missions_by_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<Mission>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, workspace_id, active_workflow_id, title, objective, state,
                    cycle_index, budget, budget_consumed, health_status, stopping_condition,
                    latest_verified_commit, final_outcome, escalation_reason, metadata,
                    created_at, updated_at
             FROM missions WHERE workspace_id = ?1 ORDER BY created_at DESC",
        )?;

        let rows = stmt.query_map(params![workspace_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, u32>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, String>(10)?,
                row.get::<_, Option<String>>(11)?,
                row.get::<_, Option<String>>(12)?,
                row.get::<_, Option<String>>(13)?,
                row.get::<_, String>(14)?,
                row.get::<_, String>(15)?,
                row.get::<_, String>(16)?,
            ))
        })?;

        let mut list = Vec::new();
        for r in rows {
            list.push(parse_mission_tuple(r?)?);
        }
        Ok(list)
    }

    async fn create_checkpoint(&self, c: &MissionCheckpoint) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO mission_checkpoints (
                id, mission_id, cycle_index, workflow_id, task_states_summary,
                active_executions, completed_tasks, unresolved_tasks, budget_consumed,
                latest_verified_commit, planner_context, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                c.id.to_string(),
                c.mission_id.to_string(),
                c.cycle_index,
                c.workflow_id.to_string(),
                serde_json::to_string(&c.task_states_summary)?,
                serde_json::to_string(&c.active_executions)?,
                serde_json::to_string(&c.completed_tasks)?,
                serde_json::to_string(&c.unresolved_tasks)?,
                serde_json::to_string(&c.budget_consumed)?,
                c.latest_verified_commit,
                serde_json::to_string(&c.planner_context)?,
                c.created_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    async fn get_checkpoint(
        &self,
        id: &CheckpointId,
    ) -> Result<Option<MissionCheckpoint>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, mission_id, cycle_index, workflow_id, task_states_summary,
                    active_executions, completed_tasks, unresolved_tasks, budget_consumed,
                    latest_verified_commit, planner_context, created_at
             FROM mission_checkpoints WHERE id = ?1",
        )?;

        let row = stmt
            .query_row(params![id.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u32>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                ))
            })
            .optional()?;

        match row {
            Some(t) => Ok(Some(parse_checkpoint_tuple(t)?)),
            None => Ok(None),
        }
    }

    async fn get_latest_checkpoint(
        &self,
        mission_id: &MissionId,
    ) -> Result<Option<MissionCheckpoint>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, mission_id, cycle_index, workflow_id, task_states_summary,
                    active_executions, completed_tasks, unresolved_tasks, budget_consumed,
                    latest_verified_commit, planner_context, created_at
             FROM mission_checkpoints WHERE mission_id = ?1
             ORDER BY cycle_index DESC, created_at DESC LIMIT 1",
        )?;

        let row = stmt
            .query_row(params![mission_id.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u32>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                ))
            })
            .optional()?;

        match row {
            Some(t) => Ok(Some(parse_checkpoint_tuple(t)?)),
            None => Ok(None),
        }
    }

    async fn list_checkpoints(
        &self,
        mission_id: &MissionId,
    ) -> Result<Vec<MissionCheckpoint>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, mission_id, cycle_index, workflow_id, task_states_summary,
                    active_executions, completed_tasks, unresolved_tasks, budget_consumed,
                    latest_verified_commit, planner_context, created_at
             FROM mission_checkpoints WHERE mission_id = ?1
             ORDER BY cycle_index ASC, created_at ASC",
        )?;

        let rows = stmt.query_map(params![mission_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, u32>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, String>(10)?,
                row.get::<_, String>(11)?,
            ))
        })?;

        let mut list = Vec::new();
        for r in rows {
            list.push(parse_checkpoint_tuple(r?)?);
        }
        Ok(list)
    }

    async fn create_cycle(&self, c: &MissionCycle) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO mission_cycles (
                id, mission_id, cycle_index, workflow_id, phase, started_at,
                completed_at, outcome, discovered_tasks_count
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                c.id,
                c.mission_id.to_string(),
                c.cycle_index,
                c.workflow_id.to_string(),
                c.phase,
                c.started_at.to_rfc3339(),
                c.completed_at.as_ref().map(|dt| dt.to_rfc3339()),
                c.outcome,
                c.discovered_tasks_count,
            ],
        )?;
        Ok(())
    }

    async fn list_cycles(&self, mission_id: &MissionId) -> Result<Vec<MissionCycle>, StorageError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, mission_id, cycle_index, workflow_id, phase, started_at,
                    completed_at, outcome, discovered_tasks_count
             FROM mission_cycles WHERE mission_id = ?1
             ORDER BY cycle_index ASC, started_at ASC",
        )?;

        let rows = stmt.query_map(params![mission_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, u32>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, u32>(8)?,
            ))
        })?;

        let mut list = Vec::new();
        for r in rows {
            list.push(parse_cycle_tuple(r?)?);
        }
        Ok(list)
    }

    async fn update_cycle(&self, c: &MissionCycle) -> Result<(), StorageError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "UPDATE mission_cycles SET
                phase = ?1,
                completed_at = ?2,
                outcome = ?3,
                discovered_tasks_count = ?4
             WHERE id = ?5",
            params![
                c.phase,
                c.completed_at.as_ref().map(|dt| dt.to_rfc3339()),
                c.outcome,
                c.discovered_tasks_count,
                c.id,
            ],
        )?;
        Ok(())
    }
}
