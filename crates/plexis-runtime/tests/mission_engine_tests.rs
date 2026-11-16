use std::sync::Arc;

use plexis_core::mission::{MissionBudget, StoppingCondition};
use plexis_core::state::{MissionState, TaskState};
use plexis_core::{Task, Workflow};
use plexis_runtime::mission::{CheckpointManager, MissionEngine};
use plexis_runtime::reconciler::Reconciler;
use plexis_storage::sqlite::SqliteStore;
use plexis_storage::traits::{EventStore, LeaseStore, MissionStore, TaskStore, WorkflowStore};

#[tokio::test]
async fn test_mission_engine_lifecycle_and_events() {
    let store = Arc::new(SqliteStore::open_in_memory().unwrap());
    let engine = MissionEngine::new(store.clone());

    // 1. Create Mission
    let mission = engine
        .create_mission(
            "Engineering Objective",
            "Refactor auth layer and verify test suite",
            None,
            Some(MissionBudget::default()),
            Some(StoppingCondition::default()),
        )
        .await
        .unwrap();

    assert_eq!(mission.state, MissionState::Created);
    assert_eq!(mission.cycle_index, 0);

    // 2. Start Mission -> Planning
    let started = engine.start_mission(mission.id).await.unwrap();
    assert_eq!(started.state, MissionState::Planning);
    assert!(engine.is_running(&mission.id).await);

    // 3. Pause Mission -> Waiting
    let paused = engine.pause_mission(&mission.id).await.unwrap();
    assert_eq!(paused.state, MissionState::Waiting);
    assert!(!engine.is_running(&mission.id).await);

    // 4. Resume Mission -> Running
    let resumed = engine.resume_mission(&mission.id).await.unwrap();
    assert_eq!(resumed.state, MissionState::Running);
    assert!(engine.is_running(&mission.id).await);

    // 5. Cancel Mission -> Cancelled
    let cancelled = engine.cancel_mission(&mission.id).await.unwrap();
    assert_eq!(cancelled.state, MissionState::Cancelled);
    assert!(!engine.is_running(&mission.id).await);

    // 6. Verify audit events recorded
    let events = store.list_events_after(0, 100).await.unwrap();
    let mission_events: Vec<_> = events
        .into_iter()
        .filter(|e| e.aggregate_type == "mission")
        .collect();
    assert!(mission_events.len() >= 4);
}

#[tokio::test]
async fn test_mission_checkpoint_creation_and_restoration() {
    let store = Arc::new(SqliteStore::open_in_memory().unwrap());
    let mgr = CheckpointManager::new(store.clone());

    let mission = plexis_core::mission::Mission::new("Checkpoint Mission", "Objective");
    store.create_mission(&mission).await.unwrap();

    let wf = Workflow::new("WF1", "Test WF");
    store.create_workflow(&wf).await.unwrap();

    let mut t1 = Task::new(wf.id, "T1");
    t1.state = TaskState::Verified;
    store.create_task(&t1).await.unwrap();

    let mut t2 = Task::new(wf.id, "T2");
    t2.state = TaskState::Running;
    store.create_task(&t2).await.unwrap();

    let budget_consumed = plexis_core::mission::MissionBudgetConsumed {
        duration_secs: 42,
        total_executions: 5,
        recovery_attempts: 1,
        planner_iterations: 1,
        stagnant_cycles: 0,
    };

    let ckpt = mgr
        .create_checkpoint(
            mission.id,
            1,
            wf.id,
            budget_consumed.clone(),
            Some("commit_abc123".into()),
            vec![],
            serde_json::json!({ "planner_notes": "cycle 1 done" }),
        )
        .await
        .unwrap();

    assert_eq!(ckpt.mission_id, mission.id);
    assert_eq!(ckpt.cycle_index, 1);
    assert_eq!(ckpt.completed_tasks, vec![t1.id]);
    assert_eq!(ckpt.unresolved_tasks, vec![t2.id]);
    assert_eq!(
        ckpt.latest_verified_commit.as_deref(),
        Some("commit_abc123")
    );

    let restored = mgr
        .get_latest_checkpoint(&mission.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.id, ckpt.id);
    assert_eq!(
        restored.latest_verified_commit.as_deref(),
        Some("commit_abc123")
    );
}

#[tokio::test]
async fn test_budget_exhaustion_enforcement() {
    let store = Arc::new(SqliteStore::open_in_memory().unwrap());
    let engine = MissionEngine::new(store.clone());

    let budget = MissionBudget {
        max_planner_iterations: 2,
        ..Default::default()
    };

    let mission = engine
        .create_mission(
            "Budget Constrained Mission",
            "Perform tight budget task",
            None,
            Some(budget),
            None,
        )
        .await
        .unwrap();

    let mut m = engine.start_mission(mission.id).await.unwrap();
    // Simulate consuming 2 planner iterations
    m.budget_consumed.planner_iterations = 2;
    store.update_mission(&m).await.unwrap();

    // Next step will evaluate budget tracker and detect exhaustion
    let stepped = engine.step_mission(mission.id).await.unwrap();
    assert_eq!(stepped.state, MissionState::BudgetExhausted);
    assert!(stepped.final_outcome.is_some());
    assert!(!stepped.final_outcome.unwrap().success);
}

#[tokio::test]
async fn test_stagnation_detection_and_human_escalation() {
    let store = Arc::new(SqliteStore::open_in_memory().unwrap());
    let engine = MissionEngine::new(store.clone());

    let budget = MissionBudget {
        max_stagnant_cycles: 2,
        ..Default::default()
    };

    let mission = engine
        .create_mission(
            "Stagnation Test Mission",
            "Objective that makes no progress",
            None,
            Some(budget),
            None,
        )
        .await
        .unwrap();

    let mut m = engine.start_mission(mission.id).await.unwrap();
    m.state = MissionState::Running;
    store.update_mission(&m).await.unwrap();

    // Step 1: no progress -> stagnant count = 1
    let m1 = engine.step_mission(mission.id).await.unwrap();
    assert_eq!(m1.budget_consumed.stagnant_cycles, 1);

    // Step 2: no progress -> stagnant count = 2 -> reaches threshold -> escalates to NeedsHuman
    let m2 = engine.step_mission(mission.id).await.unwrap();
    assert_eq!(m2.state, MissionState::NeedsHuman);
    assert!(m2.escalation_reason.is_some());

    // Operator resolves escalation by replanning
    let resolved = engine
        .resolve_escalation(&mission.id, "replan")
        .await
        .unwrap();
    assert_eq!(resolved.state, MissionState::Replanning);
    assert!(resolved.escalation_reason.is_none());
}

#[tokio::test]
async fn test_reconciler_discovers_active_missions_on_startup() {
    let store = Arc::new(SqliteStore::open_in_memory().unwrap());

    // Create an active mission in Running state
    let mut mission = plexis_core::mission::Mission::new("Resumable Mission", "Long horizon");
    mission.state = MissionState::Running;
    store.create_mission(&mission).await.unwrap();

    // Create a completed mission (should not be marked resumable)
    let mut done_mission = plexis_core::mission::Mission::new("Done Mission", "Completed");
    done_mission.state = MissionState::Completed;
    store.create_mission(&done_mission).await.unwrap();

    // Create an awaiting acceptance mission (should NOT be marked resumable, waits for human)
    let mut review_mission = plexis_core::mission::Mission::new("Review Mission", "Review ready");
    review_mission.state = MissionState::AwaitingAcceptance;
    store.create_mission(&review_mission).await.unwrap();

    // Create an accepted mission (should NOT be marked resumable, waits for integrate)
    let mut accepted_mission = plexis_core::mission::Mission::new("Accepted Mission", "Accepted");
    accepted_mission.state = MissionState::Accepted;
    store.create_mission(&accepted_mission).await.unwrap();

    let reconciler = Reconciler::new(
        store.clone() as Arc<dyn TaskStore>,
        store.clone() as Arc<dyn LeaseStore>,
        store.clone() as Arc<dyn EventStore>,
    )
    .with_workflow_store(store.clone() as Arc<dyn WorkflowStore>)
    .with_mission_store(store.clone() as Arc<dyn MissionStore>);

    let report = reconciler.reconcile_startup().await.unwrap();
    assert_eq!(report.resumable_missions.len(), 1);
    assert_eq!(report.resumable_missions[0], mission.id);

    // Verify audit event emitted
    let events = store.list_events_after(0, 100).await.unwrap();
    let reconciled_events: Vec<_> = events
        .into_iter()
        .filter(|e| e.event_type == "mission.reconciled_resumable")
        .collect();
    assert_eq!(reconciled_events.len(), 1);
}

#[tokio::test]
async fn test_escalation_resolution_lifecycle_running_missions() {
    let store = Arc::new(SqliteStore::open_in_memory().unwrap());
    let engine = MissionEngine::new(store.clone());

    let mission = engine
        .create_mission(
            "Escalation Test Mission",
            "Verify running state transitions across escalation and resolution",
            None,
            Some(MissionBudget::default()),
            None,
        )
        .await
        .unwrap();

    // 1. Start mission -> Planning -> running_missions contains id
    let started = engine.start_mission(mission.id).await.unwrap();
    assert_eq!(started.state, MissionState::Planning);
    assert!(engine.is_running(&mission.id).await);

    // 2. Escalate to human -> NeedsHuman -> running_missions removed
    let escalated = engine
        .escalate_human(&mission.id, "Need operator clarification")
        .await
        .unwrap();
    assert_eq!(escalated.state, MissionState::NeedsHuman);
    assert!(!engine.is_running(&mission.id).await);

    // 3. Resolve with resume -> Running -> running_missions restored
    let resumed = engine
        .resolve_escalation(&mission.id, "resume")
        .await
        .unwrap();
    assert_eq!(resumed.state, MissionState::Running);
    assert!(
        engine.is_running(&mission.id).await,
        "Mission should be marked running after resume resolution"
    );

    // 4. Escalate to human again -> NeedsHuman -> running_missions removed
    let escalated2 = engine
        .escalate_human(&mission.id, "Need second operator clarification")
        .await
        .unwrap();
    assert_eq!(escalated2.state, MissionState::NeedsHuman);
    assert!(!engine.is_running(&mission.id).await);

    // 5. Resolve with replan -> Replanning -> running_missions restored
    let replanned = engine
        .resolve_escalation(&mission.id, "replan")
        .await
        .unwrap();
    assert_eq!(replanned.state, MissionState::Replanning);
    assert!(
        engine.is_running(&mission.id).await,
        "Mission should be marked running after replan resolution"
    );

    // 6. Escalate to human again -> NeedsHuman -> running_missions removed
    let escalated3 = engine
        .escalate_human(&mission.id, "Unrecoverable issue")
        .await
        .unwrap();
    assert_eq!(escalated3.state, MissionState::NeedsHuman);
    assert!(!engine.is_running(&mission.id).await);

    // 7. Resolve with cancel -> Cancelled -> running_missions remains not running
    let cancelled = engine
        .resolve_escalation(&mission.id, "cancel")
        .await
        .unwrap();
    assert_eq!(cancelled.state, MissionState::Cancelled);
    assert!(
        !engine.is_running(&mission.id).await,
        "Mission should NOT be running after cancel resolution"
    );
}

#[tokio::test]
async fn test_escalation_resolution_rejects_non_needs_human() {
    let store = Arc::new(SqliteStore::open_in_memory().unwrap());
    let engine = MissionEngine::new(store.clone());

    let mission = engine
        .create_mission(
            "Conflict Test Mission",
            "Verify resolution is rejected when not in NeedsHuman",
            None,
            Some(MissionBudget::default()),
            None,
        )
        .await
        .unwrap();

    // Mission in Created state -> resolve_escalation should fail with Conflict
    let err = engine.resolve_escalation(&mission.id, "resume").await;
    assert!(matches!(
        err,
        Err(plexis_runtime::RuntimeError::Conflict(_))
    ));

    // Start mission (Planning state) -> resolve_escalation should fail with Conflict
    let _started = engine.start_mission(mission.id).await.unwrap();
    let err = engine.resolve_escalation(&mission.id, "resume").await;
    assert!(matches!(
        err,
        Err(plexis_runtime::RuntimeError::Conflict(_))
    ));

    // Escalate to human -> NeedsHuman
    let _ = engine.escalate_human(&mission.id, "help").await.unwrap();

    // First resolve succeeds
    let _ = engine
        .resolve_escalation(&mission.id, "resume")
        .await
        .unwrap();

    // Second resolve on the same mission (now in Running state) must fail with Conflict
    let err = engine.resolve_escalation(&mission.id, "resume").await;
    assert!(matches!(
        err,
        Err(plexis_runtime::RuntimeError::Conflict(_))
    ));
}

#[tokio::test]
async fn test_escalation_resolution_allows_subsequent_step_execution() {
    let store = Arc::new(SqliteStore::open_in_memory().unwrap());
    let engine = MissionEngine::new(store.clone());

    let mission = engine
        .create_mission(
            "Step Execution Test Mission",
            "Verify step execution proceeds after escalation resolution",
            None,
            Some(MissionBudget::default()),
            None,
        )
        .await
        .unwrap();

    let _started = engine.start_mission(mission.id).await.unwrap();
    let _escalated = engine.escalate_human(&mission.id, "Stalled").await.unwrap();
    assert!(!engine.is_running(&mission.id).await);

    // In NeedsHuman, step_mission returns without progressing
    let no_step = engine.step_mission(mission.id).await.unwrap();
    assert_eq!(no_step.state, MissionState::NeedsHuman);

    // Resolve with resume
    let resumed = engine
        .resolve_escalation(&mission.id, "resume")
        .await
        .unwrap();
    assert_eq!(resumed.state, MissionState::Running);
    assert!(engine.is_running(&mission.id).await);

    // Now step_mission can execute
    let stepped = engine.step_mission(mission.id).await.unwrap();
    assert_ne!(stepped.state, MissionState::NeedsHuman);
    assert!(engine.is_running(&mission.id).await);
}

#[tokio::test]
async fn test_conditional_mission_update_rejects_stale_state() {
    let store = Arc::new(SqliteStore::open_in_memory().unwrap());
    let engine = MissionEngine::new(store.clone());

    let mission = engine
        .create_mission(
            "Conditional Update Mission",
            "Verify CAS state update reject on stale state",
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let mut modified = mission.clone();
    modified.state = MissionState::Running;

    // Expected state is NeedsHuman, but actual state is Created -> must return false
    let cas_res = store
        .update_mission_conditional(&modified, MissionState::NeedsHuman)
        .await
        .unwrap();
    assert!(!cas_res);

    // Verify stored state remains Created
    let current = store.get_mission(&mission.id).await.unwrap().unwrap();
    assert_eq!(current.state, MissionState::Created);

    // Expected state is Created -> must return true
    let cas_ok = store
        .update_mission_conditional(&modified, MissionState::Created)
        .await
        .unwrap();
    assert!(cas_ok);

    // Verify stored state is now Running
    let current_updated = store.get_mission(&mission.id).await.unwrap().unwrap();
    assert_eq!(current_updated.state, MissionState::Running);
}

struct FailingWorkflowExecutor;

#[async_trait::async_trait]
impl plexis_runtime::mission::WorkflowExecutor for FailingWorkflowExecutor {
    async fn execute_workflow_cycle(
        &self,
        _workflow_id: &plexis_core::ids::WorkflowId,
        _mission_id: &plexis_core::ids::MissionId,
    ) -> Result<plexis_runtime::mission::WorkflowExecutionSummary, plexis_runtime::RuntimeError>
    {
        Err(plexis_runtime::RuntimeError::Execution(
            "Simulated infrastructure failure during cycle execution".into(),
        ))
    }
}

#[tokio::test]
async fn test_workflow_execution_error_escalates_to_needs_human_without_stalling() {
    let store = Arc::new(SqliteStore::open_in_memory().unwrap());
    let engine =
        MissionEngine::new(store.clone()).with_workflow_executor(Arc::new(FailingWorkflowExecutor));

    let mission = engine
        .create_mission(
            "Fault Injection Mission",
            "Verify failure handling does not leave state stalled",
            None,
            Some(MissionBudget::default()),
            Some(StoppingCondition::default()),
        )
        .await
        .unwrap();

    let _started = engine.start_mission(mission.id).await.unwrap();
    assert!(engine.is_running(&mission.id).await);

    // Step mission triggers failing executor
    let stepped = engine.step_mission(mission.id).await.unwrap();

    // Verify mission escalated to NeedsHuman
    assert_eq!(stepped.state, MissionState::NeedsHuman);
    assert!(stepped
        .escalation_reason
        .as_deref()
        .unwrap_or("")
        .contains("Simulated infrastructure failure"));

    // Verify engine running state is false
    assert!(!engine.is_running(&mission.id).await);

    // Verify persistent state in store is NeedsHuman (not Running)
    let persisted = store.get_mission(&mission.id).await.unwrap().unwrap();
    assert_eq!(persisted.state, MissionState::NeedsHuman);
    assert_eq!(
        persisted.health_status,
        plexis_core::mission::MissionHealth::Escalated
    );
    assert!(persisted
        .escalation_reason
        .as_deref()
        .unwrap_or("")
        .contains("Simulated infrastructure failure"));
}
