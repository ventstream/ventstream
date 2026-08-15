use std::fs;
use std::path::{Path, PathBuf};

use chrono::Utc;
use uuid::Uuid;

use crate::ConfigurationBundle;
use crate::{
    AgentError, AgentOperationKind, AgentScope, ConvergenceOperation, DesiredRunState,
    DesiredStateAcceptance, DesiredStateSnapshot, EngineAdapter, EngineAgent, ManagementState,
    ObservedRunState, OperationReceipt, ReceiptState, StateStore,
};

#[derive(Default)]
struct FakeAdapter {
    startup_calls: Vec<Option<DesiredRunState>>,
    apply_calls: Vec<DesiredRunState>,
    reconcile_calls: Vec<bool>,
    rebootstrap_calls: usize,
    configuration_calls: Vec<u64>,
    fail_next_apply: bool,
    next_observed: Option<ObservedRunState>,
}

impl EngineAdapter for FakeAdapter {
    async fn apply_startup_gate(
        &mut self,
        desired: Option<DesiredRunState>,
    ) -> Result<ObservedRunState, String> {
        self.startup_calls.push(desired);
        Ok(desired.map_or(ObservedRunState::Stopped, ObservedRunState::from))
    }

    async fn apply_run_state(
        &mut self,
        desired: DesiredRunState,
    ) -> Result<ObservedRunState, String> {
        self.apply_calls.push(desired);
        if self.fail_next_apply {
            self.fail_next_apply = false;
            return Err("sensitive adapter detail".to_owned());
        }
        if let Some(observed) = self.next_observed.take() {
            return Ok(observed);
        }
        Ok(desired.into())
    }

    async fn reconcile(&mut self, delete_orphans: bool) -> Result<(), String> {
        self.reconcile_calls.push(delete_orphans);
        Ok(())
    }

    async fn rebootstrap(&mut self) -> Result<(), String> {
        self.rebootstrap_calls += 1;
        Ok(())
    }

    async fn apply_configuration(
        &mut self,
        configuration: &ConfigurationBundle,
    ) -> Result<(), String> {
        self.configuration_calls.push(configuration.revision);
        Ok(())
    }
}

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Result<Self, std::io::Error> {
        let path = std::env::temp_dir().join(format!("fleet-engine-agent-{}", Uuid::now_v7()));
        fs::create_dir(&path)?;
        set_private_directory_permissions(&path)?;
        Ok(Self(path))
    }

    fn state_path(&self) -> PathBuf {
        self.0.join("management-state.json")
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn scope() -> AgentScope {
    AgentScope {
        pipeline_id: Uuid::now_v7(),
        deployment_id: Uuid::now_v7(),
    }
}

fn desired(scope: &AgentScope, revision: u64, run_state: DesiredRunState) -> DesiredStateSnapshot {
    desired_with_configuration(scope, revision, run_state, None)
}

fn desired_with_configuration(
    scope: &AgentScope,
    revision: u64,
    run_state: DesiredRunState,
    configuration_revision: Option<u64>,
) -> DesiredStateSnapshot {
    DesiredStateSnapshot {
        pipeline_id: scope.pipeline_id,
        deployment_id: scope.deployment_id,
        revision,
        run_state,
        configuration_revision,
    }
}

fn configuration_bundle(revision: u64) -> ConfigurationBundle {
    ConfigurationBundle {
        revision,
        schema_version: 1,
        content_digest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"
            .to_owned(),
        document_json: r#"{"version":1}"#.to_owned(),
    }
}

fn operation(
    scope: &AgentScope,
    sequence: u64,
    revision: u64,
    run_state: DesiredRunState,
) -> ConvergenceOperation {
    ConvergenceOperation {
        operation_id: Uuid::now_v7(),
        pipeline_id: scope.pipeline_id,
        deployment_id: scope.deployment_id,
        sequence,
        expected_desired_revision: revision,
        kind: AgentOperationKind::ConvergeState,
        run_state,
        delete_orphans: false,
        destructive_action_confirmed: false,
        configuration: None,
    }
}

fn reconcile_operation(scope: &AgentScope, sequence: u64, revision: u64) -> ConvergenceOperation {
    ConvergenceOperation {
        operation_id: Uuid::now_v7(),
        pipeline_id: scope.pipeline_id,
        deployment_id: scope.deployment_id,
        sequence,
        expected_desired_revision: revision,
        kind: AgentOperationKind::Reconcile,
        run_state: DesiredRunState::Running,
        delete_orphans: true,
        destructive_action_confirmed: false,
        configuration: None,
    }
}

fn test_directory() -> Result<TestDirectory, AgentError> {
    TestDirectory::new().map_err(|error| AgentError::StateIo {
        path: PathBuf::from("test directory"),
        source: error,
    })
}

#[tokio::test]
async fn new_agent_starts_fail_closed_without_desired_state() -> Result<(), AgentError> {
    let directory = test_directory()?;
    let agent = EngineAgent::load(
        scope(),
        StateStore::new(directory.state_path()),
        FakeAdapter::default(),
    )
    .await?;

    assert_eq!(agent.state().observed(), ObservedRunState::Stopped);
    assert_eq!(agent.adapter().startup_calls, vec![None]);
    Ok(())
}

#[tokio::test]
async fn paused_state_survives_restart_and_gates_startup() -> Result<(), AgentError> {
    let directory = test_directory()?;
    let scope = scope();
    let store = StateStore::new(directory.state_path());
    let mut agent = EngineAgent::load(scope.clone(), store.clone(), FakeAdapter::default()).await?;
    agent.accept_desired_state(desired(&scope, 1, DesiredRunState::Paused))?;
    let result = agent
        .execute_convergence(operation(&scope, 7, 1, DesiredRunState::Paused))
        .await?;
    assert_eq!(result.receipt.state, ReceiptState::Succeeded);
    drop(agent);

    let restarted = EngineAgent::load(scope, store, FakeAdapter::default()).await?;
    assert_eq!(restarted.state().observed(), ObservedRunState::Paused);
    assert_eq!(
        restarted.adapter().startup_calls,
        vec![Some(DesiredRunState::Paused)]
    );
    Ok(())
}

#[tokio::test]
async fn terminal_operation_replay_does_not_repeat_engine_effect() -> Result<(), AgentError> {
    let directory = test_directory()?;
    let scope = scope();
    let mut agent = EngineAgent::load(
        scope.clone(),
        StateStore::new(directory.state_path()),
        FakeAdapter::default(),
    )
    .await?;
    agent.accept_desired_state(desired(&scope, 1, DesiredRunState::Running))?;
    let operation = operation(&scope, 3, 1, DesiredRunState::Running);

    let first = agent.execute_convergence(operation.clone()).await?;
    let second = agent.execute_convergence(operation.clone()).await?;

    assert!(!first.replayed);
    assert!(second.replayed);
    assert_eq!(agent.adapter().apply_calls, vec![DesiredRunState::Running]);

    agent.accept_desired_state(desired(&scope, 2, DesiredRunState::Paused))?;
    let after_desired_advanced = agent.execute_convergence(operation).await?;
    assert!(after_desired_advanced.replayed);
    assert_eq!(agent.adapter().apply_calls, vec![DesiredRunState::Running]);
    Ok(())
}

#[tokio::test]
async fn apply_configuration_persists_bundle_and_replays_without_reapplying()
-> Result<(), AgentError> {
    let directory = test_directory()?;
    let scope = scope();
    let mut agent = EngineAgent::load(
        scope.clone(),
        StateStore::new(directory.state_path()),
        FakeAdapter::default(),
    )
    .await?;
    agent.accept_desired_state(desired_with_configuration(
        &scope,
        1,
        DesiredRunState::Running,
        Some(4),
    ))?;
    let operation = ConvergenceOperation {
        operation_id: Uuid::now_v7(),
        pipeline_id: scope.pipeline_id,
        deployment_id: scope.deployment_id,
        sequence: 4,
        expected_desired_revision: 1,
        kind: AgentOperationKind::ApplyConfiguration,
        run_state: DesiredRunState::Running,
        delete_orphans: false,
        destructive_action_confirmed: false,
        configuration: Some(configuration_bundle(4)),
    };

    let first = agent.execute_convergence(operation.clone()).await?;
    let second = agent.execute_convergence(operation).await?;

    assert_eq!(first.receipt.state, ReceiptState::Succeeded);
    assert!(!first.replayed);
    assert!(second.replayed);
    assert_eq!(agent.adapter().configuration_calls, vec![4]);
    assert_eq!(agent.state().applied_configuration_revision(), Some(4));
    assert_eq!(first.receipt.configuration_revision, Some(4));
    Ok(())
}

#[tokio::test]
async fn reconcile_operation_uses_dedicated_adapter_hook() -> Result<(), AgentError> {
    let directory = test_directory()?;
    let scope = scope();
    let mut agent = EngineAgent::load(
        scope.clone(),
        StateStore::new(directory.state_path()),
        FakeAdapter::default(),
    )
    .await?;
    agent.accept_desired_state(desired(&scope, 1, DesiredRunState::Running))?;

    let result = agent
        .execute_convergence(reconcile_operation(&scope, 2, 1))
        .await?;

    assert_eq!(result.receipt.state, ReceiptState::Succeeded);
    assert_eq!(agent.adapter().reconcile_calls, [true]);
    assert!(agent.adapter().apply_calls.is_empty());
    assert_eq!(agent.state().observed(), ObservedRunState::Stopped);
    Ok(())
}

#[tokio::test]
async fn operation_updates_are_emitted_only_after_each_persisted_transition()
-> Result<(), AgentError> {
    let directory = test_directory()?;
    let scope = scope();
    let mut agent = EngineAgent::load(
        scope.clone(),
        StateStore::new(directory.state_path()),
        FakeAdapter::default(),
    )
    .await?;
    agent.accept_desired_state(desired(&scope, 1, DesiredRunState::Paused))?;
    let mut updates = Vec::new();

    agent
        .execute_convergence_with_updates(
            operation(&scope, 1, 1, DesiredRunState::Paused),
            |_state, receipt| updates.push(receipt.state),
        )
        .await?;

    assert_eq!(
        updates,
        vec![
            ReceiptState::Acknowledged,
            ReceiptState::Running,
            ReceiptState::Succeeded
        ]
    );
    Ok(())
}

#[tokio::test]
async fn adapter_failure_is_terminal_and_hides_internal_detail() -> Result<(), AgentError> {
    let directory = test_directory()?;
    let scope = scope();
    let adapter = FakeAdapter {
        fail_next_apply: true,
        ..FakeAdapter::default()
    };
    let mut agent = EngineAgent::load(
        scope.clone(),
        StateStore::new(directory.state_path()),
        adapter,
    )
    .await?;
    agent.accept_desired_state(desired(&scope, 1, DesiredRunState::Running))?;

    let result = agent
        .execute_convergence(operation(&scope, 1, 1, DesiredRunState::Running))
        .await?;

    assert_eq!(result.receipt.state, ReceiptState::Failed);
    assert_eq!(
        result.receipt.message.as_deref(),
        Some("engine state transition failed")
    );
    assert!(
        !result
            .receipt
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("sensitive")
    );
    assert_eq!(agent.state().observed(), ObservedRunState::Error);
    Ok(())
}

#[tokio::test]
async fn adapter_must_confirm_the_requested_observed_state() -> Result<(), AgentError> {
    let directory = test_directory()?;
    let scope = scope();
    let adapter = FakeAdapter {
        next_observed: Some(ObservedRunState::Paused),
        ..FakeAdapter::default()
    };
    let mut agent = EngineAgent::load(
        scope.clone(),
        StateStore::new(directory.state_path()),
        adapter,
    )
    .await?;
    agent.accept_desired_state(desired(&scope, 1, DesiredRunState::Running))?;

    let result = agent
        .execute_convergence(operation(&scope, 1, 1, DesiredRunState::Running))
        .await?;

    assert_eq!(result.receipt.state, ReceiptState::Failed);
    assert_eq!(agent.state().observed(), ObservedRunState::Error);
    Ok(())
}

#[tokio::test]
async fn running_receipt_is_idempotently_resumed_after_restart() -> Result<(), AgentError> {
    let directory = test_directory()?;
    let scope = scope();
    let store = StateStore::new(directory.state_path());
    let operation = operation(&scope, 9, 1, DesiredRunState::Drained);
    let mut state = ManagementState::new(scope.clone());
    state.desired = Some(desired(&scope, 1, DesiredRunState::Drained));
    state.observed = ObservedRunState::Starting;
    state.last_operation_sequence = operation.sequence;
    state.operation_receipts.push(OperationReceipt {
        operation_id: operation.operation_id,
        sequence: operation.sequence,
        expected_desired_revision: operation.expected_desired_revision,
        kind: operation.kind,
        run_state: operation.run_state,
        delete_orphans: operation.delete_orphans,
        destructive_action_confirmed: operation.destructive_action_confirmed,
        configuration_revision: None,
        state: ReceiptState::Running,
        message: None,
        updated_at: Utc::now(),
    });
    store.persist(&state)?;

    let mut restarted = EngineAgent::load(scope, store, FakeAdapter::default()).await?;
    let result = restarted.execute_convergence(operation).await?;

    assert!(result.replayed);
    assert_eq!(result.receipt.state, ReceiptState::Succeeded);
    assert_eq!(
        restarted.adapter().apply_calls,
        vec![DesiredRunState::Drained]
    );
    Ok(())
}

#[tokio::test]
async fn desired_revisions_are_scoped_monotonic_and_immutable() -> Result<(), AgentError> {
    let directory = test_directory()?;
    let agent_scope = scope();
    let mut agent = EngineAgent::load(
        agent_scope.clone(),
        StateStore::new(directory.state_path()),
        FakeAdapter::default(),
    )
    .await?;
    let revision_two = desired(&agent_scope, 2, DesiredRunState::Running);

    assert_eq!(
        agent.accept_desired_state(revision_two.clone())?,
        DesiredStateAcceptance::Accepted
    );
    assert_eq!(
        agent.accept_desired_state(revision_two)?,
        DesiredStateAcceptance::Duplicate
    );
    assert_eq!(
        agent.accept_desired_state(desired(&agent_scope, 1, DesiredRunState::Paused))?,
        DesiredStateAcceptance::Stale
    );
    assert!(
        agent
            .accept_desired_state(desired(&agent_scope, 2, DesiredRunState::Paused))
            .is_err()
    );

    let another_scope = scope();
    assert!(
        agent
            .accept_desired_state(desired(&another_scope, 3, DesiredRunState::Paused))
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn state_file_cannot_be_reused_for_another_deployment() -> Result<(), AgentError> {
    let directory = test_directory()?;
    let path = directory.state_path();
    let original =
        EngineAgent::load(scope(), StateStore::new(&path), FakeAdapter::default()).await?;
    drop(original);

    let result = EngineAgent::load(scope(), StateStore::new(path), FakeAdapter::default()).await;
    assert!(matches!(result, Err(AgentError::InvalidState { .. })));
    Ok(())
}

#[tokio::test]
async fn operation_requires_matching_current_desired_state() -> Result<(), AgentError> {
    let directory = test_directory()?;
    let scope = scope();
    let mut agent = EngineAgent::load(
        scope.clone(),
        StateStore::new(directory.state_path()),
        FakeAdapter::default(),
    )
    .await?;
    agent.accept_desired_state(desired(&scope, 4, DesiredRunState::Paused))?;

    assert!(
        agent
            .execute_convergence(operation(&scope, 1, 3, DesiredRunState::Paused))
            .await
            .is_err()
    );
    assert!(
        agent
            .execute_convergence(operation(&scope, 1, 4, DesiredRunState::Running))
            .await
            .is_err()
    );
    assert!(agent.adapter().apply_calls.is_empty());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn group_readable_state_file_is_rejected() -> Result<(), AgentError> {
    use std::os::unix::fs::PermissionsExt;

    let directory = test_directory()?;
    let path = directory.state_path();
    let scope = scope();
    let agent = EngineAgent::load(
        scope.clone(),
        StateStore::new(&path),
        FakeAdapter::default(),
    )
    .await?;
    drop(agent);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).map_err(|error| {
        AgentError::StateIo {
            path: path.clone(),
            source: error,
        }
    })?;

    let result = EngineAgent::load(scope, StateStore::new(path), FakeAdapter::default()).await;
    assert!(matches!(result, Err(AgentError::InvalidState { .. })));
    Ok(())
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}
