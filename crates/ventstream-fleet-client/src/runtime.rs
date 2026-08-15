use std::future::Future;

use chrono::Utc;

use crate::error::AgentError;
use crate::model::{
    AgentOperationKind, AgentScope, ConfigurationBundle, ConvergenceOperation, DesiredRunState,
    DesiredStateAcceptance, DesiredStateSnapshot, ManagementState, ObservedRunState,
    OperationExecution, OperationReceipt, ReceiptState,
};
use crate::store::StateStore;

const MAX_OPERATION_RECEIPTS: usize = 1024;

/// Boundary implemented by the engine process or its local supervisor.
///
/// Implementations must make `apply_run_state` idempotent. The runtime persists a
/// running receipt before invoking it, so a process crash may require replaying the
/// same transition after restart.
pub trait EngineAdapter: Send {
    /// Applies the persisted startup gate before the managed data path may start.
    fn apply_startup_gate(
        &mut self,
        desired: Option<DesiredRunState>,
    ) -> impl Future<Output = Result<ObservedRunState, String>> + Send;

    /// Idempotently converges the local engine to the requested run state.
    fn apply_run_state(
        &mut self,
        desired: DesiredRunState,
    ) -> impl Future<Output = Result<ObservedRunState, String>> + Send;

    /// Idempotently reconciles local source/sink state without changing run state.
    fn reconcile(
        &mut self,
        delete_orphans: bool,
    ) -> impl Future<Output = Result<(), String>> + Send;

    /// Idempotently rebuilds local sink state from the source.
    fn rebootstrap(&mut self) -> impl Future<Output = Result<(), String>> + Send;

    /// Idempotently stages and activates a validated non-secret configuration bundle.
    fn apply_configuration(
        &mut self,
        configuration: &ConfigurationBundle,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

/// Durable local management runtime for one engine deployment.
pub struct EngineAgent<A> {
    store: StateStore,
    state: ManagementState,
    adapter: A,
}

impl<A: EngineAdapter> EngineAgent<A> {
    /// Loads private state and applies its startup gate before returning.
    pub async fn load(
        scope: AgentScope,
        store: StateStore,
        mut adapter: A,
    ) -> Result<Self, AgentError> {
        let mut state = store.load_or_initialize(&scope)?;
        let desired = state.desired.as_ref().map(|snapshot| snapshot.run_state);
        match adapter.apply_startup_gate(desired).await {
            Ok(observed) => {
                state.observed = observed;
                store.persist(&state)?;
                Ok(Self {
                    store,
                    state,
                    adapter,
                })
            }
            Err(message) => {
                state.observed = ObservedRunState::Error;
                store.persist(&state)?;
                Err(AgentError::Adapter(message))
            }
        }
    }

    /// Returns the current durable state.
    pub fn state(&self) -> &ManagementState {
        &self.state
    }

    /// Returns the engine adapter, primarily for local metrics and inspection.
    pub fn adapter(&self) -> &A {
        &self.adapter
    }

    /// Returns mutable access to the local engine adapter for process shutdown.
    pub fn adapter_mut(&mut self) -> &mut A {
        &mut self.adapter
    }

    /// Durably accepts a complete desired-state snapshot without applying it.
    pub fn accept_desired_state(
        &mut self,
        desired: DesiredStateSnapshot,
    ) -> Result<DesiredStateAcceptance, AgentError> {
        validate_desired_scope(&self.state.scope, &desired)?;
        if desired.revision == 0 {
            return Err(AgentError::InvalidInstruction(
                "desired-state revision must be greater than zero".to_owned(),
            ));
        }

        if let Some(current) = &self.state.desired {
            if desired.revision < current.revision {
                return Ok(DesiredStateAcceptance::Stale);
            }
            if desired.revision == current.revision {
                if &desired == current {
                    return Ok(DesiredStateAcceptance::Duplicate);
                }
                return Err(AgentError::InvalidInstruction(
                    "a desired-state revision cannot change content".to_owned(),
                ));
            }
        }

        self.state.desired = Some(desired);
        self.store.persist(&self.state)?;
        Ok(DesiredStateAcceptance::Accepted)
    }

    /// Executes or safely replays one ordered convergence operation.
    pub async fn execute_convergence(
        &mut self,
        operation: ConvergenceOperation,
    ) -> Result<OperationExecution, AgentError> {
        self.execute_convergence_with_updates(operation, |_, _| {})
            .await
    }

    /// Executes convergence and reports state plus receipt only after both are durable.
    ///
    /// The callback must be nonblocking. A transport implementation should use a
    /// bounded `try_send`; the next hello carries the durable completion cursor so
    /// reconnect cannot repeat a completed side effect.
    pub async fn execute_convergence_with_updates<F>(
        &mut self,
        operation: ConvergenceOperation,
        mut on_persisted_update: F,
    ) -> Result<OperationExecution, AgentError>
    where
        F: FnMut(&ManagementState, &OperationReceipt),
    {
        validate_operation_scope(&self.state.scope, &operation)?;

        if let Some(existing) = self
            .state
            .operation_receipts
            .iter()
            .find(|receipt| receipt.operation_id == operation.operation_id)
            .cloned()
        {
            validate_matching_receipt(&operation, &existing)?;
            if existing.state.is_terminal() {
                on_persisted_update(&self.state, &existing);
                return Ok(OperationExecution {
                    receipt: existing,
                    replayed: true,
                });
            }
            validate_operation_desired(&self.state, &operation)?;
            on_persisted_update(&self.state, &existing);
            return self
                .apply_operation(operation, true, &mut on_persisted_update)
                .await;
        }

        validate_operation_desired(&self.state, &operation)?;
        if operation.sequence <= self.state.last_operation_sequence {
            return Err(AgentError::InvalidInstruction(
                "operation sequence is stale or its receipt is unavailable".to_owned(),
            ));
        }
        make_receipt_capacity(&mut self.state.operation_receipts)?;
        self.state.last_operation_sequence = operation.sequence;
        self.state.operation_receipts.push(OperationReceipt {
            operation_id: operation.operation_id,
            sequence: operation.sequence,
            expected_desired_revision: operation.expected_desired_revision,
            kind: operation.kind,
            run_state: operation.run_state,
            delete_orphans: operation.delete_orphans,
            destructive_action_confirmed: operation.destructive_action_confirmed,
            configuration_revision: operation
                .configuration
                .as_ref()
                .map(|configuration| configuration.revision),
            state: ReceiptState::Acknowledged,
            message: None,
            updated_at: Utc::now(),
        });
        self.store.persist(&self.state)?;
        let acknowledged = self
            .receipt(operation.operation_id)
            .ok_or_else(|| AgentError::invalid_state(self.store.path(), "receipt disappeared"))?;
        on_persisted_update(&self.state, acknowledged);
        self.apply_operation(operation, false, &mut on_persisted_update)
            .await
    }

    async fn apply_operation<F>(
        &mut self,
        operation: ConvergenceOperation,
        replayed: bool,
        on_persisted_update: &mut F,
    ) -> Result<OperationExecution, AgentError>
    where
        F: FnMut(&ManagementState, &OperationReceipt),
    {
        self.update_receipt(operation.operation_id, ReceiptState::Running, None)?;
        if operation.kind == AgentOperationKind::ConvergeState {
            self.state.observed = ObservedRunState::Starting;
        }
        self.store.persist(&self.state)?;
        let running = self
            .receipt(operation.operation_id)
            .ok_or_else(|| AgentError::invalid_state(self.store.path(), "receipt disappeared"))?;
        on_persisted_update(&self.state, running);

        match operation.kind {
            AgentOperationKind::ConvergeState => {
                match self.adapter.apply_run_state(operation.run_state).await {
                    Ok(observed) if observed == ObservedRunState::from(operation.run_state) => {
                        self.state.observed = observed;
                        self.update_receipt(operation.operation_id, ReceiptState::Succeeded, None)?;
                    }
                    Ok(_) | Err(_) => {
                        self.state.observed = ObservedRunState::Error;
                        self.update_receipt(
                            operation.operation_id,
                            ReceiptState::Failed,
                            Some("engine state transition failed".to_owned()),
                        )?;
                    }
                }
            }
            AgentOperationKind::ApplyConfiguration => {
                let Some(configuration) = operation.configuration.as_ref() else {
                    self.update_receipt(
                        operation.operation_id,
                        ReceiptState::Failed,
                        Some("configuration bundle is missing".to_owned()),
                    )?;
                    self.store.persist(&self.state)?;
                    let receipt =
                        self.receipt(operation.operation_id)
                            .cloned()
                            .ok_or_else(|| {
                                AgentError::invalid_state(self.store.path(), "receipt disappeared")
                            })?;
                    on_persisted_update(&self.state, &receipt);
                    return Ok(OperationExecution { receipt, replayed });
                };
                if let Err(message) = self.adapter.apply_configuration(configuration).await {
                    self.state.observed = ObservedRunState::Error;
                    self.update_receipt(
                        operation.operation_id,
                        ReceiptState::Failed,
                        Some(safe_adapter_message(message)),
                    )?;
                } else {
                    self.state.applied_configuration = Some(configuration.clone());
                    self.update_receipt(operation.operation_id, ReceiptState::Succeeded, None)?;
                }
            }
            AgentOperationKind::Reconcile => {
                if let Err(message) = self.adapter.reconcile(operation.delete_orphans).await {
                    self.state.observed = ObservedRunState::Error;
                    self.update_receipt(
                        operation.operation_id,
                        ReceiptState::Failed,
                        Some(safe_adapter_message(message)),
                    )?;
                } else {
                    self.update_receipt(operation.operation_id, ReceiptState::Succeeded, None)?;
                }
            }
            AgentOperationKind::Rebootstrap => {
                if !operation.destructive_action_confirmed {
                    self.update_receipt(
                        operation.operation_id,
                        ReceiptState::Failed,
                        Some("rebootstrap confirmation is missing".to_owned()),
                    )?;
                } else if let Err(message) = self.adapter.rebootstrap().await {
                    self.state.observed = ObservedRunState::Error;
                    self.update_receipt(
                        operation.operation_id,
                        ReceiptState::Failed,
                        Some(safe_adapter_message(message)),
                    )?;
                } else {
                    self.update_receipt(operation.operation_id, ReceiptState::Succeeded, None)?;
                }
            }
        }
        self.store.persist(&self.state)?;
        let receipt = self
            .receipt(operation.operation_id)
            .cloned()
            .ok_or_else(|| AgentError::invalid_state(self.store.path(), "receipt disappeared"))?;
        on_persisted_update(&self.state, &receipt);
        Ok(OperationExecution { receipt, replayed })
    }

    fn update_receipt(
        &mut self,
        operation_id: uuid::Uuid,
        state: ReceiptState,
        message: Option<String>,
    ) -> Result<(), AgentError> {
        let receipt = self
            .state
            .operation_receipts
            .iter_mut()
            .find(|receipt| receipt.operation_id == operation_id)
            .ok_or_else(|| AgentError::invalid_state(self.store.path(), "receipt disappeared"))?;
        receipt.state = state;
        receipt.message = message;
        receipt.updated_at = Utc::now();
        Ok(())
    }

    fn receipt(&self, operation_id: uuid::Uuid) -> Option<&OperationReceipt> {
        self.state
            .operation_receipts
            .iter()
            .find(|receipt| receipt.operation_id == operation_id)
    }
}

fn safe_adapter_message(message: String) -> String {
    if message.trim() == message
        && !message.is_empty()
        && message.chars().count() <= 500
        && !message.chars().any(char::is_control)
    {
        message
    } else {
        "engine operation failed".to_owned()
    }
}

fn validate_desired_scope(
    scope: &AgentScope,
    desired: &DesiredStateSnapshot,
) -> Result<(), AgentError> {
    if desired.pipeline_id != scope.pipeline_id || desired.deployment_id != scope.deployment_id {
        return Err(AgentError::InvalidInstruction(
            "desired state is scoped to another pipeline or deployment".to_owned(),
        ));
    }
    Ok(())
}

fn validate_operation_scope(
    scope: &AgentScope,
    operation: &ConvergenceOperation,
) -> Result<(), AgentError> {
    if operation.pipeline_id != scope.pipeline_id || operation.deployment_id != scope.deployment_id
    {
        return Err(AgentError::InvalidInstruction(
            "operation is scoped to another pipeline or deployment".to_owned(),
        ));
    }
    if operation.sequence == 0 {
        return Err(AgentError::InvalidInstruction(
            "operation sequence must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

fn validate_operation_desired(
    state: &ManagementState,
    operation: &ConvergenceOperation,
) -> Result<(), AgentError> {
    let desired = state.desired.as_ref().ok_or_else(|| {
        AgentError::InvalidInstruction("operation arrived before desired state".to_owned())
    })?;
    if desired.revision != operation.expected_desired_revision
        || desired.run_state != operation.run_state
        || desired.configuration_revision
            != operation
                .configuration
                .as_ref()
                .map(|configuration| configuration.revision)
                .or(desired.configuration_revision)
    {
        return Err(AgentError::InvalidInstruction(
            "operation does not match the current desired-state snapshot".to_owned(),
        ));
    }
    Ok(())
}

fn validate_matching_receipt(
    operation: &ConvergenceOperation,
    receipt: &OperationReceipt,
) -> Result<(), AgentError> {
    if receipt.sequence != operation.sequence
        || receipt.expected_desired_revision != operation.expected_desired_revision
        || receipt.kind != operation.kind
        || receipt.run_state != operation.run_state
        || receipt.delete_orphans != operation.delete_orphans
        || receipt.destructive_action_confirmed != operation.destructive_action_confirmed
        || receipt.configuration_revision
            != operation
                .configuration
                .as_ref()
                .map(|configuration| configuration.revision)
    {
        return Err(AgentError::InvalidInstruction(
            "operation ID was reused with different content".to_owned(),
        ));
    }
    Ok(())
}

fn make_receipt_capacity(receipts: &mut Vec<OperationReceipt>) -> Result<(), AgentError> {
    while receipts.len() >= MAX_OPERATION_RECEIPTS {
        let position = receipts
            .iter()
            .position(|receipt| receipt.state.is_terminal())
            .ok_or_else(|| {
                AgentError::InvalidInstruction(
                    "operation receipt capacity is exhausted by nonterminal work".to_owned(),
                )
            })?;
        receipts.remove(position);
    }
    Ok(())
}
