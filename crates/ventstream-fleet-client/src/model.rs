use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const STATE_SCHEMA_VERSION: u32 = 1;

/// Stable ownership identifiers bound into every local state file.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentScope {
    /// Pipeline managed by the local engine process.
    pub pipeline_id: Uuid,
    /// Concrete deployment managed by the local engine process.
    pub deployment_id: Uuid,
}

/// Runtime state requested by the control plane.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DesiredRunState {
    /// Process new source events.
    Running,
    /// Keep runtime resources but stop source consumption.
    Paused,
    /// Stop source consumption after in-flight work is drained.
    Drained,
}

/// Runtime state most recently observed by the local adapter.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservedRunState {
    /// A transition is being applied.
    Starting,
    /// Source events are being processed.
    Running,
    /// Source consumption is paused.
    Paused,
    /// In-flight work has drained and source consumption is stopped.
    Drained,
    /// The last transition failed.
    Error,
    /// The managed data path is stopped.
    Stopped,
}

/// Operation kinds the local runtime can durably execute.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentOperationKind {
    /// Converges observed lifecycle state to desired state.
    ConvergeState,
    /// Applies the selected configuration bundle locally.
    ApplyConfiguration,
    /// Reconciles source and sink state.
    Reconcile,
    /// Rebuilds sink state from the source.
    Rebootstrap,
}

const fn default_operation_kind() -> AgentOperationKind {
    AgentOperationKind::ConvergeState
}

impl From<DesiredRunState> for ObservedRunState {
    fn from(value: DesiredRunState) -> Self {
        match value {
            DesiredRunState::Running => Self::Running,
            DesiredRunState::Paused => Self::Paused,
            DesiredRunState::Drained => Self::Drained,
        }
    }
}

/// One immutable desired-state revision accepted from the control plane.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DesiredStateSnapshot {
    /// Pipeline to which the snapshot applies.
    pub pipeline_id: Uuid,
    /// Deployment to which the snapshot applies.
    pub deployment_id: Uuid,
    /// Monotonically increasing immutable revision.
    pub revision: u64,
    /// Complete requested run state.
    pub run_state: DesiredRunState,
    /// Selected configuration revision, when Fleet manages configuration.
    #[serde(default)]
    pub configuration_revision: Option<u64>,
}

/// Complete non-secret configuration bundle delivered for local apply.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigurationBundle {
    /// Pipeline-local configuration revision.
    pub revision: u64,
    /// Fleet configuration schema version.
    pub schema_version: u64,
    /// SHA-256 content digest of `document_json`.
    pub content_digest: String,
    /// Canonical non-secret configuration document.
    pub document_json: String,
}

/// An ordered operation asking the adapter to converge to an accepted revision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConvergenceOperation {
    /// Globally unique operation identifier.
    pub operation_id: Uuid,
    /// Pipeline to which the operation applies.
    pub pipeline_id: Uuid,
    /// Deployment to which the operation applies.
    pub deployment_id: Uuid,
    /// Monotonically increasing deployment-local sequence.
    pub sequence: u64,
    /// Desired-state revision that the operation converges.
    pub expected_desired_revision: u64,
    /// Operation kind selected by the control plane.
    pub kind: AgentOperationKind,
    /// Requested run state copied from the desired snapshot.
    pub run_state: DesiredRunState,
    /// Whether reconcile may delete sink-side orphans.
    pub delete_orphans: bool,
    /// Whether rebootstrap was explicitly confirmed upstream.
    pub destructive_action_confirmed: bool,
    /// Configuration bundle for apply operations.
    #[serde(default)]
    pub configuration: Option<ConfigurationBundle>,
}

/// Durable lifecycle state for an operation receipt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptState {
    /// The instruction was validated and durably accepted.
    Acknowledged,
    /// The adapter transition may be in progress or require recovery.
    Running,
    /// The requested transition completed successfully.
    Succeeded,
    /// The requested transition completed with a safe failure.
    Failed,
}

impl ReceiptState {
    pub(crate) fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }
}

/// Durable evidence of local operation processing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperationReceipt {
    /// Operation represented by this receipt.
    pub operation_id: Uuid,
    /// Deployment-local operation sequence.
    pub sequence: u64,
    /// Desired-state revision used during execution.
    pub expected_desired_revision: u64,
    /// Operation kind represented by the receipt.
    #[serde(default = "default_operation_kind")]
    pub kind: AgentOperationKind,
    /// Run state requested by the operation.
    pub run_state: DesiredRunState,
    /// Whether reconcile may delete sink-side orphans.
    #[serde(default)]
    pub delete_orphans: bool,
    /// Whether rebootstrap was explicitly confirmed upstream.
    #[serde(default)]
    pub destructive_action_confirmed: bool,
    /// Configuration revision applied by this receipt, when applicable.
    #[serde(default)]
    pub configuration_revision: Option<u64>,
    /// Current durable operation lifecycle state.
    pub state: ReceiptState,
    /// Optional bounded, non-sensitive result description.
    pub message: Option<String>,
    /// Time at which this receipt was last persisted.
    pub updated_at: DateTime<Utc>,
}

/// State persisted beside one engine deployment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManagementState {
    pub(crate) schema_version: u32,
    pub(crate) scope: AgentScope,
    pub(crate) desired: Option<DesiredStateSnapshot>,
    pub(crate) observed: ObservedRunState,
    #[serde(default)]
    pub(crate) applied_configuration: Option<ConfigurationBundle>,
    pub(crate) last_operation_sequence: u64,
    pub(crate) operation_receipts: Vec<OperationReceipt>,
}

impl ManagementState {
    pub(crate) fn new(scope: AgentScope) -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            scope,
            desired: None,
            observed: ObservedRunState::Stopped,
            applied_configuration: None,
            last_operation_sequence: 0,
            operation_receipts: Vec::new(),
        }
    }

    /// Returns the pipeline and deployment bound to this state.
    pub fn scope(&self) -> &AgentScope {
        &self.scope
    }

    /// Returns the latest accepted desired-state snapshot.
    pub fn desired(&self) -> Option<&DesiredStateSnapshot> {
        self.desired.as_ref()
    }

    /// Returns the last locally observed engine run state.
    pub fn observed(&self) -> ObservedRunState {
        self.observed
    }

    /// Returns the latest locally applied configuration revision.
    pub fn applied_configuration_revision(&self) -> Option<u64> {
        self.applied_configuration
            .as_ref()
            .map(|configuration| configuration.revision)
    }

    /// Returns the highest operation sequence accepted locally.
    pub fn last_operation_sequence(&self) -> u64 {
        self.last_operation_sequence
    }

    /// Returns the highest retained terminal operation sequence.
    pub fn last_completed_operation_sequence(&self) -> u64 {
        self.operation_receipts
            .iter()
            .filter(|receipt| receipt.state.is_terminal())
            .map(|receipt| receipt.sequence)
            .max()
            .unwrap_or(0)
    }

    /// Returns retained durable operation receipts.
    pub fn operation_receipts(&self) -> &[OperationReceipt] {
        &self.operation_receipts
    }
}

/// Result of validating and durably accepting a desired-state revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DesiredStateAcceptance {
    /// A newer revision was persisted.
    Accepted,
    /// The exact revision and content were already persisted.
    Duplicate,
    /// An older revision was safely ignored.
    Stale,
}

/// Result of processing a convergence operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationExecution {
    /// Terminal receipt to report to the control plane.
    pub receipt: OperationReceipt,
    /// Whether processing resumed or returned prior durable work.
    pub replayed: bool,
}
