#![doc = "Local management runtime embedded beside a VentStream engine instance."]
// Vendored from ventstream-fleet; style allows keep the upstream diff minimal.
#![allow(clippy::clone_on_ref_ptr, clippy::needless_pass_by_value)]

mod error;
mod model;
mod protocol;
mod runtime;
mod store;
mod transport;

pub use error::AgentError;
pub use model::{
    AgentOperationKind, AgentScope, ConfigurationBundle, ConvergenceOperation, DesiredRunState,
    DesiredStateAcceptance, DesiredStateSnapshot, ManagementState, ObservedRunState,
    OperationExecution, OperationReceipt, ReceiptState,
};
pub use protocol::{
    AgentDescriptor, AgentRuntimeStatus, ControlSession, ServerHelloData, ServerInstruction,
};
pub use runtime::{EngineAdapter, EngineAgent};
pub use store::StateStore;
pub use transport::{AgentControlEndpoint, ControlStreamRunner, ReconnectPolicy};

#[cfg(test)]
mod tests;
