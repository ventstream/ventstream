use std::path::PathBuf;

/// Failures produced by local management-state handling or the engine adapter.
#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    /// State is corrupt, insecure, or belongs to another deployment.
    #[error("management state at {path} is invalid: {message}")]
    InvalidState {
        /// State file or directory involved.
        path: PathBuf,
        /// Stable validation failure description.
        message: String,
    },
    /// Local state could not be read or durably written.
    #[error("management state I/O failed at {path}: {source}")]
    StateIo {
        /// State file or directory involved.
        path: PathBuf,
        /// Underlying filesystem failure.
        #[source]
        source: std::io::Error,
    },
    /// A control-plane instruction violated a local ordering invariant.
    #[error("invalid management instruction: {0}")]
    InvalidInstruction(String),
    /// The engine rejected a requested state transition.
    #[error("engine adapter failed: {0}")]
    Adapter(String),
    /// A remote control message violated the V1 protocol contract.
    #[error("agent protocol violation: {0}")]
    Protocol(String),
    /// Agent control transport configuration is malformed or unsafe.
    #[error("agent control transport configuration is invalid: {0}")]
    InvalidTransport(String),
    /// The remote control stream ended and may be retried.
    #[error("agent control stream is unavailable")]
    ControlUnavailable,
    /// The remote control stream rejected this agent or protocol.
    #[error("agent control stream was rejected: {0}")]
    ControlRejected(String),
}

impl AgentError {
    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::StateIo {
            path: path.into(),
            source,
        }
    }

    pub(crate) fn invalid_state(path: impl Into<PathBuf>, message: impl Into<String>) -> Self {
        Self::InvalidState {
            path: path.into(),
            message: message.into(),
        }
    }
}
