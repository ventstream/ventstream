#![doc = "Managed-mode harness: an agent key attaches the engine to the control plane."]

mod config;
mod credential;
mod process;
mod runner;
mod telemetry;

pub use config::{
    AgentCertificateScope, DEFAULT_ENROLLMENT_URL, DEFAULT_GATEWAY_URL, ManagedOptions,
    ManagedPaths, ManagedRuntimeConfig,
};
pub use credential::{
    BootstrapCredential, CredentialMaterial, CredentialState, CredentialStore,
    EnrollmentCredentialRequest, PendingRenewal, RenewedCredential,
};
pub use process::{
    EngineProcessControl, ProcessEngineAdapter, SmokeEngineProcess, SupervisorEngineProcess,
    TokioEngineProcess,
};
pub use runner::run_managed;
pub use telemetry::EngineTelemetrySampler;
