use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use rand::Rng;
use rustls_pki_types::pem::PemObject;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use uuid::Uuid;
use ventstream_fleet_protocol::v1::AgentToServer;
use ventstream_fleet_protocol::v1::agent_control_service_client::AgentControlServiceClient;
use zeroize::Zeroizing;

use crate::{
    AgentDescriptor, AgentError, AgentRuntimeStatus, AgentScope, ControlSession, EngineAdapter,
    EngineAgent, ManagementState, OperationReceipt, ServerInstruction,
};

type RuntimeStatusProvider = Arc<dyn Fn() -> AgentRuntimeStatus + Send + Sync>;

const MAX_ENDPOINT_BYTES: usize = 2048;
const MAX_CERTIFICATE_CHAIN_BYTES: usize = 256 * 1024;
const MAX_PRIVATE_KEY_BYTES: usize = 64 * 1024;
const MAX_TRUST_BUNDLE_BYTES: usize = 256 * 1024;
const OUTBOUND_QUEUE_CAPACITY: usize = 64;
const MAX_GRPC_MESSAGE_BYTES: usize = 256 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Validated HTTPS/mTLS endpoint used by the agent control stream.
#[derive(Clone)]
pub struct AgentControlEndpoint {
    endpoint: Endpoint,
    authority: String,
}

impl AgentControlEndpoint {
    /// Validates endpoint and PEM material, then builds a pinned mTLS endpoint.
    pub fn from_pem(
        endpoint_url: impl Into<String>,
        certificate_chain_pem: Vec<u8>,
        private_key_pem: Vec<u8>,
        trust_bundle_pem: Vec<u8>,
    ) -> Result<Self, AgentError> {
        install_crypto_provider();
        let endpoint_url = endpoint_url.into();
        let private_key_pem = Zeroizing::new(private_key_pem);
        validate_client_identity(&certificate_chain_pem, &private_key_pem)?;
        validate_server_trust(&trust_bundle_pem)?;

        if endpoint_url.is_empty() || endpoint_url.len() > MAX_ENDPOINT_BYTES {
            return Err(invalid_transport("endpoint URL is empty or too large"));
        }
        let endpoint = Endpoint::from_shared(endpoint_url)
            .map_err(|_| invalid_transport("endpoint URL is malformed"))?;
        let uri = endpoint.uri();
        let authority = uri
            .authority()
            .map(|value| value.as_str().to_owned())
            .ok_or_else(|| invalid_transport("endpoint authority is required"))?;
        if uri.scheme_str() != Some("https")
            || uri.host().is_none()
            || authority.contains('@')
            || !matches!(uri.path(), "" | "/")
            || uri.query().is_some()
        {
            return Err(invalid_transport(
                "endpoint must be an HTTPS origin without credentials, path, or query",
            ));
        }

        let identity = Identity::from_pem(&certificate_chain_pem, &private_key_pem);
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(trust_bundle_pem))
            .identity(identity)
            .timeout(CONNECT_TIMEOUT);
        let endpoint = endpoint
            .connect_timeout(CONNECT_TIMEOUT)
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .http2_keep_alive_interval(Duration::from_secs(15))
            .keep_alive_timeout(Duration::from_secs(10))
            .keep_alive_while_idle(true)
            .buffer_size(OUTBOUND_QUEUE_CAPACITY)
            .tls_config(tls)
            .map_err(|_| invalid_transport("mTLS endpoint configuration failed"))?;
        Ok(Self {
            endpoint,
            authority,
        })
    }

    /// Opens a tonic channel using this pinned mTLS endpoint.
    pub async fn connect_channel(&self) -> Result<Channel, AgentError> {
        self.endpoint
            .connect()
            .await
            .map_err(|_| AgentError::ControlUnavailable)
    }
}

impl fmt::Debug for AgentControlEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentControlEndpoint")
            .field("authority", &self.authority)
            .field("client_identity", &"[REDACTED]")
            .finish()
    }
}

/// Bounded exponential reconnect behavior for control-plane outages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconnectPolicy {
    initial_delay: Duration,
    maximum_delay: Duration,
    stable_session: Duration,
}

impl ReconnectPolicy {
    /// Creates a reconnect policy with 20 percent delay jitter.
    pub fn new(
        initial_delay: Duration,
        maximum_delay: Duration,
        stable_session: Duration,
    ) -> Result<Self, AgentError> {
        if initial_delay < Duration::from_millis(100)
            || maximum_delay < initial_delay
            || maximum_delay > Duration::from_secs(300)
            || stable_session < Duration::from_secs(10)
            || stable_session > Duration::from_secs(600)
        {
            return Err(invalid_transport("reconnect policy is outside safe bounds"));
        }
        Ok(Self {
            initial_delay,
            maximum_delay,
            stable_session,
        })
    }

    fn jittered_delay(self, attempt: u32, jitter_percent: u32) -> Duration {
        let multiplier = 1_u32.checked_shl(attempt.min(31)).unwrap_or(u32::MAX);
        let base = self
            .initial_delay
            .saturating_mul(multiplier)
            .min(self.maximum_delay);
        base.mul_f64(f64::from(jitter_percent.clamp(80, 120)) / 100.0)
            .min(self.maximum_delay)
    }
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            maximum_delay: Duration::from_secs(60),
            stable_session: Duration::from_secs(60),
        }
    }
}

/// Reconnecting owner of one deployment's authenticated control stream.
pub struct ControlStreamRunner<A> {
    _endpoint_tx: Option<watch::Sender<AgentControlEndpoint>>,
    endpoint_rx: watch::Receiver<AgentControlEndpoint>,
    descriptor: AgentDescriptor,
    scope: AgentScope,
    agent: EngineAgent<A>,
    reconnect: ReconnectPolicy,
    runtime_status: RuntimeStatusProvider,
}

impl<A: EngineAdapter> ControlStreamRunner<A> {
    /// Creates a runner around an already startup-gated local agent.
    pub fn new(
        endpoint: AgentControlEndpoint,
        descriptor: AgentDescriptor,
        scope: AgentScope,
        agent: EngineAgent<A>,
        reconnect: ReconnectPolicy,
    ) -> Result<Self, AgentError> {
        let (endpoint_tx, endpoint_rx) = watch::channel(endpoint);
        let mut runner =
            Self::with_endpoint_updates(endpoint_rx, descriptor, scope, agent, reconnect)?;
        runner._endpoint_tx = Some(endpoint_tx);
        Ok(runner)
    }

    /// Creates a runner whose control endpoint can be replaced by a renewal task.
    pub fn with_endpoint_updates(
        endpoint_rx: watch::Receiver<AgentControlEndpoint>,
        descriptor: AgentDescriptor,
        scope: AgentScope,
        agent: EngineAgent<A>,
        reconnect: ReconnectPolicy,
    ) -> Result<Self, AgentError> {
        if agent.state().scope() != &scope {
            return Err(invalid_transport(
                "runner scope does not match the local management state",
            ));
        }
        Ok(Self {
            _endpoint_tx: None,
            endpoint_rx,
            descriptor,
            scope,
            agent,
            reconnect,
            runtime_status: Arc::new(AgentRuntimeStatus::default),
        })
    }

    /// Supplies fresh local engine telemetry for authenticated heartbeats.
    #[must_use]
    pub fn with_runtime_status_provider(
        mut self,
        provider: Arc<dyn Fn() -> AgentRuntimeStatus + Send + Sync>,
    ) -> Self {
        self.runtime_status = provider;
        self
    }

    /// Returns the startup-gated local runtime owned by this runner.
    pub fn agent(&self) -> &EngineAgent<A> {
        &self.agent
    }

    /// Returns mutable access to the local runtime after the runner stops.
    pub fn agent_mut(&mut self) -> &mut EngineAgent<A> {
        &mut self.agent
    }

    /// Reconnects through retryable outages until shutdown or a fatal rejection.
    pub async fn run_until_shutdown<F>(&mut self, shutdown: F) -> Result<(), AgentError>
    where
        F: Future<Output = ()> + Send,
    {
        tokio::pin!(shutdown);
        let mut attempt = 0_u32;
        loop {
            let started = Instant::now();
            let result = tokio::select! {
                () = &mut shutdown => return Ok(()),
                result = self.run_session() => result,
            };
            match result {
                Ok(()) | Err(AgentError::ControlUnavailable) => {}
                Err(error) => return Err(error),
            }

            if started.elapsed() >= self.reconnect.stable_session {
                attempt = 0;
            }
            let jitter = rand::thread_rng().gen_range(80..=120);
            let delay = self.reconnect.jittered_delay(attempt, jitter);
            tracing::warn!(?delay, attempt, "agent control stream reconnect scheduled");
            attempt = attempt.saturating_add(1);
            tokio::select! {
                () = &mut shutdown => return Ok(()),
                () = tokio::time::sleep(delay) => {}
            }
        }
    }

    async fn run_session(&mut self) -> Result<(), AgentError> {
        let mut endpoint_rx = self.endpoint_rx.clone();
        let endpoint = endpoint_rx.borrow_and_update().clone();
        let channel = endpoint.connect_channel().await?;
        let mut client = AgentControlServiceClient::new(channel)
            .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES);
        let mut session = ControlSession::new(self.descriptor.clone());
        let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
        let hello = session.agent_hello(self.agent.state(), Utc::now())?;
        outbound_tx
            .try_send(hello)
            .map_err(|_| AgentError::ControlUnavailable)?;

        let response = client
            .open_control_stream(ReceiverStream::new(outbound_rx))
            .await
            .map_err(classify_status)?;
        let mut inbound = response.into_inner();
        let first = tokio::time::timeout(HANDSHAKE_TIMEOUT, inbound.message())
            .await
            .map_err(|_| AgentError::ControlUnavailable)?
            .map_err(classify_status)?
            .ok_or(AgentError::ControlUnavailable)?;
        let instruction = session.accept(
            &first,
            &self.scope,
            self.agent.state().desired(),
            Utc::now(),
        )?;
        let ServerInstruction::Hello(server_hello) = instruction else {
            return Err(AgentError::Protocol(
                "first server instruction is not a hello".to_owned(),
            ));
        };
        if let Some(desired) = server_hello.desired_state {
            self.agent.accept_desired_state(desired)?;
        }

        let shared_session = Arc::new(Mutex::new(session));
        enqueue_status(
            &shared_session,
            &outbound_tx,
            self.agent.state(),
            None,
            &(self.runtime_status)(),
        )?;
        let initial_heartbeat = HeartbeatState {
            state: self.agent.state().clone(),
            active_operation_id: None,
        };
        let (heartbeat_state_tx, heartbeat_state_rx) = watch::channel(initial_heartbeat);
        let (failure_tx, mut failure_rx) = watch::channel(false);
        let heartbeat = HeartbeatTask(tokio::spawn(heartbeat_loop(
            server_hello.heartbeat_interval,
            shared_session.clone(),
            outbound_tx.clone(),
            heartbeat_state_rx,
            failure_tx,
            Arc::clone(&self.runtime_status),
        )));

        let result = loop {
            tokio::select! {
                changed = failure_rx.changed() => {
                    if changed.is_err() || *failure_rx.borrow() {
                        break Err(AgentError::ControlUnavailable);
                    }
                }
                changed = endpoint_rx.changed() => {
                    if changed.is_err() {
                        break Err(AgentError::ControlUnavailable);
                    }
                    tracing::info!("agent control endpoint updated; reconnecting with replacement credentials");
                    break Err(AgentError::ControlUnavailable);
                }
                message = inbound.message() => {
                    let message = match message {
                        Ok(Some(message)) => message,
                        Ok(None) => break Err(AgentError::ControlUnavailable),
                        Err(status) => break Err(classify_status(status)),
                    };
                    let instruction = {
                        let mut guard = shared_session
                            .lock()
                            .map_err(|_| AgentError::ControlUnavailable)?;
                        guard.accept(
                            &message,
                            &self.scope,
                            self.agent.state().desired(),
                            Utc::now(),
                        )?
                    };
                    self.apply_instruction(
                        instruction,
                        &shared_session,
                        &outbound_tx,
                        &heartbeat_state_tx,
                    )
                    .await?;
                }
            }
        };
        drop(heartbeat);
        result
    }

    async fn apply_instruction(
        &mut self,
        instruction: ServerInstruction,
        session: &Arc<Mutex<ControlSession>>,
        outbound: &mpsc::Sender<AgentToServer>,
        heartbeat_state: &watch::Sender<HeartbeatState>,
    ) -> Result<(), AgentError> {
        match instruction {
            ServerInstruction::Hello(_) => {
                Err(AgentError::Protocol("server hello was repeated".to_owned()))
            }
            ServerInstruction::DesiredState(desired) => {
                self.agent.accept_desired_state(desired)?;
                heartbeat_state.send_replace(HeartbeatState {
                    state: self.agent.state().clone(),
                    active_operation_id: None,
                });
                Ok(())
            }
            ServerInstruction::Converge(operation) => {
                let operation_id = operation.operation_id;
                let mut report_error = None;
                let runtime_status = Arc::clone(&self.runtime_status);
                self.agent
                    .execute_convergence_with_updates(operation, |state, receipt| {
                        let active_operation_id = match receipt.state {
                            crate::ReceiptState::Acknowledged | crate::ReceiptState::Running => {
                                Some(operation_id)
                            }
                            crate::ReceiptState::Succeeded | crate::ReceiptState::Failed => None,
                        };
                        heartbeat_state.send_replace(HeartbeatState {
                            state: state.clone(),
                            active_operation_id,
                        });
                        if report_error.is_none()
                            && enqueue_operation_update(session, outbound, receipt).is_err()
                        {
                            report_error = Some(AgentError::ControlUnavailable);
                        }
                        if report_error.is_none()
                            && enqueue_status(
                                session,
                                outbound,
                                state,
                                active_operation_id,
                                &runtime_status(),
                            )
                            .is_err()
                        {
                            report_error = Some(AgentError::ControlUnavailable);
                        }
                    })
                    .await?;
                heartbeat_state.send_replace(HeartbeatState {
                    state: self.agent.state().clone(),
                    active_operation_id: None,
                });
                report_error.map_or(Ok(()), Err)
            }
        }
    }
}

#[derive(Clone)]
struct HeartbeatState {
    state: ManagementState,
    active_operation_id: Option<Uuid>,
}

struct HeartbeatTask(tokio::task::JoinHandle<()>);

impl Drop for HeartbeatTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn heartbeat_loop(
    cadence: Duration,
    session: Arc<Mutex<ControlSession>>,
    outbound: mpsc::Sender<AgentToServer>,
    state: watch::Receiver<HeartbeatState>,
    failure: watch::Sender<bool>,
    runtime_status: RuntimeStatusProvider,
) {
    let start = tokio::time::Instant::now() + cadence;
    let mut interval = tokio::time::interval_at(start, cadence);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let snapshot = state.borrow().clone();
        if enqueue_status(
            &session,
            &outbound,
            &snapshot.state,
            snapshot.active_operation_id,
            &runtime_status(),
        )
        .is_err()
        {
            failure.send_replace(true);
            return;
        }
    }
}

fn enqueue_status(
    session: &Arc<Mutex<ControlSession>>,
    outbound: &mpsc::Sender<AgentToServer>,
    state: &ManagementState,
    active_operation_id: Option<Uuid>,
    runtime: &AgentRuntimeStatus,
) -> Result<(), AgentError> {
    let mut guard = session.lock().map_err(|_| AgentError::ControlUnavailable)?;
    let message =
        guard.agent_status_with_runtime(state, active_operation_id, runtime, Utc::now())?;
    outbound
        .try_send(message)
        .map_err(|_| AgentError::ControlUnavailable)
}

fn enqueue_operation_update(
    session: &Arc<Mutex<ControlSession>>,
    outbound: &mpsc::Sender<AgentToServer>,
    receipt: &OperationReceipt,
) -> Result<(), AgentError> {
    let mut guard = session.lock().map_err(|_| AgentError::ControlUnavailable)?;
    let message = guard.operation_update(receipt, Utc::now())?;
    outbound
        .try_send(message)
        .map_err(|_| AgentError::ControlUnavailable)
}

fn classify_status(status: tonic::Status) -> AgentError {
    use tonic::Code;

    match status.code() {
        Code::Unauthenticated => AgentError::ControlRejected("authentication failed".to_owned()),
        Code::PermissionDenied => AgentError::ControlRejected("authorization failed".to_owned()),
        Code::InvalidArgument | Code::FailedPrecondition | Code::Unimplemented | Code::DataLoss => {
            AgentError::ControlRejected("protocol compatibility failed".to_owned())
        }
        _ => AgentError::ControlUnavailable,
    }
}

fn validate_client_identity(
    certificate_chain: &[u8],
    private_key: &[u8],
) -> Result<(), AgentError> {
    if certificate_chain.is_empty()
        || certificate_chain.len() > MAX_CERTIFICATE_CHAIN_BYTES
        || private_key.is_empty()
        || private_key.len() > MAX_PRIVATE_KEY_BYTES
    {
        return Err(invalid_transport(
            "client identity material has an invalid size",
        ));
    }
    let sections =
        <(rustls_pki_types::pem::SectionKind, Vec<u8>)>::pem_slice_iter(certificate_chain)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| invalid_transport("client certificate chain is malformed"))?;
    let certificates = rustls_pki_types::CertificateDer::pem_slice_iter(certificate_chain)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| invalid_transport("client certificate chain is malformed"))?;
    if certificates.is_empty()
        || sections
            .iter()
            .any(|(kind, _)| *kind != rustls_pki_types::pem::SectionKind::Certificate)
    {
        return Err(invalid_transport("client certificate chain is invalid"));
    }
    for (position, certificate) in certificates.iter().enumerate() {
        let (remainder, parsed) = x509_parser::parse_x509_certificate(certificate)
            .map_err(|_| invalid_transport("client certificate chain is invalid"))?;
        if !remainder.is_empty()
            || !parsed.validity().is_valid()
            || (position > 0 && !parsed.is_ca())
        {
            return Err(invalid_transport("client certificate chain is invalid"));
        }
    }
    let (remainder, leaf) = x509_parser::parse_x509_certificate(
        certificates
            .first()
            .ok_or_else(|| invalid_transport("client leaf certificate is missing"))?,
    )
    .map_err(|_| invalid_transport("client leaf certificate is invalid"))?;
    let permits_client_auth = leaf
        .extended_key_usage()
        .ok()
        .flatten()
        .is_some_and(|usage| usage.value.client_auth);
    if !remainder.is_empty() || leaf.is_ca() || !permits_client_auth {
        return Err(invalid_transport(
            "client leaf certificate is invalid for current client authentication",
        ));
    }

    let keys = rustls_pki_types::PrivateKeyDer::pem_slice_iter(private_key)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| invalid_transport("client private key is malformed"))?;
    if keys.len() != 1 {
        return Err(invalid_transport(
            "client identity must contain exactly one private key",
        ));
    }
    Ok(())
}

fn validate_server_trust(trust_bundle: &[u8]) -> Result<(), AgentError> {
    if trust_bundle.is_empty() || trust_bundle.len() > MAX_TRUST_BUNDLE_BYTES {
        return Err(invalid_transport("server trust bundle has an invalid size"));
    }
    let sections = <(rustls_pki_types::pem::SectionKind, Vec<u8>)>::pem_slice_iter(trust_bundle)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| invalid_transport("server trust bundle is malformed"))?;
    if sections.is_empty()
        || sections.iter().any(|(kind, certificate)| {
            *kind != rustls_pki_types::pem::SectionKind::Certificate
                || x509_parser::parse_x509_certificate(certificate).map_or(true, |(rest, cert)| {
                    !rest.is_empty() || !cert.is_ca() || !cert.validity().is_valid()
                })
        })
    {
        return Err(invalid_transport(
            "server trust bundle must contain only currently valid CA certificates",
        ));
    }
    Ok(())
}

fn invalid_transport(message: impl Into<String>) -> AgentError {
    AgentError::InvalidTransport(message.into())
}

fn install_crypto_provider() {
    drop(rustls::crypto::ring::default_provider().install_default());
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::{Duration as ChronoDuration, Utc};
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    };
    use tokio::sync::{Mutex, mpsc};
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
    use tonic::{Request, Response, Status, Streaming};
    use ventstream_fleet_protocol::v1::agent_control_service_server::{
        AgentControlService, AgentControlServiceServer,
    };
    use ventstream_fleet_protocol::v1::agent_to_server::Payload as AgentPayload;
    use ventstream_fleet_protocol::v1::server_to_agent::Payload as ServerPayload;
    use ventstream_fleet_protocol::v1::{
        AgentToServer, DesiredRunState as ProtoDesiredRunState, DesiredState, Operation,
        OperationKind, OperationState, ServerHello, ServerToAgent,
    };

    use uuid::Uuid;

    use crate::{
        AgentDescriptor, AgentError, AgentScope, ConfigurationBundle, DesiredRunState,
        EngineAdapter, EngineAgent, ObservedRunState, StateStore,
    };

    use super::{AgentControlEndpoint, ControlStreamRunner, ReconnectPolicy};

    struct TestIdentity {
        chain: Vec<u8>,
        key: Vec<u8>,
        root: Vec<u8>,
        leaf: Vec<u8>,
        server_chain: Vec<u8>,
        server_key: Vec<u8>,
    }

    fn identity() -> Result<TestIdentity, Box<dyn std::error::Error>> {
        let root_key = KeyPair::generate()?;
        let mut root_parameters = CertificateParams::new(Vec::new())?;
        root_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let root = root_parameters.self_signed(&root_key)?;
        let issuer = Issuer::new(root_parameters, root_key);

        let leaf_key = KeyPair::generate()?;
        let mut leaf_parameters = CertificateParams::new(Vec::new())?;
        leaf_parameters
            .extended_key_usages
            .push(ExtendedKeyUsagePurpose::ClientAuth);
        let leaf = leaf_parameters.signed_by(&leaf_key, &issuer)?;

        let server_key = KeyPair::generate()?;
        let mut server_parameters = CertificateParams::new(vec!["localhost".to_owned()])?;
        server_parameters
            .extended_key_usages
            .push(ExtendedKeyUsagePurpose::ServerAuth);
        let server = server_parameters.signed_by(&server_key, &issuer)?;
        Ok(TestIdentity {
            chain: format!("{}{}", leaf.pem(), root.pem()).into_bytes(),
            key: leaf_key.serialize_pem().into_bytes(),
            root: root.pem().into_bytes(),
            leaf: leaf.pem().into_bytes(),
            server_chain: format!("{}{}", server.pem(), root.pem()).into_bytes(),
            server_key: server_key.serialize_pem().into_bytes(),
        })
    }

    fn timestamp(value: chrono::DateTime<Utc>) -> prost_types::Timestamp {
        prost_types::Timestamp {
            seconds: value.timestamp(),
            nanos: i32::try_from(value.timestamp_subsec_nanos()).unwrap_or_default(),
        }
    }

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Result<Self, std::io::Error> {
            let path = std::env::temp_dir().join(format!("fleet-runner-{}", Uuid::now_v7()));
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

    #[derive(Default)]
    struct TestAdapter;

    impl EngineAdapter for TestAdapter {
        async fn apply_startup_gate(
            &mut self,
            desired: Option<DesiredRunState>,
        ) -> Result<ObservedRunState, String> {
            Ok(desired.map_or(ObservedRunState::Stopped, ObservedRunState::from))
        }

        async fn apply_run_state(
            &mut self,
            desired: DesiredRunState,
        ) -> Result<ObservedRunState, String> {
            Ok(desired.into())
        }

        async fn reconcile(&mut self, _delete_orphans: bool) -> Result<(), String> {
            Ok(())
        }

        async fn rebootstrap(&mut self) -> Result<(), String> {
            Ok(())
        }

        async fn apply_configuration(
            &mut self,
            _configuration: &ConfigurationBundle,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct ScenarioControlService {
        scope: AgentScope,
        received: Arc<Mutex<Vec<AgentToServer>>>,
    }

    #[tonic::async_trait]
    impl AgentControlService for ScenarioControlService {
        type OpenControlStreamStream = ReceiverStream<Result<ServerToAgent, Status>>;

        async fn open_control_stream(
            &self,
            request: Request<Streaming<AgentToServer>>,
        ) -> Result<Response<Self::OpenControlStreamStream>, Status> {
            let mut inbound = request.into_inner();
            let hello = inbound
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("hello required"))?;
            if !matches!(hello.payload, Some(AgentPayload::Hello(_))) {
                return Err(Status::invalid_argument("hello required"));
            }
            self.received.lock().await.push(hello.clone());

            let connection_id = Uuid::now_v7();
            let operation_id = Uuid::now_v7();
            let desired = DesiredState {
                pipeline_id: self.scope.pipeline_id.to_string(),
                deployment_id: self.scope.deployment_id.to_string(),
                revision: 1,
                run_state: ProtoDesiredRunState::Paused.into(),
                configuration_revision: 0,
                changed_at: Some(timestamp(Utc::now())),
                changed_by: "operator:test".to_owned(),
                reason: "pause for test".to_owned(),
            };
            let envelopes = vec![
                ServerToAgent {
                    sequence: 1,
                    sent_at: Some(timestamp(Utc::now())),
                    correlation_id: hello.correlation_id,
                    connection_id: connection_id.to_string(),
                    protocol_major: 1,
                    protocol_minor: 0,
                    payload: Some(ServerPayload::Hello(ServerHello {
                        protocol_major: 1,
                        protocol_minor: 0,
                        server_time: Some(timestamp(Utc::now())),
                        heartbeat_interval_seconds: 5,
                        deployment_generation: 1,
                        desired_state: Some(desired.clone()),
                        required_capabilities: vec!["status.v1".to_owned()],
                        certificate_rotation_deadline: Some(timestamp(
                            Utc::now() + ChronoDuration::hours(1),
                        )),
                    })),
                },
                ServerToAgent {
                    sequence: 2,
                    sent_at: Some(timestamp(Utc::now())),
                    correlation_id: operation_id.to_string(),
                    connection_id: connection_id.to_string(),
                    protocol_major: 1,
                    protocol_minor: 0,
                    payload: Some(ServerPayload::DesiredState(desired)),
                },
                ServerToAgent {
                    sequence: 3,
                    sent_at: Some(timestamp(Utc::now())),
                    correlation_id: operation_id.to_string(),
                    connection_id: connection_id.to_string(),
                    protocol_major: 1,
                    protocol_minor: 0,
                    payload: Some(ServerPayload::Operation(Operation {
                        operation_id: operation_id.to_string(),
                        kind: OperationKind::ConvergeState.into(),
                        operation_sequence: 1,
                        expected_desired_state_revision: 1,
                        expected_configuration_revision: 0,
                        created_at: Some(timestamp(Utc::now())),
                        expires_at: Some(timestamp(Utc::now() + ChronoDuration::minutes(5))),
                        reason: "pause for test".to_owned(),
                        parameters: None,
                    })),
                },
            ];
            let (sender, receiver) = mpsc::channel(8);
            let received = self.received.clone();
            tokio::spawn(async move {
                for envelope in envelopes {
                    if sender.send(Ok(envelope)).await.is_err() {
                        return;
                    }
                }
                while let Ok(Some(message)) = inbound.message().await {
                    let terminal = matches!(
                        message.payload,
                        Some(AgentPayload::OperationUpdate(ref update))
                            if update.state == i32::from(OperationState::Succeeded)
                    );
                    received.lock().await.push(message);
                    if terminal {
                        return;
                    }
                }
            });
            Ok(Response::new(ReceiverStream::new(receiver)))
        }
    }

    #[test]
    fn endpoint_requires_private_pinned_mtls_material() -> Result<(), Box<dyn std::error::Error>> {
        let identity = identity()?;
        let endpoint = AgentControlEndpoint::from_pem(
            "https://fleet.example:8443",
            identity.chain.clone(),
            identity.key.clone(),
            identity.root.clone(),
        )?;
        let debug = format!("{endpoint:?}");
        assert!(debug.contains("fleet.example:8443"));
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("PRIVATE KEY"));

        assert!(
            AgentControlEndpoint::from_pem(
                "http://fleet.example",
                identity.chain.clone(),
                identity.key.clone(),
                identity.root.clone(),
            )
            .is_err()
        );
        assert!(
            AgentControlEndpoint::from_pem(
                "https://user@fleet.example",
                identity.chain.clone(),
                identity.key.clone(),
                identity.root.clone(),
            )
            .is_err()
        );
        assert!(
            AgentControlEndpoint::from_pem(
                "https://fleet.example/control",
                identity.chain.clone(),
                identity.key.clone(),
                identity.root.clone(),
            )
            .is_err()
        );
        assert!(
            AgentControlEndpoint::from_pem(
                "https://fleet.example",
                identity.chain,
                identity.key,
                identity.leaf,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn client_certificate_requires_client_auth_usage() -> Result<(), Box<dyn std::error::Error>> {
        let identity = identity()?;
        let key = KeyPair::generate()?;
        let leaf = CertificateParams::new(Vec::new())?.self_signed(&key)?;
        assert!(matches!(
            AgentControlEndpoint::from_pem(
                "https://fleet.example",
                leaf.pem().into_bytes(),
                key.serialize_pem().into_bytes(),
                identity.root,
            ),
            Err(AgentError::InvalidTransport(_))
        ));
        Ok(())
    }

    #[test]
    fn reconnect_policy_is_bounded_and_jittered() -> Result<(), AgentError> {
        let policy = ReconnectPolicy::new(
            Duration::from_secs(1),
            Duration::from_secs(30),
            Duration::from_secs(60),
        )?;
        assert_eq!(policy.jittered_delay(0, 100), Duration::from_secs(1));
        assert_eq!(policy.jittered_delay(3, 100), Duration::from_secs(8));
        assert_eq!(policy.jittered_delay(20, 120), Duration::from_secs(30));
        assert_eq!(policy.jittered_delay(0, 1), Duration::from_millis(800));
        assert!(
            ReconnectPolicy::new(
                Duration::from_millis(10),
                Duration::from_secs(1),
                Duration::from_secs(60),
            )
            .is_err()
        );
        Ok(())
    }

    #[derive(Default)]
    struct TestControlService;

    #[tonic::async_trait]
    impl AgentControlService for TestControlService {
        type OpenControlStreamStream = ReceiverStream<Result<ServerToAgent, Status>>;

        async fn open_control_stream(
            &self,
            _request: Request<Streaming<AgentToServer>>,
        ) -> Result<Response<Self::OpenControlStreamStream>, Status> {
            let (_sender, receiver) = mpsc::channel(1);
            Ok(Response::new(ReceiverStream::new(receiver)))
        }
    }

    #[tokio::test]
    async fn endpoint_completes_a_live_mutual_tls_handshake()
    -> Result<(), Box<dyn std::error::Error>> {
        super::install_crypto_provider();
        let identity = identity()?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(
                &identity.server_chain,
                &identity.server_key,
            ))
            .client_ca_root(Certificate::from_pem(&identity.root));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(tls)?
                .add_service(AgentControlServiceServer::new(TestControlService))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    drop(shutdown_rx.await);
                })
                .await
        });

        let endpoint = AgentControlEndpoint::from_pem(
            format!("https://localhost:{}", address.port()),
            identity.chain,
            identity.key,
            identity.root,
        )?;
        let channel = endpoint.connect_channel().await?;
        drop(channel);
        shutdown_tx.send(()).map_err(|_| "server shutdown failed")?;
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn runner_converges_and_reports_durable_states_over_mtls()
    -> Result<(), Box<dyn std::error::Error>> {
        super::install_crypto_provider();
        let identity = identity()?;
        let scope = AgentScope {
            pipeline_id: Uuid::now_v7(),
            deployment_id: Uuid::now_v7(),
        };
        let received = Arc::new(Mutex::new(Vec::new()));
        let service = ScenarioControlService {
            scope: scope.clone(),
            received: received.clone(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(
                &identity.server_chain,
                &identity.server_key,
            ))
            .client_ca_root(Certificate::from_pem(&identity.root));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(tls)?
                .add_service(AgentControlServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    drop(shutdown_rx.await);
                })
                .await
        });

        let endpoint = AgentControlEndpoint::from_pem(
            format!("https://localhost:{}", address.port()),
            identity.chain,
            identity.key,
            identity.root,
        )?;
        let descriptor = AgentDescriptor::new(
            Uuid::now_v7(),
            Uuid::now_v7(),
            "0.1.0",
            "sha256:test",
            vec!["cdc".to_owned()],
            vec!["status.v1".to_owned()],
        )?;
        let directory = TestDirectory::new()?;
        let agent = EngineAgent::load(
            scope.clone(),
            StateStore::new(directory.state_path()),
            TestAdapter,
        )
        .await?;
        let mut runner = ControlStreamRunner::new(
            endpoint,
            descriptor,
            scope,
            agent,
            ReconnectPolicy::default(),
        )?;

        let result = tokio::time::timeout(Duration::from_secs(5), runner.run_session()).await?;
        assert!(matches!(result, Err(AgentError::ControlUnavailable)));
        assert_eq!(runner.agent().state().observed(), ObservedRunState::Paused);
        let messages = received.lock().await;
        let sequences = messages
            .iter()
            .map(|message| message.sequence)
            .collect::<Vec<_>>();
        assert_eq!(sequences, vec![1, 2, 3, 4, 5, 6, 7]);
        let operation_states = messages
            .iter()
            .filter_map(|message| match message.payload.as_ref() {
                Some(AgentPayload::OperationUpdate(update)) => Some(update.state),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            operation_states,
            vec![
                i32::from(OperationState::Acknowledged),
                i32::from(OperationState::Running),
                i32::from(OperationState::Succeeded),
            ]
        );
        drop(messages);

        shutdown_tx.send(()).map_err(|_| "server shutdown failed")?;
        server.await??;
        Ok(())
    }

    #[cfg(unix)]
    fn set_private_directory_permissions(path: &std::path::Path) -> Result<(), std::io::Error> {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
    }

    #[cfg(not(unix))]
    fn set_private_directory_permissions(_path: &std::path::Path) -> Result<(), std::io::Error> {
        Ok(())
    }
}
