//! Managed-mode run loop: key handshake, control stream, engine supervision.

use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use tokio::sync::watch;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use ventstream_fleet_client::{
    AgentRuntimeStatus, ControlStreamRunner, EngineAgent, ReconnectPolicy,
};
use ventstream_fleet_protocol::v1::enrollment_service_client::EnrollmentServiceClient;
use ventstream_fleet_protocol::v1::{EnrollRequest, EnrollResponse, RenewRequest, RenewResponse};

use crate::config::{DEFAULT_ENROLLMENT_URL, ManagedOptions, ManagedPaths, ManagedRuntimeConfig};
use crate::{
    BootstrapCredential, CredentialState, CredentialStore, EngineTelemetrySampler,
    EnrollmentCredentialRequest, ProcessEngineAdapter, RenewedCredential, SmokeEngineProcess,
    SupervisorEngineProcess, TokioEngineProcess,
};

const MAX_GRPC_MESSAGE_BYTES: usize = 256 * 1024;
const MAX_TRUST_BUNDLE_BYTES: u64 = 256 * 1024;
const MAX_ENDPOINT_BYTES: usize = 2048;
const RENEWAL_RETRY_DELAY: Duration = Duration::from_secs(300);
const MIN_RENEWAL_DELAY: Duration = Duration::from_secs(60);
const RENEWAL_LEAD_FLOOR: chrono::Duration = chrono::Duration::hours(24);
const TELEMETRY_SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

/// Runs the engine in managed mode until shutdown.
///
/// The agent key drives an invisible first-connect handshake: with no bound
/// identity on disk the engine generates a keypair, enrolls with the key, and
/// persists the issued identity. Enrollment failure is fatal by design — a
/// managed engine never falls back to local configuration.
pub async fn run_managed(options: ManagedOptions) -> Result<()> {
    install_crypto_provider();
    let paths = ManagedPaths::prepare(options.state_dir.as_deref())
        .map_err(|error| anyhow!(error))
        .context("preparing the managed state directory")?;
    let credential_store = CredentialStore::new(paths.identity_state().to_path_buf())
        .map_err(|error| anyhow!(error))?;
    let credential_state = match credential_store.load().map_err(|error| anyhow!(error))? {
        Some(state) => state,
        None => enroll_with_key(&options, &credential_store)
            .await
            .context("agent-key enrollment failed; managed mode cannot start")?,
    };
    let config =
        ManagedRuntimeConfig::resolve(&options, &paths, credential_store, credential_state)
            .map_err(|error| anyhow!(error))?;
    tracing::info!(config = ?config, "managed mode starting");

    let process = engine_process(&config)?;
    let adapter = ProcessEngineAdapter::new(process);
    let scope = config.scope();
    let agent = EngineAgent::load(scope.clone(), config.state_store(), adapter)
        .await
        .context("applying the persisted engine startup gate")?;
    let sampler = EngineTelemetrySampler::new(
        config.health_url(),
        config.engine_config_path().to_path_buf(),
    )
    .map_err(|error| anyhow!(error))?;
    let runtime_status = Arc::new(RwLock::new(AgentRuntimeStatus::default()));
    let status_for_provider = Arc::clone(&runtime_status);
    let status_provider: Arc<dyn Fn() -> AgentRuntimeStatus + Send + Sync> =
        Arc::new(move || match status_for_provider.read() {
            Ok(status) => status.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        });
    let (endpoint_tx, endpoint_rx) = watch::channel(config.endpoint());
    let mut runner = ControlStreamRunner::with_endpoint_updates(
        endpoint_rx,
        config.descriptor(),
        scope,
        agent,
        ReconnectPolicy::default(),
    )
    .context("building the control-stream runner")?
    .with_runtime_status_provider(status_provider);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let telemetry_task = tokio::spawn(runtime_telemetry_loop(
        sampler,
        runtime_status,
        shutdown_rx.clone(),
    ));
    let signal_shutdown = shutdown_tx.clone();
    let shutdown_task = tokio::spawn(async move {
        shutdown_signal().await;
        signal_shutdown.send_replace(true);
    });
    let renewal_task = tokio::spawn(renewal_loop(
        config.gateway_url().to_owned(),
        config.credential_store(),
        config.credential_state(),
        endpoint_tx,
        shutdown_rx.clone(),
    ));

    let run_result = runner
        .run_until_shutdown(wait_for_shutdown(shutdown_rx.clone()))
        .await;
    shutdown_tx.send_replace(true);
    shutdown_task.abort();
    renewal_task.abort();
    telemetry_task.abort();
    let stop_result = runner.agent_mut().adapter_mut().shutdown().await;
    if let Err(error) = stop_result {
        return Err(anyhow!(error)).context("stopping the supervised engine");
    }
    run_result.context("control stream stopped")?;
    tracing::info!("managed mode stopped");
    Ok(())
}

async fn runtime_telemetry_loop(
    mut sampler: EngineTelemetrySampler,
    status: Arc<RwLock<AgentRuntimeStatus>>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let sample = sampler.sample().await;
        match status.write() {
            Ok(mut current) => *current = sample,
            Err(poisoned) => *poisoned.into_inner() = sample,
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            () = tokio::time::sleep(TELEMETRY_SAMPLE_INTERVAL) => {}
        }
    }
}

fn engine_process(config: &ManagedRuntimeConfig) -> Result<SupervisorEngineProcess> {
    if std::env::var("VS_ENGINE_MODE").ok().as_deref() == Some("smoke") {
        let listen = std::env::var("VS_HEALTH_LISTEN")
            .unwrap_or_else(|_| "0.0.0.0:4043".to_owned())
            .parse()
            .context("VS_HEALTH_LISTEN must be a socket address")?;
        tracing::warn!("VS_ENGINE_MODE=smoke is active; supervised engine process is stubbed");
        return Ok(SupervisorEngineProcess::Smoke(
            SmokeEngineProcess::with_configuration_path(
                listen,
                config.engine_config_path().to_path_buf(),
            ),
        ));
    }
    let process = TokioEngineProcess::new(
        config.engine_binary().to_path_buf(),
        config.engine_config_path().to_path_buf(),
        config.health_url(),
        config.start_timeout(),
        config.stop_timeout(),
        config.drain_timeout(),
    )
    .map_err(|error| anyhow!(error))?;
    Ok(SupervisorEngineProcess::Real(Box::new(process)))
}

/// First-connect handshake: the agent key rides the enrollment grant field
/// and the gateway binds the generated identity to the key's deployment.
async fn enroll_with_key(
    options: &ManagedOptions,
    credential_store: &CredentialStore,
) -> Result<CredentialState> {
    let enrollment_url = options
        .enrollment_url
        .clone()
        .unwrap_or_else(|| DEFAULT_ENROLLMENT_URL.to_owned());
    let server_trust = enrollment_server_trust()?;
    let request = EnrollmentCredentialRequest::generate(credential_store.path())
        .map_err(|error| anyhow!(error))
        .context("generating the enrollment keypair")?;
    tracing::info!(
        trust_mode = server_trust.mode.as_str(),
        "connecting for agent-key enrollment"
    );
    let channel = enrollment_channel(&enrollment_url, &server_trust)
        .await
        .context("connecting to the enrollment gateway")?;
    let response = enrollment_client(channel)
        .enroll(EnrollRequest {
            enrollment_grant: options.agent_key.clone(),
            certificate_signing_request_der: request.csr_der().to_vec(),
            engine_version: env!("CARGO_PKG_VERSION").to_owned(),
            protocol_major: 1,
            protocol_minor: 0,
        })
        .await
        .context("agent key was not accepted by the enrollment gateway")?
        .into_inner();
    let credential =
        enrollment_credential(response, &request).context("validating the enrollment response")?;
    let state = credential_store
        .load_or_bootstrap(credential)
        .map_err(|error| anyhow!(error))
        .context("persisting the bound agent identity")?;
    tracing::info!("agent-key enrollment completed");
    Ok(state)
}

async fn renewal_loop(
    gateway_url: String,
    store: CredentialStore,
    initial_state: CredentialState,
    endpoint_tx: watch::Sender<ventstream_fleet_client::AgentControlEndpoint>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut state = initial_state;
    loop {
        let delay = renewal_delay(state.active().certificate_expires_at(), Utc::now());
        tracing::info!(?delay, "agent certificate renewal scheduled");
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            () = tokio::time::sleep(delay) => {}
        }

        match renew_once(&gateway_url, &store, state.clone(), &endpoint_tx).await {
            Ok(renewed) => {
                tracing::info!("agent certificate renewed; replacement credentials promoted");
                state = renewed;
            }
            Err(error) => {
                let error_chain = format!("{error:#}");
                tracing::warn!(
                    error = %error_chain,
                    ?RENEWAL_RETRY_DELAY,
                    "agent certificate renewal failed"
                );
                if let Ok(Some(reloaded)) = store.load() {
                    state = reloaded;
                }
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return;
                        }
                    }
                    () = tokio::time::sleep(RENEWAL_RETRY_DELAY) => {}
                }
            }
        }
    }
}

async fn renew_once(
    gateway_url: &str,
    store: &CredentialStore,
    state: CredentialState,
    endpoint_tx: &watch::Sender<ventstream_fleet_client::AgentControlEndpoint>,
) -> Result<CredentialState> {
    let current_serial = state.active().certificate_serial().to_owned();
    let endpoint = state
        .active()
        .endpoint(gateway_url)
        .context("building current renewal endpoint")?;
    let (state, pending) = store
        .ensure_pending_renewal(state)
        .context("preparing pending renewal credential")?;
    let channel = endpoint
        .connect_channel()
        .await
        .context("connecting to the gateway for renewal")?;
    let response = renewal_client(channel)
        .renew(RenewRequest {
            certificate_signing_request_der: pending.csr_der().to_vec(),
            current_certificate_serial: current_serial,
        })
        .await
        .context("requesting agent certificate renewal")?
        .into_inner();
    let renewed = renewed_credential(response)?;
    let state = store
        .activate_renewal(state, renewed)
        .context("promoting renewed credential")?;
    let endpoint = state
        .active()
        .endpoint(gateway_url)
        .context("building renewed control endpoint")?;
    endpoint_tx.send_replace(endpoint);
    Ok(state)
}

fn renewal_client(channel: Channel) -> EnrollmentServiceClient<Channel> {
    EnrollmentServiceClient::new(channel)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES)
}

fn enrollment_client(channel: Channel) -> EnrollmentServiceClient<Channel> {
    EnrollmentServiceClient::new(channel)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES)
}

async fn enrollment_channel(
    enrollment_url: &str,
    server_trust: &EnrollmentServerTrust,
) -> Result<Channel> {
    if enrollment_url.is_empty() || enrollment_url.len() > MAX_ENDPOINT_BYTES {
        return Err(anyhow!("enrollment URL is empty or too large"));
    }
    let endpoint =
        Endpoint::from_shared(enrollment_url.to_owned()).context("enrollment URL is malformed")?;
    let uri = endpoint.uri();
    let authority = uri
        .authority()
        .map(|value| value.as_str())
        .ok_or_else(|| anyhow!("enrollment URL authority is required"))?;
    if uri.scheme_str() != Some("https")
        || uri.host().is_none()
        || authority.contains('@')
        || !matches!(uri.path(), "" | "/")
        || uri.query().is_some()
    {
        return Err(anyhow!(
            "enrollment URL must be an HTTPS origin without credentials, path, or query"
        ));
    }
    let tls = match server_trust.mode {
        EnrollmentTrustMode::Public => ClientTlsConfig::new().with_webpki_roots(),
        EnrollmentTrustMode::Custom => {
            ClientTlsConfig::new().ca_certificate(Certificate::from_pem(
                server_trust
                    .custom_ca_pem
                    .as_deref()
                    .ok_or_else(|| anyhow!("custom enrollment trust bundle is missing"))?,
            ))
        }
    }
    .timeout(Duration::from_secs(10));
    endpoint
        .connect_timeout(Duration::from_secs(10))
        .tls_config(tls)
        .context("enrollment TLS configuration failed")?
        .connect()
        .await
        .context("enrollment listener is unavailable")
}

fn enrollment_credential(
    response: EnrollResponse,
    request: &EnrollmentCredentialRequest,
) -> Result<BootstrapCredential> {
    if response.deployment_id.trim().is_empty() {
        return Err(anyhow!(
            "enrollment response is missing the bound deployment"
        ));
    }
    let _expires_at = parse_timestamp(response.certificate_expires_at)
        .context("enrollment response certificate expiry is invalid")?;
    Ok(BootstrapCredential {
        certificate_chain_pem: response.certificate_chain_pem,
        private_key_pem: request.private_key_pem().to_vec(),
        trust_bundle_pem: response.trust_bundle_pem,
        certificate_serial: Some(response.certificate_serial),
    })
}

fn renewed_credential(response: RenewResponse) -> Result<RenewedCredential> {
    let expires_at = parse_timestamp(response.certificate_expires_at)
        .context("renewal response certificate expiry is invalid")?;
    Ok(RenewedCredential {
        certificate_chain_pem: response.certificate_chain_pem,
        trust_bundle_pem: response.trust_bundle_pem,
        certificate_serial: response.certificate_serial,
        certificate_expires_at: expires_at,
    })
}

fn parse_timestamp(value: Option<prost_types::Timestamp>) -> Result<DateTime<Utc>> {
    let value = value.ok_or_else(|| anyhow!("timestamp is missing"))?;
    let nanos = u32::try_from(value.nanos).map_err(|_| anyhow!("timestamp nanos is invalid"))?;
    DateTime::from_timestamp(value.seconds, nanos).ok_or_else(|| anyhow!("timestamp is invalid"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EnrollmentTrustMode {
    Public,
    Custom,
}

impl EnrollmentTrustMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Custom => "custom",
        }
    }
}

struct EnrollmentServerTrust {
    mode: EnrollmentTrustMode,
    custom_ca_pem: Option<Vec<u8>>,
}

fn enrollment_server_trust() -> Result<EnrollmentServerTrust> {
    let mode = resolve_enrollment_trust_mode(
        std::env::var("VS_FLEET_ENROLLMENT_TRUST_MODE")
            .ok()
            .as_deref(),
        std::env::var("VS_FLEET_ENROLLMENT_TRUST_BUNDLE_PATH").is_ok(),
    )?;
    let custom_ca_pem = match mode {
        EnrollmentTrustMode::Public => None,
        EnrollmentTrustMode::Custom => Some(read_bounded_binary_file(
            "VS_FLEET_ENROLLMENT_TRUST_BUNDLE_PATH",
            MAX_TRUST_BUNDLE_BYTES,
        )?),
    };
    Ok(EnrollmentServerTrust {
        mode,
        custom_ca_pem,
    })
}

fn resolve_enrollment_trust_mode(
    configured_mode: Option<&str>,
    has_custom_ca_path: bool,
) -> Result<EnrollmentTrustMode> {
    match (configured_mode, has_custom_ca_path) {
        (None, false) => Ok(EnrollmentTrustMode::Public),
        // Backward compatibility for releases that only exposed the bundle path.
        (None, true) => Ok(EnrollmentTrustMode::Custom),
        (Some("public"), false) => Ok(EnrollmentTrustMode::Public),
        (Some("custom"), true) => Ok(EnrollmentTrustMode::Custom),
        (Some("public"), true) => Err(anyhow!(
            "VS_FLEET_ENROLLMENT_TRUST_BUNDLE_PATH must be unset when VS_FLEET_ENROLLMENT_TRUST_MODE=public"
        )),
        (Some("custom"), false) => Err(anyhow!(
            "VS_FLEET_ENROLLMENT_TRUST_BUNDLE_PATH is required when VS_FLEET_ENROLLMENT_TRUST_MODE=custom"
        )),
        (Some(_), _) => Err(anyhow!(
            "VS_FLEET_ENROLLMENT_TRUST_MODE must be public or custom"
        )),
    }
}

fn read_bounded_binary_file(name: &str, max_bytes: u64) -> Result<Vec<u8>> {
    let value = std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("{name} is required"))?;
    let path = Path::new(&value);
    if !path.is_absolute() {
        return Err(anyhow!("{name} must be an absolute path"));
    }
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("reading {name} metadata at {}", path.display()))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > max_bytes {
        return Err(anyhow!("{name} is not a bounded regular file"));
    }
    std::fs::read(path).with_context(|| format!("reading {name} at {}", path.display()))
}

fn install_crypto_provider() {
    drop(rustls::crypto::ring::default_provider().install_default());
}

fn renewal_delay(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> Duration {
    let remaining = expires_at - now;
    if remaining <= chrono::Duration::minutes(5) {
        return MIN_RENEWAL_DELAY;
    }
    // Renew at half-life or RENEWAL_LEAD_FLOOR before expiry, whichever is
    // earlier, so a renewal outage keeps a multi-day retry budget.
    let lead = (remaining / 2).max(RENEWAL_LEAD_FLOOR);
    (remaining - lead)
        .to_std()
        .unwrap_or(MIN_RENEWAL_DELAY)
        .max(MIN_RENEWAL_DELAY)
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

async fn shutdown_signal() {
    let interrupt = tokio::signal::ctrl_c();
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};

        match signal(SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(error) => {
                tracing::error!(%error, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        result = interrupt => {
            if let Err(error) = result {
                tracing::error!(%error, "failed to receive interrupt signal");
            }
        }
        () = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::{EnrollmentTrustMode, resolve_enrollment_trust_mode};
    use anyhow::{Result, anyhow};

    #[test]
    fn enrollment_uses_public_trust_without_custom_configuration() -> Result<()> {
        assert_eq!(
            resolve_enrollment_trust_mode(None, false)?,
            EnrollmentTrustMode::Public
        );
        assert_eq!(
            resolve_enrollment_trust_mode(Some("public"), false)?,
            EnrollmentTrustMode::Public
        );
        Ok(())
    }

    #[test]
    fn enrollment_preserves_legacy_custom_trust_configuration() -> Result<()> {
        assert_eq!(
            resolve_enrollment_trust_mode(None, true)?,
            EnrollmentTrustMode::Custom
        );
        assert_eq!(
            resolve_enrollment_trust_mode(Some("custom"), true)?,
            EnrollmentTrustMode::Custom
        );
        Ok(())
    }

    #[test]
    fn enrollment_rejects_inconsistent_trust_configuration() -> Result<()> {
        let public_with_bundle = match resolve_enrollment_trust_mode(Some("public"), true) {
            Ok(_) => {
                return Err(anyhow!(
                    "public trust with a custom CA unexpectedly succeeded"
                ));
            }
            Err(error) => error,
        };
        assert!(public_with_bundle.to_string().contains("must be unset"));

        let custom_without_bundle = match resolve_enrollment_trust_mode(Some("custom"), false) {
            Ok(_) => {
                return Err(anyhow!(
                    "custom trust without a custom CA unexpectedly succeeded"
                ));
            }
            Err(error) => error,
        };
        assert!(custom_without_bundle.to_string().contains("is required"));

        let unknown = match resolve_enrollment_trust_mode(Some("disabled"), false) {
            Ok(_) => return Err(anyhow!("unknown trust mode unexpectedly succeeded")),
            Err(error) => error,
        };
        assert!(unknown.to_string().contains("must be public or custom"));
        Ok(())
    }

    #[test]
    fn renewal_fires_at_half_life_with_a_full_retry_budget() -> Result<()> {
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-06T00:00:00Z")?.to_utc();

        // A seven-day certificate renews at its half-life, leaving 3.5 days
        // of retry budget.
        let seven_days = now + chrono::Duration::days(7);
        let delay = super::renewal_delay(seven_days, now);
        assert_eq!(delay, chrono::Duration::hours(84).to_std()?);

        // A two-day certificate renews a full day before expiry.
        let two_days = now + chrono::Duration::days(2);
        let delay = super::renewal_delay(two_days, now);
        assert_eq!(delay, chrono::Duration::hours(24).to_std()?);

        // Certificates already inside the 24-hour lead renew immediately
        // (at the minimum spacing), never after expiry.
        let twelve_hours = now + chrono::Duration::hours(12);
        let delay = super::renewal_delay(twelve_hours, now);
        assert_eq!(delay, super::MIN_RENEWAL_DELAY);

        // Expired or nearly expired credentials keep retrying at the floor.
        let nearly_expired = now + chrono::Duration::minutes(3);
        let delay = super::renewal_delay(nearly_expired, now);
        assert_eq!(delay, super::MIN_RENEWAL_DELAY);
        Ok(())
    }
}
