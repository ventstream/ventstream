use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use uuid::Uuid;
use ventstream_fleet_client::{
    AgentControlEndpoint, AgentDescriptor, AgentError, AgentScope, StateStore,
};

use crate::{CredentialState, CredentialStore};

/// Default platform control gateway (mTLS agent stream).
pub const DEFAULT_GATEWAY_URL: &str = "https://gateway.ventstream.dev:8445";
/// Default platform enrollment gateway (key handshake).
pub const DEFAULT_ENROLLMENT_URL: &str = "https://gateway.ventstream.dev:8444";
const DEFAULT_STATE_DIR: &str = "/var/lib/ventstream/managed";
const INSTANCE_ID_FILE: &str = "instance-id";

/// Caller-supplied managed-mode inputs: the agent key plus optional overrides.
pub struct ManagedOptions {
    /// Resolved `vsa_` agent key.
    pub agent_key: String,
    /// Control gateway URL override.
    pub gateway_url: Option<String>,
    /// Enrollment gateway URL override.
    pub enrollment_url: Option<String>,
    /// Directory holding the bound agent identity and staged config.
    pub state_dir: Option<PathBuf>,
}

/// Filesystem layout under the managed state directory.
pub struct ManagedPaths {
    state_dir: PathBuf,
    identity_state: PathBuf,
    management_state: PathBuf,
    engine_config: PathBuf,
}

impl ManagedPaths {
    /// Creates the private state layout, enforcing `0700` ownership.
    pub fn prepare(state_dir: Option<&Path>) -> Result<Self, AgentError> {
        let state_dir = state_dir
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_STATE_DIR));
        if !state_dir.is_absolute() {
            return Err(invalid("managed state directory must be an absolute path"));
        }
        let config_dir = state_dir.join("config");
        for directory in [&state_dir, &config_dir] {
            fs::create_dir_all(directory).map_err(|error| {
                invalid(format!(
                    "creating managed state directory {}: {error}",
                    directory.display()
                ))
            })?;
            set_private_mode(directory)?;
        }
        Ok(Self {
            identity_state: state_dir.join("identity-state.json"),
            management_state: state_dir.join("management-state.json"),
            engine_config: config_dir.join("applied-envelope.json"),
            state_dir,
        })
    }

    /// Returns the identity (credential) state file path.
    pub fn identity_state(&self) -> &Path {
        &self.identity_state
    }

    /// Loads the stable per-install instance id, minting one on first run.
    pub fn load_or_create_instance_id(&self) -> Result<Uuid, AgentError> {
        let path = self.state_dir.join(INSTANCE_ID_FILE);
        if let Ok(raw) = fs::read_to_string(&path) {
            return Uuid::parse_str(raw.trim())
                .map_err(|_| invalid("persisted managed instance id is invalid"));
        }
        let id = Uuid::now_v7();
        fs::write(&path, id.to_string())
            .map_err(|error| invalid(format!("persisting managed instance id: {error}")))?;
        Ok(id)
    }
}

/// Fully validated managed-mode runtime configuration.
pub struct ManagedRuntimeConfig {
    gateway_url: String,
    endpoint: AgentControlEndpoint,
    credential_store: CredentialStore,
    credential_state: CredentialState,
    descriptor: AgentDescriptor,
    scope: AgentScope,
    state_store: StateStore,
    engine_binary: PathBuf,
    engine_config_path: PathBuf,
    health_url: reqwest::Url,
    start_timeout: Duration,
    stop_timeout: Duration,
    drain_timeout: Duration,
}

impl ManagedRuntimeConfig {
    /// Assembles the runtime contract from enrolled credentials and options.
    pub fn resolve(
        options: &ManagedOptions,
        paths: &ManagedPaths,
        credential_store: CredentialStore,
        credential_state: CredentialState,
    ) -> Result<Self, AgentError> {
        let gateway_url = options
            .gateway_url
            .clone()
            .unwrap_or_else(|| DEFAULT_GATEWAY_URL.to_owned());
        let endpoint = credential_state.active().endpoint(&gateway_url)?;
        let identity =
            AgentCertificateScope::from_chain(credential_state.active().certificate_chain_pem())?;
        let instance_id = paths.load_or_create_instance_id()?;
        let roles = parse_roles(&std::env::var("VS_ROLES").unwrap_or_else(|_| "cdc".to_owned()))?;
        let descriptor = AgentDescriptor::new(
            instance_id,
            Uuid::now_v7(),
            env!("CARGO_PKG_VERSION"),
            option_env!("VENTSTREAM_BUILD_DIGEST").unwrap_or("sha256:development"),
            roles,
            vec![
                "status.v1".to_owned(),
                "converge.run-state.v1".to_owned(),
                "operation.apply-configuration.v1".to_owned(),
                "operation.reconcile.v1".to_owned(),
                "operation.rebootstrap.v1".to_owned(),
            ],
        )?;
        let engine_binary = std::env::var("VS_FLEET_ENGINE_BIN").map_or_else(
            |_| {
                std::env::current_exe()
                    .map_err(|error| invalid(format!("resolving the engine executable: {error}")))
            },
            |value| Ok(PathBuf::from(value)),
        )?;
        validate_engine_binary(&engine_binary)?;
        let health_url = validate_health_url(
            &std::env::var("VS_FLEET_ENGINE_HEALTH_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:4043/readyz".to_owned()),
        )?;

        Ok(Self {
            gateway_url,
            endpoint,
            credential_store,
            credential_state,
            descriptor,
            scope: AgentScope {
                pipeline_id: identity.pipeline_id,
                deployment_id: identity.deployment_id,
            },
            state_store: StateStore::new(paths.management_state.clone()),
            engine_binary,
            engine_config_path: paths.engine_config.clone(),
            health_url,
            start_timeout: duration_from_env("VS_FLEET_ENGINE_START_TIMEOUT_SECS", 30, 1, 300)?,
            stop_timeout: duration_from_env("VS_FLEET_ENGINE_STOP_TIMEOUT_SECS", 30, 1, 300)?,
            drain_timeout: duration_from_env("VS_FLEET_ENGINE_DRAIN_TIMEOUT_SECS", 300, 1, 1800)?,
        })
    }

    /// Returns the pinned mTLS gateway endpoint.
    pub fn endpoint(&self) -> AgentControlEndpoint {
        self.endpoint.clone()
    }

    /// Returns the configured gateway origin used when rebuilding mTLS endpoints.
    pub fn gateway_url(&self) -> &str {
        &self.gateway_url
    }

    /// Returns the supervisor-owned credential store.
    pub fn credential_store(&self) -> CredentialStore {
        self.credential_store.clone()
    }

    /// Returns the active credential state loaded at startup.
    pub fn credential_state(&self) -> CredentialState {
        self.credential_state.clone()
    }

    /// Returns the process identity and advertised capabilities.
    pub fn descriptor(&self) -> AgentDescriptor {
        self.descriptor.clone()
    }

    /// Returns the pipeline and deployment bound to the agent key.
    pub fn scope(&self) -> AgentScope {
        self.scope.clone()
    }

    /// Returns the private local management-state store.
    pub fn state_store(&self) -> StateStore {
        self.state_store.clone()
    }

    /// Returns the engine executable respawned for pipeline work.
    pub fn engine_binary(&self) -> &Path {
        &self.engine_binary
    }

    /// Returns the private path where selected non-secret engine config is staged.
    pub fn engine_config_path(&self) -> &Path {
        &self.engine_config_path
    }

    /// Returns the loopback engine readiness URL.
    pub fn health_url(&self) -> reqwest::Url {
        self.health_url.clone()
    }

    /// Returns the maximum engine startup duration.
    pub fn start_timeout(&self) -> Duration {
        self.start_timeout
    }

    /// Returns the graceful engine shutdown duration.
    pub fn stop_timeout(&self) -> Duration {
        self.stop_timeout
    }

    /// Returns the maximum local drain-command duration.
    pub fn drain_timeout(&self) -> Duration {
        self.drain_timeout
    }
}

impl fmt::Debug for ManagedRuntimeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagedRuntimeConfig")
            .field("endpoint", &self.endpoint)
            .field("scope", &self.scope)
            .field("state_path", &self.state_store.path())
            .field("credential_state_path", &self.credential_store.path())
            .field("engine_binary", &self.engine_binary)
            .field("engine_config_path", &self.engine_config_path)
            .field("health_url", &self.health_url)
            .field("workload_identity", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

/// Pipeline and deployment scope carried in the issued certificate's URI SAN:
/// `urn:ventstream:fleet:agent:v1:<org>:<env>:<pipeline>:<deployment>:<agent>`.
pub struct AgentCertificateScope {
    /// Pipeline the bound deployment belongs to.
    pub pipeline_id: Uuid,
    /// Deployment the agent key identifies.
    pub deployment_id: Uuid,
}

impl AgentCertificateScope {
    /// Extracts the scope from the leaf certificate of a PEM chain.
    pub fn from_chain(chain_pem: &[u8]) -> Result<Self, AgentError> {
        use x509_parser::extensions::GeneralName;
        use x509_parser::pem::Pem;

        let pem = Pem::iter_from_buffer(chain_pem)
            .next()
            .ok_or_else(|| invalid("certificate chain is empty"))?
            .map_err(|_| invalid("certificate chain PEM is invalid"))?;
        let (_, certificate) = x509_parser::parse_x509_certificate(&pem.contents)
            .map_err(|_| invalid("leaf certificate is invalid"))?;
        let san = certificate
            .subject_alternative_name()
            .ok()
            .flatten()
            .ok_or_else(|| invalid("leaf certificate has no subject alternative name"))?;
        let urn = san
            .value
            .general_names
            .iter()
            .find_map(|name| match name {
                GeneralName::URI(value) => Some(*value),
                _ => None,
            })
            .ok_or_else(|| invalid("leaf certificate has no URI identity"))?;
        Self::from_urn(urn)
    }

    fn from_urn(urn: &str) -> Result<Self, AgentError> {
        let parts: Vec<&str> = urn.split(':').collect();
        let [
            "urn",
            "ventstream",
            "fleet",
            "agent",
            "v1",
            _org,
            _env,
            pipeline,
            deployment,
            _agent,
        ] = parts.as_slice()
        else {
            return Err(invalid("agent identity URN has an unsupported shape"));
        };
        Ok(Self {
            pipeline_id: canonical_uuid(pipeline)?,
            deployment_id: canonical_uuid(deployment)?,
        })
    }
}

fn canonical_uuid(value: &str) -> Result<Uuid, AgentError> {
    let parsed = Uuid::parse_str(value).map_err(|_| invalid("agent identity URN id is invalid"))?;
    if parsed.is_nil() || parsed.to_string() != value {
        return Err(invalid("agent identity URN id is invalid"));
    }
    Ok(parsed)
}

fn parse_roles(value: &str) -> Result<Vec<String>, AgentError> {
    let roles = value
        .split(',')
        .map(str::trim)
        .filter(|role| !role.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if roles.is_empty()
        || roles
            .iter()
            .any(|role| !matches!(role.as_str(), "cdc" | "ws" | "graphql" | "mcp"))
        || roles.iter().collect::<std::collections::HashSet<_>>().len() != roles.len()
    {
        return Err(invalid("VS_ROLES contains an unsupported role"));
    }
    Ok(roles)
}

fn validate_engine_binary(path: &Path) -> Result<(), AgentError> {
    if !path.is_absolute() {
        return Err(invalid("engine executable path must be absolute"));
    }
    let metadata =
        fs::symlink_metadata(path).map_err(|_| invalid("engine executable cannot be inspected"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(invalid("engine executable must be a regular file"));
    }
    validate_executable_permissions(&metadata)
}

fn validate_health_url(value: &str) -> Result<reqwest::Url, AgentError> {
    let url =
        reqwest::Url::parse(value).map_err(|_| invalid("VS_FLEET_ENGINE_HEALTH_URL is invalid"))?;
    let local_host = matches!(url.host_str(), Some("127.0.0.1" | "[::1]"));
    if url.scheme() != "http"
        || !local_host
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/readyz"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid(
            "VS_FLEET_ENGINE_HEALTH_URL must be a credential-free loopback /readyz URL",
        ));
    }
    Ok(url)
}

fn duration_from_env(
    name: &str,
    default: u64,
    minimum: u64,
    maximum: u64,
) -> Result<Duration, AgentError> {
    let seconds = std::env::var(name)
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|_| invalid(format!("{name} is invalid")))?
        .unwrap_or(default);
    if !(minimum..=maximum).contains(&seconds) {
        return Err(invalid(format!("{name} is outside safe bounds")));
    }
    Ok(Duration::from_secs(seconds))
}

#[cfg(unix)]
fn set_private_mode(path: &Path) -> Result<(), AgentError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|error| invalid(format!("securing {}: {error}", path.display())))
}

#[cfg(not(unix))]
fn set_private_mode(_path: &Path) -> Result<(), AgentError> {
    Ok(())
}

#[cfg(unix)]
fn validate_executable_permissions(metadata: &fs::Metadata) -> Result<(), AgentError> {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o111 == 0 {
        return Err(invalid("engine executable is not executable"));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_executable_permissions(_metadata: &fs::Metadata) -> Result<(), AgentError> {
    Ok(())
}

fn invalid(message: impl Into<String>) -> AgentError {
    AgentError::InvalidTransport(message.into())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::{AgentCertificateScope, parse_roles, validate_health_url};

    #[test]
    fn health_probe_is_restricted_to_loopback_readyz() {
        assert!(validate_health_url("http://127.0.0.1:4043/readyz").is_ok());
        assert!(validate_health_url("http://[::1]:4043/readyz").is_ok());
        assert!(validate_health_url("http://localhost:4043/readyz").is_err());
        assert!(validate_health_url("https://127.0.0.1:4043/readyz").is_err());
        assert!(validate_health_url("http://engine:4043/readyz").is_err());
        assert!(validate_health_url("http://127.0.0.1:4043/healthz").is_err());
        assert!(validate_health_url("http://user@127.0.0.1:4043/readyz").is_err());
    }

    #[test]
    fn roles_use_the_closed_engine_vocabulary() {
        assert_eq!(
            parse_roles("cdc,ws").ok(),
            Some(vec!["cdc".to_owned(), "ws".to_owned()])
        );
        assert_eq!(parse_roles("mcp").ok(), Some(vec!["mcp".to_owned()]));
        assert!(parse_roles("").is_err());
        assert!(parse_roles("cdc,shell").is_err());
        assert!(parse_roles("cdc,cdc").is_err());
    }

    #[test]
    fn identity_urn_yields_pipeline_and_deployment_scope() {
        let urn = "urn:ventstream:fleet:agent:v1:0198c0de-0000-7000-8000-000000000001:0198c0de-0000-7000-8000-000000000002:0198c0de-0000-7000-8000-000000000003:0198c0de-0000-7000-8000-000000000004:0198c0de-0000-7000-8000-000000000005";
        let scope = AgentCertificateScope::from_urn(urn).expect("canonical URN parses");
        assert_eq!(
            scope.pipeline_id.to_string(),
            "0198c0de-0000-7000-8000-000000000003"
        );
        assert_eq!(
            scope.deployment_id.to_string(),
            "0198c0de-0000-7000-8000-000000000004"
        );
        assert!(
            AgentCertificateScope::from_urn("urn:ventstream:fleet:agent:v2:a:b:c:d:e").is_err()
        );
        assert!(AgentCertificateScope::from_urn("urn:ventstream:fleet:agent:v1:a:b").is_err());
    }
}
