use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use rcgen::{CertificateParams, KeyPair};
use rustls_pki_types::pem::PemObject;
use serde::{Deserialize, Serialize};
use ventstream_fleet_client::{AgentControlEndpoint, AgentError};

const CREDENTIAL_STATE_VERSION: u32 = 1;
const MAX_CREDENTIAL_STATE_BYTES: u64 = 768 * 1024;
const MAX_CERTIFICATE_CHAIN_BYTES: usize = 256 * 1024;
const MAX_PRIVATE_KEY_BYTES: usize = 64 * 1024;
const MAX_TRUST_BUNDLE_BYTES: usize = 256 * 1024;
const MAX_CSR_DER_BYTES: usize = 64 * 1024;

/// Bootstrap identity material mounted into the supervisor on first start.
#[derive(Clone, Debug)]
pub struct BootstrapCredential {
    /// Leaf-first client certificate chain.
    pub certificate_chain_pem: Vec<u8>,
    /// Client private key PEM.
    pub private_key_pem: Vec<u8>,
    /// Gateway trust anchors.
    pub trust_bundle_pem: Vec<u8>,
    /// CA-scoped certificate serial, when supplied by deployment tooling.
    pub certificate_serial: Option<String>,
}

/// Locally generated private key and CSR for first enrollment.
#[derive(Clone, Debug)]
pub struct EnrollmentCredentialRequest {
    private_key_pem: Vec<u8>,
    csr_der: Vec<u8>,
}

impl EnrollmentCredentialRequest {
    /// Generates a private key and DER CSR for first enrollment.
    pub fn generate(path: &Path) -> Result<Self, AgentError> {
        let key = KeyPair::generate()
            .map_err(|_| invalid_state(path, "enrollment key could not be generated"))?;
        let csr = CertificateParams::new(Vec::new())
            .map_err(|_| invalid_state(path, "enrollment CSR parameters are invalid"))?
            .serialize_request(&key)
            .map_err(|_| invalid_state(path, "enrollment CSR could not be generated"))?;
        let request = Self {
            private_key_pem: key.serialize_pem().into_bytes(),
            csr_der: csr.der().to_vec(),
        };
        request.validate(path)?;
        Ok(request)
    }

    /// Returns the DER-encoded certificate signing request.
    pub fn csr_der(&self) -> &[u8] {
        &self.csr_der
    }

    /// Returns the generated private key PEM.
    pub fn private_key_pem(&self) -> &[u8] {
        &self.private_key_pem
    }

    fn validate(&self, path: &Path) -> Result<(), AgentError> {
        validate_key_and_csr(
            path,
            &self.private_key_pem,
            &self.csr_der,
            "enrollment material is invalid",
        )
    }
}

/// Atomic store for supervisor-owned identity and pending renewal material.
#[derive(Clone, Debug)]
pub struct CredentialStore {
    path: PathBuf,
}

impl CredentialStore {
    /// Creates a store backed by an absolute JSON file path.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, AgentError> {
        let path = path.into();
        if !path.is_absolute() {
            return Err(invalid_state(
                &path,
                "credential state path must be absolute",
            ));
        }
        Ok(Self { path })
    }

    /// Returns the on-disk credential state path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Loads existing credentials or persists the supplied bootstrap credential atomically.
    pub fn load_or_bootstrap(
        &self,
        bootstrap: BootstrapCredential,
    ) -> Result<CredentialState, AgentError> {
        match self.load()? {
            Some(state) => Ok(state),
            None => {
                let material = CredentialMaterial::from_bootstrap(bootstrap)?;
                let state = CredentialState {
                    version: CREDENTIAL_STATE_VERSION,
                    active: material,
                    pending: None,
                };
                self.save(&state)?;
                Ok(state)
            }
        }
    }

    /// Loads credential state if present.
    pub fn load(&self) -> Result<Option<CredentialState>, AgentError> {
        let metadata = match fs::symlink_metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(state_io(&self.path, error)),
        };
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() == 0
            || metadata.len() > MAX_CREDENTIAL_STATE_BYTES
        {
            return Err(invalid_state(
                &self.path,
                "credential state is not a bounded regular file",
            ));
        }
        validate_private_permissions(&self.path, &metadata)?;
        let encoded = fs::read(&self.path).map_err(|error| state_io(&self.path, error))?;
        let state: CredentialState = serde_json::from_slice(&encoded)
            .map_err(|_| invalid_state(&self.path, "credential state is malformed"))?;
        state.validate(&self.path)?;
        Ok(Some(state))
    }

    /// Atomically persists credential state with private file permissions.
    pub fn save(&self, state: &CredentialState) -> Result<(), AgentError> {
        state.validate(&self.path)?;
        let parent = self
            .path
            .parent()
            .ok_or_else(|| invalid_state(&self.path, "credential state parent is missing"))?;
        fs::create_dir_all(parent).map_err(|error| state_io(parent, error))?;
        let temporary = self
            .path
            .with_extension(format!("tmp-{}", uuid::Uuid::now_v7().as_simple()));
        let encoded = serde_json::to_vec(state)
            .map_err(|_| invalid_state(&self.path, "credential state could not be encoded"))?;
        {
            let mut file = open_private_new_file(&temporary)?;
            file.write_all(&encoded)
                .map_err(|error| state_io(&temporary, error))?;
            file.sync_all()
                .map_err(|error| state_io(&temporary, error))?;
        }
        if let Err(error) = fs::rename(&temporary, &self.path) {
            let _ = fs::remove_file(&temporary);
            return Err(state_io(&self.path, error));
        }
        sync_parent(parent)?;
        Ok(())
    }

    /// Returns existing pending renewal material or creates and persists a new CSR/key pair.
    pub fn ensure_pending_renewal(
        &self,
        mut state: CredentialState,
    ) -> Result<(CredentialState, PendingRenewal), AgentError> {
        if let Some(pending) = state.pending.clone() {
            pending.validate(&self.path)?;
            return Ok((state, pending));
        }
        let pending = PendingRenewal::generate(&self.path)?;
        state.pending = Some(pending.clone());
        self.save(&state)?;
        Ok((state, pending))
    }

    /// Promotes a renewed certificate and clears the pending CSR/key.
    pub fn activate_renewal(
        &self,
        mut state: CredentialState,
        response: RenewedCredential,
    ) -> Result<CredentialState, AgentError> {
        let pending = state
            .pending
            .take()
            .ok_or_else(|| invalid_state(&self.path, "pending renewal key is missing"))?;
        let material = CredentialMaterial::new(
            response.certificate_chain_pem,
            pending.private_key_pem,
            response.trust_bundle_pem,
            response.certificate_serial,
            response.certificate_expires_at,
        )?;
        state.active = material;
        self.save(&state)?;
        Ok(state)
    }
}

/// Persisted supervisor identity state.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CredentialState {
    version: u32,
    active: CredentialMaterial,
    pending: Option<PendingRenewal>,
}

impl CredentialState {
    /// Returns active identity material.
    pub fn active(&self) -> &CredentialMaterial {
        &self.active
    }

    fn validate(&self, path: &Path) -> Result<(), AgentError> {
        if self.version != CREDENTIAL_STATE_VERSION {
            return Err(invalid_state(
                path,
                "credential state version is unsupported",
            ));
        }
        self.active.validate(path)?;
        if let Some(pending) = &self.pending {
            pending.validate(path)?;
        }
        Ok(())
    }
}

/// Active mTLS material and CA metadata used by the supervisor.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CredentialMaterial {
    certificate_chain_pem: Vec<u8>,
    private_key_pem: Vec<u8>,
    trust_bundle_pem: Vec<u8>,
    certificate_serial: String,
    certificate_expires_at: DateTime<Utc>,
}

impl CredentialMaterial {
    fn from_bootstrap(bootstrap: BootstrapCredential) -> Result<Self, AgentError> {
        let parsed = parse_leaf_certificate(&bootstrap.certificate_chain_pem)?;
        let certificate_serial = bootstrap
            .certificate_serial
            .unwrap_or(parsed.certificate_serial);
        Self::new(
            bootstrap.certificate_chain_pem,
            bootstrap.private_key_pem,
            bootstrap.trust_bundle_pem,
            certificate_serial,
            parsed.certificate_expires_at,
        )
    }

    fn new(
        certificate_chain_pem: Vec<u8>,
        private_key_pem: Vec<u8>,
        trust_bundle_pem: Vec<u8>,
        certificate_serial: String,
        certificate_expires_at: DateTime<Utc>,
    ) -> Result<Self, AgentError> {
        let material = Self {
            certificate_chain_pem,
            private_key_pem,
            trust_bundle_pem,
            certificate_serial,
            certificate_expires_at,
        };
        material.validate(Path::new("<credential-state>"))?;
        Ok(material)
    }

    /// Builds a pinned mTLS endpoint from active material.
    pub fn endpoint(&self, gateway_url: &str) -> Result<AgentControlEndpoint, AgentError> {
        AgentControlEndpoint::from_pem(
            gateway_url.to_owned(),
            self.certificate_chain_pem.clone(),
            self.private_key_pem.clone(),
            self.trust_bundle_pem.clone(),
        )
    }

    /// Returns the CA-scoped serial used for authenticated renewal.
    pub fn certificate_serial(&self) -> &str {
        &self.certificate_serial
    }

    /// Returns the PEM certificate chain, leaf first.
    pub fn certificate_chain_pem(&self) -> &[u8] {
        &self.certificate_chain_pem
    }

    /// Returns the active certificate expiry.
    pub const fn certificate_expires_at(&self) -> DateTime<Utc> {
        self.certificate_expires_at
    }

    fn validate(&self, path: &Path) -> Result<(), AgentError> {
        if self.certificate_chain_pem.is_empty()
            || self.certificate_chain_pem.len() > MAX_CERTIFICATE_CHAIN_BYTES
            || self.private_key_pem.is_empty()
            || self.private_key_pem.len() > MAX_PRIVATE_KEY_BYTES
            || self.trust_bundle_pem.is_empty()
            || self.trust_bundle_pem.len() > MAX_TRUST_BUNDLE_BYTES
            || !valid_serial(&self.certificate_serial)
            || self.certificate_expires_at <= Utc::now()
        {
            return Err(invalid_state(path, "active credential material is invalid"));
        }
        let parsed = parse_leaf_certificate(&self.certificate_chain_pem)?;
        if parsed.certificate_expires_at != self.certificate_expires_at {
            return Err(invalid_state(
                path,
                "active credential expiry does not match the leaf certificate",
            ));
        }
        AgentControlEndpoint::from_pem(
            "https://credential-validation.invalid".to_owned(),
            self.certificate_chain_pem.clone(),
            self.private_key_pem.clone(),
            self.trust_bundle_pem.clone(),
        )
        .map(|_| ())
    }
}

/// Pending renewal key and deterministic CSR bytes retained across retry.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PendingRenewal {
    private_key_pem: Vec<u8>,
    csr_der: Vec<u8>,
}

impl PendingRenewal {
    fn generate(path: &Path) -> Result<Self, AgentError> {
        let key = KeyPair::generate()
            .map_err(|_| invalid_state(path, "renewal key could not be generated"))?;
        let csr = CertificateParams::new(Vec::new())
            .map_err(|_| invalid_state(path, "renewal CSR parameters are invalid"))?
            .serialize_request(&key)
            .map_err(|_| invalid_state(path, "renewal CSR could not be generated"))?;
        let pending = Self {
            private_key_pem: key.serialize_pem().into_bytes(),
            csr_der: csr.der().to_vec(),
        };
        pending.validate(path)?;
        Ok(pending)
    }

    /// Returns the DER-encoded certificate signing request.
    pub fn csr_der(&self) -> &[u8] {
        &self.csr_der
    }

    fn validate(&self, path: &Path) -> Result<(), AgentError> {
        validate_key_and_csr(
            path,
            &self.private_key_pem,
            &self.csr_der,
            "pending renewal material is invalid",
        )
    }
}

/// Certificate material returned by a successful renewal.
#[derive(Clone, Debug)]
pub struct RenewedCredential {
    /// Leaf-first client certificate chain.
    pub certificate_chain_pem: Vec<u8>,
    /// Gateway trust anchors.
    pub trust_bundle_pem: Vec<u8>,
    /// CA-scoped certificate serial.
    pub certificate_serial: String,
    /// Leaf certificate expiry.
    pub certificate_expires_at: DateTime<Utc>,
}

struct ParsedLeafCertificate {
    certificate_serial: String,
    certificate_expires_at: DateTime<Utc>,
}

fn parse_leaf_certificate(
    certificate_chain_pem: &[u8],
) -> Result<ParsedLeafCertificate, AgentError> {
    let mut certificates = rustls_pki_types::CertificateDer::pem_slice_iter(certificate_chain_pem);
    let certificate = certificates
        .next()
        .transpose()
        .map_err(|_| AgentError::InvalidTransport("client certificate chain is malformed".into()))?
        .ok_or_else(|| AgentError::InvalidTransport("client certificate chain is empty".into()))?;
    let (remainder, parsed) = x509_parser::parse_x509_certificate(certificate.as_ref())
        .map_err(|_| AgentError::InvalidTransport("client leaf certificate is invalid".into()))?;
    if !remainder.is_empty() {
        return Err(AgentError::InvalidTransport(
            "client leaf certificate is invalid".into(),
        ));
    }
    let expires_at = DateTime::from_timestamp(parsed.validity().not_after.timestamp(), 0)
        .ok_or_else(|| AgentError::InvalidTransport("client leaf expiry is invalid".into()))?;
    Ok(ParsedLeafCertificate {
        certificate_serial: parsed.tbs_certificate.raw_serial_as_string(),
        certificate_expires_at: expires_at,
    })
}

fn valid_serial(value: &str) -> bool {
    value.trim() == value
        && !value.is_empty()
        && value.chars().count() <= 256
        && !value.chars().any(char::is_control)
}

fn validate_key_and_csr(
    path: &Path,
    private_key_pem: &[u8],
    csr_der: &[u8],
    message: &str,
) -> Result<(), AgentError> {
    if private_key_pem.is_empty()
        || private_key_pem.len() > MAX_PRIVATE_KEY_BYTES
        || csr_der.is_empty()
        || csr_der.len() > MAX_CSR_DER_BYTES
        || rustls_pki_types::PrivateKeyDer::pem_slice_iter(private_key_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_or(true, |keys| keys.len() != 1)
    {
        return Err(invalid_state(path, message));
    }
    Ok(())
}

fn open_private_new_file(path: &Path) -> Result<fs::File, AgentError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    set_private_mode(&mut options);
    options.open(path).map_err(|error| state_io(path, error))
}

#[cfg(unix)]
fn set_private_mode(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn set_private_mode(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn validate_private_permissions(path: &Path, metadata: &fs::Metadata) -> Result<(), AgentError> {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(invalid_state(
            path,
            "credential state must not be accessible by group or other users",
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_private_permissions(_path: &Path, _metadata: &fs::Metadata) -> Result<(), AgentError> {
    Ok(())
}

fn sync_parent(path: &Path) -> Result<(), AgentError> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| state_io(path, error))
}

fn invalid_state(path: &Path, message: impl Into<String>) -> AgentError {
    AgentError::InvalidState {
        path: path.to_path_buf(),
        message: message.into(),
    }
}

fn state_io(path: &Path, source: std::io::Error) -> AgentError {
    AgentError::StateIo {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::str;

    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    };
    use uuid::Uuid;

    use super::{
        BootstrapCredential, CredentialStore, EnrollmentCredentialRequest, RenewedCredential,
        parse_leaf_certificate,
    };

    struct TestCredential {
        chain: Vec<u8>,
        key: Vec<u8>,
        trust: Vec<u8>,
    }

    fn test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ventstream-fleet-agent-{name}-{}.json",
            Uuid::now_v7()
        ))
    }

    fn test_credential() -> Result<TestCredential, Box<dyn std::error::Error>> {
        let leaf_key = KeyPair::generate()?;
        test_credential_for_leaf_key(&leaf_key)
    }

    fn test_credential_for_leaf_key(
        leaf_key: &KeyPair,
    ) -> Result<TestCredential, Box<dyn std::error::Error>> {
        let root_key = KeyPair::generate()?;
        let mut root_parameters = CertificateParams::new(Vec::new())?;
        root_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let root = root_parameters.self_signed(&root_key)?;
        let issuer = Issuer::new(root_parameters, root_key);

        let mut leaf_parameters = CertificateParams::new(Vec::new())?;
        leaf_parameters
            .extended_key_usages
            .push(ExtendedKeyUsagePurpose::ClientAuth);
        let leaf = leaf_parameters.signed_by(&leaf_key, &issuer)?;
        Ok(TestCredential {
            chain: format!("{}{}", leaf.pem(), root.pem()).into_bytes(),
            key: leaf_key.serialize_pem().into_bytes(),
            trust: root.pem().into_bytes(),
        })
    }

    #[test]
    fn bootstraps_credentials_and_reuses_pending_renewal() -> Result<(), Box<dyn std::error::Error>>
    {
        let path = test_path("bootstrap");
        let credential = test_credential()?;
        let store = CredentialStore::new(path.clone())?;
        let state = store.load_or_bootstrap(BootstrapCredential {
            certificate_chain_pem: credential.chain,
            private_key_pem: credential.key,
            trust_bundle_pem: credential.trust,
            certificate_serial: Some("ca-serial-1".to_owned()),
        })?;

        assert_eq!(state.active().certificate_serial(), "ca-serial-1");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let metadata = fs::metadata(&path)?;
            assert_eq!(metadata.permissions().mode() & 0o077, 0);
        }
        let (state, first) = store.ensure_pending_renewal(state)?;
        let (_, second) = store.ensure_pending_renewal(state)?;
        assert_eq!(first.csr_der(), second.csr_der());
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn activates_renewal_with_the_pending_private_key() -> Result<(), Box<dyn std::error::Error>> {
        let path = test_path("activate");
        let credential = test_credential()?;
        let store = CredentialStore::new(path.clone())?;
        let state = store.load_or_bootstrap(BootstrapCredential {
            certificate_chain_pem: credential.chain,
            private_key_pem: credential.key,
            trust_bundle_pem: credential.trust,
            certificate_serial: Some("ca-serial-1".to_owned()),
        })?;
        let (state, pending) = store.ensure_pending_renewal(state)?;
        let pending_key = KeyPair::from_pem(str::from_utf8(&pending.private_key_pem)?)?;
        let renewed = test_credential_for_leaf_key(&pending_key)?;
        let renewed_expiry = parse_leaf_certificate(&renewed.chain)?.certificate_expires_at;
        let activated = store.activate_renewal(
            state,
            RenewedCredential {
                certificate_chain_pem: renewed.chain,
                trust_bundle_pem: renewed.trust,
                certificate_serial: "ca-serial-2".to_owned(),
                certificate_expires_at: renewed_expiry,
            },
        )?;

        assert_eq!(activated.active().certificate_serial(), "ca-serial-2");
        let reloaded = store.load()?.ok_or("missing credential state")?;
        assert_eq!(reloaded.active().certificate_serial(), "ca-serial-2");
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn enrollment_request_generates_bounded_csr_and_private_key()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = test_path("enrollment-request");
        let request = EnrollmentCredentialRequest::generate(&path)?;
        assert!(!request.csr_der().is_empty());
        assert!(request.csr_der().len() <= super::MAX_CSR_DER_BYTES);
        assert!(!request.private_key_pem().is_empty());
        assert!(request.private_key_pem().len() <= super::MAX_PRIVATE_KEY_BYTES);
        Ok(())
    }
}
