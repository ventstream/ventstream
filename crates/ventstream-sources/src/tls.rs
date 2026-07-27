//! Shared database transport-security policy.

use std::path::PathBuf;

/// TLS policy applied consistently to every connection opened by a source.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DatabaseTlsConfig {
    /// Transport mode.
    pub mode: DatabaseTlsMode,
    /// Optional PEM CA bundle for a private certificate authority.
    pub ca_file: Option<PathBuf>,
}

/// Database TLS modes supported by the engine.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DatabaseTlsMode {
    /// Require TLS and validate both the certificate chain and hostname.
    #[default]
    VerifyFull,
    /// Use an unencrypted connection.
    Disabled,
}

/// Select the workspace's Rustls provider before a connector builds a TLS client.
///
/// Installing an already-selected provider is harmless. Keeping this beside the
/// shared policy also makes connector crates safe to use outside the engine
/// binary, where `main` has not initialized Rustls for them.
pub(crate) fn ensure_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
