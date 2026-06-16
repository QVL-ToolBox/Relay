//! TLS support (V2): builds a rustls server configuration from PEM files so the
//! broker can expose a secure **mqtts** listener.
//!
//! Uses the `ring` crypto provider explicitly (via `builder_with_provider`), so
//! no process-wide default provider has to be installed and the build stays
//! free of cmake/NASM on Windows. The resulting [`TlsAcceptor`] wraps an accepted
//! `TcpStream` into a `TlsStream`, which — being `AsyncRead + AsyncWrite` — drops
//! straight into the same [`crate::connection::handle`] loop as plain TCP.

use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

/// Errors while preparing the TLS configuration.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("failed to read TLS file: {0}")]
    Io(#[from] std::io::Error),
    #[error("no private key found in key file")]
    NoKey,
    #[error("no certificate found in cert file")]
    NoCert,
    #[error("rustls error: {0}")]
    Rustls(#[from] rustls::Error),
}

/// Build a [`TlsAcceptor`] from a PEM certificate chain and a PEM private key.
pub fn acceptor(cert_path: &Path, key_path: &Path) -> Result<TlsAcceptor, TlsError> {
    let cert_pem = std::fs::read(cert_path)?;
    let key_pem = std::fs::read(key_path)?;

    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut cert_pem.as_slice()).collect::<Result<_, _>>()?;
    if certs.is_empty() {
        return Err(TlsError::NoCert);
    }
    let key: PrivateKeyDer<'static> =
        rustls_pemfile::private_key(&mut key_pem.as_slice())?.ok_or(TlsError::NoKey)?;

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("ring provider supports the default protocol versions")
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}
