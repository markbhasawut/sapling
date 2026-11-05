/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! Mutual TLS 1.3 for both ends, with `openssl`, as Mononoke's other servers
//! use it.
//!
//! Each end trusts only the CA it is given, not the system's, and requires
//! the other to present a certificate that CA signed. Both negotiate ALPN
//! `h2`. Neither keeps sessions to resume, so every connection makes a full
//! handshake and checks the peer's certificate again, expiry included.

use std::path::Path;
use std::pin::Pin;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use openssl::pkey::PKey;
use openssl::pkey::Private;
use openssl::ssl::AlpnError;
use openssl::ssl::Ssl;
use openssl::ssl::SslAcceptor;
use openssl::ssl::SslConnector;
use openssl::ssl::SslContextBuilder;
use openssl::ssl::SslMethod;
use openssl::ssl::SslOptions;
use openssl::ssl::SslSessionCacheMode;
use openssl::ssl::SslVerifyMode;
use openssl::ssl::SslVersion;
use openssl::ssl::select_next_proto;
use openssl::x509::X509;
use openssl::x509::store::X509StoreBuilder;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio_openssl::SslStream;

/// ALPN protocol list holding only HTTP/2.
const ALPN_H2: &[u8] = b"\x02h2";

/// Certificate, key and CA for a server: it presents the certificate, and
/// accepts only clients whose certificates the CA signed.
#[derive(Clone)]
pub struct ServerTls {
    acceptor: SslAcceptor,
}

impl ServerTls {
    /// Read PEM files, as given to Mononoke's `--tls-ca`,
    /// `--tls-certificate` and `--tls-private-key`. The certificate file may
    /// hold a chain, leaf first.
    pub fn from_files(
        ca: impl AsRef<Path>,
        certificate: impl AsRef<Path>,
        private_key: impl AsRef<Path>,
    ) -> Result<Self> {
        Self::from_pem(
            &read(ca.as_ref())?,
            &read(certificate.as_ref())?,
            &read(private_key.as_ref())?,
        )
    }

    pub fn from_pem(ca: &[u8], certificate: &[u8], private_key: &[u8]) -> Result<Self> {
        let mut builder = SslAcceptor::mozilla_modern_v5(SslMethod::tls_server())
            .context("failed to create a TLS acceptor")?;
        configure_context(&mut builder, ca, certificate, private_key)?;
        builder.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
        builder.set_alpn_select_callback(|_, offered| {
            select_next_proto(ALPN_H2, offered).ok_or(AlpnError::ALERT_FATAL)
        });
        Ok(Self {
            acceptor: builder.build(),
        })
    }

    /// Make the server side of a TLS handshake on `io`, and return the
    /// stream with the client's certificate.
    pub(crate) async fn accept<S>(&self, io: S) -> Result<(SslStream<S>, X509)>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let ssl = Ssl::new(self.acceptor.context()).context("failed to create a TLS session")?;
        let mut stream = SslStream::new(ssl, io).context("failed to create a TLS stream")?;
        Pin::new(&mut stream)
            .accept()
            .await
            .context("TLS handshake with client failed")?;
        check_alpn(&stream)?;
        let certificate = stream
            .ssl()
            .peer_certificate()
            .context("client presented no certificate")?;
        Ok((stream, certificate))
    }
}

/// Certificate, key and CA for a client: it presents the certificate, and
/// accepts only servers whose certificates the CA signed for the name it
/// connects to.
#[derive(Clone)]
pub struct ClientTls {
    connector: SslConnector,
}

impl ClientTls {
    /// Read PEM files. The certificate file may hold a chain, leaf first.
    pub fn from_files(
        ca: impl AsRef<Path>,
        certificate: impl AsRef<Path>,
        private_key: impl AsRef<Path>,
    ) -> Result<Self> {
        Self::from_pem(
            &read(ca.as_ref())?,
            &read(certificate.as_ref())?,
            &read(private_key.as_ref())?,
        )
    }

    pub fn from_pem(ca: &[u8], certificate: &[u8], private_key: &[u8]) -> Result<Self> {
        let mut builder = SslConnector::builder(SslMethod::tls_client())
            .context("failed to create a TLS connector")?;
        builder
            .set_min_proto_version(Some(SslVersion::TLS1_3))
            .context("failed to require TLS 1.3")?;
        configure_context(&mut builder, ca, certificate, private_key)?;
        builder
            .set_alpn_protos(ALPN_H2)
            .context("failed to offer ALPN h2")?;
        Ok(Self {
            connector: builder.build(),
        })
    }

    #[cfg(test)]
    pub(crate) fn from_connector(connector: SslConnector) -> Self {
        Self { connector }
    }

    /// Make the client side of a TLS handshake on `io`, checking that the
    /// server's certificate is valid for `server_name`, a host name or an IP
    /// address.
    pub(crate) async fn connect<S>(&self, server_name: &str, io: S) -> Result<SslStream<S>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let ssl = self
            .connector
            .configure()
            .context("failed to configure a TLS session")?
            .into_ssl(server_name)
            .context("failed to create a TLS session")?;
        let mut stream = SslStream::new(ssl, io).context("failed to create a TLS stream")?;
        Pin::new(&mut stream)
            .connect()
            .await
            .with_context(|| format!("TLS handshake with {server_name} failed"))?;
        check_alpn(&stream)?;
        Ok(stream)
    }
}

/// What both ends share: the trust store, the certificate and key, and no
/// session resumption.
fn configure_context(
    builder: &mut SslContextBuilder,
    ca: &[u8],
    certificate: &[u8],
    private_key: &[u8],
) -> Result<()> {
    let mut store = X509StoreBuilder::new().context("failed to create a certificate store")?;
    let ca_certificates = X509::stack_from_pem(ca).context("failed to parse the CA file")?;
    if ca_certificates.is_empty() {
        bail!("the CA file holds no certificates");
    }
    for ca_certificate in ca_certificates {
        store
            .add_cert(ca_certificate)
            .context("failed to add a CA certificate")?;
    }
    // Replaces the default store, so that only this CA is trusted.
    builder.set_cert_store(store.build());

    let mut chain = X509::stack_from_pem(certificate)
        .context("failed to parse the certificate file")?
        .into_iter();
    let leaf = chain
        .next()
        .context("the certificate file holds no certificates")?;
    builder
        .set_certificate(&leaf)
        .context("failed to use the certificate")?;
    for intermediate in chain {
        builder
            .add_extra_chain_cert(intermediate)
            .context("failed to add an intermediate certificate")?;
    }
    let key: PKey<Private> =
        PKey::private_key_from_pem(private_key).context("failed to parse the private key")?;
    builder
        .set_private_key(&key)
        .context("failed to use the private key")?;
    builder
        .check_private_key()
        .context("the private key does not match the certificate")?;

    builder.set_session_cache_mode(SslSessionCacheMode::OFF);
    builder.set_options(SslOptions::NO_TICKET);
    Ok(())
}

fn check_alpn<S>(stream: &SslStream<S>) -> Result<()> {
    match stream.ssl().selected_alpn_protocol() {
        Some(b"h2") => Ok(()),
        Some(other) => bail!(
            "peer negotiated ALPN {:?} instead of h2",
            String::from_utf8_lossy(other)
        ),
        None => bail!("peer did not negotiate ALPN h2"),
    }
}

fn read(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))
}
