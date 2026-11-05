/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! OSS backend, which serves over `oss_thrift_transport`: mutual TLS on any
//! address when given certificates, and otherwise plaintext on loopback.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use bytes::Bytes;
use fbinit::FacebookInit;
use fbthrift::ApplicationException;
use fbthrift::ProtocolID;
use fbthrift::processor::ThriftService;
pub use oss_thrift_transport::ServerTls;
pub use oss_thrift_transport::server::Limits;
use oss_thrift_transport::server::ReplyState;
use oss_thrift_transport::server::ServerConfig;
use oss_thrift_transport::server::ServiceFactory;
use oss_thrift_transport::server::box_service;
use tls::TLSArgs;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::config::LoadMonitorConfig;

/// Servers listen on a loopback address unless told otherwise. Without TLS
/// certificates, they refuse any other address.
pub const DEFAULT_THRIFT_HOST: &str = "127.0.0.1";

/// Request context passed to services: the identities in the caller's TLS
/// certificate, if it connected over TLS.
pub type RequestContext = oss_thrift_transport::server::RequestContext;

/// How long [`ThriftServerShutdownHandle::stop`] waits for connections to
/// finish their current request before aborting them.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// A running server. Dropping it, or its shutdown handle, stops the server
/// as [`ThriftServerShutdownHandle::stop`] does, but without waiting.
pub struct MononokeThriftServer {
    bound_address: SocketAddr,
    shutdown: ThriftServerShutdownHandle,
}

impl MononokeThriftServer {
    pub fn bound_address(&mut self) -> Result<SocketAddr> {
        Ok(self.bound_address)
    }

    pub fn into_shutdown_handle(self) -> ThriftServerShutdownHandle {
        self.shutdown
    }
}

pub struct ThriftServerShutdownHandle {
    shutdown_tx: oneshot::Sender<()>,
    server: JoinHandle<()>,
    runtime: Handle,
}

impl ThriftServerShutdownHandle {
    /// Stop accepting connections and wait up to `SHUTDOWN_TIMEOUT` for
    /// each open connection to finish the request it is serving, then abort
    /// any that have not. A streaming reply counts as one request.
    ///
    /// Must be called from a blocking context (e.g. `tokio::task::spawn_blocking`).
    pub fn stop(self) {
        let Self {
            shutdown_tx,
            mut server,
            runtime,
        } = self;
        let _ = shutdown_tx.send(());
        match runtime.block_on(timeout(SHUTDOWN_TIMEOUT, &mut server)) {
            Ok(Ok(())) => {}
            Ok(Err(join_error)) => error!("thrift server task failed: {join_error:#}"),
            Err(_) => {
                error!("thrift server did not stop within {SHUTDOWN_TIMEOUT:?}; aborting it");
                server.abort();
                let _ = runtime.block_on(server);
            }
        }
    }
}

/// Builds an OSS Thrift server.
///
/// This has the same methods as the fbcode builder, apart from those that
/// configure fbcode-only infrastructure, plus [`Self::with_tls_certificates`]
/// and [`Self::with_limits`], which configure the OSS transport. Methods for
/// features the OSS transport lacks are accepted and ignored.
pub struct ThriftServerBuilder {
    service_name: Option<String>,
    host: String,
    port: u16,
    factory: Option<Arc<ServiceFactory>>,
    handle: Option<Handle>,
    tls_requested: bool,
    config: ServerConfig,
}

impl ThriftServerBuilder {
    pub fn new(_fb: FacebookInit) -> Self {
        Self {
            service_name: None,
            host: DEFAULT_THRIFT_HOST.to_owned(),
            port: 0,
            factory: None,
            handle: None,
            tls_requested: false,
            config: ServerConfig::default(),
        }
    }

    pub fn service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = Some(name.into());
        self
    }

    /// The host to listen on. Without TLS certificates, it must resolve only
    /// to loopback addresses.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Serve TLS, with the certificates given to
    /// [`Self::with_tls_certificates`]. Without them, the server serves
    /// plaintext on loopback only, with a warning.
    pub fn with_tls(mut self) -> Self {
        self.tls_requested = true;
        self
    }

    /// Serve mutual TLS with this certificate, key and CA, as a server's
    /// `--tls-*` arguments give them. Clients must present a certificate the
    /// CA signed, and services see the identities in it.
    pub fn with_tls_certificates(mut self, tls: ServerTls) -> Self {
        self.config.tls = Some(tls);
        self
    }

    /// Serve mutual TLS with the files a server's `--tls-ca`,
    /// `--tls-certificate` and `--tls-private-key` name, if they are given.
    pub fn with_tls_args(self, args: &TLSArgs) -> Result<Self> {
        if args.tls_certificate.is_empty() {
            return Ok(self);
        }
        if args.disable_mtls {
            anyhow::bail!("thrift servers require mutual TLS; --disable-mtls is not supported");
        }
        let tls = ServerTls::from_files(&args.tls_ca, &args.tls_certificate, &args.tls_private_key)
            .context("failed to load the thrift server's TLS certificates")?;
        Ok(self.with_tls_certificates(tls))
    }

    /// The OSS transport's limits and timeouts.
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.config.limits = limits;
        self
    }

    /// Stop a request's handler when its client resets the request.
    pub fn with_cancel_on_client_disconnect(mut self) -> Self {
        self.config.cancel_on_client_disconnect = true;
        self
    }

    /// Accepted and ignored: profiling is a ServiceFramework module.
    pub fn with_profile_module(self) -> Self {
        self
    }

    /// Accepted and ignored: load monitoring is a ServiceFramework module.
    pub fn with_load_monitor(self, _config: LoadMonitorConfig) -> Self {
        self
    }

    /// The runtime to serve connections on. Defaults to the runtime that
    /// calls [`Self::build`].
    pub fn with_handle(mut self, handle: Handle) -> Self {
        self.handle = Some(handle);
        self
    }

    /// Accepted and ignored: the OSS transport does not propagate context.
    pub fn without_context_prop_module(self) -> Self {
        self
    }

    /// Set the service factory.
    ///
    /// `f` is called once to produce a protocol selector, which is then
    /// called for each connection to create that connection's service. The
    /// executor is only used by the fbcode backend.
    pub fn factory<S, FF, F, H>(mut self, _exec: S, f: FF) -> Self
    where
        S: 'static,
        FF: FnOnce() -> F + Send + Sync + 'static + Clone,
        F: Fn(
                ProtocolID,
            ) -> Result<
                Box<
                    dyn ThriftService<
                            Bytes,
                            Handler = H,
                            RequestContext = RequestContext,
                            ReplyState = ReplyState,
                        > + Send
                        + 'static,
                >,
                ApplicationException,
            > + Send
            + Sync
            + 'static,
        H: 'static,
    {
        let selector = f();
        self.factory = Some(Arc::new(move |proto| Ok(box_service(selector(proto)?))));
        self
    }

    pub fn build(self) -> Result<MononokeThriftServer> {
        let service_name = self.service_name.context("service_name is required")?;
        let factory = self
            .factory
            .context("factory is required: call .factory() before .build()")?;
        let runtime = match self.handle {
            Some(handle) => handle,
            None => Handle::try_current().context(
                "build() must be called from a tokio runtime unless with_handle() is used",
            )?,
        };
        if self.tls_requested && self.config.tls.is_none() {
            warn!(
                "TLS was requested for {service_name}, but no certificates were given; serving plaintext, on loopback addresses only"
            );
        }

        let listener =
            oss_thrift_transport::server::bind_with(&self.host, self.port, &self.config)?;
        let listener = {
            let _runtime = runtime.enter();
            tokio::net::TcpListener::from_std(listener)
                .context("failed to register the listener with the runtime")?
        };
        let bound_address = listener
            .local_addr()
            .context("failed to get the bound address")?;
        info!(
            "{service_name} thrift server listening on {bound_address}{}",
            if self.config.tls.is_some() {
                " with TLS"
            } else {
                ""
            }
        );

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = runtime.spawn(oss_thrift_transport::server::serve_with(
            listener,
            factory,
            shutdown_rx,
            self.config,
        ));

        Ok(MononokeThriftServer {
            bound_address,
            shutdown: ThriftServerShutdownHandle {
                shutdown_tx,
                server,
                runtime,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use fb303_core::fb303_status;
    use fb303_core_clients::make_BaseService;
    use fb303_core_services::BaseService;
    use fb303_core_services::errors::GetNameExn;
    use fb303_core_services::errors::GetStatusDetailsExn;
    use fb303_core_services::errors::GetStatusExn;
    use fb303_core_services::make_BaseService_server;
    use mononoke_macros::mononoke;
    use openssl::asn1::Asn1Time;
    use openssl::ec::EcGroup;
    use openssl::ec::EcKey;
    use openssl::hash::MessageDigest;
    use openssl::nid::Nid;
    use openssl::pkey::PKey;
    use openssl::pkey::Private;
    use openssl::x509::X509;
    use openssl::x509::X509Builder;
    use openssl::x509::X509NameBuilder;
    use openssl::x509::extension::BasicConstraints;
    use openssl::x509::extension::SubjectAlternativeName;
    use oss_thrift_transport::ClientTls;
    use oss_thrift_transport::client::ClientConfig;

    use super::*;

    #[derive(Clone)]
    struct NamedService;

    #[async_trait]
    impl BaseService for NamedService {
        async fn getName(&self) -> Result<String, GetNameExn> {
            Ok("test_service".to_owned())
        }

        async fn getStatus(&self) -> Result<fb303_status, GetStatusExn> {
            Ok(fb303_status::ALIVE)
        }

        async fn getStatusDetails(&self) -> Result<String, GetStatusDetailsExn> {
            Ok(String::new())
        }
    }

    fn build(fb: FacebookInit, runtime: Handle, host: &str) -> Result<MononokeThriftServer> {
        ThriftServerBuilder::new(fb)
            .service_name("test_service")
            .host(host)
            .with_handle(runtime.clone())
            .factory(runtime, move || {
                move |proto| make_BaseService_server(proto, NamedService)
            })
            .build()
    }

    #[mononoke::fbinit_test]
    async fn serves_until_stopped(fb: FacebookInit) -> Result<()> {
        let mut server = build(fb, Handle::current(), "::1")?;
        let address = server.bound_address()?;
        assert!(address.ip().is_loopback());

        let client =
            oss_thrift_transport::client::connect::<make_BaseService>(&address.to_string())?;
        assert_eq!(client.getName().await?, "test_service");

        let shutdown = server.into_shutdown_handle();
        tokio::task::spawn_blocking(move || shutdown.stop()).await?;
        assert!(
            client.getName().await.is_err(),
            "a stopped server should refuse calls"
        );
        Ok(())
    }

    #[mononoke::fbinit_test]
    async fn refuses_non_loopback_hosts_without_tls(fb: FacebookInit) {
        assert!(build(fb, Handle::current(), "0.0.0.0").is_err());
        assert!(build(fb, Handle::current(), "::").is_err());
    }

    /// A self-signed CA and a certificate it signs, as PEM.
    fn certificate(
        ca: Option<&(X509, PKey<Private>)>,
        subject: &str,
        names: impl FnOnce(&mut SubjectAlternativeName),
    ) -> (X509, PKey<Private>) {
        let key = PKey::from_ec_key(
            EcKey::generate(&EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap()).unwrap(),
        )
        .unwrap();
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", subject).unwrap();
        name.append_entry_by_text("O", "mononoke-local").unwrap();
        let name = name.build();
        let mut builder = X509Builder::new().unwrap();
        builder.set_version(2).unwrap();
        builder.set_subject_name(&name).unwrap();
        builder
            .set_issuer_name(ca.map_or(&*name, |(ca, _)| ca.subject_name()))
            .unwrap();
        builder.set_pubkey(&key).unwrap();
        builder
            .set_not_before(&Asn1Time::days_from_now(0).unwrap())
            .unwrap();
        builder
            .set_not_after(&Asn1Time::days_from_now(1).unwrap())
            .unwrap();
        if ca.is_none() {
            builder
                .append_extension(BasicConstraints::new().critical().ca().build().unwrap())
                .unwrap();
        } else {
            let mut san = SubjectAlternativeName::new();
            names(&mut san);
            let san = san
                .build(&builder.x509v3_context(ca.map(|(ca, _)| &**ca), None))
                .unwrap();
            builder.append_extension(san).unwrap();
        }
        builder
            .sign(ca.map_or(&key, |(_, key)| key), MessageDigest::sha256())
            .unwrap();
        (builder.build(), key)
    }

    fn pem((certificate, key): &(X509, PKey<Private>)) -> (Vec<u8>, Vec<u8>) {
        (
            certificate.to_pem().unwrap(),
            key.private_key_to_pem_pkcs8().unwrap(),
        )
    }

    #[mononoke::fbinit_test]
    async fn serves_tls_on_any_address(fb: FacebookInit) -> Result<()> {
        let ca = certificate(None, "ca", |_| {});
        let (ca_pem, _) = pem(&ca);
        let (server_cert, server_key) = pem(&certificate(Some(&ca), "server", |san| {
            san.ip("127.0.0.1");
        }));
        let (client_cert, client_key) = pem(&certificate(Some(&ca), "alice", |san| {
            san.uri("user:alice");
        }));

        let runtime = Handle::current();
        let mut server = ThriftServerBuilder::new(fb)
            .service_name("test_service")
            .host("0.0.0.0")
            .with_tls()
            .with_tls_certificates(ServerTls::from_pem(&ca_pem, &server_cert, &server_key)?)
            .with_handle(runtime.clone())
            .factory(runtime, move || {
                move |proto| make_BaseService_server(proto, NamedService)
            })
            .build()?;
        let address = format!("127.0.0.1:{}", server.bound_address()?.port());

        let client = oss_thrift_transport::client::connect_with::<make_BaseService>(
            &address,
            ClientConfig {
                tls: Some(ClientTls::from_pem(&ca_pem, &client_cert, &client_key)?),
                ..ClientConfig::default()
            },
        )?;
        assert_eq!(client.getName().await?, "test_service");

        let plaintext = oss_thrift_transport::client::connect::<make_BaseService>(&address)?;
        assert!(
            plaintext.getName().await.is_err(),
            "a TLS server should refuse plaintext"
        );

        let shutdown = server.into_shutdown_handle();
        tokio::task::spawn_blocking(move || shutdown.stop()).await?;
        Ok(())
    }
}
