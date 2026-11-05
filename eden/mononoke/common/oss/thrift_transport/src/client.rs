/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! Client end of the transport.
//!
//! A [`ClientTransport`] keeps one HTTP/2 connection to its server, over TLS
//! unless the server is on loopback, and sends each call as a stream on it,
//! so calls run concurrently up to the server's limit.
//!
//! A call is sent again only when the server certainly did not process it:
//! when the connection or its handshake failed before the request was sent,
//! when the server refused the stream (`REFUSED_STREAM`), or when the server
//! said in a `GOAWAY` that it would not process the stream. Any other failure
//! after the request was sent fails the call: the server may have acted on
//! it, and some methods are writes.

use std::ffi::CStr;
use std::future::Future;
use std::future::poll_fn;
use std::io::Cursor;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::task::Poll;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use bytes::Bytes;
use bytes::BytesMut;
use fbthrift::BinaryProtocol;
use fbthrift::ClientFactory;
use fbthrift::ClientStreamElement;
use fbthrift::Framing;
use fbthrift::FramingDecoded;
use fbthrift::FramingEncodedFinal;
use fbthrift::Transport;
use futures::future::FutureExt;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use h2::Reason;
use h2::RecvStream;
use h2::client::SendRequest;
use mononoke_macros::mononoke;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio::time::timeout;
use tracing::debug;

use crate::framing::FrameDecoder;
use crate::framing::MAX_FRAME_SIZE;
use crate::framing::StreamFrame;
use crate::loopback::resolve;
use crate::tls::ClientTls;

/// How a client connects, and how long it waits.
#[derive(Clone)]
pub struct ClientConfig {
    /// Mutual TLS. Without it, the client only connects to loopback.
    pub tls: Option<ClientTls>,
    /// Time to connect and finish the TLS and HTTP/2 handshakes.
    pub connect_timeout: Duration,
    /// Time for a call's reply, or a stream's initial response. A call that
    /// times out is not sent again.
    pub request_timeout: Option<Duration>,
    pub max_request_size: usize,
    pub max_response_size: usize,
    /// Times a call is sent, at most, when the server certainly did not
    /// process it.
    pub max_attempts: usize,
    /// Wait before the first resend; it doubles for each further one.
    pub retry_backoff: Duration,
    /// Flow-control windows the client gives the server to reply in.
    pub stream_window: u32,
    pub connection_window: u32,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            tls: None,
            connect_timeout: Duration::from_secs(10),
            request_timeout: None,
            max_request_size: MAX_FRAME_SIZE,
            max_response_size: MAX_FRAME_SIZE,
            max_attempts: 4,
            retry_backoff: Duration::from_millis(50),
            stream_window: 1024 * 1024,
            connection_window: 16 * 1024 * 1024,
        }
    }
}

static DEFAULT_CONFIG: OnceLock<ClientConfig> = OnceLock::new();

/// Set the configuration [`connect`] and [`ClientTransport::new`] use, such
/// as TLS from a binary's `--tls-*` arguments, so that the clients built
/// deep inside a server pick it up. It can be set once, before connecting.
pub fn set_default_config(config: ClientConfig) -> Result<()> {
    DEFAULT_CONFIG
        .set(config)
        .map_err(|_| anyhow!("the default OSS thrift client configuration is already set"))
}

fn default_config() -> ClientConfig {
    DEFAULT_CONFIG.get().cloned().unwrap_or_default()
}

/// Create a client for the service `F` that talks to a server at `address`,
/// a `host:port` string, with the configuration [`set_default_config`] set,
/// or else over plaintext, which only loopback addresses accept.
pub fn connect<F>(address: &str) -> Result<Arc<F::Api>>
where
    F: ClientFactory,
{
    connect_with::<F>(address, default_config())
}

/// Create a client for the service `F` that talks to a server at `address`,
/// a `host:port` string, with `config`.
pub fn connect_with<F>(address: &str, config: ClientConfig) -> Result<Arc<F::Api>>
where
    F: ClientFactory,
{
    let transport = ClientTransport::with_config(address, config)?;
    Ok(F::new(BinaryProtocol, transport))
}

/// Client transport for unary and streaming RPCs.
#[derive(Clone)]
pub struct ClientTransport {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for ClientTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientTransport")
            .field("address", &self.inner.address)
            .field("tls", &self.inner.config.tls.is_some())
            .finish()
    }
}

struct Inner {
    address: String,
    /// The host part of the address, which the server's certificate must
    /// be valid for.
    server_name: String,
    resolved: Vec<SocketAddr>,
    config: ClientConfig,
    connection: Mutex<Option<Connection>>,
}

/// An open connection, numbered so that a caller that finds it broken
/// doesn't drop a newer one.
#[derive(Clone)]
struct Connection {
    generation: u64,
    send_request: SendRequest<Bytes>,
}

/// Why an exchange failed, which decides whether it may be tried again.
enum Failure {
    /// The server certainly did not process the request.
    NotProcessed(anyhow::Error),
    /// The server may have processed the request.
    MaybeProcessed(anyhow::Error),
}

impl Failure {
    /// Classify an HTTP/2 error on a stream whose request was sent. `h2`
    /// fails a stream with a remote GOAWAY error only if its id is above the
    /// GOAWAY's last-stream-id, which RFC 9113 §6.8 says the server did not
    /// process; streams at or below it that never finish fail with an I/O
    /// error instead.
    fn after_sending(error: h2::Error) -> Self {
        let refused = error.is_reset() && error.reason() == Some(Reason::REFUSED_STREAM);
        if error.is_remote() && (refused || error.is_go_away()) {
            Self::NotProcessed(anyhow!(error).context("the server did not process the request"))
        } else {
            Self::MaybeProcessed(anyhow!(error))
        }
    }

    fn into_error(self) -> anyhow::Error {
        match self {
            Self::NotProcessed(error) | Self::MaybeProcessed(error) => error,
        }
    }
}

impl ClientTransport {
    /// `address` is a `host:port` string. It is resolved once, here. Uses the
    /// configuration [`set_default_config`] set, or else plaintext, for
    /// which the address must resolve only to loopback addresses.
    pub fn new(address: &str) -> Result<Self> {
        Self::with_config(address, default_config())
    }

    /// `address` is a `host:port` string. It is resolved once, here; without
    /// TLS in `config`, it must resolve only to loopback addresses.
    pub fn with_config(address: &str, config: ClientConfig) -> Result<Self> {
        let resolved = resolve(address, address, config.tls.is_some())?;
        Ok(Self {
            inner: Arc::new(Inner {
                address: address.to_owned(),
                server_name: host_of(address).to_owned(),
                resolved,
                config,
                connection: Mutex::new(None),
            }),
        })
    }

    /// Succeeds if the server accepts a connection, including the TLS and
    /// HTTP/2 handshakes.
    pub async fn probe(&self) -> Result<()> {
        self.connection().await.map(|_| ())
    }

    async fn connection(&self) -> Result<Connection> {
        let mut connection = self.inner.connection.lock().await;
        if let Some(connection) = &*connection {
            return Ok(connection.clone());
        }
        let generation = next_generation();
        let send_request = timeout(self.inner.config.connect_timeout, self.open())
            .await
            .with_context(|| format!("timed out connecting to {}", self.inner.address))??;
        let opened = Connection {
            generation,
            send_request,
        };
        *connection = Some(opened.clone());
        Ok(opened)
    }

    async fn forget(&self, generation: u64) {
        let mut connection = self.inner.connection.lock().await;
        if connection
            .as_ref()
            .is_some_and(|connection| connection.generation == generation)
        {
            *connection = None;
        }
    }

    async fn open(&self) -> Result<SendRequest<Bytes>> {
        let inner = &self.inner;
        let tcp = TcpStream::connect(&*inner.resolved)
            .await
            .with_context(|| format!("failed to connect to {}", inner.address))?;
        // HTTP/2 frames are small and flushed promptly; Nagle would stall
        // them on the peer's delayed ACK.
        tcp.set_nodelay(true)
            .with_context(|| format!("failed to set TCP_NODELAY on {}", inner.address))?;
        match &inner.config.tls {
            Some(tls) => {
                let stream = tls.connect(&inner.server_name, tcp).await?;
                self.handshake(stream).await
            }
            None => self.handshake(tcp).await,
        }
    }

    async fn handshake<T>(&self, io: T) -> Result<SendRequest<Bytes>>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let config = &self.inner.config;
        let (send_request, mut connection) = h2::client::Builder::new()
            .initial_window_size(config.stream_window)
            .initial_connection_window_size(config.connection_window)
            .enable_push(false)
            .handshake::<_, Bytes>(io)
            .await
            .with_context(|| format!("HTTP/2 handshake with {} failed", self.inner.address))?;
        let mut ping_pong = connection
            .ping_pong()
            .context("the connection's pings were already taken")?;
        let address = self.inner.address.clone();
        // The connection closes once every handle to it is dropped.
        mononoke::spawn_task(async move {
            if let Err(error) = connection.await {
                debug!("thrift connection to {address} ended: {error:#}");
            }
        });
        // The handshake returns before the server has said anything, and a
        // TLS 1.3 server rejects a client's certificate only after the
        // client's handshake has finished. A ping's answer shows that the
        // server is serving the connection, before any request is sent on it.
        ping_pong
            .ping(h2::Ping::opaque())
            .await
            .with_context(|| format!("{} did not accept the connection", self.inner.address))?;
        Ok(send_request)
    }

    /// Send `request` and wait for the reply's head. Tries again when the
    /// server certainly did not process it.
    async fn exchange(&self, path: String, request: Bytes) -> Result<ResponseBody> {
        let config = &self.inner.config;
        if request.len() > config.max_request_size {
            bail!(
                "request is {} bytes, over the {} byte limit",
                request.len(),
                config.max_request_size
            );
        }
        let mut backoff = config.retry_backoff;
        let mut attempt = 1;
        loop {
            match self.try_exchange(&path, request.clone()).await {
                Ok(response) => return Ok(response),
                Err(Failure::NotProcessed(error)) if attempt < config.max_attempts => {
                    debug!(
                        "sending {path} to {} again after attempt {attempt}: {error:#}",
                        self.inner.address
                    );
                    attempt += 1;
                    sleep(backoff).await;
                    backoff *= 2;
                }
                Err(failure) => {
                    return Err(failure
                        .into_error()
                        .context(format!("thrift call {path} to {} failed", self.inner.address)));
                }
            }
        }
    }

    async fn try_exchange(&self, path: &str, request: Bytes) -> Result<ResponseBody, Failure> {
        let connection = self.connection().await.map_err(Failure::NotProcessed)?;
        // Failures until `send_request` succeeds leave no stream open, so
        // nothing reached the server.
        let mut send_request = match connection.send_request.clone().ready().await {
            Ok(send_request) => send_request,
            Err(error) => {
                self.forget(connection.generation).await;
                return Err(Failure::NotProcessed(anyhow!(error)));
            }
        };
        let scheme = if self.inner.config.tls.is_some() {
            "https"
        } else {
            "http"
        };
        let head = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("{scheme}://{}{path}", self.inner.address))
            .header(http::header::CONTENT_TYPE, crate::CONTENT_TYPE)
            .body(())
            .map_err(|error| Failure::MaybeProcessed(anyhow!(error)))?;
        let (response, mut body) = match send_request.send_request(head, false) {
            Ok(sent) => sent,
            Err(error) => {
                self.forget(connection.generation).await;
                return Err(Failure::NotProcessed(anyhow!(error)));
            }
        };

        let result = async {
            let mut request = request;
            while !request.is_empty() {
                body.reserve_capacity(request.len());
                let capacity = poll_fn(|cx| {
                    if body.capacity() > 0 {
                        return Poll::Ready(Ok(body.capacity()));
                    }
                    match body.poll_capacity(cx) {
                        Poll::Ready(Some(result)) => Poll::Ready(result),
                        Poll::Ready(None) => Poll::Ready(Ok(0)),
                        Poll::Pending => Poll::Pending,
                    }
                })
                .await
                .map_err(Failure::after_sending)?;
                if capacity == 0 {
                    // The stream closed; the response says why.
                    break;
                }
                let chunk = request.split_to(capacity.min(request.len()));
                body.send_data(chunk, request.is_empty())
                    .map_err(Failure::after_sending)?;
            }
            response.await.map_err(Failure::after_sending)
        };
        let response = match result.await {
            Ok(response) => response,
            Err(failure) => {
                if !matches!(&failure, Failure::NotProcessed(error) if is_refused(error)) {
                    self.forget(connection.generation).await;
                }
                return Err(failure);
            }
        };

        match response.status() {
            http::StatusCode::OK => Ok(ResponseBody::new(
                response.into_body(),
                self.inner.config.max_response_size,
            )),
            http::StatusCode::PAYLOAD_TOO_LARGE => Err(Failure::MaybeProcessed(anyhow!(
                "the request exceeds the server's size limit"
            ))),
            status => Err(Failure::MaybeProcessed(anyhow!(
                "the server answered HTTP {status}"
            ))),
        }
    }

    /// The reply's first frame, within the request timeout.
    async fn first_frame(&self, path: String, request: Bytes) -> Result<(Bytes, ResponseBody)> {
        let call = async {
            let mut body = self.exchange(path, request).await?;
            let first = body
                .next_frame()
                .await?
                .context("server closed the stream without a response")?;
            Ok((first, body))
        };
        match self.inner.config.request_timeout {
            Some(limit) => timeout(limit, call)
                .await
                .with_context(|| format!("no reply from {} within {limit:?}", self.inner.address))?,
            None => call.await,
        }
    }
}

fn is_refused(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<h2::Error>()
        .is_some_and(|error| error.reason() == Some(Reason::REFUSED_STREAM))
}

/// A number for a new connection, unique enough to tell connections apart.
fn next_generation() -> u64 {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// The host of a `host:port` or `[v6]:port` address.
fn host_of(address: &str) -> &str {
    if let Some(rest) = address.strip_prefix('[') {
        if let Some((host, _)) = rest.split_once(']') {
            return host;
        }
    }
    address
        .rsplit_once(':')
        .map_or(address, |(host, _)| host)
}

/// A reply's body, read as the caller asks for frames, so the server can
/// send only as fast as the caller reads.
struct ResponseBody {
    body: RecvStream,
    decoder: FrameDecoder,
    ended: bool,
}

impl ResponseBody {
    fn new(body: RecvStream, max_frame_size: usize) -> Self {
        Self {
            body,
            decoder: FrameDecoder::new(max_frame_size),
            ended: false,
        }
    }

    async fn next_frame(&mut self) -> Result<Option<Bytes>> {
        loop {
            if let Some(frame) = self.decoder.next_frame()? {
                return Ok(Some(frame));
            }
            if self.ended {
                self.decoder.finish()?;
                return Ok(None);
            }
            match self.body.data().await {
                Some(Ok(chunk)) => {
                    self.decoder.push(&chunk);
                    let _ = self.body.flow_control().release_capacity(chunk.len());
                }
                Some(Err(error)) => return Err(anyhow!(error).context("reply stream failed")),
                None => self.ended = true,
            }
        }
    }
}

/// The path a call is sent to, for the server's logs.
fn call_path(service_name: &CStr, fn_name: &CStr) -> String {
    format!(
        "/{}/{}",
        service_name.to_string_lossy(),
        fn_name.to_string_lossy()
    )
}

impl Framing for ClientTransport {
    type EncBuf = BytesMut;
    type DecBuf = Cursor<Bytes>;

    fn enc_with_capacity(cap: usize) -> Self::EncBuf {
        BytesMut::with_capacity(cap)
    }
}

impl Transport for ClientTransport {
    type RpcOptions = ();

    fn call(
        &self,
        service_name: &'static CStr,
        fn_name: &'static CStr,
        req: FramingEncodedFinal<Self>,
        _rpc_options: Self::RpcOptions,
    ) -> Pin<Box<dyn Future<Output = Result<FramingDecoded<Self>>> + Send + 'static>> {
        let transport = self.clone();
        async move {
            let (response, _body) = transport
                .first_frame(call_path(service_name, fn_name), req)
                .await?;
            Ok(Cursor::new(response))
        }
        .boxed()
    }

    fn call_stream(
        &self,
        service_name: &'static CStr,
        fn_name: &'static CStr,
        req: FramingEncodedFinal<Self>,
        _rpc_options: Self::RpcOptions,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<(
                        FramingDecoded<Self>,
                        BoxStream<'static, Result<ClientStreamElement<FramingDecoded<Self>>>>,
                    )>,
                > + Send
                + 'static,
        >,
    > {
        let transport = self.clone();
        async move {
            let (first, body) = transport
                .first_frame(call_path(service_name, fn_name), req)
                .await?;
            // Dropping the stream drops the body, which resets the HTTP/2
            // stream, so the server stops producing.
            let elements = futures::stream::unfold(Some(body), |body| async move {
                let mut body = body?;
                let (element, more) = match body.next_frame().await {
                    Ok(Some(frame)) => match StreamFrame::decode(frame) {
                        Ok(StreamFrame::Success(payload)) => {
                            (Ok(ClientStreamElement::Reply(Cursor::new(payload))), true)
                        }
                        Ok(StreamFrame::DeclaredException(payload)) => {
                            (Ok(ClientStreamElement::DeclaredEx(Cursor::new(payload))), false)
                        }
                        Ok(StreamFrame::ApplicationException(payload)) => (
                            Ok(ClientStreamElement::ApplicationEx(Cursor::new(payload))),
                            false,
                        ),
                        Ok(StreamFrame::EndOfStream) => return None,
                        Err(error) => (Err(error), false),
                    },
                    Ok(None) => (
                        Err(anyhow!("server closed stream before end-of-stream frame")),
                        false,
                    ),
                    Err(error) => (Err(error), false),
                };
                Some((element, more.then_some(body)))
            });
            Ok((Cursor::new(first), elements.boxed()))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use mononoke_macros::mononoke;

    use super::*;

    #[mononoke::test]
    fn hosts_of_addresses() {
        assert_eq!(host_of("localhost:8000"), "localhost");
        assert_eq!(host_of("127.0.0.1:8000"), "127.0.0.1");
        assert_eq!(host_of("[::1]:8000"), "::1");
        assert_eq!(host_of("mononoke.example.com:443"), "mononoke.example.com");
    }

    #[mononoke::test]
    fn plaintext_refuses_non_loopback_addresses() {
        assert!(ClientTransport::with_config("192.0.2.1:1234", ClientConfig::default()).is_err());
    }
}
