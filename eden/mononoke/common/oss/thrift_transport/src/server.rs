/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! Server end of the transport.
//!
//! Each connection is an HTTP/2 connection, over TLS unless it is on
//! loopback, and each request is one of its streams, served by its own task.
//! A streaming reply's elements are pulled from the service only as the
//! client's flow-control window lets them be sent.

use std::ffi::CStr;
use std::future::poll_fn;
use std::io::Cursor;
use std::net::SocketAddr;
use std::net::TcpListener as StdTcpListener;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context as TaskContext;
use std::task::Poll;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use async_trait::async_trait;
use bytes::Bytes;
use bytes::BytesMut;
use fbthrift::ApplicationException;
use fbthrift::ApplicationExceptionErrorCode;
use fbthrift::BinaryProtocol;
use fbthrift::CompactProtocol;
use fbthrift::FramingDecoded;
use fbthrift::FramingEncodedFinal;
use fbthrift::MessageType;
use fbthrift::Protocol;
use fbthrift::ProtocolReader;
use fbthrift::ProtocolWriter;
use fbthrift::Serialize;
use fbthrift::processor::MethodMetadata;
use fbthrift::processor::ReplyState as ThriftReplyState;
use fbthrift::processor::SerializedStreamElement;
use fbthrift::processor::ThriftService;
use fbthrift::thrift_protocol::ProtocolID;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use h2::Reason;
use h2::RecvStream;
use h2::SendStream;
use h2::server::SendResponse;
use http::Method;
use http::Response;
use http::StatusCode;
use permission_checker::MononokeIdentity;
use permission_checker::MononokeIdentitySetExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio::time::sleep;
use tokio::time::sleep_until;
use tokio::time::timeout;
use tokio::time::timeout_at;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

pub use crate::context::RequestContext;
use crate::context::ConnectionInfo;
use crate::framing::MAX_FRAME_SIZE;
use crate::framing::StreamFrame;
use crate::framing::encode_frame;
use crate::loopback::resolve;
use crate::loopback::resolve_loopback;
use crate::tls::ServerTls;

const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);
/// How long to keep flushing a connection once it has been aborted.
const ABORT_FLUSH_TIMEOUT: Duration = Duration::from_secs(1);

/// What a server allows its clients. See `DESIGN.md` for why each exists.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Largest request message.
    pub max_request_size: usize,
    /// Largest unary reply, initial response, or stream element.
    pub max_response_size: usize,
    /// Connections served at once; further clients wait to be accepted.
    pub max_connections: usize,
    /// Requests in flight on one connection, advertised to the client.
    pub max_concurrent_streams: u32,
    /// Requests in flight on the whole server; more are refused, unprocessed.
    pub max_in_flight_requests: usize,
    /// HTTP/2 frame size and header list size the server accepts.
    pub max_frame_size: u32,
    pub max_header_list_size: u32,
    /// Flow-control windows the server gives the client to send in.
    pub stream_window: u32,
    pub connection_window: u32,
    /// Time to finish the TLS and HTTP/2 handshakes.
    pub handshake_timeout: Duration,
    /// Time from a request's start to its last byte.
    pub request_read_timeout: Duration,
    /// Time a connection may stay open with no requests.
    pub idle_timeout: Duration,
    /// Time for a handler to produce its reply, or a stream's first
    /// response.
    pub handler_timeout: Duration,
    /// Time a streaming or large reply may wait for the client to read.
    pub stream_stall_timeout: Duration,
    /// Time in-flight requests get to finish once shutdown begins.
    pub drain_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_request_size: MAX_FRAME_SIZE,
            max_response_size: MAX_FRAME_SIZE,
            max_connections: 1024,
            max_concurrent_streams: 128,
            max_in_flight_requests: 4096,
            max_frame_size: 16 * 1024,
            max_header_list_size: 16 * 1024,
            stream_window: 1024 * 1024,
            connection_window: 16 * 1024 * 1024,
            handshake_timeout: Duration::from_secs(10),
            request_read_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(5 * 60),
            handler_timeout: Duration::from_secs(15 * 60),
            stream_stall_timeout: Duration::from_secs(60),
            drain_timeout: Duration::from_secs(25),
        }
    }
}

/// How a server is secured and limited.
#[derive(Clone, Default)]
pub struct ServerConfig {
    /// Mutual TLS. Without it, the server only listens on loopback.
    pub tls: Option<ServerTls>,
    pub limits: Limits,
    /// Stop a unary handler when its client resets the request. Otherwise
    /// it runs to completion. Streams always stop when their client resets
    /// them.
    pub cancel_on_client_disconnect: bool,
}

/// A service as served by this transport.
pub type BoxService = Box<
    dyn ThriftService<Bytes, Handler = (), RequestContext = RequestContext, ReplyState = ReplyState>
        + Send
        + 'static,
>;

/// Creates a service for a connection, given the protocol of a request it
/// received.
pub type ServiceFactory =
    dyn Fn(ProtocolID) -> Result<BoxService, ApplicationException> + Send + Sync;

/// Box a generated Thrift service so it can be served.
pub fn box_service<T, H>(service: T) -> BoxService
where
    H: 'static,
    T: ThriftService<Bytes, Handler = H, RequestContext = RequestContext, ReplyState = ReplyState>
        + Send
        + Sync
        + 'static,
{
    Box::new(HandlerErased(service))
}

/// Bind a plaintext listener to `host` and `port`. `host` is resolved once,
/// here, and must resolve only to loopback addresses.
pub fn bind(host: &str, port: u16) -> Result<StdTcpListener> {
    let resolved = resolve_loopback((host, port), &format!("{host}:{port}"))?;
    bind_resolved(host, port, &resolved)
}

/// Bind a listener for a server with `config`. Without TLS, `host` must
/// resolve only to loopback addresses.
pub fn bind_with(host: &str, port: u16, config: &ServerConfig) -> Result<StdTcpListener> {
    let resolved = resolve(
        (host, port),
        &format!("{host}:{port}"),
        config.tls.is_some(),
    )?;
    bind_resolved(host, port, &resolved)
}

fn bind_resolved(host: &str, port: u16, resolved: &[SocketAddr]) -> Result<StdTcpListener> {
    let listener =
        StdTcpListener::bind(resolved).with_context(|| format!("failed to bind to {host}:{port}"))?;
    listener
        .set_nonblocking(true)
        .context("failed to make the listener non-blocking")?;
    Ok(listener)
}

/// Serve plaintext connections on `listener`, which must be on loopback,
/// with the default limits. See [`serve_with`].
pub async fn serve(
    listener: TcpListener,
    service_factory: Arc<ServiceFactory>,
    shutdown: oneshot::Receiver<()>,
) {
    serve_with(listener, service_factory, shutdown, ServerConfig::default()).await
}

/// Serve connections on `listener` until `shutdown` fires or its sender is
/// dropped. Then stop accepting, tell each client which of its requests will
/// still be answered, let those finish for up to the drain timeout, and
/// return.
///
/// Without TLS in `config`, a listener that isn't on loopback is refused.
pub async fn serve_with(
    listener: TcpListener,
    service_factory: Arc<ServiceFactory>,
    mut shutdown: oneshot::Receiver<()>,
    config: ServerConfig,
) {
    if config.tls.is_none() {
        match listener.local_addr() {
            Ok(address) if address.ip().is_loopback() => {}
            Ok(address) => {
                error!(
                    "refusing to serve plaintext thrift on non-loopback address {address}; configure TLS"
                );
                return;
            }
            Err(error) => {
                error!("cannot check the thrift listener's address: {error:#}");
                return;
            }
        }
    }

    let limits = config.limits.clone();
    let shared = Arc::new(Shared {
        in_flight: Arc::new(Semaphore::new(limits.max_in_flight_requests)),
        factory: service_factory,
        config,
    });
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            accepted = listener.accept(), if connections.len() < limits.max_connections => {
                match accepted {
                    Ok((stream, peer)) => {
                        debug!("accepted thrift connection from {peer}");
                        // HTTP/2 frames are small and flushed promptly;
                        // Nagle would stall them on the peer's delayed ACK.
                        if let Err(error) = stream.set_nodelay(true) {
                            warn!("failed to set TCP_NODELAY on connection from {peer}: {error:#}");
                        }
                        let shared = shared.clone();
                        let shutdown = shutdown_rx.clone();
                        connections.spawn(async move {
                            if let Err(error) = handle_connection(stream, peer, shared, shutdown).await {
                                debug!("thrift connection from {peer} ended: {error:#}");
                            }
                        });
                    }
                    Err(error) => {
                        error!("failed to accept thrift connection: {error:#}");
                        // Errors such as running out of file descriptors
                        // recur immediately; don't spin on them.
                        sleep(ACCEPT_ERROR_BACKOFF).await;
                    }
                }
            }
            Some(joined) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = joined {
                    error!("thrift connection task failed: {error:#}");
                }
            }
            _ = &mut shutdown => {
                info!("thrift server shutting down");
                break;
            }
        }
    }

    drop(listener);
    let _ = shutdown_tx.send(true);
    // Each connection drains within the drain timeout, after its handshake
    // if it is still in one; this only guards against one that doesn't.
    let deadline = Instant::now() + limits.drain_timeout + limits.handshake_timeout;
    loop {
        match timeout_at(deadline, connections.join_next()).await {
            Ok(Some(Ok(()))) => {}
            Ok(Some(Err(error))) => {
                error!("thrift connection task failed during shutdown: {error:#}");
            }
            Ok(None) => break,
            Err(_) => {
                error!("thrift connections did not drain; aborting them");
                connections.shutdown().await;
                break;
            }
        }
    }
}

/// What every connection of a server shares.
struct Shared {
    factory: Arc<ServiceFactory>,
    config: ServerConfig,
    in_flight: Arc<Semaphore>,
}

async fn handle_connection(
    tcp: TcpStream,
    peer: SocketAddr,
    shared: Arc<Shared>,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let handshake_deadline = Instant::now() + shared.config.limits.handshake_timeout;
    match shared.config.tls.clone() {
        Some(tls) => {
            let (stream, certificate) = timeout_at(handshake_deadline, tls.accept(tcp))
                .await
                .context("timed out in the TLS handshake")??;
            let identities = MononokeIdentity::try_from_x509(&certificate)
                .context("cannot take identities from the client's certificate")?;
            if identities.is_empty() {
                bail!("the client's certificate has no identities");
            }
            debug!(
                "thrift connection from {peer} identified as {}",
                identities.to_string()
            );
            let info = Arc::new(ConnectionInfo {
                peer_addr: peer,
                identities: Some(identities),
            });
            serve_connection(stream, info, shared, shutdown, handshake_deadline).await
        }
        None => {
            let info = Arc::new(ConnectionInfo {
                peer_addr: peer,
                identities: None,
            });
            serve_connection(tcp, info, shared, shutdown, handshake_deadline).await
        }
    }
}

fn http2_builder(limits: &Limits) -> h2::server::Builder {
    let mut builder = h2::server::Builder::new();
    builder
        .max_concurrent_streams(limits.max_concurrent_streams)
        .max_frame_size(limits.max_frame_size)
        .max_header_list_size(limits.max_header_list_size)
        .initial_window_size(limits.stream_window)
        .initial_connection_window_size(limits.connection_window)
        // Mitigations for floods of reset streams (CVE-2023-44487 and
        // CVE-2024-27316's kin).
        .max_pending_accept_reset_streams(20)
        .max_local_error_reset_streams(Some(256));
    builder
}

async fn serve_connection<T>(
    io: T,
    info: Arc<ConnectionInfo>,
    shared: Arc<Shared>,
    mut shutdown: watch::Receiver<bool>,
    handshake_deadline: Instant,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let limits = shared.config.limits.clone();
    let mut connection = timeout_at(
        handshake_deadline,
        http2_builder(&limits).handshake::<_, Bytes>(io),
    )
    .await
    .context("timed out in the HTTP/2 handshake")?
    .context("HTTP/2 handshake failed")?;

    let services = Arc::new(Services::new(shared.factory.clone()));
    let mut requests = JoinSet::new();
    let mut drain_deadline: Option<Instant> = None;
    let idle = sleep(limits.idle_timeout);
    tokio::pin!(idle);

    let result = loop {
        tokio::select! {
            accepted = connection.accept() => match accepted {
                Some(Ok((request, respond))) => {
                    requests.spawn(handle_request(
                        request,
                        respond,
                        services.clone(),
                        info.clone(),
                        shared.clone(),
                    ));
                }
                Some(Err(error)) => break Err(anyhow!(error).context("HTTP/2 connection failed")),
                None => break Ok(()),
            },
            Some(joined) = requests.join_next(), if !requests.is_empty() => {
                if let Err(error) = joined {
                    if error.is_panic() {
                        error!("thrift request task panicked: {error:#}");
                    }
                }
                if requests.is_empty() {
                    idle.as_mut().reset(Instant::now() + limits.idle_timeout);
                }
            }
            _ = &mut idle, if requests.is_empty() && drain_deadline.is_none() => {
                debug!("closing idle thrift connection from {}", info.peer_addr);
                connection.graceful_shutdown();
                drain_deadline = Some(Instant::now() + limits.drain_timeout);
            }
            _ = shutdown.changed(), if drain_deadline.is_none() => {
                // GOAWAY tells the client which requests will be answered,
                // so it can send the rest to another server.
                connection.graceful_shutdown();
                drain_deadline = Some(Instant::now() + limits.drain_timeout);
            }
            _ = sleep_until(drain_deadline.unwrap_or_else(Instant::now)), if drain_deadline.is_some() => {
                if !requests.is_empty() {
                    warn!(
                        "aborting {} thrift requests from {} still running after the drain timeout",
                        requests.len(),
                        info.peer_addr,
                    );
                }
                requests.abort_all();
                connection.abrupt_shutdown(Reason::NO_ERROR);
                let _ = timeout(ABORT_FLUSH_TIMEOUT, poll_fn(|cx| connection.poll_closed(cx))).await;
                break Ok(());
            }
        }
    };

    requests.shutdown().await;
    services.terminate().await;
    result
}

/// The services a connection has created, one per protocol its requests use.
struct Services {
    factory: Arc<ServiceFactory>,
    created: Mutex<Vec<(ProtocolID, Arc<BoxService>)>>,
}

impl Services {
    fn new(factory: Arc<ServiceFactory>) -> Self {
        Self {
            factory,
            created: Mutex::new(Vec::new()),
        }
    }

    fn get(&self, proto: ProtocolID) -> Result<Arc<BoxService>, ApplicationException> {
        let mut created = self.created.lock().expect("services lock poisoned");
        if let Some((_, service)) = created.iter().find(|(created, _)| *created == proto) {
            return Ok(service.clone());
        }
        let service = Arc::new((self.factory)(proto)?);
        created.push((proto, service.clone()));
        Ok(service)
    }

    async fn terminate(&self) {
        let created = std::mem::take(&mut *self.created.lock().expect("services lock poisoned"));
        for (_, service) in created {
            service.on_termination().await;
        }
    }
}

enum ReadError {
    TooLarge,
    Stream(h2::Error),
}

async fn read_body(mut body: RecvStream, max: usize) -> Result<Bytes, ReadError> {
    let mut buffer = BytesMut::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(ReadError::Stream)?;
        let _ = body.flow_control().release_capacity(chunk.len());
        if buffer.len() + chunk.len() > max {
            return Err(ReadError::TooLarge);
        }
        buffer.extend_from_slice(&chunk);
    }
    Ok(buffer.freeze())
}

fn send_status(respond: &mut SendResponse<Bytes>, status: StatusCode) {
    let response = Response::builder()
        .status(status)
        .body(())
        .expect("a status-only response is valid");
    if let Err(error) = respond.send_response(response, true) {
        debug!("failed to send HTTP status {status}: {error:#}");
    }
}

fn start_reply(respond: &mut SendResponse<Bytes>) -> Result<SendStream<Bytes>> {
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, crate::CONTENT_TYPE)
        .body(())
        .expect("a reply's response head is valid");
    respond
        .send_response(response, false)
        .context("failed to start the reply")
}

async fn handle_request(
    request: http::Request<RecvStream>,
    mut respond: SendResponse<Bytes>,
    services: Arc<Services>,
    info: Arc<ConnectionInfo>,
    shared: Arc<Shared>,
) {
    let limits = &shared.config.limits;
    // Refused before anything runs, so the client may send it elsewhere.
    let Ok(_in_flight) = shared.in_flight.clone().try_acquire_owned() else {
        debug!("refusing a thrift request: too many requests in flight");
        respond.send_reset(Reason::REFUSED_STREAM);
        return;
    };
    if request.method() != Method::POST {
        send_status(&mut respond, StatusCode::METHOD_NOT_ALLOWED);
        return;
    }
    let path = request.uri().path().to_owned();
    let body = match timeout(
        limits.request_read_timeout,
        read_body(request.into_body(), limits.max_request_size),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(ReadError::TooLarge)) => {
            send_status(&mut respond, StatusCode::PAYLOAD_TOO_LARGE);
            return;
        }
        Ok(Err(ReadError::Stream(error))) => {
            debug!("failed to read thrift request {path}: {error:#}");
            return;
        }
        Err(_) => {
            debug!("timed out reading thrift request {path}");
            respond.send_reset(Reason::CANCEL);
            return;
        }
    };
    let Ok(proto) = detect_protocol(&body) else {
        send_status(&mut respond, StatusCode::BAD_REQUEST);
        return;
    };

    if let Err(error) = run_request(&mut respond, body, proto, services, info, &shared).await {
        debug!("thrift request {path} failed: {error:#}");
        respond.send_reset(Reason::INTERNAL_ERROR);
    }
}

/// Run one request through its service and send the reply. An error means
/// the reply could not be sent, and the stream should be reset.
async fn run_request(
    respond: &mut SendResponse<Bytes>,
    request: Bytes,
    proto: ProtocolID,
    services: Arc<Services>,
    info: Arc<ConnectionInfo>,
    shared: &Shared,
) -> Result<()> {
    let limits = &shared.config.limits;
    let service = match services.get(proto) {
        Ok(service) => service,
        Err(error) => {
            let reply = exception_reply(
                &request,
                proto,
                ApplicationExceptionErrorCode::InternalError,
                format!("failed to create service for protocol {proto:?}: {error}"),
            )?;
            return send_unary(respond, &reply, limits).await;
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let reply_state = Arc::new(ReplyState::new(reply_tx));
    let request_context = RequestContext::for_connection(info);
    let call_request = request.clone();
    let call = async move {
        service
            .call(Cursor::new(call_request), &request_context, reply_state)
            .await
            .context("thrift service call failed")?;
        reply_rx.await.map_err(|_| anyhow!("service did not reply"))
    };
    tokio::pin!(call);

    let deadline = Instant::now() + limits.handler_timeout;
    let reply = tokio::select! {
        reply = timeout_at(deadline, &mut call) => reply,
        reset = poll_fn(|cx| respond.poll_reset(cx)) => {
            debug!("client reset the request: {reset:?}");
            if !shared.config.cancel_on_client_disconnect {
                // Run to completion as asked; nobody waits for the reply.
                let _ = timeout_at(deadline, &mut call).await;
            }
            return Ok(());
        }
    };

    let reply = match reply {
        Ok(Ok(reply)) => reply,
        Ok(Err(error)) => {
            let reply = exception_reply(
                &request,
                proto,
                ApplicationExceptionErrorCode::InternalError,
                format!("{error:#}"),
            )?;
            return send_unary(respond, &reply, limits).await;
        }
        Err(_) => {
            let reply = exception_reply(
                &request,
                proto,
                ApplicationExceptionErrorCode::Timeout,
                format!(
                    "the handler did not reply within {:?}",
                    limits.handler_timeout
                ),
            )?;
            return send_unary(respond, &reply, limits).await;
        }
    };

    match reply {
        Reply::Unary(response) => {
            let response = if response.len() > limits.max_response_size {
                error!(
                    "unary response is {} bytes, over the {} byte limit; replying with an application exception",
                    response.len(),
                    limits.max_response_size,
                );
                exception_reply(
                    &request,
                    proto,
                    ApplicationExceptionErrorCode::InternalError,
                    format!(
                        "response exceeds the OSS transport limit of {} bytes",
                        limits.max_response_size
                    ),
                )?
            } else {
                response
            };
            send_unary(respond, &response, limits).await
        }
        Reply::Stream {
            first,
            stream: elements,
            protocol_id,
        } => {
            if first.len() > limits.max_response_size {
                let reply = exception_reply(
                    &request,
                    proto,
                    ApplicationExceptionErrorCode::InternalError,
                    format!(
                        "initial stream response exceeds the OSS transport limit of {} bytes",
                        limits.max_response_size
                    ),
                )?;
                return send_unary(respond, &reply, limits).await;
            }
            let mut stream = start_reply(respond)?;
            let frame = encode_frame(&first, limits.max_response_size)?;
            send_bytes(&mut stream, frame, false, limits.stream_stall_timeout).await?;
            write_stream(&mut stream, elements, protocol_id, limits).await
        }
    }
}

async fn send_unary(
    respond: &mut SendResponse<Bytes>,
    reply: &[u8],
    limits: &Limits,
) -> Result<()> {
    // An exception reply can be a little over the limit that its message
    // reports; frame it anyway.
    let frame = encode_frame(reply, limits.max_response_size.max(reply.len()))?;
    let mut stream = start_reply(respond)?;
    send_bytes(&mut stream, frame, true, limits.stream_stall_timeout).await
}

/// Wait until the client's flow-control window has room, failing if the
/// client resets the stream.
fn poll_capacity(stream: &mut SendStream<Bytes>, cx: &mut TaskContext<'_>) -> Poll<Result<usize>> {
    if let Poll::Ready(reset) = stream.poll_reset(cx) {
        return Poll::Ready(Err(anyhow!("client reset the stream: {reset:?}")));
    }
    if stream.capacity() > 0 {
        return Poll::Ready(Ok(stream.capacity()));
    }
    match stream.poll_capacity(cx) {
        Poll::Ready(Some(Ok(capacity))) => Poll::Ready(Ok(capacity)),
        Poll::Ready(Some(Err(error))) => Poll::Ready(Err(anyhow!(error))),
        Poll::Ready(None) => Poll::Ready(Err(anyhow!("the stream closed"))),
        Poll::Pending => Poll::Pending,
    }
}

/// Send `data` as the window allows, so a client that reads slowly holds
/// back the sender rather than filling its memory.
async fn send_bytes(
    stream: &mut SendStream<Bytes>,
    mut data: Bytes,
    end_of_stream: bool,
    stall_timeout: Duration,
) -> Result<()> {
    while !data.is_empty() {
        stream.reserve_capacity(data.len());
        let capacity = timeout(stall_timeout, poll_fn(|cx| poll_capacity(stream, cx)))
            .await
            .context("the client stopped reading")??;
        let chunk = data.split_to(capacity.min(data.len()));
        stream
            .send_data(chunk, end_of_stream && data.is_empty())
            .context("failed to send reply data")?;
    }
    Ok(())
}

/// Write the elements of a streaming reply, pulling each from the service
/// only once the previous one is sent, and stopping if the client resets
/// the stream, which drops `elements` and so cancels its producer.
async fn write_stream(
    stream: &mut SendStream<Bytes>,
    elements: Option<BoxStream<'static, SerializedStreamElement<FramingEncodedFinal<Bytes>>>>,
    protocol_id: ProtocolID,
    limits: &Limits,
) -> Result<()> {
    if let Some(mut elements) = elements {
        loop {
            let next = poll_fn(|cx| {
                if let Poll::Ready(reset) = stream.poll_reset(cx) {
                    return Poll::Ready(Err(anyhow!("client reset the stream: {reset:?}")));
                }
                elements.poll_next_unpin(cx).map(Ok)
            })
            .await?;
            let Some(element) = next else {
                break;
            };
            let (frame, terminal) = match element {
                SerializedStreamElement::Success(payload) => (StreamFrame::Success(payload), false),
                SerializedStreamElement::DeclaredException { payload, .. } => {
                    (StreamFrame::DeclaredException(payload), true)
                }
                SerializedStreamElement::ApplicationException(ex) => (
                    StreamFrame::ApplicationException(serialize_application_exception(
                        &ex,
                        protocol_id,
                    )),
                    true,
                ),
            };
            let frame = if frame.encode(limits.max_response_size).is_ok() {
                frame
            } else {
                error!("stream element exceeds the frame limit; ending the stream with an exception");
                let ex = ApplicationException::new(
                    ApplicationExceptionErrorCode::InternalError,
                    "stream element exceeds the OSS transport frame limit".to_string(),
                );
                return send_stream_frame(
                    stream,
                    StreamFrame::ApplicationException(serialize_application_exception(
                        &ex,
                        protocol_id,
                    )),
                    true,
                    limits,
                )
                .await;
            };
            send_stream_frame(stream, frame, terminal, limits).await?;
            if terminal {
                return Ok(());
            }
        }
    }
    send_stream_frame(stream, StreamFrame::EndOfStream, true, limits).await
}

async fn send_stream_frame(
    stream: &mut SendStream<Bytes>,
    frame: StreamFrame,
    end_of_stream: bool,
    limits: &Limits,
) -> Result<()> {
    let frame = encode_frame(
        &frame.encode(limits.max_response_size)?,
        limits.max_response_size,
    )?;
    send_bytes(stream, frame, end_of_stream, limits.stream_stall_timeout).await
}

/// Reply sent from the service to the request's task.
enum Reply {
    Unary(Bytes),
    Stream {
        first: Bytes,
        stream: Option<BoxStream<'static, SerializedStreamElement<FramingEncodedFinal<Bytes>>>>,
        protocol_id: ProtocolID,
    },
}

/// Hands the service's reply to the task serving the request.
pub struct ReplyState {
    reply_tx: std::sync::Mutex<Option<oneshot::Sender<Reply>>>,
}

impl ReplyState {
    fn new(reply_tx: oneshot::Sender<Reply>) -> Self {
        Self {
            reply_tx: std::sync::Mutex::new(Some(reply_tx)),
        }
    }

    fn send(&self, reply: Reply) {
        if let Some(tx) = self.reply_tx.lock().expect("reply_tx lock poisoned").take() {
            let _ = tx.send(reply);
        }
    }

    fn send_unsupported_stream(
        &self,
        first: Bytes,
        protocol_id: ProtocolID,
        kind: &str,
    ) -> ApplicationException {
        let error = ApplicationException::new(
            ApplicationExceptionErrorCode::UnsupportedClientType,
            format!("{kind} RPCs are not supported by the OSS thrift transport"),
        );
        let outbound = error.clone();
        self.send(Reply::Stream {
            first,
            stream: Some(
                futures::stream::once(async move {
                    SerializedStreamElement::ApplicationException(outbound)
                })
                .boxed(),
            ),
            protocol_id,
        });
        error
    }
}

impl ThriftReplyState<Bytes> for ReplyState {
    type RequestContext = RequestContext;

    fn send_reply(&self, reply: FramingEncodedFinal<Bytes>) {
        self.send(Reply::Unary(reply));
    }

    fn send_stream_reply(
        &self,
        response: FramingEncodedFinal<Bytes>,
        stream: Option<BoxStream<'static, SerializedStreamElement<FramingEncodedFinal<Bytes>>>>,
        protocol_id: ProtocolID,
    ) -> Result<()> {
        self.send(Reply::Stream {
            first: response,
            stream,
            protocol_id,
        });
        Ok(())
    }

    fn send_sink_reply(
        &self,
        first_response: FramingEncodedFinal<Bytes>,
        _buffer_size: u64,
        _chunk_timeout: Duration,
        protocol_id: ProtocolID,
    ) -> (
        BoxStream<'static, Result<FramingDecoded<Bytes>, ApplicationException>>,
        impl FnOnce(SerializedStreamElement<FramingEncodedFinal<Bytes>>) + Send,
    ) {
        let error = self.send_unsupported_stream(first_response, protocol_id, "sink");
        (
            futures::stream::once(async move { Err(error) }).boxed(),
            |_| {},
        )
    }

    fn send_bidirectional_reply(
        &self,
        first_response: FramingEncodedFinal<Bytes>,
        _stream: BoxStream<'static, SerializedStreamElement<FramingEncodedFinal<Bytes>>>,
        protocol_id: ProtocolID,
    ) -> BoxStream<'static, Result<FramingDecoded<Bytes>, ApplicationException>> {
        let error =
            self.send_unsupported_stream(first_response, protocol_id, "bidirectional streaming");
        futures::stream::once(async move { Err(error) }).boxed()
    }
}

/// Erases the `Handler` marker type, which dispatch never uses, so services
/// of different types fit behind one [`BoxService`].
struct HandlerErased<T>(T);

#[async_trait]
impl<H, T> ThriftService<Bytes> for HandlerErased<T>
where
    H: 'static,
    T: ThriftService<Bytes, Handler = H, RequestContext = RequestContext, ReplyState = ReplyState>
        + Send
        + Sync
        + 'static,
{
    type Handler = ();
    type RequestContext = RequestContext;
    type ReplyState = ReplyState;

    async fn call(
        &self,
        req: FramingDecoded<Bytes>,
        req_ctxt: &RequestContext,
        reply_state: Arc<ReplyState>,
    ) -> Result<()> {
        self.0.call(req, req_ctxt, reply_state).await
    }

    fn create_interaction(
        &self,
        _name: &str,
    ) -> Result<
        Arc<
            dyn ThriftService<
                    Bytes,
                    Handler = (),
                    RequestContext = RequestContext,
                    ReplyState = ReplyState,
                > + Send
                + 'static,
        >,
    > {
        bail!("interactions are not supported by the OSS thrift transport")
    }

    fn get_method_metadata(&self) -> &'static [MethodMetadata] {
        self.0.get_method_metadata()
    }

    async fn on_termination(&self) {
        self.0.on_termination().await
    }
}

fn detect_protocol(data: &[u8]) -> Result<ProtocolID> {
    match data {
        [0x80, 0x01, ..] => Ok(ProtocolID::BinaryProtocol),
        [0x82, _, ..] => Ok(ProtocolID::CompactProtocol),
        [first, second, ..] => bail!("unrecognized thrift protocol: {first:02x} {second:02x}"),
        _ => bail!(
            "frame too short ({} bytes) for protocol detection",
            data.len()
        ),
    }
}

fn serialize_application_exception(ex: &ApplicationException, proto: ProtocolID) -> Bytes {
    match proto {
        ProtocolID::CompactProtocol => fbthrift::compact_protocol::serialize(ex),
        _ => fbthrift::binary_protocol::serialize(ex),
    }
}

/// An application exception, addressed to the method and sequence id the
/// caller of `request` is waiting on.
fn exception_reply(
    request: &Bytes,
    proto: ProtocolID,
    code: ApplicationExceptionErrorCode,
    message: String,
) -> Result<Bytes> {
    fn header<P: Protocol<Frame = Bytes>>(request: &Bytes) -> Result<(String, u32)> {
        let mut deserializer = P::deserializer(Cursor::new(request.clone()));
        let (method, _, seqid) = deserializer
            .read_message_begin(|method| String::from_utf8_lossy(method).into_owned())?;
        Ok((method, seqid))
    }
    let (method, seqid) = match proto {
        ProtocolID::CompactProtocol => header::<CompactProtocol>(request),
        _ => header::<BinaryProtocol>(request),
    }
    .context("cannot address an exception reply to the caller")?;

    let ex = ApplicationException::new(code, message);
    Ok(match proto {
        ProtocolID::CompactProtocol => fbthrift::serialize!(CompactProtocol, |p| {
            p.write_message_begin(&method, MessageType::Exception, seqid);
            ex.rs_thrift_write(p);
            p.write_message_end();
        }),
        _ => fbthrift::serialize!(BinaryProtocol, |p| {
            p.write_message_begin(&method, MessageType::Exception, seqid);
            ex.rs_thrift_write(p);
            p.write_message_end();
        }),
    })
}

// Type assertion: the request context works with generated services, whose
// `RequestContext` must name its function names as `CStr`s.
const _: fn() = || {
    fn assert_request_context<R: fbthrift::RequestContext<Name = CStr>>() {}
    assert_request_context::<RequestContext>();
};

#[cfg(test)]
mod tests;
