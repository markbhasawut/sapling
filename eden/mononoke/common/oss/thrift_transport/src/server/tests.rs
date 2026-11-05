/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use fb303_core::fb303_status;
use fb303_core_clients::make_BaseService;
use fb303_core_services::BaseService;
use fb303_core_services::errors::GetNameExn;
use fb303_core_services::errors::GetStatusDetailsExn;
use fb303_core_services::errors::GetStatusExn;
use fb303_core_services::make_BaseService_server;
use fbthrift::ClientStreamElement;
use fbthrift::Deserialize;
use fbthrift::Framing;
use fbthrift::Transport;
use mononoke_macros::mononoke;
use openssl::ssl::SslVersion;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use super::*;
use crate::client::ClientConfig;
use crate::client::ClientTransport;
use crate::client::connect_with;
use crate::framing::FrameDecoder;
use crate::test_certs::Ca;
use crate::test_certs::VALID;
use crate::test_certs::lax_client_tls;

#[derive(Clone)]
struct NamedService(&'static str);

#[async_trait]
impl BaseService for NamedService {
    async fn getName(&self) -> Result<String, GetNameExn> {
        Ok(self.0.to_owned())
    }

    async fn getStatus(&self) -> Result<fb303_status, GetStatusExn> {
        Ok(fb303_status::ALIVE)
    }

    async fn getStatusDetails(&self) -> Result<String, GetStatusDetailsExn> {
        Ok(String::new())
    }
}

/// What every request to a [`TestService`] sees and does.
#[derive(Default)]
struct Observed {
    started: AtomicUsize,
    finished: AtomicUsize,
    running: AtomicUsize,
    most_running: AtomicUsize,
    stream_elements_produced: AtomicUsize,
    stream_dropped: AtomicBool,
    handler_dropped: AtomicBool,
    identities: std::sync::Mutex<Vec<Option<Vec<String>>>>,
    peers: std::sync::Mutex<HashSet<SocketAddr>>,
}

#[derive(Clone)]
enum Behavior {
    /// Reply with these bytes after waiting.
    Unary { reply: Bytes, delay: Duration },
    /// Reply with an initial response and these elements.
    Stream { first: Bytes, elements: Vec<Bytes> },
    /// Reply with an initial response and elements of this size, forever.
    EndlessStream { element_size: usize },
}

/// Service that answers every request as its [`Behavior`] says, recording
/// what it sees in [`Observed`].
#[derive(Clone)]
struct TestService {
    behavior: Behavior,
    observed: Arc<Observed>,
    proto: ProtocolID,
}

/// Sets a flag when dropped.
struct DropFlag(Arc<Observed>, fn(&Observed) -> &AtomicBool);

impl Drop for DropFlag {
    fn drop(&mut self) {
        (self.1)(&self.0).store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl ThriftService<Bytes> for TestService {
    type Handler = ();
    type RequestContext = RequestContext;
    type ReplyState = ReplyState;

    async fn call(
        &self,
        _req: FramingDecoded<Bytes>,
        req_ctxt: &Self::RequestContext,
        reply_state: Arc<Self::ReplyState>,
    ) -> Result<()> {
        let observed = &self.observed;
        observed.started.fetch_add(1, Ordering::SeqCst);
        observed.identities.lock().unwrap().push(
            req_ctxt
                .identities()
                .map(|ids| ids.iter().map(|id| id.to_string()).sorted_vec()),
        );
        if let Some(peer) = req_ctxt.peer_addr() {
            observed.peers.lock().unwrap().insert(peer);
        }
        let running = observed.running.fetch_add(1, Ordering::SeqCst) + 1;
        observed.most_running.fetch_max(running, Ordering::SeqCst);
        let _running = scopeguard(observed.clone());

        match &self.behavior {
            Behavior::Unary { reply, delay } => {
                let _dropped = DropFlag(observed.clone(), |o| &o.handler_dropped);
                sleep(*delay).await;
                std::mem::forget(_dropped);
                reply_state.send_reply(reply.clone());
            }
            Behavior::Stream { first, elements } => {
                let stream = futures::stream::iter(
                    elements.clone().into_iter().map(SerializedStreamElement::Success),
                )
                .boxed();
                reply_state.send_stream_reply(first.clone(), Some(stream), self.proto)?;
            }
            Behavior::EndlessStream { element_size } => {
                let observed = observed.clone();
                let element = Bytes::from(vec![7; *element_size]);
                let guard = DropFlag(observed.clone(), |o| &o.stream_dropped);
                let stream = futures::stream::repeat_with(move || {
                    let _ = &guard;
                    observed
                        .stream_elements_produced
                        .fetch_add(1, Ordering::SeqCst);
                    SerializedStreamElement::Success(element.clone())
                })
                .boxed();
                reply_state.send_stream_reply(Bytes::from_static(b"first"), Some(stream), self.proto)?;
            }
        }
        observed.finished.fetch_add(1, Ordering::SeqCst);
        Ok(())
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
        bail!("interactions not supported")
    }

    fn get_method_metadata(&self) -> &'static [MethodMetadata] {
        &[]
    }

    async fn on_termination(&self) {}
}

trait SortedVec {
    fn sorted_vec(self) -> Vec<String>;
}

impl<I: Iterator<Item = String>> SortedVec for I {
    fn sorted_vec(self) -> Vec<String> {
        let mut items: Vec<String> = self.collect();
        items.sort();
        items
    }
}

/// Decrements the running count when a handler ends, however it ends.
fn scopeguard(observed: Arc<Observed>) -> impl Drop {
    struct Running(Arc<Observed>);
    impl Drop for Running {
        fn drop(&mut self) {
            self.0.running.fetch_sub(1, Ordering::SeqCst);
        }
    }
    Running(observed)
}

fn test_service(behavior: Behavior) -> (Arc<ServiceFactory>, Arc<Observed>) {
    let observed = Arc::new(Observed::default());
    let shared = observed.clone();
    let factory: Arc<ServiceFactory> = Arc::new(move |proto| {
        Ok(box_service(TestService {
            behavior: behavior.clone(),
            observed: shared.clone(),
            proto,
        }))
    });
    (factory, observed)
}

fn unary(reply: &'static [u8], delay: Duration) -> Behavior {
    Behavior::Unary {
        reply: Bytes::from_static(reply),
        delay,
    }
}

fn named_service(name: &'static str) -> Arc<ServiceFactory> {
    Arc::new(move |proto| {
        Ok(box_service(make_BaseService_server(
            proto,
            NamedService(name),
        )?))
    })
}

struct TestServer {
    address: String,
    port: u16,
    shutdown: oneshot::Sender<()>,
    server: tokio::task::JoinHandle<()>,
}

impl TestServer {
    async fn start(factory: Arc<ServiceFactory>) -> Self {
        Self::start_with(factory, ServerConfig::default()).await
    }

    async fn start_with(factory: Arc<ServiceFactory>, config: ServerConfig) -> Self {
        let listener = TcpListener::from_std(
            bind_with("127.0.0.1", 0, &config).expect("loopback should bind"),
        )
        .expect("listener should convert");
        let local = listener
            .local_addr()
            .expect("listener should have an address");
        let (shutdown, shutdown_rx) = oneshot::channel();
        let server = mononoke::spawn_task(serve_with(listener, factory, shutdown_rx, config));
        Self {
            address: local.to_string(),
            port: local.port(),
            shutdown,
            server,
        }
    }

    fn client(&self, config: ClientConfig) -> ClientTransport {
        ClientTransport::with_config(&self.address, config).expect("address should be accepted")
    }

    /// A client that dials `localhost`, as TLS clients name the server.
    fn tls_client(&self, config: ClientConfig) -> ClientTransport {
        ClientTransport::with_config(&format!("localhost:{}", self.port), config)
            .expect("address should be accepted")
    }

    async fn stop(self) {
        let _ = self.shutdown.send(());
        self.server.await.expect("server task should not panic");
    }
}

fn binary_call(method: &str, seq_id: i32) -> Bytes {
    let mut buf = Vec::new();
    // Version and message type CALL.
    buf.extend_from_slice(&[0x80, 0x01, 0x00, 0x01]);
    buf.extend_from_slice(&(method.len() as u32).to_be_bytes());
    buf.extend_from_slice(method.as_bytes());
    buf.extend_from_slice(&seq_id.to_be_bytes());
    // Empty argument struct.
    buf.push(0x00);
    Bytes::from(buf)
}

async fn call(transport: &ClientTransport, request: Bytes) -> Result<Bytes> {
    let mut encoded = ClientTransport::enc_with_capacity(request.len());
    encoded.extend_from_slice(&request);
    transport
        .call(c"test", c"method", encoded.freeze(), ())
        .await
        .map(|reply| reply.into_inner())
}

async fn call_stream(
    transport: &ClientTransport,
) -> Result<(
    Bytes,
    BoxStream<'static, Result<ClientStreamElement<Cursor<Bytes>>>>,
)> {
    let (first, stream) = transport
        .call_stream(c"test", c"stream", binary_call("stream", 1), ())
        .await?;
    Ok((first.into_inner(), stream))
}

fn reply_payloads(
    elements: Vec<Result<ClientStreamElement<Cursor<Bytes>>>>,
) -> Vec<Bytes> {
    elements
        .into_iter()
        .map(
            |element| match element.expect("stream element should succeed") {
                ClientStreamElement::Reply(cursor) => cursor.into_inner(),
                other => panic!("unexpected stream element: {other:?}"),
            },
        )
        .collect()
}

/// The application exception in a reply, with its method and sequence id.
fn exception_in(reply: Bytes) -> (String, i32, ApplicationException) {
    let mut deserializer = <BinaryProtocol>::deserializer(Cursor::new(reply));
    let (method, message_type, seqid) = deserializer
        .read_message_begin(|method| String::from_utf8_lossy(method).into_owned())
        .expect("reply should carry a message envelope");
    assert_eq!(message_type, MessageType::Exception);
    let exception = ApplicationException::rs_thrift_read(&mut deserializer)
        .expect("reply should carry an application exception");
    (method, seqid as i32, exception)
}

fn quick_limits() -> Limits {
    Limits {
        handshake_timeout: Duration::from_secs(5),
        ..Limits::default()
    }
}

#[mononoke::test]
fn detects_protocols() {
    assert_eq!(
        detect_protocol(&[0x80, 0x01, 0x00, 0x01]).expect("binary should be detected"),
        ProtocolID::BinaryProtocol
    );
    assert_eq!(
        detect_protocol(&[0x82, 0x21, 0x00]).expect("compact should be detected"),
        ProtocolID::CompactProtocol
    );
    assert!(detect_protocol(&[0x00, 0x00, 0x00]).is_err());
    assert!(detect_protocol(&[0x80]).is_err());
}

#[mononoke::test]
fn plaintext_binds_refuse_non_loopback_hosts() {
    assert!(bind("0.0.0.0", 0).is_err());
    assert!(bind("::", 0).is_err());
    assert!(bind_with("0.0.0.0", 0, &ServerConfig::default()).is_err());
}

#[mononoke::test]
fn tls_binds_accept_any_host() {
    let config = ServerConfig {
        tls: Some(Ca::new("ca").server_tls()),
        ..ServerConfig::default()
    };
    bind_with("0.0.0.0", 0, &config).expect("a TLS server may listen on any address");
}

#[mononoke::test]
async fn plaintext_serving_refuses_non_loopback_listeners() {
    let listener = TcpListener::bind("0.0.0.0:0")
        .await
        .expect("listener should bind");
    let (_shutdown, shutdown_rx) = oneshot::channel();
    timeout(
        Duration::from_secs(5),
        serve_with(
            listener,
            named_service("x"),
            shutdown_rx,
            ServerConfig::default(),
        ),
    )
    .await
    .expect("serving plaintext on a non-loopback address should stop at once");
}

#[mononoke::test]
async fn generated_clients_call_generated_services() {
    let server = TestServer::start(named_service("TestServer")).await;
    let client = connect_with::<make_BaseService>(&server.address, ClientConfig::default())
        .expect("client should be created");
    for _ in 0..3 {
        assert_eq!(client.getName().await.expect("call should succeed"), "TestServer");
    }
    server.stop().await;
}

#[mononoke::test]
async fn calls_overlap_on_one_connection_up_to_its_limit() {
    let (factory, observed) = test_service(unary(b"reply", Duration::from_millis(200)));
    let server = TestServer::start_with(
        factory,
        ServerConfig {
            limits: Limits {
                max_concurrent_streams: 8,
                ..quick_limits()
            },
            ..ServerConfig::default()
        },
    )
    .await;
    let transport = server.client(ClientConfig::default());

    let calls = (0..32).map(|i| {
        let transport = transport.clone();
        async move { call(&transport, binary_call("m", i)).await }
    });
    let results = futures::future::join_all(calls).await;
    assert!(results.iter().all(Result::is_ok), "all calls should succeed");

    let most = observed.most_running.load(Ordering::SeqCst);
    assert!(most > 1, "calls should run at once, but at most {most} did");
    assert!(most <= 8, "at most 8 calls should run at once, but {most} did");
    assert_eq!(observed.started.load(Ordering::SeqCst), 32);
    assert_eq!(
        observed.peers.lock().unwrap().len(),
        1,
        "every call should use one connection"
    );
    server.stop().await;
}

/// Requests over the server's in-flight limit are refused before they run,
/// so the client sends them again, and each still runs exactly once.
#[mononoke::test]
async fn refused_requests_are_sent_again_and_run_once() {
    let (factory, observed) = test_service(unary(b"reply", Duration::from_millis(150)));
    let server = TestServer::start_with(
        factory,
        ServerConfig {
            limits: Limits {
                max_in_flight_requests: 1,
                ..quick_limits()
            },
            ..ServerConfig::default()
        },
    )
    .await;
    let transport = server.client(ClientConfig {
        max_attempts: 10,
        retry_backoff: Duration::from_millis(40),
        ..ClientConfig::default()
    });

    let (first, second) = tokio::join!(
        call(&transport, binary_call("m", 1)),
        call(&transport, binary_call("m", 2)),
    );
    first.expect("the first call should succeed");
    second.expect("the refused call should succeed when sent again");
    assert_eq!(
        observed.started.load(Ordering::SeqCst),
        2,
        "each call should have run exactly once"
    );
    server.stop().await;
}

#[mononoke::test]
async fn streams_elements_then_end_of_stream() {
    let elements = vec![
        Bytes::from_static(b"elem1"),
        Bytes::from_static(b"elem2"),
        Bytes::from_static(b"elem3"),
    ];
    let (factory, _) = test_service(Behavior::Stream {
        first: Bytes::from_static(b"initial"),
        elements: elements.clone(),
    });
    let server = TestServer::start(factory).await;
    let transport = server.client(ClientConfig::default());

    let (first, stream) = call_stream(&transport).await.expect("stream should start");
    assert_eq!(first, Bytes::from_static(b"initial"));
    assert_eq!(reply_payloads(stream.collect().await), elements);
    server.stop().await;
}

#[mononoke::test]
async fn an_empty_stream_is_just_end_of_stream() {
    let (factory, _) = test_service(Behavior::Stream {
        first: Bytes::from_static(b"only"),
        elements: vec![],
    });
    let server = TestServer::start(factory).await;
    let transport = server.client(ClientConfig::default());

    let (first, stream) = call_stream(&transport).await.expect("stream should start");
    assert_eq!(first, Bytes::from_static(b"only"));
    assert!(stream.collect::<Vec<_>>().await.is_empty());
    server.stop().await;
}

#[mononoke::test]
async fn an_oversized_stream_element_ends_the_stream_with_an_exception() {
    let (factory, _) = test_service(Behavior::Stream {
        first: Bytes::from_static(b"initial"),
        elements: vec![Bytes::from(vec![0; 1024])],
    });
    let server = TestServer::start_with(
        factory,
        ServerConfig {
            limits: Limits {
                max_response_size: 512,
                ..quick_limits()
            },
            ..ServerConfig::default()
        },
    )
    .await;
    let transport = server.client(ClientConfig::default());

    let (_, stream) = call_stream(&transport).await.expect("stream should start");
    let elements: Vec<_> = stream.collect().await;
    assert_eq!(elements.len(), 1, "the exception should end the stream");
    assert!(matches!(
        elements.into_iter().next().unwrap(),
        Ok(ClientStreamElement::ApplicationEx(_))
    ));
    server.stop().await;
}

/// A unary response too large to send comes back as an exception addressed
/// to the caller.
#[mononoke::test]
async fn an_oversized_unary_response_is_an_exception() {
    let (factory, _) = test_service(Behavior::Unary {
        reply: Bytes::from(vec![0; 1024]),
        delay: Duration::ZERO,
    });
    let server = TestServer::start_with(
        factory,
        ServerConfig {
            limits: Limits {
                max_response_size: 512,
                ..quick_limits()
            },
            ..ServerConfig::default()
        },
    )
    .await;
    let transport = server.client(ClientConfig::default());

    let reply = call(&transport, binary_call("oversized", 7))
        .await
        .expect("the call should get an exception reply");
    let (method, seqid, exception) = exception_in(reply);
    assert_eq!(method, "oversized");
    assert_eq!(seqid, 7);
    assert_eq!(exception.type_, ApplicationExceptionErrorCode::InternalError);
    assert!(exception.message.contains("limit"));
    server.stop().await;
}

/// An oversized request is refused without running, and not sent again: it
/// would be refused again.
#[mononoke::test]
async fn an_oversized_request_is_refused_once() {
    let (factory, observed) = test_service(unary(b"reply", Duration::ZERO));
    let server = TestServer::start_with(
        factory,
        ServerConfig {
            limits: Limits {
                max_request_size: 64,
                ..quick_limits()
            },
            ..ServerConfig::default()
        },
    )
    .await;
    let transport = server.client(ClientConfig::default());

    let mut request = binary_call("m", 1).to_vec();
    request.extend_from_slice(&[0; 128]);
    let error = call(&transport, Bytes::from(request))
        .await
        .expect_err("an oversized request should fail");
    assert!(format!("{error:#}").contains("size limit"), "{error:#}");
    assert_eq!(observed.started.load(Ordering::SeqCst), 0);
    server.stop().await;
}

#[mononoke::test]
async fn a_slow_handler_times_out_with_an_exception() {
    let (factory, _) = test_service(unary(b"reply", Duration::from_secs(60)));
    let server = TestServer::start_with(
        factory,
        ServerConfig {
            limits: Limits {
                handler_timeout: Duration::from_millis(100),
                ..quick_limits()
            },
            ..ServerConfig::default()
        },
    )
    .await;
    let transport = server.client(ClientConfig::default());

    let reply = call(&transport, binary_call("slow", 3))
        .await
        .expect("the call should get an exception reply");
    let (_, seqid, exception) = exception_in(reply);
    assert_eq!(seqid, 3);
    assert_eq!(exception.type_, ApplicationExceptionErrorCode::Timeout);
    server.stop().await;
}

/// A client that reads a stream slowly holds back its producer.
#[mononoke::test]
async fn a_slow_reader_holds_back_the_producer() {
    let (factory, observed) = test_service(Behavior::EndlessStream {
        element_size: 16 * 1024,
    });
    let server = TestServer::start(factory).await;
    let transport = server.client(ClientConfig {
        stream_window: 64 * 1024,
        connection_window: 256 * 1024,
        ..ClientConfig::default()
    });

    let (_, mut stream) = call_stream(&transport).await.expect("stream should start");
    for _ in 0..3 {
        stream
            .next()
            .await
            .expect("the stream should go on")
            .expect("elements should arrive");
    }
    sleep(Duration::from_millis(500)).await;
    let produced = observed.stream_elements_produced.load(Ordering::SeqCst);
    // A 64 KiB window holds four elements; allow for buffering at each end.
    assert!(
        produced < 16,
        "a stalled reader should stop the producer, but {produced} elements were produced"
    );
    sleep(Duration::from_millis(300)).await;
    assert_eq!(
        observed.stream_elements_produced.load(Ordering::SeqCst),
        produced,
        "production should stay stopped while the reader is stalled"
    );
    drop(stream);
    server.stop().await;
}

/// Dropping a stream resets it, which drops the service's stream.
#[mononoke::test]
async fn dropping_a_stream_stops_its_producer() {
    let (factory, observed) = test_service(Behavior::EndlessStream { element_size: 1024 });
    let server = TestServer::start(factory).await;
    let transport = server.client(ClientConfig::default());

    let (_, mut stream) = call_stream(&transport).await.expect("stream should start");
    stream
        .next()
        .await
        .expect("the stream should go on")
        .expect("an element should arrive");
    drop(stream);
    timeout(Duration::from_secs(5), async {
        while !observed.stream_dropped.load(Ordering::SeqCst) {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the service's stream should be dropped");
    server.stop().await;
}

async fn abandon_a_slow_call(cancel_on_client_disconnect: bool) -> Arc<Observed> {
    let (factory, observed) = test_service(unary(b"reply", Duration::from_millis(400)));
    let server = TestServer::start_with(
        factory,
        ServerConfig {
            cancel_on_client_disconnect,
            limits: quick_limits(),
            ..ServerConfig::default()
        },
    )
    .await;
    let transport = server.client(ClientConfig {
        request_timeout: Some(Duration::from_millis(100)),
        ..ClientConfig::default()
    });
    call(&transport, binary_call("slow", 1))
        .await
        .expect_err("the call should time out");
    sleep(Duration::from_millis(700)).await;
    server.stop().await;
    observed
}

#[mononoke::test]
async fn abandoned_calls_run_to_completion_by_default() {
    let observed = abandon_a_slow_call(false).await;
    assert_eq!(observed.finished.load(Ordering::SeqCst), 1);
    assert!(!observed.handler_dropped.load(Ordering::SeqCst));
}

#[mononoke::test]
async fn abandoned_calls_are_cancelled_when_asked() {
    let observed = abandon_a_slow_call(true).await;
    assert_eq!(observed.finished.load(Ordering::SeqCst), 0);
    assert!(observed.handler_dropped.load(Ordering::SeqCst));
}

/// An idle connection is closed with GOAWAY, and the next call opens a new
/// one, running once.
#[mononoke::test]
async fn idle_connections_close_and_clients_reconnect() {
    let (factory, observed) = test_service(unary(b"reply", Duration::ZERO));
    let server = TestServer::start_with(
        factory,
        ServerConfig {
            limits: Limits {
                idle_timeout: Duration::from_millis(100),
                ..quick_limits()
            },
            ..ServerConfig::default()
        },
    )
    .await;
    let transport = server.client(ClientConfig::default());

    call(&transport, binary_call("m", 1))
        .await
        .expect("the first call should succeed");
    sleep(Duration::from_millis(400)).await;
    call(&transport, binary_call("m", 2))
        .await
        .expect("the call after the idle timeout should succeed");
    assert_eq!(observed.started.load(Ordering::SeqCst), 2);
    assert_eq!(
        observed.peers.lock().unwrap().len(),
        2,
        "the second call should have used a new connection"
    );
    server.stop().await;
}

#[mononoke::test]
async fn connections_over_the_limit_wait() {
    let server = TestServer::start_with(
        named_service("limited"),
        ServerConfig {
            limits: Limits {
                max_connections: 1,
                ..quick_limits()
            },
            ..ServerConfig::default()
        },
    )
    .await;
    let first = server.client(ClientConfig::default());
    first.probe().await.expect("the first client should connect");

    let second = server.client(ClientConfig {
        connect_timeout: Duration::from_millis(300),
        ..ClientConfig::default()
    });
    assert!(
        second.probe().await.is_err(),
        "a second connection should wait for the first to close"
    );
    drop(first);
    let third = server.client(ClientConfig::default());
    timeout(Duration::from_secs(5), third.probe())
        .await
        .expect("a connection should be accepted once the first closes")
        .expect("the connection should succeed");
    server.stop().await;
}

#[mononoke::test]
async fn silent_clients_are_dropped_after_the_handshake_timeout() {
    let server = TestServer::start_with(
        named_service("x"),
        ServerConfig {
            limits: Limits {
                handshake_timeout: Duration::from_millis(200),
                ..Limits::default()
            },
            ..ServerConfig::default()
        },
    )
    .await;
    let mut silent = TcpStream::connect(&server.address)
        .await
        .expect("TCP should connect");
    let mut buffer = [0_u8; 256];
    let closed = timeout(Duration::from_secs(5), async {
        loop {
            match silent.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await;
    assert!(closed.is_ok(), "the server should close a silent connection");
    server.stop().await;
}

/// Shutdown lets a request in flight finish, then stops serving.
#[mononoke::test]
async fn shutdown_lets_requests_in_flight_finish() {
    let (factory, observed) = test_service(unary(b"reply", Duration::from_millis(300)));
    let server = TestServer::start(factory).await;
    let transport = server.client(ClientConfig::default());

    let in_flight = mononoke::spawn_task({
        let transport = transport.clone();
        async move { call(&transport, binary_call("m", 1)).await }
    });
    while observed.started.load(Ordering::SeqCst) == 0 {
        sleep(Duration::from_millis(10)).await;
    }
    timeout(Duration::from_secs(5), server.stop())
        .await
        .expect("shutdown should finish once the request has");
    assert_eq!(
        in_flight
            .await
            .expect("task should not panic")
            .expect("the request in flight should succeed"),
        Bytes::from_static(b"reply")
    );
    assert!(
        call(&transport, binary_call("m", 2)).await.is_err(),
        "a stopped server should refuse calls"
    );
}

mod tls {
    use super::*;

    fn tls_server_config(ca: &Ca) -> ServerConfig {
        ServerConfig {
            tls: Some(ca.server_tls()),
            limits: quick_limits(),
            ..ServerConfig::default()
        }
    }

    fn client_config(tls: crate::ClientTls) -> ClientConfig {
        ClientConfig {
            tls: Some(tls),
            ..ClientConfig::default()
        }
    }

    /// Whether a client that should work still does, so a failed handshake
    /// has not harmed the server.
    async fn still_serves(server: &TestServer, ca: &Ca) {
        let good = server.tls_client(client_config(ca.client_tls(&ca.user("bob"))));
        call(&good, binary_call("m", 1))
            .await
            .expect("a valid client should still be served");
    }

    async fn assert_refused(server: &TestServer, ca: &Ca, client: ClientTransport) {
        let error = call(&client, binary_call("m", 1))
            .await
            .expect_err("the connection should be refused");
        debug!("refused as expected: {error:#}");
        still_serves(server, ca).await;
    }

    #[mononoke::test]
    async fn certificates_identify_the_caller() {
        let ca = Ca::new("ca");
        let (factory, observed) = test_service(unary(b"reply", Duration::ZERO));
        let server = TestServer::start_with(factory, tls_server_config(&ca)).await;
        let client = server.tls_client(client_config(ca.client_tls(&ca.user("alice"))));

        assert_eq!(
            call(&client, binary_call("m", 1))
                .await
                .expect("the call should succeed"),
            Bytes::from_static(b"reply")
        );
        assert_eq!(
            *observed.identities.lock().unwrap(),
            vec![Some(vec![
                "USER:alice".to_owned(),
                "X509_SUBJECT_NAME:CN=alice,O=mononoke-local".to_owned(),
            ])]
        );
        server.stop().await;
    }

    #[mononoke::test]
    async fn subjects_stay_escaped() {
        let ca = Ca::new("ca");
        let (factory, observed) = test_service(unary(b"reply", Duration::ZERO));
        let server = TestServer::start_with(factory, tls_server_config(&ca)).await;
        let sneaky = ca.client(
            &[("CN", "alice,O=admins"), ("O", "mononoke-local")],
            "user:alice",
            VALID,
        );
        let client = server.tls_client(client_config(ca.client_tls(&sneaky)));

        call(&client, binary_call("m", 1))
            .await
            .expect("the call should succeed");
        assert_eq!(
            *observed.identities.lock().unwrap(),
            vec![Some(vec![
                "USER:alice".to_owned(),
                "X509_SUBJECT_NAME:CN=alice\\,O=admins,O=mononoke-local".to_owned(),
            ])]
        );
        server.stop().await;
    }

    #[mononoke::test]
    async fn plaintext_requests_carry_no_identities() {
        let (factory, observed) = test_service(unary(b"reply", Duration::ZERO));
        let server = TestServer::start(factory).await;
        call(&server.client(ClientConfig::default()), binary_call("m", 1))
            .await
            .expect("the call should succeed");
        assert_eq!(*observed.identities.lock().unwrap(), vec![None]);
        server.stop().await;
    }

    #[mononoke::test]
    async fn clients_from_another_ca_are_refused() {
        let ca = Ca::new("ca");
        let other = Ca::new("other");
        let server = TestServer::start_with(named_service("x"), tls_server_config(&ca)).await;
        // Trusts the server's CA, but presents a certificate it doesn't sign.
        let issued = other.user("mallory");
        let tls = crate::ClientTls::from_pem(&ca.pem(), &issued.certificate, &issued.key);
        let client = server.tls_client(client_config(tls.expect("the client config is valid")));
        assert_refused(&server, &ca, client).await;
        server.stop().await;
    }

    #[mononoke::test]
    async fn servers_from_another_ca_are_refused() {
        let ca = Ca::new("ca");
        let other = Ca::new("other");
        let server = TestServer::start_with(named_service("x"), tls_server_config(&ca)).await;
        // Presents a certificate the server trusts, but trusts another CA.
        let issued = ca.user("alice");
        let tls = crate::ClientTls::from_pem(&other.pem(), &issued.certificate, &issued.key)
            .expect("the client config is valid");
        assert_refused(&server, &ca, server.tls_client(client_config(tls))).await;
        server.stop().await;
    }

    #[mononoke::test]
    async fn expired_client_certificates_are_refused() {
        let ca = Ca::new("ca");
        let server = TestServer::start_with(named_service("x"), tls_server_config(&ca)).await;
        let client = server.tls_client(client_config(ca.client_tls(&ca.expired_user("alice"))));
        assert_refused(&server, &ca, client).await;
        server.stop().await;
    }

    #[mononoke::test]
    async fn expired_server_certificates_are_refused() {
        let ca = Ca::new("ca");
        let expired = ca.expired_server();
        let config = ServerConfig {
            tls: Some(
                ServerTls::from_pem(&ca.pem(), &expired.certificate, &expired.key)
                    .expect("the server config is valid"),
            ),
            limits: quick_limits(),
            ..ServerConfig::default()
        };
        let server = TestServer::start_with(named_service("x"), config).await;
        let client = server.tls_client(client_config(ca.client_tls(&ca.user("alice"))));
        call(&client, binary_call("m", 1))
            .await
            .expect_err("a server with an expired certificate should be refused");
        server.stop().await;
    }

    #[mononoke::test]
    async fn clients_without_certificates_are_refused() {
        let ca = Ca::new("ca");
        let server = TestServer::start_with(named_service("x"), tls_server_config(&ca)).await;
        let tls = lax_client_tls(&ca, None, true, SslVersion::TLS1_3);
        assert_refused(&server, &ca, server.tls_client(client_config(tls))).await;
        server.stop().await;
    }

    #[mononoke::test]
    async fn servers_must_hold_a_certificate_for_the_name_dialled() {
        let ca = Ca::new("ca");
        let elsewhere = ca.server_for(&["mononoke.example.com"], &[], VALID);
        let config = ServerConfig {
            tls: Some(
                ServerTls::from_pem(&ca.pem(), &elsewhere.certificate, &elsewhere.key)
                    .expect("the server config is valid"),
            ),
            limits: quick_limits(),
            ..ServerConfig::default()
        };
        let server = TestServer::start_with(named_service("x"), config).await;
        let client = server.tls_client(client_config(ca.client_tls(&ca.user("alice"))));
        let error = call(&client, binary_call("m", 1))
            .await
            .expect_err("a certificate for another name should be refused");
        assert!(format!("{error:#}").contains("TLS handshake"), "{error:#}");
        server.stop().await;
    }

    #[mononoke::test]
    async fn plaintext_clients_cannot_use_a_tls_port() {
        let ca = Ca::new("ca");
        let server = TestServer::start_with(named_service("x"), tls_server_config(&ca)).await;
        let plaintext = server.client(ClientConfig {
            connect_timeout: Duration::from_secs(2),
            ..ClientConfig::default()
        });
        assert_refused(&server, &ca, plaintext).await;
        server.stop().await;
    }

    #[mononoke::test]
    async fn tls_clients_cannot_use_a_plaintext_port() {
        let ca = Ca::new("ca");
        let server = TestServer::start(named_service("x")).await;
        let client = server.tls_client(ClientConfig {
            connect_timeout: Duration::from_secs(2),
            ..client_config(ca.client_tls(&ca.user("alice")))
        });
        call(&client, binary_call("m", 1))
            .await
            .expect_err("TLS to a plaintext port should fail");
        server.stop().await;
    }

    #[mononoke::test]
    async fn tls_1_2_is_refused() {
        let ca = Ca::new("ca");
        let server = TestServer::start_with(named_service("x"), tls_server_config(&ca)).await;
        let issued = ca.user("alice");
        let tls = lax_client_tls(&ca, Some(&issued), true, SslVersion::TLS1_2);
        assert_refused(&server, &ca, server.tls_client(client_config(tls))).await;
        server.stop().await;
    }

    #[mononoke::test]
    async fn clients_must_negotiate_h2() {
        let ca = Ca::new("ca");
        let server = TestServer::start_with(named_service("x"), tls_server_config(&ca)).await;
        let issued = ca.user("alice");
        let tls = lax_client_tls(&ca, Some(&issued), false, SslVersion::TLS1_3);
        assert_refused(&server, &ca, server.tls_client(client_config(tls))).await;
        server.stop().await;
    }
}

/// A minimal HTTP/2 server, written frame by frame, to make the server-side
/// events whose handling decides whether a client may send a call again.
mod raw_http2 {
    use super::*;

    const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
    const DATA: u8 = 0;
    const HEADERS: u8 = 1;
    const SETTINGS: u8 = 4;
    const PING: u8 = 6;
    const GOAWAY: u8 = 7;
    const END_STREAM: u8 = 0x1;
    const ACK: u8 = 0x1;
    const END_HEADERS: u8 = 0x4;

    #[derive(Clone, Copy)]
    enum Behavior {
        /// Answer every request.
        Answer,
        /// Once two requests arrived, send GOAWAY with this last stream id,
        /// answer the requests up to it, and leave the rest unanswered.
        GoAwayAnswering(u32),
        /// Once two requests arrived, send GOAWAY with this last stream id
        /// and close the connection without answering.
        GoAwayAndClose(u32),
        /// Close the connection once a request arrived.
        HangUp,
    }

    fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
        let mut frame = Vec::with_capacity(9 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
        frame.push(kind);
        frame.push(flags);
        frame.extend_from_slice(&stream.to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    async fn read_frame(socket: &mut TcpStream) -> Option<(u8, u8, u32, Vec<u8>)> {
        let mut header = [0_u8; 9];
        socket.read_exact(&mut header).await.ok()?;
        let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        let stream = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
        let mut payload = vec![0; len];
        socket.read_exact(&mut payload).await.ok()?;
        Some((header[3], header[4], stream, payload))
    }

    async fn answer(socket: &mut TcpStream, stream: u32) {
        // `:status: 200` from HPACK's static table.
        let mut reply = frame(HEADERS, END_HEADERS, stream, &[0x88]);
        let body = encode_frame(b"reply", MAX_FRAME_SIZE).unwrap();
        reply.extend(frame(DATA, END_STREAM, stream, &body));
        let _ = socket.write_all(&reply).await;
    }

    /// Serves one connection, recording each request that arrived.
    async fn serve_one(mut socket: TcpStream, behavior: Behavior, arrived: Arc<AtomicUsize>) {
        let mut preface = [0_u8; PREFACE.len()];
        if socket.read_exact(&mut preface).await.is_err() || preface != PREFACE {
            return;
        }
        let _ = socket.write_all(&frame(SETTINGS, 0, 0, &[])).await;
        let mut complete = Vec::new();
        while let Some((kind, flags, stream, payload)) = read_frame(&mut socket).await {
            match kind {
                SETTINGS if flags & ACK == 0 => {
                    let _ = socket.write_all(&frame(SETTINGS, ACK, 0, &[])).await;
                }
                PING if flags & ACK == 0 => {
                    let _ = socket.write_all(&frame(PING, ACK, 0, &payload)).await;
                }
                DATA | HEADERS if flags & END_STREAM != 0 => {
                    arrived.fetch_add(1, Ordering::SeqCst);
                    complete.push(stream);
                    match behavior {
                        Behavior::Answer => answer(&mut socket, stream).await,
                        Behavior::HangUp => return,
                        Behavior::GoAwayAnswering(last) | Behavior::GoAwayAndClose(last)
                            if complete.len() == 2 =>
                        {
                            let mut go_away = last.to_be_bytes().to_vec();
                            go_away.extend_from_slice(&0_u32.to_be_bytes());
                            let _ = socket.write_all(&frame(GOAWAY, 0, 0, &go_away)).await;
                            if let Behavior::GoAwayAndClose(_) = behavior {
                                return;
                            }
                            for &stream in complete.iter().filter(|&&s| s <= last) {
                                answer(&mut socket, stream).await;
                            }
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }

    /// Serves the first connection with `first`, and any later ones by
    /// answering. Returns the address, the number of connections, and the
    /// number of requests that arrived.
    async fn start(first: Behavior) -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().unwrap().to_string();
        let connections = Arc::new(AtomicUsize::new(0));
        let arrived = Arc::new(AtomicUsize::new(0));
        let (seen, counted) = (connections.clone(), arrived.clone());
        mononoke::spawn_task(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let behavior = if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                    first
                } else {
                    Behavior::Answer
                };
                mononoke::spawn_task(serve_one(socket, behavior, counted.clone()));
            }
        });
        (address, connections, arrived)
    }

    fn client(address: &str) -> ClientTransport {
        ClientTransport::with_config(
            address,
            ClientConfig {
                request_timeout: Some(Duration::from_secs(10)),
                ..ClientConfig::default()
            },
        )
        .expect("loopback should be accepted")
    }

    #[mononoke::test]
    async fn the_raw_server_answers() {
        let (address, _, _) = start(Behavior::Answer).await;
        assert_eq!(
            call(&client(&address), binary_call("m", 1)).await.unwrap(),
            Bytes::from_static(b"reply")
        );
    }

    /// A request above a GOAWAY's last stream id was not processed, so the
    /// client sends it again, on a new connection.
    #[mononoke::test]
    async fn requests_above_a_goaway_are_sent_again() {
        let (address, connections, arrived) = start(Behavior::GoAwayAnswering(1)).await;
        let transport = client(&address);
        let (first, second) = tokio::join!(
            call(&transport, binary_call("m", 1)),
            call(&transport, binary_call("m", 2)),
        );
        first.expect("the request under the GOAWAY should be answered");
        second.expect("the request above the GOAWAY should succeed when sent again");
        assert_eq!(connections.load(Ordering::SeqCst), 2);
        assert_eq!(arrived.load(Ordering::SeqCst), 3);
    }

    /// Requests a GOAWAY covers may have been processed, so when the
    /// connection then closes they fail instead of being sent again.
    #[mononoke::test]
    async fn requests_a_goaway_covers_are_not_sent_again() {
        let (address, connections, arrived) = start(Behavior::GoAwayAndClose(3)).await;
        let transport = client(&address);
        let (first, second) = tokio::join!(
            call(&transport, binary_call("m", 1)),
            call(&transport, binary_call("m", 2)),
        );
        assert!(first.is_err() && second.is_err());
        assert_eq!(connections.load(Ordering::SeqCst), 1);
        assert_eq!(arrived.load(Ordering::SeqCst), 2);
    }

    /// A server that hangs up after reading a request may have acted on it,
    /// so the call fails instead of being sent again.
    #[mononoke::test]
    async fn a_request_is_never_resent_after_the_server_hangs_up() {
        let (address, connections, arrived) = start(Behavior::HangUp).await;
        call(&client(&address), binary_call("m", 1))
            .await
            .expect_err("a server that never replies should fail the call");
        assert_eq!(connections.load(Ordering::SeqCst), 1);
        assert_eq!(arrived.load(Ordering::SeqCst), 1);
    }

    #[mononoke::test]
    fn frames_decode_as_written() {
        let mut decoder = FrameDecoder::new(MAX_FRAME_SIZE);
        decoder.push(&encode_frame(b"reply", MAX_FRAME_SIZE).unwrap());
        assert_eq!(
            decoder.next_frame().unwrap(),
            Some(Bytes::from_static(b"reply"))
        );
    }
}
