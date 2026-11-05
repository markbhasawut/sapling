# A deployable Thrift transport for OSS builds

Status: proposal, for agreement before the bulk of the work.

`oss_thrift_transport` is how OSS builds of Mononoke's Thrift services (SCS,
the diff, land and bookmark services, and their clients) talk, through
`mononoke_thrift_server::ThriftServerBuilder`. Today it is a reference
transport: plaintext, unauthenticated, loopback only, one request at a time
per connection, and one connection per client call. This proposal makes it
fit to deploy, keeping its API and its main guarantee: **a request that may
have run is never sent again**.

## Goals

- Mutual TLS 1.3. Both ends verify the other's certificate against a
  configured CA.
- Callers are identified from their certificates as SLAPI identifies them, so
  services can apply the same ACLs.
- Many requests in flight on one connection, with bounds.
- Limits and timeouts for everything a peer controls.
- Streaming replies with backpressure and cancellation.
- Graceful shutdown.
- `server::serve`, `client::connect` and the builder keep their shape, and the
  crate keeps building in both OSS and fbcode modes.

Out of scope: sink, bidirectional and interaction RPCs, which stay refused;
CRL and OCSP checks (use short-lived certificates); reloading certificates
without a restart; Rocket or header-protocol compatibility with fbcode
clients.

## Wire protocol: Thrift over HTTP/2

Each call is one HTTP/2 stream, using the `h2` crate, which hyper already
brings into Mononoke (SLAPI serves HTTP/2 through hyper 1.11).

- **Request:** `POST /<service>/<method>`, with `content-type:
  application/x-thrift`. The body is one serialized Thrift message. The
  service and method names come from fbthrift's `Transport::call` and only
  serve logging. Dispatch still reads the message.
- **Response:** `:status 200`. The body is a sequence of the length-prefixed
  frames that `framing.rs` defines today: the first frame is the reply or a
  stream's initial response; a streaming reply follows it with the typed
  stream frames (success, declared exception, application exception,
  end-of-stream). A transport-level refusal, such as a request over the size
  limit, has a 4xx/5xx status and no body.

**Why HTTP/2 and not request ids on the current framing:** the requirements
are, almost item for item, what HTTP/2 already specifies and `h2` has
hardened:

- Stream ids carry requests.
- `SETTINGS_MAX_CONCURRENT_STREAMS` bounds them.
- Per-stream flow control gives a streaming reply backpressure.
- `RST_STREAM` cancels a single call.
- `GOAWAY` shuts down gracefully.
- Above all, `REFUSED_STREAM` and the `GOAWAY` last-stream-id tell a client
  that a request was certainly not processed (RFC 9113 §8.7).

That last signal is what lets one connection carry many requests without
giving up the never-resend guarantee. Re-creating it on our own frames would
mean writing and fuzzing our own flow control and shutdown handshake.

The cost is a dependency surface with a CVE history (rapid reset, CONTINUATION
floods). That is handled by an `h2` new enough to have the mitigations and by
setting its limits explicitly (below).

## Security

**TLS library: `openssl`**, through `tokio-openssl`, which Mononoke's servers
already use: SLAPI's acceptor comes from `secure_utils::SslConfig`, and
`permission_checker::MononokeIdentity::try_from_x509` takes an `openssl` X509.
This choice means:

- one TLS stack, configured like SLAPI's, from the same `--tls-ca`,
  `--tls-certificate` and `--tls-private-key` files;
- identities computed by the same code as SLAPI's, rather than a second X509
  parser that could escape subjects differently;
- the system OpenSSL, patched by the distribution.

rustls would bring memory safety and no system library. It would also bring
a second TLS configuration to keep in step and a DER-to-openssl conversion on
every handshake to reuse `try_from_x509`. It remains an option if the user
prefers it.

**Settings:**

- **Server:** `SslAcceptor::mozilla_modern_v5` (TLS 1.3 only, its default
  ciphersuites). The CA file is the trust store. It requires a client
  certificate (`PEER | FAIL_IF_NO_PEER_CERT`) and ALPN `h2`; a client that
  offers no `h2` fails the handshake.
- **Client:** `SslConnector` with TLS 1.3 as its minimum, the CA file as the
  only trust root, the client's certificate and key, SNI, and hostname
  verification of the server's certificate against the host it dialled
  (`verify_hostname`, the default). It requires ALPN `h2`.
- Expiry, chain and signature checks are OpenSSL's verification, on both
  ends.

**Plaintext** stays for tests and local development only, as cleartext
HTTP/2 with prior knowledge, and only on loopback, as today. The rule is
enforced in one place: a listener or a client on a non-loopback address must
have TLS configured, or `bind` and `connect` fail. A TLS listener may bind
any address.

## Identity and authorization

After the handshake, the server turns the client's certificate into a
`MononokeIdentitySet` with `MononokeIdentity::try_from_x509`, once per
connection:

- `X509_SUBJECT_NAME:<escaped subject>`, for example
  `X509_SUBJECT_NAME:CN=alice,O=mononoke-local`;
- `USER:alice`, from a SAN `URI:user:alice`.

If the certificate yields no identities, the connection is closed.

`server::RequestContext` becomes a struct instead of `DummyRequestContext`.
It still implements `fbthrift::RequestContext` with a dummy context stack, so
generated services take it unchanged. It carries:

- `identities() -> Option<&MononokeIdentitySet>`: `None` on plaintext
  loopback connections;
- `peer_addr() -> Option<SocketAddr>`.

`RequestContext::new()` stays, as a context with neither, for the tests that
use it.

The services' OSS `create_metadata` and `request_authorization_context` (in
`scs_methods`, `diff_service`, `land_service` and `bookmark_service`) then do:

- **Authenticated (TLS):** `Metadata` with the certificate's identities, and
  `AuthorizationContext::new(ctx)`. Repo access goes through the same
  `AclProvider` and `RepoPermissionChecker` as SLAPI. Service writes
  (`service_identity`) are checked against the configured service ACLs
  instead of being refused.
- **Plaintext loopback:** `oss_local` and full access, as today.

This makes the transport crate depend on `permission_checker`, which is
light: openssl, serde and fbinit.

## Concurrency, and never resending

**Server:**

- Each accepted stream is spawned as its own task, so one connection serves
  up to `max_concurrent_streams` (default 128) requests at once. `h2`
  advertises this limit and enforces it.
- A global in-flight limit (default 4096) is checked before the handler runs.
  A stream over the limit is reset with `REFUSED_STREAM`, which means "not
  processed".

**Client:**

- `ClientTransport` keeps one HTTP/2 connection per address, made on first
  use and replaced when it fails or receives `GOAWAY`. `SendRequest::ready()`
  waits for a stream slot, so callers queue rather than fail when the server's
  limit is reached.

**Retries** happen only when the server certainly did not run the request:

- the connection or the TLS/HTTP/2 handshake failed, so nothing was sent;
- the stream was reset by the server with `REFUSED_STREAM`;
- the stream was above the last-stream-id of a `GOAWAY` from the server.

The client then retries on a fresh connection, at most 3 times, with backoff.

Anything else after the request headers were sent fails the call and is not
retried. That includes a reset with another code, a connection drop, a
timeout, or a `GOAWAY` that covers the stream. The tests below pin each case.

## Limits and timeouts

All of these are in `server::Limits` and `client::ClientConfig`, with
defaults:

| Limit | Default | Applies to |
| --- | --- | --- |
| request message size | 16 MiB | server stops reading and answers 413; client refuses to send |
| unary reply, initial response, stream item | 16 MiB each | as today: an `ApplicationException` to the caller, or a terminal stream exception |
| HTTP/2 frame size, header list size | 16 KiB / 16 KiB | `h2` settings |
| reset-stream flood | `h2` `max_pending_accept_reset_streams` 20, `max_local_error_reset_streams` 256 | rapid-reset mitigations |
| connections | 1024 | the accept loop waits for a slot, as today |
| concurrent streams per connection | 128 | `h2` setting |
| in-flight requests, whole server | 4096 | `REFUSED_STREAM` above it |
| TLS + HTTP/2 handshake | 10 s | connection dropped |
| request body read | 30 s from the stream opening | stream reset |
| idle connection, no open streams | 5 min | `GOAWAY`, then close |
| handler | 15 min until the reply or a stream's first response | `ApplicationException` (TIMEOUT) |
| stalled stream: no window from the client | 60 s | stream reset |
| client connect + handshake | 10 s | error, retried as "not sent" |
| client request | none, can be set | error, never retried |

Buffers grow only as data arrives, never from a declared length, so a peer
can't make the server allocate a frame it never sends. That fixes the known
limit in today's `framing.rs`.

## Streaming, cancellation and shutdown

**Backpressure:** the server pulls the next element of a service's stream
only once `h2` has send capacity for the frame it holds. The client releases
flow-control capacity only as its caller consumes the stream. A slow reader
therefore slows the producer instead of filling memory.

**Cancellation:**

- Dropping the client's stream or response future resets the HTTP/2 stream
  (`CANCEL`). The server sees the reset and drops the service's stream,
  stopping the producer.
- For unary calls, dropping the handler on reset is what
  `ThriftServerBuilder::with_cancel_on_client_disconnect()` asks for, and it
  is honored when set. Without it, a handler runs to completion, as today.

**Graceful shutdown** (`serve`'s `shutdown` future):

1. Stop accepting connections.
2. Send `GOAWAY` on each connection, so clients know which requests will
   still be answered and resend the others elsewhere.
3. Let in-flight requests and streams finish for a drain timeout (default
   25 s, inside the builder's 30 s `stop()`), then close what remains.

## API compatibility

Kept as they are, with the semantics above:

- `server::bind(host, port)`: loopback, for plaintext.
- `server::serve(listener, factory, shutdown)`: plaintext, default limits.
- `client::connect::<F>(address)`: plaintext, loopback.
- `ClientTransport::new(address)`, `probe()`, `box_service` and `ReplyState`.

New:

- `server::ServerConfig { tls: Option<ServerTls>, limits: Limits,
  cancel_on_client_disconnect: bool }`, and
  `server::bind_with(host, port, &ServerConfig)`, which refuses a non-loopback
  address without TLS.
- `server::serve_with(listener, factory, shutdown, ServerConfig)`.
- `ServerTls::from_files(ca, cert, key)`, matching `TLSArgs`.
- `client::ClientConfig { tls: Option<ClientTls>, timeouts }`,
  `client::connect_with::<F>(address, ClientConfig)` and
  `ClientTransport::with_config(address, ClientConfig)`.
- `ClientTls::from_files(ca, cert, key)`.

**Builder (`cmdlib/thrift_server` OSS backend):**

- `with_tls()` stops being a warning.
- `with_tls_args(&TLSArgs)` (OSS only, like the fbcode-only options the other
  way round) supplies the files. `with_tls()` without them fails `build()`
  rather than serving plaintext.
- A non-loopback `host()` needs TLS.

The services and the `scsc`, diff, bookmark and pushrebase clients gain the
existing `TLSArgs` flags in OSS builds.

**Build modes:** the BUCK target gains `fbsource//third-party/rust:` `h2`,
`http`, `openssl`, `tokio-openssl` and (for tests) `quickcheck`, plus
`//eden/mononoke/common/permission_checker:permission_checker`. Nothing in the
crate is OSS-only, so its tests also run in fbcode mode, as today.

## Tests

**Frame parsing:**

- quickcheck properties for the incremental frame decoder:
  - any sequence of frames survives encoding, splitting into arbitrary
    chunks, and decoding;
  - arbitrary bytes never panic, and either decode or fail;
  - a declared length over the limit fails before anything is allocated;
  - truncation at any point fails at end of stream;
  - stream-frame decoding of arbitrary payloads never panics.
- A `cargo fuzz` target for the same decoder, for those with a nightly
  toolchain.

**TLS, negative:**

- client certificate from another CA;
- server certificate from another CA;
- expired client certificate;
- expired server certificate;
- no client certificate;
- server name mismatch;
- plaintext client to a TLS port;
- TLS client to a plaintext port;
- TLS 1.2-only client;
- client without ALPN `h2`.

In each case the handshake fails with a clear error and the server keeps
serving others.

**Identity:** a certificate `CN=alice,O=mononoke-local` with SAN
`URI:user:alice` reaches the service as exactly those two identities. A
subject with `,` stays escaped.

**Concurrency and resending:**

- 64 slow calls on one connection overlap;
- no more than the configured limit run at once;
- a server that hangs up after reading a request fails the call and sees it
  exactly once;
- requests refused with `REFUSED_STREAM`, or above a `GOAWAY`, run exactly
  once after the client retries them.

**Limits:**

- oversized request (413, not retried);
- oversized reply (`ApplicationException`);
- connection limit;
- handshake timeout against a client that never speaks;
- idle timeout followed by a transparent reconnect;
- handler timeout.

**Streaming:**

- a fast producer and a slow reader keep the producer bounded;
- dropping the client's stream drops the server's;
- streams end only with an end-of-stream frame, as today.

**Shutdown:** an in-flight request completes after shutdown begins; new
connections are refused.

**Kept:** upstream's existing tests, adapted to the new wire format.

**Integration:** the builder test, with TLS and a non-loopback address. If
the user agrees, also an SCS read and a denied read through TLS with the ACL
file that SLAPI's tests use.
