/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! Thrift transport for OSS builds of Mononoke's services, which have no
//! srserver or ServiceRouter.
//!
//! [`server::serve_with`] serves any generated service, and
//! [`client::connect_with`] builds a client for one. Each call is a stream
//! of an HTTP/2 connection:
//!
//! - **Security:** connections use mutual TLS 1.3 ([`ServerTls`],
//!   [`ClientTls`]): each end verifies the other's certificate against its
//!   configured CA. Plaintext is for tests and local development: both ends
//!   refuse it on addresses other than loopback.
//! - **Identity:** the server takes the client's identities from its
//!   certificate as SLAPI does, with
//!   `permission_checker::MononokeIdentity::try_from_x509`, and hands them
//!   to services in their [`server::RequestContext`].
//! - **Concurrency:** one connection carries many calls at once, up to the
//!   server's limits. A call is sent again only when the server certainly did
//!   not process it.
//! - **Streaming:** streaming replies have backpressure and cancellation.
//! - **Limits and shutdown:** the limits and timeouts are in
//!   [`server::Limits`] and [`client::ClientConfig`]. Shutdown is graceful.
//!
//! On the wire, a request is `POST /<service>/<method>` with one serialized
//! Thrift message as its body. A reply's body is a sequence of frames, each
//! a big-endian `u32` length followed by that many bytes: the reply, or a
//! streaming reply's initial response followed by stream frames, each tagged
//! with a one-byte kind, and ended by an explicit end-of-stream frame.
//!
//! Sink, bidirectional and interaction RPCs are refused. See `DESIGN.md` for
//! the reasons behind each choice.

pub mod client;
mod context;
mod framing;
mod loopback;
pub mod server;
#[cfg(test)]
mod test_certs;
mod tls;

pub use framing::MAX_FRAME_SIZE;
pub use tls::ClientTls;
pub use tls::ServerTls;

/// Content type of requests and replies.
const CONTENT_TYPE: &str = "application/x-thrift";
