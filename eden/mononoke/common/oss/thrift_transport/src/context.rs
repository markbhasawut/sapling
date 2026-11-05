/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use std::ffi::CStr;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use fbthrift::DummyContextStack;
use permission_checker::MononokeIdentitySet;

/// The connection a request arrived on.
#[derive(Debug)]
pub(crate) struct ConnectionInfo {
    pub(crate) peer_addr: SocketAddr,
    /// From the client's certificate, on TLS connections.
    pub(crate) identities: Option<MononokeIdentitySet>,
}

/// Request context passed to services: who sent the request, and from where.
#[derive(Clone, Debug, Default)]
pub struct RequestContext {
    connection: Option<Arc<ConnectionInfo>>,
}

impl RequestContext {
    /// A context with no connection, for calling a service directly.
    pub fn new() -> Self {
        Self::default()
    }

    /// A context for a request from `peer_addr`, by a caller with
    /// `identities` from its certificate, or none over plaintext.
    pub fn for_peer(peer_addr: SocketAddr, identities: Option<MononokeIdentitySet>) -> Self {
        Self::for_connection(Arc::new(ConnectionInfo {
            peer_addr,
            identities,
        }))
    }

    pub(crate) fn for_connection(connection: Arc<ConnectionInfo>) -> Self {
        Self {
            connection: Some(connection),
        }
    }

    /// The identities in the client's certificate, as
    /// `MononokeIdentity::try_from_x509` gives them. `None` when the request
    /// came over plaintext, which only loopback addresses accept, or did not
    /// come over a connection.
    pub fn identities(&self) -> Option<&MononokeIdentitySet> {
        self.connection.as_ref()?.identities.as_ref()
    }

    /// The address the request came from.
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.connection.as_ref().map(|connection| connection.peer_addr)
    }
}

impl fbthrift::RequestContext for RequestContext {
    type ContextStack = DummyContextStack<CStr, Bytes>;
    type Name = CStr;

    fn get_context_stack(
        &self,
        _service_name: &'static Self::Name,
        _fn_name: &'static Self::Name,
    ) -> Result<Self::ContextStack> {
        Ok(DummyContextStack::new())
    }

    fn set_user_exception_header(&self, _ex_type: &str, _ex_reason: &str) -> Result<()> {
        Ok(())
    }
}
