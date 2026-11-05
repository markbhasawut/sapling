/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use metadata::Metadata;
use mononoke_app::MononokeApp;
use mononoke_thrift_server::RequestContext;
use mononoke_thrift_server::ThriftServerBuilder;
use permission_checker::MononokeIdentity;
use scuba_ext::MononokeScubaSampleBuilder;

use crate::errors::ThriftError;

/// Metadata for a request: the identities in the caller's TLS certificate,
/// or `OSS_LOCAL` for a caller over plaintext loopback.
pub(crate) fn create_metadata(req_ctxt: &RequestContext) -> Result<Metadata, ThriftError> {
    let identities = req_ctxt
        .identities()
        .cloned()
        .unwrap_or_else(|| [MononokeIdentity::oss_local()].into_iter().collect());
    let peer = req_ctxt.peer_addr();
    Ok(Metadata::default()
        .set_identities(identities)
        .set_client_ip(peer.map(|peer| peer.ip()))
        .set_client_port(peer.map(|peer| peer.port())))
}

pub(crate) fn add_scuba_params(
    _req_ctxt: &RequestContext,
    _scuba: &mut MononokeScubaSampleBuilder,
) -> Result<(), ThriftError> {
    Ok(())
}

pub(crate) fn configure_server(
    builder: ThriftServerBuilder,
    _app: &MononokeApp,
) -> ThriftServerBuilder {
    builder
}

#[cfg(test)]
mod tests {
    use mononoke_macros::mononoke;

    use super::*;

    #[mononoke::test]
    fn requests_carry_their_callers_identities() {
        let local = create_metadata(&RequestContext::new()).expect("metadata should be created");
        assert_eq!(
            local.identities().iter().cloned().collect::<Vec<_>>(),
            vec![MononokeIdentity::oss_local()]
        );

        let alice = MononokeIdentity::from_legacy_type_data("USER", "alice");
        let tls = create_metadata(&RequestContext::for_peer(
            "192.0.2.1:4000".parse().unwrap(),
            Some([alice.clone()].into_iter().collect()),
        ))
        .expect("metadata should be created");
        assert_eq!(
            tls.identities().iter().cloned().collect::<Vec<_>>(),
            vec![alice]
        );
        assert_eq!(tls.client_ip(), Some(&"192.0.2.1".parse().unwrap()));
    }
}
