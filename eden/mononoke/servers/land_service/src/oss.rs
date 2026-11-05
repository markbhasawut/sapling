/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use clap::Args;
use context::CoreContext;
use mononoke_app::args::TLSArgs;
use metadata::Metadata;
use mononoke_app::MononokeApp;
use mononoke_thrift_server::RequestContext;
use mononoke_thrift_server::ThriftServerBuilder;
use permission_checker::MononokeIdentity;
use repo_authorization::AuthorizationContext;
use scuba_ext::MononokeScubaSampleBuilder;

use crate::errors::LandChangesetsError;

#[derive(Debug, Args)]
pub(crate) struct ServerArgs {
    /// Serve mutual TLS. TLSArgs' fields are required once any is given,
    /// so it is optional as a whole: without it, the server serves
    /// plaintext on loopback.
    #[clap(flatten)]
    pub(crate) tls_args: Option<TLSArgs>,
}

#[derive(Clone)]
pub(crate) struct ServiceState;

impl ServiceState {
    pub(crate) async fn new(_app: &MononokeApp, _args: &ServerArgs) -> anyhow::Result<Self> {
        Ok(Self)
    }

    pub(crate) async fn caller_admission(
        &self,
        _ctx: &CoreContext,
    ) -> Result<Option<&'static str>, LandChangesetsError> {
        Ok(None)
    }

    pub(crate) fn add_scuba_params(
        &self,
        _req_ctxt: &RequestContext,
        _scuba: &mut MononokeScubaSampleBuilder,
    ) -> Result<(), LandChangesetsError> {
        Ok(())
    }

    /// Metadata for a request: the identities in the caller's TLS
    /// certificate, or `OSS_LOCAL` for a caller over plaintext loopback.
    pub(crate) async fn create_metadata(
        &self,
        req_ctxt: &RequestContext,
    ) -> Result<Metadata, LandChangesetsError> {
        let identities = req_ctxt
            .identities()
            .cloned()
            .unwrap_or_else(|| [MononokeIdentity::oss_local()].into_iter().collect());
        let peer = req_ctxt.peer_addr();
        Ok(Metadata::new(
            None,
            identities,
            false,
            false,
            peer.map(|peer| peer.ip()),
            peer.map(|peer| peer.port()),
        )
        .await)
    }
}

pub(crate) fn configure_server(
    builder: ThriftServerBuilder,
    _app: &MononokeApp,
) -> ThriftServerBuilder {
    builder
}

/// Callers with TLS certificates are subject to the repos' ACLs, as SLAPI's
/// are, and may act as a service if its ACL allows them, which each write
/// checks. Callers over plaintext loopback are not subject to ACLs, and
/// can't be identified, so their requests naming a service are refused.
pub(crate) fn authorization_context(
    ctx: &CoreContext,
    service_identity: Option<String>,
) -> Result<AuthorizationContext, LandChangesetsError> {
    let authz = AuthorizationContext::new_oss(ctx);
    match service_identity {
        None => Ok(authz),
        Some(_) if authz == AuthorizationContext::new_oss_local() => {
            Err(LandChangesetsError::AuthorizationError(
                "service_identity needs a TLS client certificate in OSS builds".to_string(),
            ))
        }
        Some(service_identity) => Ok(AuthorizationContext::new_for_service_writes(
            service_identity,
        )),
    }
}

#[cfg(test)]
mod tests {
    use fbinit::FacebookInit;
    use mononoke_macros::mononoke;

    use super::*;

    async fn ctx_for(fb: FacebookInit, req_ctxt: &RequestContext) -> CoreContext {
        let Ok(metadata) = ServiceState.create_metadata(req_ctxt).await else {
            panic!("OSS request metadata should be created");
        };
        context::SessionContainer::builder(fb)
            .metadata(std::sync::Arc::new(metadata))
            .build()
            .new_context(CoreContext::test_mock(fb).scuba().clone())
    }

    #[mononoke::fbinit_test]
    async fn loopback_requests_are_not_subject_to_access_control(fb: FacebookInit) {
        let ctx = ctx_for(fb, &RequestContext::new()).await;
        let Ok(authz) = authorization_context(&ctx, None) else {
            panic!("a request without a service identity should be accepted");
        };
        assert_eq!(authz, AuthorizationContext::new_oss_local());
    }

    #[mononoke::fbinit_test]
    async fn loopback_requests_naming_a_service_are_refused(fb: FacebookInit) {
        let ctx = ctx_for(fb, &RequestContext::new()).await;
        assert!(authorization_context(&ctx, Some("some_service".to_string())).is_err());
    }

    #[mononoke::fbinit_test]
    async fn tls_requests_are_subject_to_access_control(fb: FacebookInit) {
        let identities = [MononokeIdentity::from_legacy_type_data("USER", "alice")];
        let req_ctxt = RequestContext::for_peer(
            "192.0.2.1:4000".parse().unwrap(),
            Some(identities.into_iter().collect()),
        );
        let ctx = ctx_for(fb, &req_ctxt).await;
        let Ok(authz) = authorization_context(&ctx, None) else {
            panic!("a TLS request should be accepted");
        };
        assert_eq!(authz, AuthorizationContext::new(&ctx));
        let Ok(service) = authorization_context(&ctx, Some("some_service".to_string())) else {
            panic!("a TLS caller may ask to act as a service");
        };
        assert_eq!(
            service,
            AuthorizationContext::new_for_service_writes("some_service")
        );
    }

    #[mononoke::test]
    async fn requests_carry_the_oss_local_identity() {
        let Ok(metadata) = ServiceState.create_metadata(&RequestContext::new()).await else {
            panic!("OSS request metadata should be created");
        };
        assert_eq!(
            metadata.identities().iter().cloned().collect::<Vec<_>>(),
            vec![MononokeIdentity::oss_local()]
        );
    }
}
