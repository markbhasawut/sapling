/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use context::CoreContext;
use metadata::Metadata;
use mononoke_app::MononokeApp;
use mononoke_thrift_server::RequestContext;
use mononoke_thrift_server::ThriftServerBuilder;
use permission_checker::MononokeIdentity;
use repo_authorization::AuthorizationContext;
use scuba_ext::MononokeScubaSampleBuilder;

use crate::errors::ServerError;

#[derive(Clone)]
pub(crate) struct ServiceState;

impl ServiceState {
    pub(crate) async fn new(_app: &MononokeApp) -> anyhow::Result<Self> {
        Ok(Self)
    }

    pub(crate) fn add_scuba_params(
        &self,
        _req_ctxt: &RequestContext,
        _scuba: &mut MononokeScubaSampleBuilder,
    ) -> Result<(), ServerError> {
        Ok(())
    }

    /// Metadata for a request: the identities in the caller's TLS
    /// certificate, or `OSS_LOCAL` for a caller over plaintext loopback.
    pub(crate) async fn create_metadata(
        &self,
        req_ctxt: &RequestContext,
    ) -> Result<Metadata, ServerError> {
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

/// Callers with TLS certificates are subject to the repos' ACLs, as SLAPI's
/// are; callers over plaintext loopback are not.
pub(crate) fn authorization_context(ctx: &CoreContext) -> AuthorizationContext {
    AuthorizationContext::new_oss(ctx)
}

pub(crate) fn set_nocache_header(_req_ctxt: &RequestContext) {}

pub(crate) fn configure_server(
    builder: ThriftServerBuilder,
    _app: &MononokeApp,
) -> ThriftServerBuilder {
    builder
}

pub(crate) struct ServerFactory;

impl ServerFactory {
    pub(crate) async fn new(
        _fb: fbinit::FacebookInit,
        _service_name: &'static str,
    ) -> anyhow::Result<Self> {
        Ok(Self)
    }

    pub(crate) fn executor(&self) {}

    pub(crate) async fn shutdown(self) {}
}

#[cfg(test)]
mod tests {
    use fbinit::FacebookInit;
    use mononoke_macros::mononoke;

    use super::*;

    async fn ctx_for(fb: FacebookInit, req_ctxt: &RequestContext) -> CoreContext {
        let metadata = ServiceState
            .create_metadata(req_ctxt)
            .await
            .expect("OSS request metadata should be created");
        context::SessionContainer::builder(fb)
            .metadata(std::sync::Arc::new(metadata))
            .build()
            .new_context(CoreContext::test_mock(fb).scuba().clone())
    }

    #[mononoke::fbinit_test]
    async fn loopback_requests_are_not_subject_to_access_control(fb: FacebookInit) {
        let ctx = ctx_for(fb, &RequestContext::new()).await;
        assert_eq!(
            authorization_context(&ctx),
            AuthorizationContext::new_oss_local()
        );
    }

    #[mononoke::fbinit_test]
    async fn tls_requests_are_subject_to_access_control(fb: FacebookInit) {
        let identities = [MononokeIdentity::from_legacy_type_data("USER", "alice")];
        let req_ctxt = RequestContext::for_peer(
            "192.0.2.1:4000".parse().unwrap(),
            Some(identities.into_iter().collect()),
        );
        let ctx = ctx_for(fb, &req_ctxt).await;
        assert_eq!(
            ctx.metadata().identities(),
            req_ctxt.identities().expect("the context has identities")
        );
        assert_eq!(authorization_context(&ctx), AuthorizationContext::new(&ctx));
    }

    #[mononoke::test]
    async fn requests_carry_the_oss_local_identity() -> anyhow::Result<()> {
        let req_ctxt = RequestContext::new();
        let metadata = ServiceState.create_metadata(&req_ctxt).await?;
        assert_eq!(
            metadata.identities().iter().cloned().collect::<Vec<_>>(),
            vec![MononokeIdentity::oss_local()]
        );
        Ok(())
    }
}
