/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use metadata::Metadata;
use mononoke_api::CoreContext;
use mononoke_app::MononokeApp;
use mononoke_configs::MononokeConfigs;
use mononoke_thrift_server::RequestContext;
use permission_checker::MononokeIdentity;
use repo_authorization::AuthorizationContext;
use scuba_ext::MononokeScubaSampleBuilder;

use crate::request_observability::RequestEvent;
use crate::request_observability::RequestOutcome;
use crate::request_observability::RequestType;
use crate::specifiers::SpecifierExt;

/// No-op: the fbcode implementation feeds an ods3 schematized logger, which
/// has no OSS counterpart. Scuba request logging is unaffected and still works
/// via `--scuba-log-file`.
pub(crate) fn observe_request(
    _method: &str,
    _repo: Option<String>,
    _request_type: RequestType,
    _event: RequestEvent,
    _outcome: Option<RequestOutcome>,
    _duration: Option<std::time::Duration>,
) {
}

#[derive(Clone, Default)]
pub(crate) struct RequestState;

impl RequestState {
    pub(crate) async fn new(_app: &MononokeApp) -> anyhow::Result<Self> {
        Ok(Self)
    }

    pub(crate) fn always_log(
        &self,
        _req_ctxt: &RequestContext,
    ) -> Result<bool, scs_errors::ServiceError> {
        Ok(false)
    }

    pub(crate) fn add_scuba_params(
        &self,
        _req_ctxt: &RequestContext,
        _specifier: Option<&dyn SpecifierExt>,
        _configs: &MononokeConfigs,
        _scuba: &mut MononokeScubaSampleBuilder,
    ) -> Result<(), scs_errors::ServiceError> {
        Ok(())
    }

    /// Metadata for a request: the identities in the caller's TLS
    /// certificate, or `OSS_LOCAL` for a caller over plaintext loopback.
    pub(crate) async fn create_metadata(
        &self,
        req_ctxt: &RequestContext,
    ) -> Result<Metadata, scs_errors::ServiceError> {
        let client_info = clientinfo::ClientInfo::default_with_entry_point(
            clientinfo::ClientEntryPoint::ScsServer,
        );
        let identities = req_ctxt
            .identities()
            .cloned()
            .unwrap_or_else(|| [MononokeIdentity::oss_local()].into_iter().collect());
        let peer = req_ctxt.peer_addr();
        let mut metadata = Metadata::new(
            None,
            identities,
            false,
            false,
            peer.map(|peer| peer.ip()),
            peer.map(|peer| peer.port()),
        )
        .await;
        metadata.add_client_info(client_info);
        Ok(metadata)
    }
}

/// Callers with TLS certificates are subject to the repos' ACLs, as SLAPI's
/// are; callers over plaintext loopback are not.
pub(crate) fn request_authorization_context(ctx: &CoreContext) -> AuthorizationContext {
    AuthorizationContext::new_oss(ctx)
}

/// A caller with a TLS certificate may act as a service if the service's
/// ACL allows its identities, which each write checks. Callers over
/// plaintext loopback can't be identified, so their requests naming a
/// service are refused.
pub(crate) fn service_authorization_context(
    ctx: &CoreContext,
    service_name: Option<String>,
) -> Result<AuthorizationContext, scs_errors::ServiceError> {
    let authz = request_authorization_context(ctx);
    match service_name {
        None => Ok(authz),
        Some(_) if authz == AuthorizationContext::new_oss_local() => Err(
            scs_errors::invalid_request(
                "service_identity needs a TLS client certificate in OSS builds".to_string(),
            )
            .into(),
        ),
        Some(service_name) => Ok(AuthorizationContext::new_for_service_writes(service_name)),
    }
}

pub(crate) fn set_nocache_header(_req_ctxt: &RequestContext) {}

#[derive(Clone, Default)]
pub(crate) struct ThriftCachePolicy;

impl ThriftCachePolicy {
    pub(crate) fn new(_app: &MononokeApp) -> Self {
        Self
    }

    pub(crate) fn apply(
        &self,
        _ctx: &CoreContext,
        _repo: &mononoke_api::RepoContext<mononoke_api::Repo>,
    ) -> Result<(), scs_errors::ServiceError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use fbinit::FacebookInit;
    use mononoke_macros::mononoke;

    use super::*;

    async fn ctx_for(fb: FacebookInit, req_ctxt: &RequestContext) -> CoreContext {
        let metadata = RequestState
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
            request_authorization_context(&ctx),
            AuthorizationContext::new_oss_local()
        );
        assert_eq!(
            service_authorization_context(&ctx, None)
                .expect("a request without a service identity should be accepted"),
            AuthorizationContext::new_oss_local()
        );
    }

    #[mononoke::fbinit_test]
    async fn loopback_requests_naming_a_service_are_refused(fb: FacebookInit) {
        let ctx = ctx_for(fb, &RequestContext::new()).await;
        assert!(service_authorization_context(&ctx, Some("some_service".to_string())).is_err());
    }

    fn alice() -> RequestContext {
        let identities = [
            MononokeIdentity::from_legacy_type_data("USER", "alice"),
            MononokeIdentity::from_legacy_type_data(
                "X509_SUBJECT_NAME",
                "CN=alice,O=mononoke-local",
            ),
        ];
        RequestContext::for_peer(
            "192.0.2.1:4000".parse().unwrap(),
            Some(identities.into_iter().collect()),
        )
    }

    #[mononoke::fbinit_test]
    async fn tls_requests_carry_their_certificate_identities(fb: FacebookInit) {
        let req_ctxt = alice();
        let ctx = ctx_for(fb, &req_ctxt).await;
        assert_eq!(
            ctx.metadata().identities(),
            req_ctxt.identities().expect("the context has identities")
        );
        assert_eq!(
            ctx.metadata().client_ip(),
            Some(&"192.0.2.1".parse().unwrap())
        );
        assert_eq!(
            request_authorization_context(&ctx),
            AuthorizationContext::new(&ctx)
        );
        assert_eq!(
            service_authorization_context(&ctx, Some("some_service".to_string()))
                .expect("a TLS caller may ask to act as a service"),
            AuthorizationContext::new_for_service_writes("some_service")
        );
    }

    #[mononoke::test]
    async fn requests_carry_the_oss_local_identity() {
        let Ok(metadata) = RequestState.create_metadata(&RequestContext::new()).await else {
            panic!("OSS request metadata should be created");
        };
        assert_eq!(
            metadata.identities().iter().cloned().collect::<Vec<_>>(),
            vec![MononokeIdentity::oss_local()]
        );
    }
}
