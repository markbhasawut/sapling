/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use std::sync::Arc;
use std::sync::OnceLock;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use bookmarks_types::BookmarkKey;
use context::CoreContext;
use land_service_if_clients::LandService;
use land_service_if_clients::make_LandService;
use metaconfig_types::RepoConfigRef;
use oss_thrift_transport::ClientTls;
use oss_thrift_transport::client::ClientConfig;
use oss_thrift_transport::client::connect;
use oss_thrift_transport::client::connect_with;

/// The certificate, key and CA to reach the land service with over mutual
/// TLS, as `--land-service-client-cert`, `--land-service-client-private-key`
/// and `--tls-ca` give them.
static CERTIFICATE_PATHS: OnceLock<(String, String, String)> = OnceLock::new();

/// Connect to the land service over mutual TLS with these files. Without
/// them, the connection is plaintext, to loopback addresses only.
pub fn override_certificate_paths(certificate: &str, private_key: &str, ca: &str) {
    let _ = CERTIFICATE_PATHS.set((certificate.to_owned(), private_key.to_owned(), ca.to_owned()));
}

pub(crate) struct LandServiceClient {
    client: Arc<dyn LandService + Send + Sync>,
}

impl LandServiceClient {
    pub(crate) async fn from_tier(
        _ctx: &CoreContext,
        tier: String,
        _repo: &impl RepoConfigRef,
    ) -> Result<Self> {
        bail!(
            "land service tier address `{tier}` is unavailable in OSS builds; configure a loopback host:port address"
        )
    }

    pub(crate) fn from_host_port(_ctx: &CoreContext, host_port: String) -> Result<Self> {
        let client = match CERTIFICATE_PATHS.get() {
            Some((certificate, private_key, ca)) => {
                let config = ClientConfig {
                    tls: Some(ClientTls::from_files(ca, certificate, private_key)?),
                    ..ClientConfig::default()
                };
                connect_with::<make_LandService>(&host_port, config)
            }
            None => connect::<make_LandService>(&host_port),
        }
        .with_context(|| format!("failed to create land service client for {host_port}"))?;
        Ok(Self { client })
    }

    pub(crate) fn get_service_client(
        &self,
        _ctx: &CoreContext,
        _repo: &str,
        _bookmark: &BookmarkKey,
    ) -> Result<Arc<dyn LandService + Send + Sync>> {
        Ok(self.client.clone())
    }
}
