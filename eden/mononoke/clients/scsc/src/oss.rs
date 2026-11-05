/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use anyhow::Error;
use anyhow::Result;
use fbinit::FacebookInit;
use scs_client_raw::ScsClient;

const DEFAULT_SCS_HOST: &str = "localhost:8367";

/// No `--just-knobs-config-path`: honouring it needs the cached_config
/// JustKnobs worker, which OSS `justknobs_ext` does not provide. Absent rather
/// than ignored, so passing it errors instead of silently dropping overrides.
#[derive(clap::Args)]
pub(crate) struct PlatformArgs {}

impl PlatformArgs {
    pub(crate) async fn initialize(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

pub(crate) fn prepare(_fb: FacebookInit) -> anyhow::Result<()> {
    Ok(())
}

pub(crate) fn disable_cpp_log(_fb: FacebookInit) {}

pub(crate) fn short_version() -> String {
    String::from("dev")
}

pub(crate) fn long_version() -> String {
    String::from("dev")
}

#[derive(clap::Args)]
pub(crate) struct ConnectionArgs {
    #[clap(long, short = 'H', global = true, env = "SCS_HOST")]
    /// Connect to SCS at a given host and port, format HOST:PORT (default: localhost:8367)
    host: Option<String>,
    /// Connect over mutual TLS, trusting the server certificates this CA
    /// signs. Needed for any host other than loopback.
    #[clap(
        long,
        global = true,
        env = "SCS_TLS_CA",
        requires_all = ["tls_certificate", "tls_private_key"]
    )]
    tls_ca: Option<String>,
    /// Client certificate to present over TLS
    #[clap(long, global = true, env = "SCS_TLS_CERTIFICATE", requires = "tls_ca")]
    tls_certificate: Option<String>,
    /// Private key of the client certificate
    #[clap(long, global = true, env = "SCS_TLS_PRIVATE_KEY", requires = "tls_ca")]
    tls_private_key: Option<String>,
}

impl ConnectionArgs {
    pub async fn get_connection(
        &self,
        _fb: FacebookInit,
        _repo: Option<&str>,
    ) -> Result<ScsClient, Error> {
        let host = self.host.as_deref().unwrap_or(DEFAULT_SCS_HOST);
        match (&self.tls_ca, &self.tls_certificate, &self.tls_private_key) {
            (Some(ca), Some(certificate), Some(private_key)) => {
                ScsClient::from_host_port_with_tls(host, ca, certificate, private_key).await
            }
            _ => ScsClient::from_host_port(host).await,
        }
    }
}
