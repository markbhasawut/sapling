/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use oss_thrift_transport::ClientTls;
use oss_thrift_transport::client::ClientConfig;
use oss_thrift_transport::client::connect;
use oss_thrift_transport::client::connect_with;
use source_control_clients::make_SourceControlService;

use crate::ScsClient;

pub(crate) fn from_host_port(host_port: &str) -> anyhow::Result<ScsClient> {
    Ok(ScsClient::new(
        connect::<make_SourceControlService>(host_port)?,
        None,
    ))
}

pub(crate) fn from_host_port_with_tls(
    host_port: &str,
    ca: &str,
    certificate: &str,
    private_key: &str,
) -> anyhow::Result<ScsClient> {
    let config = ClientConfig {
        tls: Some(ClientTls::from_files(ca, certificate, private_key)?),
        ..ClientConfig::default()
    };
    Ok(ScsClient::new(
        connect_with::<make_SourceControlService>(host_port, config)?,
        None,
    ))
}
