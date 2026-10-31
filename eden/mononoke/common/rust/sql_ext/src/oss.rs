/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Error;
use anyhow::Result;
use async_trait::async_trait;
use fbinit::FacebookInit;
use sql_common::mysql::set_oss_transaction_statement_timeout;

use crate::facebook::*;
use crate::oss_mysql::OssMysqlRegistry;
use crate::replication::ReplicaLag;
use crate::replication::ReplicaLagMonitor;
use crate::*;

macro_rules! fb_unimplemented {
    () => {
        unimplemented!("This is implemented only for fbcode_build!")
    };
}

/// Settings of the OSS MySQL pools, from the `--mysql-*` flags.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct PoolConfig {
    /// Maximum number of connections of each pool
    /// (`--mysql-pool-per-key-limit`).
    pub per_key_limit: u64,
    /// Age after which a connection is closed, in milliseconds, plus up to a
    /// tenth more (`--mysql-pool-age-timeout`). 0 means no limit.
    pub conn_age_timeout_ms: u64,
    /// Idle time after which a connection above the minimum is closed, in
    /// milliseconds (`--mysql-pool-idle-timeout`).
    pub conn_idle_timeout_ms: u64,
    /// Time to open a connection, in milliseconds
    /// (`--mysql-conn-open-timeout`). Probes get one second more.
    pub conn_open_timeout_ms: u64,
    /// Time limit of a statement (`--mysql-query-time-limit`). Zero means no
    /// limit.
    pub query_time_limit: Duration,
}

impl PoolConfig {
    pub fn new(
        _size: usize,
        _threads_num: i32,
        per_key_limit: u64,
        conn_age_timeout_ms: u64,
        conn_idle_timeout_ms: u64,
        conn_open_timeout_ms: u64,
        query_time_limit: Duration,
    ) -> Self {
        Self {
            per_key_limit,
            conn_age_timeout_ms,
            conn_idle_timeout_ms,
            conn_open_timeout_ms,
            query_time_limit,
        }
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            per_key_limit: 100,
            conn_age_timeout_ms: 60_000,
            conn_idle_timeout_ms: 4_000,
            conn_open_timeout_ms: 3_000,
            query_time_limit: Duration::from_secs(10),
        }
    }
}

/// How OSS builds follow a change of MySQL primary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OssFailoverSettings {
    /// Time between two probe rounds while the primary is settled
    /// (`--oss-mysql-probe-interval-ms`). Zero means that settled rounds
    /// run only when a pool error, or a caller waiting for a primary, kicks
    /// the prober; while there is no settled primary, rounds still run
    /// every 250 ms. In none mode, the time between two reads of the hosts
    /// file and the secrets, with zero meaning only after rejected
    /// credentials.
    pub probe_interval: Duration,
    /// How the primary is recognised and fenced
    /// (`--oss-mysql-primary-check`).
    pub primary_check: PrimaryCheck,
    /// Where secrets given by a bare name are kept
    /// (`--oss-mysql-secrets-dir`).
    pub secrets_dir: Option<PathBuf>,
    /// Longest time a checkout may take, including the wait for a primary
    /// (`--oss-mysql-checkout-timeout-ms`). Zero means no limit.
    pub checkout_timeout: Duration,
}

impl Default for OssFailoverSettings {
    fn default() -> Self {
        Self {
            probe_interval: Duration::from_secs(1),
            primary_check: PrimaryCheck::default(),
            secrets_dir: None,
            checkout_timeout: Duration::from_secs(10),
        }
    }
}

/// The MySQL pools of a process, shared by every store built with the same
/// `MysqlOptions`. The stores' connections keep their cluster, and with it
/// its prober, alive: a cluster stops once no store uses it, whether or not
/// this is dropped.
#[derive(Clone, Default)]
pub struct SharedConnectionPool(pub(crate) Arc<OssMysqlRegistry>);

impl SharedConnectionPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_settings(settings: OssFailoverSettings) -> Self {
        Self(Arc::new(OssMysqlRegistry::new(settings)))
    }
}

/// Sets the time limit of each statement of an OSS MySQL transaction, for the
/// whole process (`--mysql-query-time-limit`). Zero means no limit.
/// Statements outside transactions get the limit of their pool's
/// `PoolConfig`.
pub fn set_oss_transaction_time_limit(limit: Duration) {
    set_oss_transaction_statement_timeout((!limit.is_zero()).then_some(limit));
}

pub fn create_mysql_connections_unsharded(
    _fb: FacebookInit,
    _mysql_options: MysqlOptions,
    _label: String,
    _tier: String,
    _readonly: bool,
) -> Result<SqlConnections, Error> {
    fb_unimplemented!()
}

#[cfg(fbcode_build)]
pub fn create_oss_mysql_connections_unsharded(
    _fb: FacebookInit,
    _host: String,
    _port: i16,
    _user: String,
    _secret_name: String,
    _database: String,
    _label: String,
) -> Result<SqlConnections, Error> {
    fb_unimplemented!()
}

pub fn create_mysql_connections_sharded<S>(
    _fb: FacebookInit,
    _global_connection_pool: MysqlOptions,
    _label: String,
    _shardmap: String,
    _shards: S,
    _readonly: bool,
) -> Result<SqlShardedConnections, Error>
where
    S: IntoIterator<Item = usize> + Clone,
{
    fb_unimplemented!()
}

pub struct MyAdmin;
pub struct MyAdminLagMonitor;

impl MyAdmin {
    pub fn new(_: FacebookInit) -> Result<Self> {
        fb_unimplemented!()
    }

    pub fn single_shard_lag_monitor(&self, _: String) -> MyAdminLagMonitor {
        fb_unimplemented!()
    }

    pub fn shardmap_lag_monitor(&self, _: String) -> MyAdminLagMonitor {
        fb_unimplemented!()
    }
}

#[async_trait]
impl ReplicaLagMonitor for MyAdminLagMonitor {
    async fn get_replica_lag(&self) -> Result<Vec<ReplicaLag>> {
        fb_unimplemented!()
    }
}
