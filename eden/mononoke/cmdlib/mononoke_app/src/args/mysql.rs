/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

#[cfg(not(fbcode_build))]
use std::path::PathBuf;

use clap::Args;
#[cfg(not(fbcode_build))]
use sql_ext::facebook::PrimaryCheck;

/// Command line arguments for controlling MySql
// Defaults are derived from `sql_ext::facebook::mysql`
// https://fburl.com/diffusion/n5isd68j, last synced on 17/12/2020
#[derive(Args, Debug)]
pub struct MysqlArgs {
    /// Connect to MySql master only.
    #[clap(long)]
    pub mysql_master_only: bool,

    /// Size of the MySql connection pool
    #[clap(long, default_value = "10000")]
    pub mysql_pool_limit: usize,

    /// MySql connection pool per key limit
    #[clap(long, default_value = "100")]
    pub mysql_pool_per_key_limit: u64,

    /// Number of threads in MySql connection pool (number of real pools)
    #[clap(long, default_value = "10")]
    pub mysql_pool_threads_num: i32,

    /// Mysql connection pool age timeout in millisecs
    #[clap(long, default_value = "60000")]
    pub mysql_pool_age_timeout: u64,

    /// Mysql connection pool idle timeout in millisecs
    #[clap(long, default_value = "4000")]
    pub mysql_pool_idle_timeout: u64,

    /// Size of the MySql connection pool for SqlBlob
    #[clap(long, default_value = "10000", alias = "mysql-sqblob-pool-limit")]
    pub mysql_sqlblob_pool_limit: usize,

    /// MySql connection pool per key limit for SqlBlob
    #[clap(long, default_value = "100", alias = "mysql-sqblob-pool-per-key-limit")]
    pub mysql_sqlblob_pool_per_key_limit: u64,

    /// Number of threads in MySql connection pool (number of real pools) for
    /// SqlBlob
    #[clap(long, default_value = "10", alias = "mysql-sqblob-pool-threads-num")]
    pub mysql_sqlblob_pool_threads_num: i32,

    /// MySql connection pool age timeout in millisecs for SqlBlob
    #[clap(long, default_value = "60000", alias = "mysql-sqblob-pool-age-timeout")]
    pub mysql_sqlblob_pool_age_timeout: u64,

    /// MySql connection pool idle timeout in millisecs for SqlBlob
    #[clap(long, default_value = "4000", alias = "mysql-sqblob-pool-idle-timeout")]
    pub mysql_sqlblob_pool_idle_timeout: u64,

    /// MySql connection open timeout in millisecs
    #[clap(long, default_value = "3000")]
    pub mysql_conn_open_timeout: u64,

    /// Mysql query time limit in millisecs
    #[clap(long, default_value = "10000", alias = "mysql-max-query-time")]
    pub mysql_query_time_limit: u64,

    /// Time between two probes of the MySQL servers of a database while its
    /// primary is settled, in millisecs (with --oss-mysql-primary-check
    /// none, between two reads of the hosts file and the secrets). 0 probes
    /// a settled primary only after errors, or when a caller waits for a
    /// primary, and cannot be combined with --oss-mysql-checkout-timeout-ms
    /// 0 unless the primary check is none. Without a settled primary, probes
    /// run every 250 ms whatever the value
    #[cfg(not(fbcode_build))]
    #[clap(long, default_value = "1000")]
    pub oss_mysql_probe_interval_ms: u64,

    /// How the MySQL primary is found: identity (the only writable server
    /// listed, checked by server_id, hostname and port on each new
    /// connection), writable (the single server listed, while writable; for
    /// multi-primary clusters behind a balancer) or none (the single server
    /// listed, unchecked; for SQL-aware proxies)
    #[cfg(not(fbcode_build))]
    #[clap(long, default_value = "identity")]
    pub oss_mysql_primary_check: PrimaryCheck,

    /// Directory of the MySQL user and password secrets that the repo config
    /// gives by a bare name
    #[cfg(not(fbcode_build))]
    #[clap(long)]
    pub oss_mysql_secrets_dir: Option<PathBuf>,

    /// Longest wait for a MySQL connection, including the wait for a new
    /// primary, in millisecs. 0 means no limit: callers then wait for as
    /// long as there is no usable primary. It needs a nonzero
    /// --oss-mysql-probe-interval-ms, unless the primary check is none
    #[cfg(not(fbcode_build))]
    #[clap(long, default_value = "10000")]
    pub oss_mysql_checkout_timeout_ms: u64,
}
