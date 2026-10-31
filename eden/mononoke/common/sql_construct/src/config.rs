/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
#[cfg(any(fbcode_build, test))]
use anyhow::bail;
use fbinit::FacebookInit;
use metaconfig_types::DatabaseConfig;
use metaconfig_types::LocalDatabaseConfig;
use metaconfig_types::MetadataDatabaseConfig;
#[cfg(any(fbcode_build, test))]
use metaconfig_types::OssDbEndpoint;
use metaconfig_types::OssRemoteDatabaseConfig;
use metaconfig_types::OssRemoteMetadataDatabaseConfig;
use metaconfig_types::RemoteDatabaseConfig;
use metaconfig_types::RemoteMetadataDatabaseConfig;
use metaconfig_types::ShardableRemoteDatabaseConfig;
use metaconfig_types::ShardedDatabaseConfig;
use sql_ext::facebook::MysqlOptions;

use crate::construct::SqlConstruct;
use crate::facebook::FbSqlConstruct;
use crate::facebook::FbSqlShardedConstruct;

/// Fbcode builds connect only to the legacy `host` and `port` of an OSS
/// database. Refuse a config that names other servers, which these builds
/// would otherwise ignore.
#[cfg(any(fbcode_build, test))]
fn check_legacy_endpoint_only(config: &OssRemoteDatabaseConfig) -> Result<()> {
    if config.hosts_file.is_some() {
        bail!("hosts_file is supported only in OSS builds");
    }
    let legacy = u16::try_from(config.port).ok().map(|port| OssDbEndpoint {
        host: config.host.clone(),
        port,
    });
    if config.endpoints.as_slice() != legacy.as_slice() {
        bail!(
            "hosts is supported only in OSS builds; this build connects to host:port \
             ({}:{}), so hosts may list only that server",
            config.host,
            config.port
        );
    }
    Ok(())
}

/// Trait that allows construction from database config.
pub trait SqlConstructFromDatabaseConfig: FbSqlConstruct + SqlConstruct {
    fn with_database_config(
        fb: FacebookInit,
        database_config: &DatabaseConfig,
        mysql_options: &MysqlOptions,
        readonly: bool,
    ) -> Result<Self> {
        match database_config {
            DatabaseConfig::Local(LocalDatabaseConfig { path }) => {
                Self::with_sqlite_path(path.join("sqlite_dbs"), readonly)
            }
            DatabaseConfig::Remote(config) => {
                Self::with_mysql(fb, config.db_address.clone(), mysql_options, readonly)
            }
        }
        .with_context(|| {
            format!("While connecting to {database_config:?} (with options {mysql_options:?})")
        })
    }
}

#[async_trait::async_trait]
impl<T: SqlConstruct + FbSqlConstruct> SqlConstructFromDatabaseConfig for T {}

/// Trait that allows construction from sharded database config.
pub trait SqlConstructFromShardedDatabaseConfig: FbSqlShardedConstruct {
    fn with_sharded_database_config(
        fb: FacebookInit,
        database_config: &ShardedDatabaseConfig,
        mysql_options: &MysqlOptions,
        readonly: bool,
    ) -> Result<Self> {
        match database_config {
            ShardedDatabaseConfig::Local(LocalDatabaseConfig { path }) => {
                Self::with_sqlite_path(path.join("sqlite_dbs"), readonly)
            }
            ShardedDatabaseConfig::Sharded(config) => Self::with_sharded_mysql(
                fb,
                config.shard_map.clone(),
                config.shard_num,
                mysql_options,
                readonly,
            ),
            ShardedDatabaseConfig::Unsharded(config) => {
                Self::with_mysql(fb, config.db_address.clone(), mysql_options, readonly)
            }
        }
        .with_context(|| {
            format!("While connecting to {database_config:?} (with options {mysql_options:?})")
        })
    }
}

#[async_trait::async_trait]
impl<T: FbSqlShardedConstruct> SqlConstructFromShardedDatabaseConfig for T {}

/// Trait that allows construction from the metadata database config.
#[async_trait::async_trait]
pub trait SqlConstructFromMetadataDatabaseConfig: FbSqlConstruct + SqlConstruct {
    async fn with_metadata_database_config(
        fb: FacebookInit,
        metadata_database_config: &MetadataDatabaseConfig,
        mysql_options: &MysqlOptions,
        readonly: bool,
    ) -> Result<Self> {
        match metadata_database_config {
            MetadataDatabaseConfig::Local(LocalDatabaseConfig { path }) => {
                Self::with_sqlite_path(path.join("sqlite_dbs"), readonly)
            }
            MetadataDatabaseConfig::Remote(remote) => {
                Self::with_remote_metadata_database_config(fb, remote, mysql_options, readonly)
            }
            MetadataDatabaseConfig::OssRemote(ossremote) => {
                Self::with_oss_remote_metadata_database_config(ossremote, mysql_options, readonly)
                    .await
            }
        }
    }

    fn with_remote_metadata_database_config(
        fb: FacebookInit,
        remote: &RemoteMetadataDatabaseConfig,
        mysql_options: &MysqlOptions,
        readonly: bool,
    ) -> Result<Self> {
        let config = Self::remote_database_config(remote)
            .ok_or_else(|| anyhow!("no configuration available"))?;
        Self::with_mysql(fb, config.db_address.clone(), mysql_options, readonly)
    }

    async fn with_oss_remote_metadata_database_config(
        remote: &OssRemoteMetadataDatabaseConfig,
        mysql_options: &MysqlOptions,
        readonly: bool,
    ) -> Result<Self> {
        let config = Self::oss_remote_database_config(remote)
            .ok_or_else(|| anyhow!("no configuration available"))?;

        #[cfg(not(fbcode_build))]
        {
            Self::with_oss_mysql_config(config, mysql_options, readonly).await
        }
        #[cfg(fbcode_build)]
        {
            let _ = mysql_options;
            check_legacy_endpoint_only(config)?;
            Self::with_oss_mysql(
                config.host.clone(),
                config.port,
                config.database.clone(),
                config.user_secret.clone(),
                config.password_secret.clone(),
                readonly,
            )
            .await
        }
    }

    /// Get the remote database config for this type.
    fn remote_database_config(
        remote: &RemoteMetadataDatabaseConfig,
    ) -> Option<&RemoteDatabaseConfig>;

    fn oss_remote_database_config(
        remote: &OssRemoteMetadataDatabaseConfig,
    ) -> Option<&OssRemoteDatabaseConfig>;
}

/// Trait that allows construction of shardable databases from the metadata database config.
#[async_trait::async_trait]
pub trait SqlShardableConstructFromMetadataDatabaseConfig:
    FbSqlConstruct + FbSqlShardedConstruct + SqlConstruct
{
    async fn with_metadata_database_config(
        fb: FacebookInit,
        metadata_database_config: &MetadataDatabaseConfig,
        mysql_options: &MysqlOptions,
        readonly: bool,
    ) -> Result<Self> {
        match metadata_database_config {
            MetadataDatabaseConfig::Local(LocalDatabaseConfig { path }) => {
                Self::with_sqlite_path(path.join("sqlite_dbs"), readonly)
            }
            MetadataDatabaseConfig::Remote(remote) => {
                Self::with_remote_metadata_database_config(fb, remote, mysql_options, readonly)
            }
            MetadataDatabaseConfig::OssRemote(remote) => {
                Self::with_oss_remote_metadata_database_config(remote, mysql_options, readonly)
                    .await
            }
        }
    }

    fn with_remote_metadata_database_config(
        fb: FacebookInit,
        remote: &RemoteMetadataDatabaseConfig,
        mysql_options: &MysqlOptions,
        readonly: bool,
    ) -> Result<Self> {
        let config = Self::remote_database_config(remote)
            .ok_or_else(|| anyhow!("no configuration available"))?;
        match config {
            ShardableRemoteDatabaseConfig::Unsharded(config) => {
                Self::with_mysql(fb, config.db_address.clone(), mysql_options, readonly)
            }
            ShardableRemoteDatabaseConfig::Sharded(config) => Self::with_sharded_mysql(
                fb,
                config.shard_map.clone(),
                config.shard_num,
                mysql_options,
                readonly,
            ),
        }
    }

    async fn with_oss_remote_metadata_database_config(
        remote: &OssRemoteMetadataDatabaseConfig,
        mysql_options: &MysqlOptions,
        readonly: bool,
    ) -> Result<Self> {
        let config = Self::oss_remote_database_config(remote)
            .ok_or_else(|| anyhow!("no configuration available"))?;

        #[cfg(not(fbcode_build))]
        {
            Self::with_oss_mysql_config(config, mysql_options, readonly).await
        }
        #[cfg(fbcode_build)]
        {
            let _ = mysql_options;
            check_legacy_endpoint_only(config)?;
            Self::with_oss_mysql(
                config.host.clone(),
                config.port,
                config.database.clone(),
                config.user_secret.clone(),
                config.password_secret.clone(),
                readonly,
            )
            .await
        }
    }

    /// Get the remote database config for this type.
    fn remote_database_config(
        remote: &RemoteMetadataDatabaseConfig,
    ) -> Option<&ShardableRemoteDatabaseConfig>;

    fn oss_remote_database_config(
        remote: &OssRemoteMetadataDatabaseConfig,
    ) -> Option<&OssRemoteDatabaseConfig>;
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use mononoke_macros::mononoke;

    use super::*;

    fn endpoint(host: &str, port: u16) -> OssDbEndpoint {
        OssDbEndpoint {
            host: host.to_string(),
            port,
        }
    }

    fn config(host: &str, port: i16, endpoints: Vec<OssDbEndpoint>) -> OssRemoteDatabaseConfig {
        OssRemoteDatabaseConfig {
            host: host.to_string(),
            port,
            database: "mononoke".to_string(),
            user_secret: "literal:mononoke".to_string(),
            password_secret: "file:/etc/mononoke/mysql.pw".to_string(),
            endpoints,
            hosts_file: None,
        }
    }

    fn error(config: &OssRemoteDatabaseConfig) -> String {
        format!(
            "{:#}",
            check_legacy_endpoint_only(config).expect_err("the config should be refused")
        )
    }

    #[mononoke::test]
    fn test_check_legacy_endpoint_only() {
        // host:port alone, as the parser fills endpoints without hosts, or
        // with hosts listing just that server.
        check_legacy_endpoint_only(&config("db1", 3306, vec![endpoint("db1", 3306)])).unwrap();

        // Other servers would be ignored.
        let hosts_only = "hosts is supported only in OSS builds; this build connects to host:port";
        for (host, port, endpoints) in [
            (
                "db1",
                3306,
                vec![endpoint("db1", 3306), endpoint("db2", 3306)],
            ),
            ("db1", 3306, vec![endpoint("db2", 3306)]),
            ("db1", 3306, vec![endpoint("db1", 3307)]),
            ("", 3306, vec![endpoint("db1", 3306)]),
            ("db1", -1, vec![endpoint("db1", 65535)]),
        ] {
            let err = error(&config(host, port, endpoints.clone()));
            assert!(err.starts_with(hosts_only), "{endpoints:?}: {err}");
            assert!(err.contains(&format!("({host}:{port})")), "{err}");
        }

        let with_file = OssRemoteDatabaseConfig {
            hosts_file: Some(PathBuf::from("/etc/mononoke/mysql-hosts")),
            ..config("db1", 3306, vec![endpoint("db1", 3306)])
        };
        assert_eq!(
            error(&with_file),
            "hosts_file is supported only in OSS builds"
        );
    }
}
