/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use std::num::NonZeroUsize;
use std::sync::LazyLock;

use anyhow::Result;
use anyhow::anyhow;
use fbinit::FacebookInit;
use metaconfig_types::OssRemoteDatabaseConfig;
use sql_ext::facebook::MysqlOptions;
use sql_ext::facebook::OssMysqlTarget;
use sql_ext::facebook::create_oss_mysql_connections;

use crate::SqlConstruct;
use crate::SqlShardedConstruct;

macro_rules! fb_unimplemented {
    () => {
        unimplemented!("This is implemented only for fbcode_build!")
    };
}

/// The MySQL pools of `with_oss_mysql`, with the default settings, kept for
/// the whole process so that stores of the same server share them. A
/// cluster's prober runs while some store uses its pools.
static DEFAULT_MYSQL_OPTIONS: LazyLock<MysqlOptions> = LazyLock::new(MysqlOptions::default);

/// Construct a SQL data manager backed by Facebook infrastructure
#[async_trait::async_trait]
pub trait FbSqlConstruct: SqlConstruct + Sized + Send + Sync + 'static {
    fn with_mysql<'a>(_: FacebookInit, _: String, _: &'a MysqlOptions, _: bool) -> Result<Self> {
        fb_unimplemented!()
    }

    /// Construct an instance connected to the MySQL server at `host:port`,
    /// through process-wide pools with the default settings.
    async fn with_oss_mysql(
        host: String,
        port: i16,
        database: String,
        user_secret: String,
        password_secret: String,
        readonly: bool,
    ) -> Result<Self> {
        let port = oss_mysql_port(port)?;
        let target = OssMysqlTarget {
            endpoints: vec![(host, port)],
            hosts_file: None,
            default_port: port,
            database,
            user_secret,
            password_secret,
        };
        let connections =
            create_oss_mysql_connections(&target, &DEFAULT_MYSQL_OPTIONS, Self::LABEL, readonly)
                .await?;
        Ok(Self::from_sql_connections(connections))
    }

    /// Construct an instance connected to the primary of the MySQL servers
    /// of `config`, through the pools of `mysql_options`.
    async fn with_oss_mysql_config(
        config: &OssRemoteDatabaseConfig,
        mysql_options: &MysqlOptions,
        readonly: bool,
    ) -> Result<Self> {
        let target = oss_mysql_target(config)?;
        let connections =
            create_oss_mysql_connections(&target, mysql_options, Self::LABEL, readonly).await?;
        Ok(Self::from_sql_connections(connections))
    }
}

impl<T: SqlConstruct> FbSqlConstruct for T {}

/// Construct a sharded SQL data manager backed by Facebook infrastructure
pub trait FbSqlShardedConstruct: SqlShardedConstruct + Sized + Send + Sync + 'static {
    fn with_sharded_mysql(
        _: FacebookInit,
        _: String,
        _: NonZeroUsize,
        _: &MysqlOptions,
        _: bool,
    ) -> Result<Self> {
        fb_unimplemented!()
    }
}

impl<T: SqlShardedConstruct> FbSqlShardedConstruct for T {}

/// The MySQL servers and database of `config`, as stores built with
/// `with_oss_mysql_config` connect to them.
pub fn oss_mysql_target(config: &OssRemoteDatabaseConfig) -> Result<OssMysqlTarget> {
    Ok(OssMysqlTarget {
        endpoints: config
            .endpoints
            .iter()
            .map(|endpoint| (endpoint.host.clone(), endpoint.port))
            .collect(),
        hosts_file: config.hosts_file.clone(),
        default_port: oss_mysql_port(config.port)?,
        database: config.database.clone(),
        user_secret: config.user_secret.clone(),
        password_secret: config.password_secret.clone(),
    })
}

/// The legacy `port` of a database entry, which is also the port of hosts
/// file entries that have none.
fn oss_mysql_port(port: i16) -> Result<u16> {
    u16::try_from(port)
        .ok()
        .filter(|port| *port != 0)
        .ok_or_else(|| anyhow!("invalid MySQL port {port}"))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use metaconfig_types::OssDbEndpoint;
    use metaconfig_types::OssRemoteMetadataDatabaseConfig;
    use metaconfig_types::RemoteDatabaseConfig;
    use metaconfig_types::RemoteMetadataDatabaseConfig;
    use mononoke_macros::mononoke;
    use sql::OssPool;
    use sql_ext::SqlConnections;

    use super::*;
    use crate::SqlConstructFromMetadataDatabaseConfig;

    struct TestStore(SqlConnections);

    impl SqlConstruct for TestStore {
        const LABEL: &'static str = "test_store";

        const CREATION_QUERY: &'static str = "";

        fn from_sql_connections(connections: SqlConnections) -> Self {
            Self(connections)
        }
    }

    impl SqlConstructFromMetadataDatabaseConfig for TestStore {
        fn remote_database_config(
            remote: &RemoteMetadataDatabaseConfig,
        ) -> Option<&RemoteDatabaseConfig> {
            Some(&remote.bookmarks)
        }

        fn oss_remote_database_config(
            remote: &OssRemoteMetadataDatabaseConfig,
        ) -> Option<&OssRemoteDatabaseConfig> {
            Some(&remote.bookmarks)
        }
    }

    /// A database on a port where nothing listens.
    fn config() -> OssRemoteDatabaseConfig {
        OssRemoteDatabaseConfig {
            host: "127.0.0.1".to_string(),
            port: 1,
            database: "mononoke".to_string(),
            user_secret: "literal:mononoke".to_string(),
            password_secret: "literal:mononoke".to_string(),
            endpoints: vec![OssDbEndpoint {
                host: "127.0.0.1".to_string(),
                port: 1,
            }],
            hosts_file: None,
        }
    }

    fn write_pool(store: &TestStore) -> OssPool {
        match &store.0.write_connection.inner {
            sql::Connection::OssMysql(connection) => connection.pool.clone(),
            other => panic!("not an OSS MySQL connection: {other:?}"),
        }
    }

    /// Whether two handles drive the same pool: marking one unavailable
    /// moves the generation of the other.
    fn same_pool(a: &OssPool, b: &OssPool) -> bool {
        let before = b.generation();
        a.mark_unavailable("checking which pool this is");
        b.generation() != before
    }

    #[mononoke::test]
    async fn test_with_oss_mysql_config() -> Result<()> {
        // No server answers: the store is built anyway, and its queries
        // wait for a primary.
        let options = MysqlOptions::default();
        let store = TestStore::with_oss_mysql_config(&config(), &options, false).await?;
        assert_eq!(store.0.write_connection.shard_name, "mononoke");

        // The metadata config path uses the pools of the options it is given.
        let remote = OssRemoteMetadataDatabaseConfig {
            production: config(),
            commit_graph: config(),
            bookmarks: config(),
            bonsai_hg_mapping: config(),
            filenodes: config(),
            mutation: config(),
            sparse_profiles: config(),
            bonsai_blob_mapping: None,
            deletion_log: None,
            commit_derived_data_mapping: None,
        };
        let same_options =
            TestStore::with_oss_remote_metadata_database_config(&remote, &options, false).await?;
        assert!(same_pool(&write_pool(&store), &write_pool(&same_options)));
        let other_options = TestStore::with_oss_remote_metadata_database_config(
            &remote,
            &MysqlOptions::default(),
            false,
        )
        .await?;
        assert!(!same_pool(&write_pool(&store), &write_pool(&other_options)));

        let invalid = OssRemoteDatabaseConfig {
            port: 0,
            ..config()
        };
        assert!(
            TestStore::with_oss_mysql_config(&invalid, &options, false)
                .await
                .is_err()
        );
        Ok(())
    }

    #[mononoke::test]
    async fn test_oss_mysql_target() -> Result<()> {
        // The legacy port is the default port of hosts file entries.
        let config = OssRemoteDatabaseConfig {
            port: 3307,
            endpoints: vec![
                OssDbEndpoint {
                    host: "db1.internal".to_string(),
                    port: 3306,
                },
                OssDbEndpoint {
                    host: "db2.internal".to_string(),
                    port: 3307,
                },
            ],
            hosts_file: Some(PathBuf::from("/etc/mononoke/mysql-hosts")),
            ..config()
        };
        assert_eq!(
            oss_mysql_target(&config)?,
            OssMysqlTarget {
                endpoints: vec![
                    ("db1.internal".to_string(), 3306),
                    ("db2.internal".to_string(), 3307),
                ],
                hosts_file: Some(PathBuf::from("/etc/mononoke/mysql-hosts")),
                default_port: 3307,
                database: "mononoke".to_string(),
                user_secret: "literal:mononoke".to_string(),
                password_secret: "literal:mononoke".to_string(),
            }
        );
        let invalid = OssRemoteDatabaseConfig {
            port: 0,
            ..config.clone()
        };
        assert_eq!(
            oss_mysql_target(&invalid).err().map(|err| err.to_string()),
            Some("invalid MySQL port 0".to_string())
        );

        Ok(())
    }

    #[mononoke::test]
    async fn test_with_oss_mysql() -> Result<()> {
        let open = || {
            TestStore::with_oss_mysql(
                "127.0.0.1".to_string(),
                1,
                "mononoke".to_string(),
                "literal:mononoke".to_string(),
                "literal:mononoke".to_string(),
                false,
            )
        };
        // Stores share process-wide pools.
        let first = open().await?;
        let second = open().await?;
        assert!(same_pool(&write_pool(&first), &write_pool(&second)));

        let err = TestStore::with_oss_mysql(
            "127.0.0.1".to_string(),
            -1,
            "mononoke".to_string(),
            "literal:mononoke".to_string(),
            "literal:mononoke".to_string(),
            false,
        )
        .await
        .err()
        .expect("a negative port is refused");
        assert_eq!(err.to_string(), "invalid MySQL port -1");
        Ok(())
    }
}
