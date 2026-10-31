/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use std::collections::HashSet;
use std::net::Ipv6Addr;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use metaconfig_types::BlobConfig;
use metaconfig_types::BlobstoreId;
use metaconfig_types::BubbleDeletionMode;
use metaconfig_types::DatabaseConfig;
use metaconfig_types::EphemeralBlobstoreConfig;
use metaconfig_types::FilestoreParams;
use metaconfig_types::LocalDatabaseConfig;
use metaconfig_types::MetadataDatabaseConfig;
use metaconfig_types::MultiplexId;
use metaconfig_types::MultiplexedStoreType;
use metaconfig_types::OssDbEndpoint;
use metaconfig_types::OssRemoteDatabaseConfig;
use metaconfig_types::OssRemoteMetadataDatabaseConfig;
use metaconfig_types::PackConfig;
use metaconfig_types::PackFormat;
use metaconfig_types::RemoteDatabaseConfig;
use metaconfig_types::RemoteMetadataDatabaseConfig;
use metaconfig_types::ShardableRemoteDatabaseConfig;
use metaconfig_types::ShardedDatabaseConfig;
use metaconfig_types::ShardedRemoteDatabaseConfig;
use metaconfig_types::StorageConfig;
use nonzero_ext::nonzero;
use repos::RawBlobstoreConfig;
use repos::RawBlobstoreMultiplexedWal;
use repos::RawBlobstorePackConfig;
use repos::RawBlobstorePackFormat;
use repos::RawBubbleDeletionMode;
use repos::RawDbConfig;
use repos::RawDbLocal;
use repos::RawDbRemote;
use repos::RawDbShardableRemote;
use repos::RawDbShardedRemote;
use repos::RawEphemeralBlobstoreConfig;
use repos::RawFilestoreParams;
use repos::RawMetadataConfig;
use repos::RawMultiplexedStoreNormal;
use repos::RawMultiplexedStoreType;
use repos::RawMultiplexedStoreWriteOnly;
use repos::RawOssDbRemote;
use repos::RawShardedDbConfig;
use repos::RawStorageConfig;

use crate::convert::Convert;

impl Convert for RawStorageConfig {
    type Output = StorageConfig;

    fn convert(self) -> Result<Self::Output> {
        Ok(StorageConfig {
            metadata: self.metadata.convert()?,
            blobstore: self.blobstore.convert()?,
            ephemeral_blobstore: self
                .ephemeral_blobstore
                .map(RawEphemeralBlobstoreConfig::convert)
                .transpose()?,
            mutable_blobstore: self.mutable_blobstore.convert()?,
        })
    }
}

impl Convert for RawBubbleDeletionMode {
    type Output = BubbleDeletionMode;

    fn convert(self) -> Result<Self::Output> {
        let deletion_mode = match self {
            RawBubbleDeletionMode::DISABLED => BubbleDeletionMode::Disabled,
            RawBubbleDeletionMode::MARK_ONLY => BubbleDeletionMode::MarkOnly,
            RawBubbleDeletionMode::MARK_AND_DELETE => BubbleDeletionMode::MarkAndDelete,
            v => return Err(anyhow!("Invalid value {v} for enum BubbleDeletionMode")),
        };
        Ok(deletion_mode)
    }
}

impl Convert for RawEphemeralBlobstoreConfig {
    type Output = EphemeralBlobstoreConfig;

    fn convert(self) -> Result<Self::Output> {
        Ok(EphemeralBlobstoreConfig {
            metadata: self.metadata.convert()?,
            blobstore: self.blobstore.convert()?,
            initial_bubble_lifespan: Duration::from_secs(
                self.initial_bubble_lifespan_secs
                    .try_into()
                    .context("Failed to convert initial_bubble_lifespan")?,
            ),
            bubble_expiration_grace: Duration::from_secs(
                self.bubble_expiration_grace_secs
                    .try_into()
                    .context("Failed to convert bubble_expiration_grace")?,
            ),
            bubble_deletion_mode: self.bubble_deletion_mode.convert()?,
        })
    }
}

impl Convert for RawBlobstoreConfig {
    type Output = BlobConfig;

    fn convert(self) -> Result<Self::Output> {
        let config = match self {
            RawBlobstoreConfig::disabled(_) => BlobConfig::Disabled,
            RawBlobstoreConfig::blob_files(raw) => BlobConfig::Files {
                path: PathBuf::from(raw.path),
            },
            RawBlobstoreConfig::blob_sqlite(raw) => BlobConfig::Sqlite {
                path: PathBuf::from(raw.path),
            },
            RawBlobstoreConfig::manifold(raw) => BlobConfig::Manifold {
                bucket: raw.manifold_bucket,
                prefix: raw.manifold_prefix,
            },
            RawBlobstoreConfig::mysql(raw) => BlobConfig::Mysql {
                remote: raw.remote.convert()?,
            },
            RawBlobstoreConfig::multiplexed_wal(RawBlobstoreMultiplexedWal {
                write_quorum,
                components,
                multiplex_id,
                queue_db,
                inner_blobstores_scuba_table,
                multiplex_scuba_table,
                scuba_sample_rate,
            }) => {
                let write_quorum: usize = write_quorum.try_into()?;
                if write_quorum > components.len() {
                    return Err(anyhow!(
                        "Not enough blobstores for {} write quorum (have {})",
                        write_quorum,
                        components.len()
                    ));
                }

                BlobConfig::MultiplexedWal {
                    multiplex_id: MultiplexId::new(multiplex_id),
                    blobstores: components
                        .into_iter()
                        .map(|comp| {
                            Ok((
                                BlobstoreId::new(comp.blobstore_id.try_into()?),
                                comp.store_type
                                    .convert()?
                                    .unwrap_or(MultiplexedStoreType::Normal),
                                comp.blobstore.convert()?,
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?,
                    write_quorum,
                    queue_db: queue_db.convert()?,
                    inner_blobstores_scuba_table,
                    multiplex_scuba_table,
                    scuba_sample_rate: parse_scuba_sample_rate(scuba_sample_rate)?,
                }
            }

            RawBlobstoreConfig::manifold_with_ttl(raw) => {
                let ttl = Duration::from_secs(raw.ttl_secs.try_into()?);
                BlobConfig::ManifoldWithTtl {
                    bucket: raw.manifold_bucket,
                    prefix: raw.manifold_prefix,
                    ttl,
                }
            }
            RawBlobstoreConfig::logging(raw) => BlobConfig::Logging {
                scuba_table: raw.scuba_table,
                scuba_sample_rate: parse_scuba_sample_rate(raw.scuba_sample_rate)?,
                blobconfig: Box::new(raw.blobstore.convert()?),
            },
            RawBlobstoreConfig::pack(raw) => BlobConfig::Pack {
                blobconfig: Box::new(raw.blobstore.convert()?),
                pack_config: raw.pack_config.map(|c| c.convert()).transpose()?,
            },
            RawBlobstoreConfig::aws_s3(raw) => BlobConfig::AwsS3 {
                bucket: raw.bucket,
                region: rusoto_core::Region::from_str(&raw.region)?,
                num_concurrent_operations: raw
                    .num_concurrent_operations
                    .map(|x| x.try_into())
                    .transpose()?,
            },
            RawBlobstoreConfig::UnknownField(f) => {
                return Err(anyhow!("unsupported blobstore configuration ({f})"));
            }
        };
        Ok(config)
    }
}

fn parse_scuba_sample_rate(sample_rate: Option<i64>) -> Result<NonZeroU64> {
    let rate = sample_rate
        .map(|rate| {
            NonZeroU64::new(rate.try_into()?)
                .ok_or_else(|| anyhow!("scuba_sample_rate must be an integer larger than zero"))
        })
        .transpose()?
        .unwrap_or(nonzero!(100_u64));

    Ok(rate)
}

impl Convert for RawBlobstorePackFormat {
    type Output = PackFormat;

    fn convert(self) -> Result<Self::Output> {
        let pack_format = match self {
            RawBlobstorePackFormat::Raw(_) => PackFormat::Raw,
            RawBlobstorePackFormat::ZstdIndividual(zstd) => {
                PackFormat::ZstdIndividual(zstd.compression_level)
            }
            RawBlobstorePackFormat::UnknownField(f) => bail!("Unsupported PackFormat {f}"),
        };
        Ok(pack_format)
    }
}

impl Convert for RawBlobstorePackConfig {
    type Output = PackConfig;

    fn convert(self) -> Result<Self::Output> {
        let put_format = self.put_format.convert()?;
        Ok(PackConfig { put_format })
    }
}

impl Convert for RawDbLocal {
    type Output = LocalDatabaseConfig;

    fn convert(self) -> Result<Self::Output> {
        Ok(LocalDatabaseConfig {
            path: PathBuf::from(self.local_db_path),
        })
    }
}

impl Convert for RawDbRemote {
    type Output = RemoteDatabaseConfig;

    fn convert(self) -> Result<Self::Output> {
        Ok(RemoteDatabaseConfig {
            db_address: self.db_address,
        })
    }
}

impl Convert for RawOssDbRemote {
    type Output = OssRemoteDatabaseConfig;

    fn convert(self) -> Result<Self::Output> {
        // The legacy port is also the default port of `hosts` and hosts file
        // entries, so it must be valid even when `hosts` is set.
        let default_port = match u16::try_from(self.port) {
            Ok(port) if port > 0 => port,
            _ => bail!(
                "OSS database port {} is out of range 1..=32767 (it is also the default port \
                 of hosts and hosts_file entries)",
                self.port
            ),
        };
        let endpoints = match &self.hosts {
            Some(hosts) => parse_oss_db_hosts(hosts, default_port)?,
            None => {
                if self.host.is_empty() {
                    bail!("OSS database needs a host or a non-empty hosts list");
                }
                vec![OssDbEndpoint {
                    host: self.host.clone(),
                    port: default_port,
                }]
            }
        };
        let hosts_file = match self.hosts_file {
            Some(path) => {
                let path = PathBuf::from(path);
                if !path.is_absolute() {
                    bail!(
                        "OSS database hosts_file '{}' must be an absolute path",
                        path.display()
                    );
                }
                Some(path)
            }
            None => None,
        };
        Ok(OssRemoteDatabaseConfig {
            host: self.host,
            port: self.port,
            database: self.database,
            user_secret: self.user_secret,
            password_secret: self.password_secret,
            endpoints,
            hosts_file,
        })
    }
}

/// Parse the `hosts` list of an OSS database: it must be non-empty and
/// contain no duplicates.
///
/// sql_ext parses hosts files with the same grammar in
/// `oss_mysql::endpoint`; keep the two in sync.
fn parse_oss_db_hosts(hosts: &[String], default_port: u16) -> Result<Vec<OssDbEndpoint>> {
    if hosts.is_empty() {
        bail!("OSS database hosts list must not be empty");
    }
    let mut seen = HashSet::new();
    let mut endpoints = Vec::with_capacity(hosts.len());
    for entry in hosts {
        let endpoint = parse_oss_db_endpoint(entry, default_port)
            .with_context(|| format!("Invalid OSS database hosts entry '{entry}'"))?;
        if !seen.insert((endpoint.host.to_ascii_lowercase(), endpoint.port)) {
            bail!("Duplicate OSS database hosts entry '{entry}' ({endpoint})");
        }
        endpoints.push(endpoint);
    }
    Ok(endpoints)
}

/// Parse one candidate server: "host", "host:port", "[v6]" or "[v6]:port".
/// The port defaults to `default_port`.
fn parse_oss_db_endpoint(entry: &str, default_port: u16) -> Result<OssDbEndpoint> {
    let entry = entry.trim();
    let (host, port) = if let Some(rest) = entry.strip_prefix('[') {
        let (addr, after) = rest
            .split_once(']')
            .ok_or_else(|| anyhow!("missing ']' after IPv6 address"))?;
        if addr.parse::<Ipv6Addr>().is_err() {
            bail!("'{addr}' is not an IPv6 address");
        }
        let port = match after {
            "" => None,
            _ => Some(
                after
                    .strip_prefix(':')
                    .ok_or_else(|| anyhow!("expected ':port' after ']'"))?,
            ),
        };
        (addr, port)
    } else {
        if entry.parse::<Ipv6Addr>().is_ok() {
            bail!("IPv6 addresses must be in brackets, as in [{entry}]:3306");
        }
        let (host, port) = match entry.split_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (entry, None),
        };
        if host.is_empty() {
            bail!("empty host");
        }
        if !host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_'))
        {
            bail!("'{host}' is not a valid host name or IPv4 address");
        }
        (host, port)
    };
    let port = match port {
        None => default_port,
        Some(port) => {
            if port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit()) {
                bail!("'{port}' is not a valid port");
            }
            match port.parse::<u16>() {
                Ok(port) if port > 0 => port,
                _ => bail!("port {port} is out of range 1..=65535"),
            }
        }
    };
    Ok(OssDbEndpoint {
        host: host.to_string(),
        port,
    })
}

impl Convert for RawDbShardedRemote {
    type Output = ShardedRemoteDatabaseConfig;

    fn convert(self) -> Result<Self::Output> {
        let shard_num = NonZeroUsize::new(self.shard_num.try_into()?)
            .ok_or_else(|| anyhow!("sharded remote shard_num must be > 0"))?;

        Ok(ShardedRemoteDatabaseConfig {
            shard_map: self.shard_map,
            shard_num,
        })
    }
}

impl Convert for RawDbShardableRemote {
    type Output = ShardableRemoteDatabaseConfig;

    fn convert(self) -> Result<Self::Output> {
        match self {
            RawDbShardableRemote::unsharded(raw) => {
                Ok(ShardableRemoteDatabaseConfig::Unsharded(raw.convert()?))
            }
            RawDbShardableRemote::sharded(raw) => {
                Ok(ShardableRemoteDatabaseConfig::Sharded(raw.convert()?))
            }
            RawDbShardableRemote::UnknownField(f) => {
                Err(anyhow!("unsupported database configuration ({f})"))
            }
        }
    }
}

impl Convert for RawDbConfig {
    type Output = DatabaseConfig;

    fn convert(self) -> Result<Self::Output> {
        match self {
            RawDbConfig::local(raw) => Ok(DatabaseConfig::Local(raw.convert()?)),
            RawDbConfig::remote(raw) => Ok(DatabaseConfig::Remote(raw.convert()?)),
            RawDbConfig::UnknownField(f) => {
                Err(anyhow!("unsupported database configuration ({f})"))
            }
        }
    }
}

impl Convert for RawShardedDbConfig {
    type Output = ShardedDatabaseConfig;

    fn convert(self) -> Result<Self::Output> {
        match self {
            RawShardedDbConfig::local(raw) => Ok(ShardedDatabaseConfig::Local(raw.convert()?)),
            RawShardedDbConfig::remote(raw) => Ok(ShardedDatabaseConfig::Sharded(raw.convert()?)),
            RawShardedDbConfig::unsharded(raw) => {
                Ok(ShardedDatabaseConfig::Unsharded(raw.convert()?))
            }
            RawShardedDbConfig::UnknownField(f) => {
                Err(anyhow!("unsupported database configuration ({f})"))
            }
        }
    }
}

impl Convert for RawMetadataConfig {
    type Output = MetadataDatabaseConfig;

    fn convert(self) -> Result<Self::Output> {
        match self {
            RawMetadataConfig::local(raw) => Ok(MetadataDatabaseConfig::Local(raw.convert()?)),
            RawMetadataConfig::remote(raw) => Ok(MetadataDatabaseConfig::Remote(
                RemoteMetadataDatabaseConfig {
                    production: raw.production.unwrap_or(raw.primary.clone()).convert()?,
                    commit_graph: raw.commit_graph.unwrap_or(raw.primary.clone()).convert()?,
                    bookmarks: raw.bookmarks.unwrap_or(raw.primary.clone()).convert()?,
                    bonsai_hg_mapping: raw
                        .bonsai_hg_mapping
                        .unwrap_or(raw.primary.clone())
                        .convert()?,
                    filenodes: raw.filenodes.convert()?,
                    mutation: raw.mutation.convert()?,
                    sparse_profiles: raw.sparse_profiles.convert()?,
                    bonsai_blob_mapping: raw.bonsai_blob_mapping.convert()?,
                    deletion_log: raw.deletion_log.convert()?,
                    commit_cloud: raw.commit_cloud.convert()?,
                    git_bundle_metadata: raw.git_bundles.convert()?,
                    repo_metadata: raw.repo_metadata.convert()?,
                    restricted_paths: raw.restricted_paths.convert()?,
                    commit_derived_data_mapping: raw.derived_data_mapping.convert()?,
                    repo_manifest_mapping: raw.repo_manifest_mapping.convert()?,
                },
            )),
            RawMetadataConfig::oss_remote(raw) => Ok(MetadataDatabaseConfig::OssRemote(
                OssRemoteMetadataDatabaseConfig {
                    production: raw.production.unwrap_or(raw.primary.clone()).convert()?,
                    commit_graph: raw.commit_graph.unwrap_or(raw.primary.clone()).convert()?,
                    bookmarks: raw.bookmarks.unwrap_or(raw.primary.clone()).convert()?,
                    bonsai_hg_mapping: raw
                        .bonsai_hg_mapping
                        .unwrap_or(raw.primary.clone())
                        .convert()?,
                    filenodes: raw.filenodes.convert()?,
                    mutation: raw.mutation.convert()?,
                    sparse_profiles: raw.sparse_profiles.convert()?,
                    bonsai_blob_mapping: raw.bonsai_blob_mapping.convert()?,
                    deletion_log: raw.deletion_log.convert()?,
                    commit_derived_data_mapping: raw.derived_data_mapping.convert()?,
                },
            )),
            RawMetadataConfig::UnknownField(f) => {
                Err(anyhow!("unsupported metadata database configuration ({f})"))
            }
        }
    }
}

impl Convert for RawFilestoreParams {
    type Output = FilestoreParams;

    fn convert(self) -> Result<Self::Output> {
        Ok(FilestoreParams {
            chunk_size: self.chunk_size.try_into()?,
            concurrency: self.concurrency.try_into()?,
        })
    }
}

impl Convert for RawMultiplexedStoreType {
    type Output = MultiplexedStoreType;

    fn convert(self) -> Result<Self::Output> {
        match self {
            RawMultiplexedStoreType::normal(RawMultiplexedStoreNormal {}) => {
                Ok(MultiplexedStoreType::Normal)
            }
            RawMultiplexedStoreType::write_only(RawMultiplexedStoreWriteOnly {}) => {
                Ok(MultiplexedStoreType::WriteOnly)
            }
            RawMultiplexedStoreType::UnknownField(field) => {
                Err(anyhow!("unknown store type {field}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use mononoke_macros::mononoke;

    use super::*;

    fn raw_oss_db(
        host: &str,
        port: i16,
        hosts: Option<&[&str]>,
        hosts_file: Option<&str>,
    ) -> RawOssDbRemote {
        RawOssDbRemote {
            host: host.to_string(),
            port,
            database: "mononoke".to_string(),
            password_secret: "file:/etc/mononoke/mysql.pw".to_string(),
            user_secret: "literal:mononoke".to_string(),
            hosts: hosts.map(|hosts| hosts.iter().map(|h| h.to_string()).collect()),
            hosts_file: hosts_file.map(String::from),
        }
    }

    fn endpoint(host: &str, port: u16) -> OssDbEndpoint {
        OssDbEndpoint {
            host: host.to_string(),
            port,
        }
    }

    /// Converts a config whose legacy port is `default_port` and returns
    /// the endpoints parsed from `hosts`.
    fn convert_hosts(hosts: &[&str], default_port: i16) -> Result<Vec<OssDbEndpoint>> {
        Ok(raw_oss_db("legacy", default_port, Some(hosts), None)
            .convert()?
            .endpoints)
    }

    fn convert_error(raw: RawOssDbRemote) -> String {
        format!("{:#}", raw.convert().expect_err("conversion should fail"))
    }

    fn hosts_error(entry: &str) -> String {
        convert_error(raw_oss_db("legacy", 3306, Some(&[entry]), None))
    }

    #[mononoke::test]
    fn test_oss_db_legacy_host_port() {
        let config = raw_oss_db("127.0.0.1", 13306, None, None)
            .convert()
            .unwrap();
        assert_eq!(config.endpoints, vec![endpoint("127.0.0.1", 13306)]);
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 13306);
        assert_eq!(config.database, "mononoke");
        assert_eq!(config.user_secret, "literal:mononoke");
        assert_eq!(config.password_secret, "file:/etc/mononoke/mysql.pw");
        assert_eq!(config.hosts_file, None);

        let config = raw_oss_db("db1", 32767, None, None).convert().unwrap();
        assert_eq!(config.endpoints, vec![endpoint("db1", 32767)]);
    }

    #[mononoke::test]
    fn test_oss_db_legacy_host_required_without_hosts() {
        let err = convert_error(raw_oss_db("", 3306, None, None));
        assert!(err.contains("needs a host"), "{err}");
    }

    #[mononoke::test]
    fn test_oss_db_legacy_port_bounds() {
        for port in [-1, -32768, 0] {
            let err = convert_error(raw_oss_db("db1", port, None, None));
            assert!(err.contains("out of range 1..=32767"), "{port}: {err}");
            // The legacy port is validated even when `hosts` has explicit
            // ports, since it is the default for hosts file entries.
            let err = convert_error(raw_oss_db("db1", port, Some(&["db2:3306"]), None));
            assert!(err.contains("out of range 1..=32767"), "{port}: {err}");
        }
    }

    #[mononoke::test]
    fn test_oss_db_hosts_forms() -> Result<()> {
        assert_eq!(
            convert_hosts(
                &[
                    "db1",
                    "db2:3307",
                    " db-3.example.com:13306 ",
                    "10.0.0.1",
                    "10.0.0.2:65535",
                    "my_db:1",
                ],
                3306,
            )?,
            vec![
                endpoint("db1", 3306),
                endpoint("db2", 3307),
                endpoint("db-3.example.com", 13306),
                endpoint("10.0.0.1", 3306),
                endpoint("10.0.0.2", 65535),
                endpoint("my_db", 1),
            ]
        );
        Ok(())
    }

    #[mononoke::test]
    fn test_oss_db_hosts_take_precedence_over_legacy() -> Result<()> {
        let config =
            raw_oss_db("legacy", 3306, Some(&["db1:13306", "db2:13307"]), None).convert()?;
        assert_eq!(
            config.endpoints,
            vec![endpoint("db1", 13306), endpoint("db2", 13307)]
        );
        // The legacy fields are kept as they were.
        assert_eq!(config.host, "legacy");
        assert_eq!(config.port, 3306);

        // With `hosts`, the legacy host may be empty.
        let config = raw_oss_db("", 3306, Some(&["db1"]), None).convert()?;
        assert_eq!(config.endpoints, vec![endpoint("db1", 3306)]);
        Ok(())
    }

    #[mononoke::test]
    fn test_oss_db_hosts_ipv6() -> Result<()> {
        assert_eq!(
            convert_hosts(&["[::1]:13307", "[fe80::1]", "[2001:db8::2]:3306"], 13306)?,
            vec![
                endpoint("::1", 13307),
                endpoint("fe80::1", 13306),
                endpoint("2001:db8::2", 3306),
            ]
        );

        let err = hosts_error("::1");
        assert!(
            err.contains("must be in brackets, as in [::1]:3306"),
            "{err}"
        );
        let err = hosts_error("fe80::1:3306");
        assert!(err.contains("must be in brackets"), "{err}");
        let err = hosts_error("[::1");
        assert!(err.contains("missing ']'"), "{err}");
        let err = hosts_error("[::1]3306");
        assert!(err.contains("expected ':port'"), "{err}");
        let err = hosts_error("[db1]:3306");
        assert!(err.contains("not an IPv6 address"), "{err}");
        let err = hosts_error("[]:3306");
        assert!(err.contains("not an IPv6 address"), "{err}");
        Ok(())
    }

    #[mononoke::test]
    fn test_oss_db_hosts_port_bounds() -> Result<()> {
        assert_eq!(
            convert_hosts(&["db1:1", "db2:65535", "db3:003306"], 3306)?,
            vec![
                endpoint("db1", 1),
                endpoint("db2", 65535),
                endpoint("db3", 3306),
            ]
        );
        for entry in [
            "db1:0",
            "db1:65536",
            "db1:99999999999",
            "[::1]:0",
            "[::1]:70000",
        ] {
            let err = hosts_error(entry);
            assert!(err.contains("out of range 1..=65535"), "{entry}: {err}");
            assert!(err.contains(&format!("'{entry}'")), "{entry}: {err}");
        }
        for entry in [
            "db1:",
            "db1:+3306",
            "db1:-1",
            "db1:abc",
            "db1: 3306",
            "[::1]:",
        ] {
            let err = hosts_error(entry);
            assert!(err.contains("is not a valid port"), "{entry}: {err}");
        }
        Ok(())
    }

    #[mononoke::test]
    fn test_oss_db_hosts_invalid_hosts() {
        for entry in ["", "  ", ":3306", "db 1:3306", "db1/x", "db1,db2", "a:1:2"] {
            let err = hosts_error(entry);
            assert!(
                err.contains("Invalid OSS database hosts entry"),
                "{entry}: {err}"
            );
        }
        let err = hosts_error("db1,db2:3306");
        assert!(err.contains("not a valid host name"), "{err}");
        let err = hosts_error("db1:3306,db2:3306");
        assert!(err.contains("is not a valid port"), "{err}");
    }

    #[mononoke::test]
    fn test_oss_db_hosts_empty_and_duplicates() {
        let err = convert_error(raw_oss_db("db1", 3306, Some(&[]), None));
        assert!(err.contains("must not be empty"), "{err}");

        // Duplicates are detected after the default port is applied, and
        // host names are compared case-insensitively.
        for hosts in [
            &["db1:3306", "db2:3306", "db1:3306"][..],
            &["db1", "db1:3306"],
            &["DB1:3306", "db1"],
        ] {
            let err = convert_error(raw_oss_db("legacy", 3306, Some(hosts), None));
            assert!(err.contains("Duplicate OSS database hosts entry"), "{err}");
        }
        let err = convert_error(raw_oss_db(
            "legacy",
            3306,
            Some(&["[::1]", "[::1]:3306"]),
            None,
        ));
        assert!(err.contains("([::1]:3306)"), "{err}");

        // The same host on different ports is not a duplicate.
        assert_eq!(
            convert_hosts(&["db1:3306", "db1:3307"], 3306).unwrap(),
            vec![endpoint("db1", 3306), endpoint("db1", 3307)]
        );
    }

    #[mononoke::test]
    fn test_oss_db_hosts_file() -> Result<()> {
        let config = raw_oss_db(
            "db1",
            3306,
            Some(&["db1", "db2"]),
            Some("/etc/mononoke/mysql-hosts"),
        )
        .convert()?;
        assert_eq!(
            config.hosts_file,
            Some(PathBuf::from("/etc/mononoke/mysql-hosts"))
        );
        // The file is not read while parsing: `endpoints` stays the
        // fallback.
        assert_eq!(
            config.endpoints,
            vec![endpoint("db1", 3306), endpoint("db2", 3306)]
        );

        let config = raw_oss_db("db1", 3306, None, Some("/nonexistent/hosts")).convert()?;
        assert_eq!(config.hosts_file, Some(PathBuf::from("/nonexistent/hosts")));
        assert_eq!(config.endpoints, vec![endpoint("db1", 3306)]);

        for path in ["", "mysql-hosts", "./mysql-hosts"] {
            let err = convert_error(raw_oss_db("db1", 3306, None, Some(path)));
            assert!(err.contains("must be an absolute path"), "{path}: {err}");
        }
        Ok(())
    }

    #[mononoke::test]
    fn test_oss_db_toml_keys() -> Result<()> {
        let raw: RawOssDbRemote = toml::from_str(
            r#"
            host = "127.0.0.1"
            port = 13306
            database = "mononoke"
            user_secret = "literal:mononoke"
            password_secret = "file:/etc/mononoke/mysql.pw"
            hosts = ["127.0.0.1:13306", "127.0.0.1:13307"]
            hosts_file = "/etc/mononoke/mysql-hosts"
            "#,
        )?;
        let config = raw.convert()?;
        assert_eq!(
            config.endpoints,
            vec![endpoint("127.0.0.1", 13306), endpoint("127.0.0.1", 13307)]
        );
        assert_eq!(
            config.hosts_file,
            Some(PathBuf::from("/etc/mononoke/mysql-hosts"))
        );

        // A config written for older binaries has neither key.
        let raw: RawOssDbRemote = toml::from_str(
            r#"
            host = "127.0.0.1"
            port = 13306
            database = "mononoke"
            user_secret = "literal:mononoke"
            password_secret = "file:/etc/mononoke/mysql.pw"
            "#,
        )?;
        assert_eq!(raw.hosts, None);
        assert_eq!(raw.hosts_file, None);
        assert_eq!(raw.convert()?.endpoints, vec![endpoint("127.0.0.1", 13306)]);
        Ok(())
    }
}
