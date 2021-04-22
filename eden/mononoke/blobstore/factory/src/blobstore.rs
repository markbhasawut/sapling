/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use anyhow::{bail, Context, Error};
use blobstore::{
    Blobstore, BlobstorePutOps, BlobstoreWithLink, DisabledBlob, ErrorKind, PutBehaviour,
    DEFAULT_PUT_BEHAVIOUR,
};
use blobstore_sync_queue::SqlBlobstoreSyncQueue;
use cacheblob::CachelibBlobstoreOptions;
use cached_config::ConfigStore;
use chaosblob::{ChaosBlobstore, ChaosOptions};
use fbinit::FacebookInit;
use fileblob::Fileblob;
use futures::future::{self, BoxFuture, FutureExt};
use futures_watchdog::WatchdogExt;
use logblob::LogBlob;
use metaconfig_types::{
    BlobConfig, BlobstoreId, DatabaseConfig, MultiplexId, MultiplexedStoreType,
    ShardableRemoteDatabaseConfig,
};
use multiplexedblob::{
    MultiplexedBlobstore, ScrubAction, ScrubBlobstore, ScrubHandler, ScrubOptions,
};
use packblob::{PackBlob, PackOptions};
use readonlyblob::ReadOnlyBlobstore;
use scuba_ext::MononokeScubaSampleBuilder;
use slog::Logger;
use sql_construct::SqlConstructFromDatabaseConfig;
use sql_ext::facebook::{MysqlConnectionType, MysqlOptions};
use sqlblob::{CountedSqlblob, Sqlblob};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;
use throttledblob::{ThrottleOptions, ThrottledBlob};

use crate::ReadOnlyStorage;

#[cfg(fbcode_build)]
use crate::facebook::ManifoldBlobstore;
#[cfg(not(fbcode_build))]
type ManifoldBlobstore = DisabledBlob;

#[derive(Clone, Debug)]
pub struct BlobstoreOptions {
    pub chaos_options: ChaosOptions,
    pub throttle_options: ThrottleOptions,
    pub manifold_api_key: Option<String>,
    pub pack_options: PackOptions,
    pub cachelib_options: CachelibBlobstoreOptions,
    pub put_behaviour: PutBehaviour,
    pub scrub_options: Option<ScrubOptions>,
    pub sqlblob_mysql_options: MysqlOptions,
}

impl BlobstoreOptions {
    pub fn new(
        chaos_options: ChaosOptions,
        throttle_options: ThrottleOptions,
        manifold_api_key: Option<String>,
        pack_options: PackOptions,
        cachelib_options: CachelibBlobstoreOptions,
        put_behaviour: Option<PutBehaviour>,
        sqlblob_mysql_options: MysqlOptions,
    ) -> Self {
        Self {
            chaos_options,
            throttle_options,
            manifold_api_key,
            pack_options,
            cachelib_options,
            // If not specified, maintain status quo, which is overwrite
            put_behaviour: put_behaviour.unwrap_or(DEFAULT_PUT_BEHAVIOUR),
            // These are added via the builder methods
            scrub_options: None,
            sqlblob_mysql_options,
        }
    }

    pub fn with_scrub_action(self, scrub_action: Option<ScrubAction>) -> Self {
        if let Some(scrub_action) = scrub_action {
            let mut scrub_options = self.scrub_options.unwrap_or_default();
            scrub_options.scrub_action = scrub_action;
            Self {
                scrub_options: Some(scrub_options),
                ..self
            }
        } else {
            self
        }
    }

    pub fn with_scrub_grace(self, scrub_grace: Option<u64>) -> Self {
        if let Some(mut scrub_options) = self.scrub_options {
            scrub_options.scrub_grace = scrub_grace.map(Duration::from_secs);
            Self {
                scrub_options: Some(scrub_options),
                ..self
            }
        } else {
            self
        }
    }
}

/// Construct a blobstore according to the specification. The multiplexed blobstore
/// needs an SQL DB for its queue, as does the MySQL blobstore.
/// If `throttling.read_qps` or `throttling.write_qps` are Some then ThrottledBlob will be used to limit
/// QPS to the underlying blobstore
pub fn make_blobstore<'a>(
    fb: FacebookInit,
    blobconfig: BlobConfig,
    mysql_options: &'a MysqlOptions,
    readonly_storage: ReadOnlyStorage,
    blobstore_options: &'a BlobstoreOptions,
    logger: &'a Logger,
    config_store: &'a ConfigStore,
    scrub_handler: &'a Arc<dyn ScrubHandler>,
) -> BoxFuture<'a, Result<Arc<dyn Blobstore>, Error>> {
    async move {
        let store = make_blobstore_put_ops(
            fb,
            blobconfig,
            mysql_options,
            readonly_storage,
            blobstore_options,
            logger,
            config_store,
            scrub_handler,
        )
        .await?;
        // Workaround for trait A {} trait B:A {} but Arc<dyn B> is not a Arc<dyn A>
        // See https://github.com/rust-lang/rfcs/issues/2765 if interested
        Ok(Arc::new(store) as Arc<dyn Blobstore>)
    }
    .boxed()
}

pub async fn make_sql_blobstore<'a>(
    fb: FacebookInit,
    blobconfig: BlobConfig,
    readonly_storage: ReadOnlyStorage,
    blobstore_options: &'a BlobstoreOptions,
    config_store: &'a ConfigStore,
) -> Result<CountedSqlblob, Error> {
    use BlobConfig::*;
    match blobconfig {
        Sqlite { path } => Sqlblob::with_sqlite_path(
            path.join("blobs"),
            readonly_storage.0,
            blobstore_options.put_behaviour,
            config_store,
        )
        .context(ErrorKind::StateOpen),
        Mysql { remote } => {
            let (tier_name, shard_count) = match remote {
                ShardableRemoteDatabaseConfig::Unsharded(config) => (config.db_address, None),
                ShardableRemoteDatabaseConfig::Sharded(config) => {
                    (config.shard_map.clone(), Some(config.shard_num))
                }
            };
            make_sql_blobstore_xdb(
                fb,
                tier_name,
                shard_count,
                blobstore_options,
                readonly_storage,
                blobstore_options.put_behaviour,
                config_store,
            )
            .await
        }
        _ => bail!("Not an SQL blobstore"),
    }
}

// Most users should call `make_sql_blobstore` instead, however its useful to expose this to reduce duplication with benchmark tools.
pub async fn make_sql_blobstore_xdb<'a>(
    fb: FacebookInit,
    tier_name: String,
    shard_count: Option<NonZeroUsize>,
    blobstore_options: &'a BlobstoreOptions,
    readonly_storage: ReadOnlyStorage,
    put_behaviour: PutBehaviour,
    config_store: &'a ConfigStore,
) -> Result<CountedSqlblob, Error> {
    let read_conn_type = blobstore_options
        .sqlblob_mysql_options
        .read_connection_type();
    match (
        blobstore_options
            .sqlblob_mysql_options
            .connection_type
            .clone(),
        shard_count,
    ) {
        (MysqlConnectionType::Myrouter(myrouter_port), None) => {
            Sqlblob::with_myrouter_unsharded(
                fb,
                tier_name,
                myrouter_port,
                read_conn_type,
                readonly_storage.0,
                put_behaviour,
                config_store,
            )
            .await
        }
        (MysqlConnectionType::Myrouter(myrouter_port), Some(shard_num)) => {
            Sqlblob::with_myrouter(
                fb,
                tier_name,
                myrouter_port,
                read_conn_type,
                shard_num,
                readonly_storage.0,
                put_behaviour,
                config_store,
            )
            .await
        }
        (MysqlConnectionType::Mysql(pool, pool_config), None) => {
            Sqlblob::with_mysql_unsharded(
                fb,
                tier_name,
                pool,
                pool_config,
                read_conn_type,
                readonly_storage.0,
                put_behaviour,
                config_store,
            )
            .await
        }
        (MysqlConnectionType::Mysql(pool, pool_config), Some(shard_num)) => {
            Sqlblob::with_mysql(
                fb,
                tier_name,
                shard_num,
                pool,
                pool_config,
                read_conn_type,
                readonly_storage.0,
                put_behaviour,
                config_store,
            )
            .await
        }
    }
}

/// Construct a PackBlob according to the spec; you are responsible for
/// finding a PackBlob config
pub async fn make_packblob<'a>(
    fb: FacebookInit,
    blobconfig: BlobConfig,
    readonly_storage: ReadOnlyStorage,
    blobstore_options: &'a BlobstoreOptions,
    logger: &'a Logger,
    config_store: &'a ConfigStore,
) -> Result<PackBlob<Arc<dyn BlobstoreWithLink>>, Error> {
    if let BlobConfig::Pack {
        pack_config,
        blobconfig,
    } = blobconfig
    {
        let store = make_blobstore_with_link(
            fb,
            *blobconfig,
            readonly_storage,
            &blobstore_options,
            logger,
            config_store,
        )
        .watched(logger)
        .await?;

        // Take the user specified option if provided, otherwise use the config
        let put_format =
            if let Some(put_format) = blobstore_options.pack_options.override_put_format {
                put_format
            } else {
                pack_config.map(|c| c.put_format).unwrap_or_default()
            };

        Ok(PackBlob::new(store, put_format))
    } else {
        bail!("Not a PackBlob")
    }
}

#[cfg(fbcode_build)]
async fn make_manifold_blobstore(
    fb: FacebookInit,
    blobconfig: BlobConfig,
    blobstore_options: &BlobstoreOptions,
) -> Result<ManifoldBlobstore, Error> {
    use BlobConfig::*;
    match blobconfig {
        Manifold { bucket, prefix } => crate::facebook::make_manifold_blobstore(
            fb,
            &prefix,
            &bucket,
            None,
            blobstore_options.manifold_api_key.as_deref(),
            blobstore_options.put_behaviour,
        ),
        ManifoldWithTtl {
            bucket,
            prefix,
            ttl,
        } => crate::facebook::make_manifold_blobstore(
            fb,
            &prefix,
            &bucket,
            Some(ttl),
            blobstore_options.manifold_api_key.as_deref(),
            blobstore_options.put_behaviour,
        ),
        _ => bail!("Not a Manifold blobstore"),
    }
}

#[cfg(not(fbcode_build))]
async fn make_manifold_blobstore(
    _fb: FacebookInit,
    _blobconfig: BlobConfig,
    _blobstore_options: &BlobstoreOptions,
) -> Result<ManifoldBlobstore, Error> {
    unimplemented!("This is implemented only for fbcode_build")
}

async fn make_files_blobstore(
    blobconfig: BlobConfig,
    blobstore_options: &BlobstoreOptions,
) -> Result<Fileblob, Error> {
    if let BlobConfig::Files { path } = blobconfig {
        Fileblob::create(path.join("blobs"), blobstore_options.put_behaviour)
            .context(ErrorKind::StateOpen)
    } else {
        bail!("Not a file blobstore")
    }
}

async fn make_blobstore_with_link<'a>(
    fb: FacebookInit,
    blobconfig: BlobConfig,
    readonly_storage: ReadOnlyStorage,
    blobstore_options: &'a BlobstoreOptions,
    logger: &'a Logger,
    config_store: &'a ConfigStore,
) -> Result<Arc<dyn BlobstoreWithLink>, Error> {
    use BlobConfig::*;
    match blobconfig {
        Sqlite { .. } | Mysql { .. } => make_sql_blobstore(
            fb,
            blobconfig,
            readonly_storage,
            blobstore_options,
            config_store,
        )
        .watched(logger)
        .await
        .map(|store| Arc::new(store) as Arc<dyn BlobstoreWithLink>),
        Manifold { .. } | ManifoldWithTtl { .. } => {
            make_manifold_blobstore(fb, blobconfig, blobstore_options)
                .watched(logger)
                .await
                .map(|store| Arc::new(store) as Arc<dyn BlobstoreWithLink>)
        }
        Files { .. } => make_files_blobstore(blobconfig, blobstore_options)
            .await
            .map(|store| Arc::new(store) as Arc<dyn BlobstoreWithLink>),
        _ => bail!("Not a physical blobstore"),
    }
}

// Constructs the BlobstorePutOps store implementations for low level blobstore access
fn make_blobstore_put_ops<'a>(
    fb: FacebookInit,
    blobconfig: BlobConfig,
    mysql_options: &'a MysqlOptions,
    readonly_storage: ReadOnlyStorage,
    blobstore_options: &'a BlobstoreOptions,
    logger: &'a Logger,
    config_store: &'a ConfigStore,
    scrub_handler: &'a Arc<dyn ScrubHandler>,
) -> BoxFuture<'a, Result<Arc<dyn BlobstorePutOps>, Error>> {
    // NOTE: This needs to return a BoxFuture because it recurses.
    async move {
        use BlobConfig::*;

        let mut has_components = false;
        let store = match blobconfig {
            // Physical blobstores
            Sqlite { .. } | Mysql { .. } => make_sql_blobstore(
                fb,
                blobconfig,
                readonly_storage,
                blobstore_options,
                config_store,
            )
            .watched(logger)
            .await
            .map(|store| Arc::new(store) as Arc<dyn BlobstorePutOps>)?,
            Manifold { .. } | ManifoldWithTtl { .. } => {
                make_manifold_blobstore(fb, blobconfig, blobstore_options)
                    .watched(logger)
                    .await
                    .map(|store| Arc::new(store) as Arc<dyn BlobstorePutOps>)?
            }
            Files { .. } => make_files_blobstore(blobconfig, blobstore_options)
                .await
                .map(|store| Arc::new(store) as Arc<dyn BlobstorePutOps>)?,
            S3 {
                bucket,
                keychain_group,
                region_name,
                endpoint,
                num_concurrent_operations,
            } => {
                #[cfg(fbcode_build)]
                {
                    ::s3blob::S3Blob::new(
                        fb,
                        bucket,
                        keychain_group,
                        region_name,
                        endpoint,
                        blobstore_options.put_behaviour,
                        logger,
                        num_concurrent_operations,
                    )
                    .watched(logger)
                    .await
                    .context(ErrorKind::StateOpen)
                    .map(|store| Arc::new(store) as Arc<dyn BlobstorePutOps>)?
                }
                #[cfg(not(fbcode_build))]
                {
                    let _ = (
                        bucket,
                        keychain_group,
                        region_name,
                        endpoint,
                        num_concurrent_operations,
                    );
                    unimplemented!("This is implemented only for fbcode_build")
                }
            }

            // Special case
            Disabled => {
                Arc::new(DisabledBlob::new("Disabled by configuration")) as Arc<dyn BlobstorePutOps>
            }

            // Wrapper blobstores
            Multiplexed {
                multiplex_id,
                scuba_table,
                scuba_sample_rate,
                blobstores,
                minimum_successful_writes,
                queue_db,
            } => {
                has_components = true;
                make_blobstore_multiplexed(
                    fb,
                    multiplex_id,
                    queue_db,
                    scuba_table,
                    scuba_sample_rate,
                    blobstores,
                    minimum_successful_writes,
                    mysql_options,
                    readonly_storage,
                    blobstore_options,
                    logger,
                    config_store,
                    scrub_handler,
                )
                .watched(logger)
                .await?
            }
            Logging {
                blobconfig,
                scuba_table,
                scuba_sample_rate,
            } => {
                let scuba = scuba_table
                    .map_or(MononokeScubaSampleBuilder::with_discard(), |table| {
                        MononokeScubaSampleBuilder::new(fb, &table)
                    });

                let store = make_blobstore_put_ops(
                    fb,
                    *blobconfig,
                    mysql_options,
                    readonly_storage,
                    &blobstore_options,
                    logger,
                    config_store,
                    scrub_handler,
                )
                .watched(logger)
                .await?;

                Arc::new(LogBlob::new(store, scuba, scuba_sample_rate)) as Arc<dyn BlobstorePutOps>
            }
            Pack { .. } => make_packblob(
                fb,
                blobconfig,
                readonly_storage,
                blobstore_options,
                logger,
                config_store,
            )
            .watched(logger)
            .await
            .map(|store| Arc::new(store) as Arc<dyn BlobstorePutOps>)?,
        };

        let store = if readonly_storage.0 {
            Arc::new(ReadOnlyBlobstore::new(store)) as Arc<dyn BlobstorePutOps>
        } else {
            store
        };

        let store = if blobstore_options.throttle_options.has_throttle() {
            Arc::new(
                ThrottledBlob::new(store, blobstore_options.throttle_options)
                    .watched(logger)
                    .await,
            ) as Arc<dyn BlobstorePutOps>
        } else {
            store
        };

        // For stores with components only set chaos on their components
        let store = if !has_components && blobstore_options.chaos_options.has_chaos() {
            Arc::new(ChaosBlobstore::new(store, blobstore_options.chaos_options))
                as Arc<dyn BlobstorePutOps>
        } else {
            store
        };

        // NOTE: Do not add wrappers here that should only be added once per repository, since this
        // function will get called recursively for each member of a Multiplex! For those, use
        // RepoBlobstoreArgs::new instead.

        Ok(store)
    }
    .boxed()
}

async fn make_blobstore_multiplexed<'a>(
    fb: FacebookInit,
    multiplex_id: MultiplexId,
    queue_db: DatabaseConfig,
    scuba_table: Option<String>,
    scuba_sample_rate: NonZeroU64,
    inner_config: Vec<(BlobstoreId, MultiplexedStoreType, BlobConfig)>,
    minimum_successful_writes: NonZeroUsize,
    mysql_options: &'a MysqlOptions,
    readonly_storage: ReadOnlyStorage,
    blobstore_options: &'a BlobstoreOptions,
    logger: &'a Logger,
    config_store: &'a ConfigStore,
    scrub_handler: &'a Arc<dyn ScrubHandler>,
) -> Result<Arc<dyn BlobstorePutOps>, Error> {
    let component_readonly = blobstore_options
        .scrub_options
        .as_ref()
        .map_or(ReadOnlyStorage(false), |v| {
            ReadOnlyStorage(v.scrub_action != ScrubAction::Repair)
        });

    let mut applied_chaos = false;

    let components = future::try_join_all(inner_config.into_iter().map({
        move |(blobstoreid, store_type, config)| {
            let mut blobstore_options = blobstore_options.clone();

            if blobstore_options.chaos_options.has_chaos() {
                if applied_chaos {
                    blobstore_options = BlobstoreOptions {
                        chaos_options: ChaosOptions::new(None, None),
                        ..blobstore_options
                    };
                } else {
                    applied_chaos = true;
                }
            }

            async move {
                let store = make_blobstore_put_ops(
                    fb,
                    config,
                    mysql_options,
                    component_readonly,
                    &blobstore_options,
                    logger,
                    config_store,
                    scrub_handler,
                )
                .watched(logger)
                .await?;

                Ok((blobstoreid, store_type, store))
            }
        }
    }));

    let queue = SqlBlobstoreSyncQueue::with_database_config(
        fb,
        &queue_db,
        mysql_options,
        readonly_storage.0,
    )
    .watched(logger);

    let (components, queue) = future::try_join(components, queue).await?;

    // For now, `partition` could do this, but this will be easier to extend when we introduce more store types
    let (normal_components, write_mostly_components) = {
        let mut normal_components = vec![];
        let mut write_mostly_components = vec![];
        for (blobstore_id, store_type, store) in components.into_iter() {
            match store_type {
                MultiplexedStoreType::Normal => normal_components.push((blobstore_id, store)),
                MultiplexedStoreType::WriteMostly => {
                    write_mostly_components.push((blobstore_id, store))
                }
            }
        }
        (normal_components, write_mostly_components)
    };

    let blobstore = match &blobstore_options.scrub_options {
        Some(scrub_options) => Arc::new(ScrubBlobstore::new(
            multiplex_id,
            normal_components,
            write_mostly_components,
            minimum_successful_writes,
            Arc::new(queue),
            scuba_table.map_or(MononokeScubaSampleBuilder::with_discard(), |table| {
                MononokeScubaSampleBuilder::new(fb, &table)
            }),
            scuba_sample_rate,
            scrub_options.clone(),
            scrub_handler.clone(),
        )) as Arc<dyn BlobstorePutOps>,
        None => Arc::new(MultiplexedBlobstore::new(
            multiplex_id,
            normal_components,
            write_mostly_components,
            minimum_successful_writes,
            Arc::new(queue),
            scuba_table.map_or(MononokeScubaSampleBuilder::with_discard(), |table| {
                MononokeScubaSampleBuilder::new(fb, &table)
            }),
            scuba_sample_rate,
        )) as Arc<dyn BlobstorePutOps>,
    };

    Ok(blobstore)
}
