/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! MySQL failover support for OSS builds.
//!
//! A cluster is the set of candidate servers of one database entry, with its
//! credentials. The registry shares one [`Cluster`] per cluster key among
//! the stores of a process: one prober that finds the primary, and two pools
//! per database, Primary and Read. The prober points the pools at the
//! servers it finds; stores keep the same pool handles throughout, and those
//! handles keep the cluster alive.

pub(crate) mod classify;
pub(crate) mod decide;
pub(crate) mod endpoint;
#[cfg(test)]
mod fake;
#[cfg(test)]
mod fake_mysqld;
pub(crate) mod fence;
pub(crate) mod hosts_file;
#[cfg(test)]
mod lab;
pub(crate) mod probe;
pub(crate) mod retry;
pub(crate) mod secrets;
pub(crate) mod status;

use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;

use anyhow::Result;
use anyhow::bail;
use sql::mysql_async::Error as MysqlError;
use sql::mysql_async::Opts;
use sql::mysql_async::OptsBuilder;
use sql::mysql_async::Pool;
use sql::mysql_async::PoolConstraints;
use sql::mysql_async::PoolOpts;
use sql_common::mysql::ConnectionStats;
use sql_common::mysql::OssConnection;
use sql_common::mysql::OssErrorHook;
use sql_common::mysql::OssErrorKind;
use sql_common::mysql::OssMysqlError;
use sql_common::mysql::OssPool;
use sql_common::mysql::OssPoolState;
use sql_common::mysql::OssWaitHook;
use tokio::sync::Notify;
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use self::classify::ER_ACCESS_DENIED_ERROR;
use self::classify::refuses_writes;
use self::classify::should_kick;
use self::decide::Action;
use self::decide::Candidate;
use self::decide::PrimaryCheck;
use self::decide::ServerIdentity;
use self::decide::Target;
use self::fence::PrimaryFence;
use self::fence::primary_pool_setup;
use self::fence::read_pool_setup;
use self::hosts_file::CandidateList;
use self::probe::MysqlProbeTransport;
use self::probe::ProbeTransport;
use self::probe::Prober;
use self::probe::Refresher;
use self::secrets::Credentials;
use self::status::RoundStatus;
use crate::Connection;
use crate::SqlConnections;
use crate::facebook::MysqlOptions;
use crate::facebook::ReadConnectionType;
use crate::oss::OssFailoverSettings;
use crate::oss::PoolConfig;

/// TCP keepalive of pool connections, in milliseconds.
const TCP_KEEPALIVE_MS: u32 = 10_000;

/// How often pools close connections past their idle or age limit.
const TTL_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Connections a pool keeps open once the others have been idle for the
/// idle timeout.
const MIN_IDLE_CONNECTIONS: usize = 2;

/// Extra time a probe gets on top of the connection open timeout.
const PROBE_QUERY_TIME: Duration = Duration::from_secs(1);

/// How much longer than the client the server lets a SELECT run. The client
/// then gives up first, so a slow read fails as a timeout, which is retried,
/// rather than with the server's own error (3024), which is not: the server
/// only stops reads the client already gave up on.
const SERVER_TIME_LIMIT_MARGIN: Duration = Duration::from_secs(1);

/// An OSS MySQL database, as configured.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct OssMysqlTarget {
    /// The candidate servers, as (host, port), with IPv6 hosts without
    /// brackets. Used when there is no hosts file or it cannot be used.
    pub endpoints: Vec<(String, u16)>,
    /// A file listing the candidate servers, read again when it changes.
    pub hosts_file: Option<PathBuf>,
    /// The port of hosts file entries that have none.
    pub default_port: u16,
    /// The database name.
    pub database: String,
    /// Reference to the user name (see `secrets`).
    pub user_secret: String,
    /// Reference to the password (see `secrets`).
    pub password_secret: String,
}

impl fmt::Debug for OssMysqlTarget {
    // The secret references are shown as `printable_secret` does.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OssMysqlTarget")
            .field("endpoints", &self.endpoints)
            .field("hosts_file", &self.hosts_file)
            .field("default_port", &self.default_port)
            .field("database", &self.database)
            .field("user_secret", &printable_secret(&self.user_secret))
            .field("password_secret", &printable_secret(&self.password_secret))
            .finish()
    }
}

/// A secret reference as it may be logged. `literal:` references show only
/// their form. So do references of no known form, bare names included: like
/// `secrets`, which does not echo them in errors, this assumes they may be a
/// password that lacks `literal:`.
fn printable_secret(reference: &str) -> &str {
    if reference.starts_with("literal:") {
        "literal:<redacted>"
    } else if ["env:", "file:", "aws-sm:"]
        .iter()
        .any(|form| reference.starts_with(form))
    {
        reference
    } else {
        "<redacted>"
    }
}

/// Creates the connections of a store to `target`, sharing the pools of
/// every other store of the same cluster and database built with the same
/// `mysql_options`.
///
/// The first store of a cluster probes its servers before this returns.
/// When no primary is found, writes wait for one, up to the checkout
/// timeout, instead of failing here. The connections keep the cluster alive:
/// once no store holds any, its prober stops.
pub async fn create_oss_mysql_connections(
    target: &OssMysqlTarget,
    mysql_options: &MysqlOptions,
    label: &str,
    readonly: bool,
) -> Result<SqlConnections> {
    mysql_options
        .pool
        .0
        .connections(
            target,
            &mysql_options.pool_config,
            mysql_options.read_connection_type,
            label,
            readonly,
        )
        .await
}

/// The two pools of a database.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Role {
    /// Connects to the primary, and checks with a fence that it reached it.
    Primary,
    /// Connects to the primary while there is one, else to another server;
    /// its sessions are read-only.
    Read,
}

/// What tells clusters apart: where the candidates come from, and the
/// secret references (never their values).
#[derive(Clone, PartialEq, Eq, Hash)]
struct ClusterKey {
    candidates: CandidateSource,
    user_secret: String,
    password_secret: String,
}

impl fmt::Debug for ClusterKey {
    // The secret references are shown as `printable_secret` does.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterKey")
            .field("candidates", &self.candidates)
            .field("user_secret", &printable_secret(&self.user_secret))
            .field("password_secret", &printable_secret(&self.password_secret))
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum CandidateSource {
    /// The path, not the contents: an edit of the file changes the
    /// candidates of the existing cluster.
    HostsFile { path: PathBuf, default_port: u16 },
    /// The servers, with host names in lower case, sorted and without
    /// duplicates: the same servers listed in another order or case are the
    /// same cluster. The order only chooses where reads go before a primary
    /// is found.
    Endpoints(Vec<(String, u16)>),
}

impl ClusterKey {
    fn of(target: &OssMysqlTarget) -> Self {
        let candidates = match &target.hosts_file {
            Some(path) => CandidateSource::HostsFile {
                path: path.clone(),
                default_port: target.default_port,
            },
            None => {
                let mut endpoints: Vec<(String, u16)> = target
                    .endpoints
                    .iter()
                    .map(|(host, port)| (host.to_ascii_lowercase(), *port))
                    .collect();
                endpoints.sort();
                endpoints.dedup();
                CandidateSource::Endpoints(endpoints)
            }
        };
        Self {
            candidates,
            user_secret: target.user_secret.clone(),
            password_secret: target.password_secret.clone(),
        }
    }
}

/// The clusters of a process, with their pools.
pub struct OssMysqlRegistry {
    settings: OssFailoverSettings,
    transport: Arc<dyn ProbeTransport>,
    /// The clusters in use. The stores keep their cluster alive, not the
    /// registry: a cluster that no store uses any more is dropped, which
    /// stops its prober, and its entry is removed at a later lookup.
    clusters: Mutex<HashMap<ClusterKey, Arc<OnceCell<Weak<Cluster>>>>>,
    stats: Mutex<HashMap<String, Arc<ConnectionStats>>>,
}

impl Default for OssMysqlRegistry {
    fn default() -> Self {
        Self::new(OssFailoverSettings::default())
    }
}

impl OssMysqlRegistry {
    pub fn new(settings: OssFailoverSettings) -> Self {
        Self::with_transport(settings, Arc::new(MysqlProbeTransport))
    }

    pub fn with_transport(
        settings: OssFailoverSettings,
        transport: Arc<dyn ProbeTransport>,
    ) -> Self {
        Self {
            settings,
            transport,
            clusters: Mutex::new(HashMap::new()),
            stats: Mutex::new(HashMap::new()),
        }
    }

    /// The connections of a store: see [`create_oss_mysql_connections`].
    pub async fn connections(
        &self,
        target: &OssMysqlTarget,
        pool_config: &PoolConfig,
        read_connection_type: ReadConnectionType,
        label: &str,
        readonly: bool,
    ) -> Result<SqlConnections> {
        let cluster = self.cluster(target, pool_config).await?;
        // The handles of the store keep the cluster alive. Those the cluster
        // keeps itself do not, so there is no reference cycle.
        let keepalive: Arc<dyn Any + Send + Sync> = cluster.clone();
        let primary = cluster
            .pool(&target.database, Role::Primary)
            .with_keepalive(keepalive.clone());
        let read = cluster
            .pool(&target.database, Role::Read)
            .with_keepalive(keepalive);
        let master_only = matches!(read_connection_type, ReadConnectionType::Master);
        let stats = self.stats(label);
        let connection = |pool: &OssPool| Connection {
            inner: OssConnection::new(pool.clone(), stats.clone()).into(),
            shard_name: target.database.clone(),
        };
        Ok(SqlConnections {
            // Read pools refuse writes, as read-only SQLite stores do.
            write_connection: connection(if readonly { &read } else { &primary }),
            read_connection: connection(if master_only { &primary } else { &read }),
            read_master_connection: connection(&primary),
        })
    }

    /// The cluster of `target`, created with `pool_config` if there is none
    /// in use. Callers keep it alive for as long as they use its pools.
    pub async fn cluster(
        &self,
        target: &OssMysqlTarget,
        pool_config: &PoolConfig,
    ) -> Result<Arc<Cluster>> {
        let key = ClusterKey::of(target);
        loop {
            let cell = {
                let mut clusters = self.clusters.lock().expect("MySQL registry lock poisoned");
                clusters.retain(|_, cell| match cell.get() {
                    Some(cluster) => cluster.strong_count() > 0,
                    // Being created, unless only the registry holds it, after
                    // a failed creation.
                    None => Arc::strong_count(cell) > 1,
                });
                clusters.entry(key.clone()).or_default().clone()
            };
            // Concurrent callers wait for one creation. A failed creation is
            // not kept, so the next caller tries again.
            let mut created = None;
            let cluster = cell
                .get_or_try_init(|| async {
                    let cluster = Cluster::create(
                        target,
                        &self.settings,
                        pool_config,
                        self.transport.clone(),
                    )
                    .await?;
                    let weak = Arc::downgrade(&cluster);
                    created = Some(cluster);
                    anyhow::Ok(weak)
                })
                .await?;
            if let Some(cluster) = created.or_else(|| cluster.upgrade()) {
                return Ok(cluster);
            }
            // Its last store dropped it in the meantime: create another.
            let mut clusters = self.clusters.lock().expect("MySQL registry lock poisoned");
            if clusters
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, &cell))
            {
                clusters.remove(&key);
            }
        }
    }

    fn stats(&self, label: &str) -> Arc<ConnectionStats> {
        self.stats
            .lock()
            .expect("MySQL registry lock poisoned")
            .entry(label.to_string())
            .or_insert_with(|| Arc::new(ConnectionStats::new(label.to_string())))
            .clone()
    }
}

/// Where a pool connects, and which server its fence expects.
#[derive(Clone, Debug, PartialEq, Eq)]
struct PoolEndpoint {
    candidate: Candidate,
    /// The server a Primary pool must reach, in identity mode.
    identity: Option<ServerIdentity>,
}

/// The servers of one cluster, its pools, and its prober.
pub struct Cluster {
    /// Unique in this process.
    id: u64,
    key: ClusterKey,
    /// The hosts file or the candidates, for logs.
    name: String,
    mode: PrimaryCheck,
    user_secret: String,
    password_secret: String,
    secrets_dir: Option<PathBuf>,
    pool_config: PoolConfig,
    checkout_timeout: Duration,
    /// Wakes the prober for a round, or in none mode the refresher: see
    /// [`probe`].
    kick: Arc<Notify>,
    /// Where Primary pools had a statement refused because the server is
    /// read-only, since the prober last took them: the endpoints of those
    /// pools, as their errors give them.
    write_refusals: Arc<Mutex<HashSet<Arc<str>>>>,
    shared: Mutex<Shared>,
    /// What the last probe round found, for [`Cluster::status`]. `None` in
    /// none mode and before the first round ends.
    round: Mutex<Option<RoundStatus>>,
    /// The prober, or in none mode the refresher.
    prober: Mutex<Option<JoinHandle<()>>>,
}

/// What the pools of a cluster point at. New pools start from it.
struct Shared {
    credentials: Credentials,
    /// Where the Primary pools point, or why they are unavailable.
    primary: Result<PoolEndpoint, String>,
    /// Where the Read pools point, or why they are unavailable.
    read: Result<PoolEndpoint, String>,
    /// Times `primary` was repointed or made unavailable.
    primary_generation: u64,
    pools: HashMap<(String, Role), OssPool>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if let Some(prober) = self
            .prober
            .get_mut()
            .expect("MySQL cluster lock poisoned")
            .take()
        {
            prober.abort();
        }
    }
}

impl Cluster {
    /// Resolves the secrets, runs the first probe round, and starts the
    /// prober, or in none mode the refresher. Fails only on configuration
    /// errors: the pools are unavailable, not an error, while no primary is
    /// found.
    async fn create(
        target: &OssMysqlTarget,
        settings: &OssFailoverSettings,
        pool_config: &PoolConfig,
        transport: Arc<dyn ProbeTransport>,
    ) -> Result<Arc<Self>> {
        if target.endpoints.is_empty() {
            bail!(
                "no MySQL servers configured for database {}",
                target.database
            );
        }
        let credentials = Credentials::resolve(
            &target.user_secret,
            &target.password_secret,
            settings.secrets_dir.as_deref(),
        )?;
        let now = Instant::now();
        let candidates = CandidateList::new(
            target.hosts_file.clone(),
            target.default_port,
            target
                .endpoints
                .iter()
                .map(|(host, port)| Candidate::new(host.as_str(), *port))
                .collect(),
            now,
        );
        let mode = settings.primary_check;
        let count = candidates.candidates().len();
        if mode != PrimaryCheck::Identity && count != 1 {
            bail!(
                "--oss-mysql-primary-check {mode} needs exactly one MySQL server, \
                 but {count} are listed"
            );
        }
        let name = match candidates.hosts_file() {
            Some(path) => path.display().to_string(),
            None => candidate_list(candidates.candidates()),
        };
        let unprobed = |what: &str| Err(format!("no {what} found yet"));
        let (primary, read) = match mode {
            // Without a prober, the single candidate is used as it is. The
            // refresher follows changes of the hosts file and the secrets.
            PrimaryCheck::None => {
                let endpoint = PoolEndpoint {
                    candidate: candidates.candidates()[0].clone(),
                    identity: None,
                };
                (Ok(endpoint.clone()), Ok(endpoint))
            }
            PrimaryCheck::Identity | PrimaryCheck::Writable => {
                (unprobed("primary"), unprobed("server to read from"))
            }
        };
        let cluster = Arc::new(Cluster {
            id: status::next_cluster_id(),
            key: ClusterKey::of(target),
            name,
            mode,
            user_secret: target.user_secret.clone(),
            password_secret: target.password_secret.clone(),
            secrets_dir: settings.secrets_dir.clone(),
            pool_config: *pool_config,
            checkout_timeout: settings.checkout_timeout,
            kick: Arc::new(Notify::new()),
            write_refusals: Arc::new(Mutex::new(HashSet::new())),
            shared: Mutex::new(Shared {
                credentials,
                primary,
                read,
                primary_generation: 0,
                pools: HashMap::new(),
            }),
            round: Mutex::new(None),
            prober: Mutex::new(None),
        });
        status::register(&cluster);
        if mode == PrimaryCheck::None {
            let refresher = Refresher::new(
                Arc::downgrade(&cluster),
                candidates,
                settings.probe_interval,
            );
            let handle = tokio::spawn(refresher.run(cluster.kick.clone()));
            *cluster.prober.lock().expect("MySQL cluster lock poisoned") = Some(handle);
            return Ok(cluster);
        }

        let probe_timeout =
            Duration::from_millis(pool_config.conn_open_timeout_ms) + PROBE_QUERY_TIME;
        let mut prober = Prober::new(
            Arc::downgrade(&cluster),
            transport,
            candidates,
            probe_timeout,
            settings.probe_interval,
        );
        let start = Instant::now();
        prober.round(&cluster).await;
        let handle = tokio::spawn(prober.run(cluster.kick.clone(), start));
        *cluster.prober.lock().expect("MySQL cluster lock poisoned") = Some(handle);
        Ok(cluster)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn mode(&self) -> PrimaryCheck {
        self.mode
    }

    /// The credentials in use.
    pub fn credentials(&self) -> Credentials {
        self.lock().credentials.clone()
    }

    /// Resolves the secrets again. Returns the new credentials if they
    /// changed; on failure, warns and keeps the ones in use.
    pub fn resolve_credentials(&self) -> Option<Credentials> {
        self.changed_credentials().unwrap_or_else(|err| {
            tracing::warn!(
                "MySQL cluster {}: keeping the current credentials: {err:#}",
                self.name
            );
            None
        })
    }

    /// Resolves the secrets again, and returns the new credentials if they
    /// changed.
    pub fn changed_credentials(&self) -> Result<Option<Credentials>> {
        let credentials = Credentials::resolve(
            &self.user_secret,
            &self.password_secret,
            self.secrets_dir.as_deref(),
        )?;
        if credentials == self.credentials() {
            return Ok(None);
        }
        tracing::info!("MySQL cluster {}: the credentials changed", self.name);
        Ok(Some(credentials))
    }

    /// Takes the endpoints of the Primary pools that had a statement refused
    /// because the server is read-only, since this was last called.
    pub(crate) fn take_write_refusals(&self) -> HashSet<Arc<str>> {
        std::mem::take(
            &mut *self
                .write_refusals
                .lock()
                .expect("MySQL cluster lock poisoned"),
        )
    }

    /// Warns when the hosts file lists a number of servers the mode does
    /// not support.
    pub fn check_candidate_count(&self, count: usize) {
        if self.mode != PrimaryCheck::Identity && count != 1 {
            tracing::warn!(
                "MySQL cluster {}: --oss-mysql-primary-check {} needs exactly one MySQL \
                 server, but the hosts file lists {count}",
                self.name,
                self.mode
            );
        }
    }

    /// The pool of `database` with the given role, created from the current
    /// targets if it is new.
    pub fn pool(&self, database: &str, role: Role) -> OssPool {
        let mut shared = self.lock();
        if let Some(pool) = shared.pools.get(&(database.to_string(), role)) {
            return pool.clone();
        }
        let target = match role {
            Role::Primary => &shared.primary,
            Role::Read => &shared.read,
        };
        let initial = match target {
            Ok(endpoint) => {
                let generation = 0;
                OssPoolState::Active {
                    generation,
                    pool: self.mysql_pool(
                        &shared.credentials,
                        database,
                        role,
                        endpoint,
                        generation,
                    ),
                    endpoint: pool_label(endpoint, database).into(),
                }
            }
            Err(reason) => OssPoolState::Unavailable {
                generation: 0,
                reason: reason.as_str().into(),
            },
        };
        let mut builder = OssPool::builder(initial);
        if let Some(timeout) = nonzero(self.checkout_timeout) {
            builder = builder.checkout_timeout(timeout);
        }
        if let Some(timeout) = nonzero(self.pool_config.query_time_limit) {
            builder = builder.query_timeout(timeout);
        }
        let kick = self.kick.clone();
        if self.mode == PrimaryCheck::None {
            // The refresher only needs to know about rejected credentials.
            let hook: OssErrorHook = Arc::new(move |err| {
                if rejects_credentials(err) {
                    kick.notify_one();
                }
            });
            builder = builder.on_error(hook);
        } else {
            let on_error: OssErrorHook = {
                let kick = kick.clone();
                let write_refusals = (role == Role::Primary).then(|| self.write_refusals.clone());
                Arc::new(move |err| {
                    if let Some(write_refusals) = &write_refusals
                        && refuses_writes(err)
                    {
                        write_refusals
                            .lock()
                            .expect("MySQL cluster lock poisoned")
                            .insert(err.endpoint.clone());
                    }
                    if should_kick(err) {
                        kick.notify_one();
                    }
                })
            };
            // A caller that waits for a primary returns no error before its
            // checkout timeout, if ever: look for one now.
            let on_wait: OssWaitHook = Arc::new(move |_: u64, _: &str| kick.notify_one());
            builder = builder.on_error(on_error).on_wait(on_wait);
        }
        let pool = builder.build();
        shared
            .pools
            .insert((database.to_string(), role), pool.clone());
        pool
    }

    /// Applies the actions of a round to the pools. `credentials` are new
    /// credentials, if they changed: pools that point at a server are then
    /// rebuilt with them. `unavailable_reason` is why the Primary pools are
    /// unavailable after the round, if they are, for pools created later.
    pub(crate) fn apply(
        &self,
        actions: &[Action],
        credentials: Option<Credentials>,
        unavailable_reason: Option<String>,
    ) {
        let mut shared = self.lock();
        let shared = &mut *shared;
        let rebuild = credentials.is_some();
        if let Some(credentials) = credentials {
            shared.credentials = credentials;
        }
        let mut primary_changed = false;
        let mut read_changed = false;
        for action in actions {
            match action {
                Action::SwitchPrimary(target) => {
                    shared.primary = Ok(self.endpoint(target));
                    primary_changed = true;
                }
                Action::ReplaceRead(target) => {
                    shared.read = Ok(self.endpoint(target));
                    read_changed = true;
                }
                Action::MarkPrimaryUnavailable(reason) => {
                    shared.primary = Err(reason.clone());
                    primary_changed = true;
                }
            }
        }
        if primary_changed {
            shared.primary_generation += 1;
        }
        self.update_pools(shared, primary_changed, read_changed, rebuild);
        // A reason given by an action is more precise.
        if !primary_changed
            && let (Some(reason), Err(current)) = (unavailable_reason, &mut shared.primary)
        {
            *current = reason;
        }
    }

    /// Without a prober (none mode): points every pool at `candidate`, if it
    /// is given and new, and rebuilds them with `credentials`, if given.
    pub(crate) fn retarget(&self, candidate: Option<Candidate>, credentials: Option<Credentials>) {
        let mut shared = self.lock();
        let shared = &mut *shared;
        let rebuild = credentials.is_some();
        if let Some(credentials) = credentials {
            shared.credentials = credentials;
        }
        let mut changed = false;
        if let Some(candidate) = candidate {
            let endpoint = PoolEndpoint {
                candidate,
                identity: None,
            };
            if shared.primary.as_ref().ok() != Some(&endpoint) {
                tracing::info!(
                    "MySQL cluster {}: the pools now connect to {}",
                    self.name,
                    endpoint.candidate
                );
                shared.primary = Ok(endpoint.clone());
                shared.read = Ok(endpoint);
                shared.primary_generation += 1;
                changed = true;
            }
        }
        self.update_pools(shared, changed, changed, rebuild);
    }

    /// Points the pools whose target changed at it, and rebuilds the others
    /// with the current credentials if `rebuild`.
    fn update_pools(
        &self,
        shared: &Shared,
        primary_changed: bool,
        read_changed: bool,
        rebuild: bool,
    ) {
        for ((database, role), pool) in &shared.pools {
            let (target, changed) = match role {
                Role::Primary => (&shared.primary, primary_changed),
                Role::Read => (&shared.read, read_changed),
            };
            match target {
                Ok(endpoint) if changed || rebuild => {
                    let generation = pool.generation() + 1;
                    let mysql_pool =
                        self.mysql_pool(&shared.credentials, database, *role, endpoint, generation);
                    pool.replace(mysql_pool, pool_label(endpoint, database));
                }
                Err(reason) if changed => {
                    pool.mark_unavailable(reason.as_str());
                }
                Ok(_) | Err(_) => {}
            }
        }
    }

    /// Where pools for `target` connect.
    fn endpoint(&self, target: &Target) -> PoolEndpoint {
        PoolEndpoint {
            candidate: target.candidate.clone(),
            identity: (self.mode == PrimaryCheck::Identity).then(|| target.identity.clone()),
        }
    }

    fn mysql_pool(
        &self,
        credentials: &Credentials,
        database: &str,
        role: Role,
        endpoint: &PoolEndpoint,
        generation: u64,
    ) -> Pool {
        Pool::new(self.mysql_opts(credentials, database, role, endpoint, generation))
    }

    /// The options of a pool of `role` for `database`, connecting to
    /// `endpoint`, in its `generation`.
    fn mysql_opts(
        &self,
        credentials: &Credentials,
        database: &str,
        role: Role,
        endpoint: &PoolEndpoint,
        generation: u64,
    ) -> Opts {
        let config = &self.pool_config;
        // The server stops the SELECTs the client gave up on. In none mode
        // the pools set nothing more than before, for SQL-aware proxies.
        let select_time_limit = match self.mode {
            PrimaryCheck::Identity | PrimaryCheck::Writable => {
                nonzero(config.query_time_limit).map(|limit| limit + SERVER_TIME_LIMIT_MARGIN)
            }
            PrimaryCheck::None => None,
        };
        let setup = match role {
            Role::Primary => {
                let fence = match (self.mode, &endpoint.identity) {
                    (PrimaryCheck::Identity, Some(identity)) => PrimaryFence::Identity(identity),
                    (PrimaryCheck::Identity | PrimaryCheck::Writable, _) => PrimaryFence::Writable,
                    (PrimaryCheck::None, _) => PrimaryFence::None,
                };
                primary_pool_setup(fence, generation, select_time_limit)
            }
            Role::Read => read_pool_setup(select_time_limit),
        };
        let max = usize::try_from(config.per_key_limit)
            .unwrap_or(usize::MAX)
            .max(1);
        let constraints = PoolConstraints::new(MIN_IDLE_CONNECTIONS.min(max), max)
            .expect("the minimum is at most the maximum");
        let age = nonzero(Duration::from_millis(config.conn_age_timeout_ms));
        let pool_opts = PoolOpts::default()
            .with_constraints(constraints)
            .with_inactive_connection_ttl(Duration::from_millis(config.conn_idle_timeout_ms))
            .with_ttl_check_interval(TTL_CHECK_INTERVAL)
            .with_abs_conn_ttl(age)
            .with_abs_conn_ttl_jitter(age.map(|age| age / 10))
            // Mononoke sets no session state, and this keeps setup statements
            // from running again on every checkout.
            .with_reset_connection(false);
        OptsBuilder::default()
            .ip_or_hostname(endpoint.candidate.host.clone())
            .tcp_port(endpoint.candidate.port)
            .user(Some(credentials.user.expose()))
            .pass(Some(credentials.password.expose()))
            .db_name(Some(database))
            // Never switch to the server's local socket: it may bypass a proxy,
            // or reach another server on the same host.
            .prefer_socket(false)
            .tcp_keepalive(Some(TCP_KEEPALIVE_MS))
            .setup(setup)
            .pool_opts(pool_opts)
            .into()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Shared> {
        self.shared.lock().expect("MySQL cluster lock poisoned")
    }
}

/// How pool errors name where a pool connects.
fn pool_label(endpoint: &PoolEndpoint, database: &str) -> String {
    format!("{}/{database}", endpoint.candidate)
}

/// Whether `label`, where a pool error says the pool connects, is
/// `candidate`.
fn pool_connects_to(label: &str, candidate: &Candidate) -> bool {
    label
        .strip_prefix(candidate.to_string().as_str())
        .is_some_and(|database| database.starts_with('/'))
}

fn candidate_list(candidates: &[Candidate]) -> String {
    candidates
        .iter()
        .map(|c| c.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn nonzero(duration: Duration) -> Option<Duration> {
    (!duration.is_zero()).then_some(duration)
}

/// Whether a pool error is the server rejecting the user or password.
fn rejects_credentials(err: &OssMysqlError) -> bool {
    matches!(
        &err.kind,
        OssErrorKind::Mysql(MysqlError::Server(err)) if err.code == ER_ACCESS_DENIED_ERROR
    )
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::SystemTime;

    use mononoke_macros::mononoke;
    use sql_common::mysql::oss_transaction_statement_timeout;

    use super::fake::*;
    use super::fake_mysqld::FakeMysqld;
    use super::fake_mysqld::Reply;
    use super::*;
    use crate::oss::SharedConnectionPool;
    use crate::oss::set_oss_transaction_time_limit;

    const DB: &str = "mononoke";

    fn mysql_options(
        registry: OssMysqlRegistry,
        read_connection_type: ReadConnectionType,
    ) -> MysqlOptions {
        MysqlOptions {
            pool: SharedConnectionPool(Arc::new(registry)),
            pool_config: PoolConfig::default(),
            read_connection_type,
        }
    }

    fn registry_of(options: &MysqlOptions) -> &OssMysqlRegistry {
        &options.pool.0
    }

    fn oss_pool(connection: &Connection) -> OssPool {
        match &connection.inner {
            sql::Connection::OssMysql(conn) => conn.pool.clone(),
            other => panic!("not an OSS MySQL connection: {other:?}"),
        }
    }

    fn pool_count(cluster: &Cluster) -> usize {
        cluster.lock().pools.len()
    }

    /// Writes a file that only its owner can read, with the given
    /// modification time in seconds since the epoch, if any.
    fn write_file(path: &Path, contents: &str, modified: Option<u64>) -> Result<()> {
        std::fs::write(path, contents)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        if let Some(modified) = modified {
            std::fs::File::options()
                .write(true)
                .open(path)?
                .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(modified))?;
        }
        Ok(())
    }

    #[mononoke::test]
    async fn test_primary_pools_report_write_refusals() -> Result<()> {
        // A demoted server: it refuses every write.
        let server = FakeMysqld::start(|query: &str| {
            if query.starts_with("INSERT") {
                Reply::Error(1290)
            } else {
                Reply::Ok
            }
        })
        .await?;
        let port = server.addr().port();
        let fake = FakeTransport::new(&[(("127.0.0.1", port), Server::Writable)]);
        let registry = registry(&fake, settings(Duration::from_secs(3_600)));
        let cluster = registry
            .cluster(&target(&[("127.0.0.1", port)], DB), &PoolConfig::default())
            .await?;
        let stats = Arc::new(ConnectionStats::new("refusals".to_string()));
        for role in [Role::Read, Role::Primary] {
            let connection = OssConnection::new(cluster.pool(DB, role), stats.clone());
            let err = match connection
                .write_query("INSERT INTO t VALUES (1)".to_string())
                .await
            {
                Ok(_) => panic!("the server should refuse the write"),
                Err(err) => err,
            };
            assert!(format!("{err:#}").contains("1290"), "{err:#}");
        }
        // Only the Primary pool's refusal counts, and it is taken once.
        assert_eq!(
            cluster.take_write_refusals(),
            HashSet::from([Arc::from(format!("127.0.0.1:{port}/{DB}"))])
        );
        assert!(cluster.take_write_refusals().is_empty());
        Ok(())
    }

    #[mononoke::test]
    fn test_pool_connects_to() {
        assert!(pool_connects_to(
            "127.0.0.1:13306/mononoke",
            &Candidate::new("127.0.0.1", 13306)
        ));
        assert!(pool_connects_to(
            "[::1]:3306/db",
            &Candidate::new("::1", 3306)
        ));
        assert!(!pool_connects_to(
            "127.0.0.1:133060/mononoke",
            &Candidate::new("127.0.0.1", 13306)
        ));
        assert!(!pool_connects_to(
            "127.0.0.1:13307/mononoke",
            &Candidate::new("127.0.0.1", 13306)
        ));
        assert!(!pool_connects_to(
            "<none>",
            &Candidate::new("127.0.0.1", 13306)
        ));
    }

    #[mononoke::test]
    fn test_transaction_time_limit() {
        set_oss_transaction_time_limit(Duration::ZERO);
        assert_eq!(oss_transaction_statement_timeout(), None);
        set_oss_transaction_time_limit(Duration::from_secs(10));
        assert_eq!(
            oss_transaction_statement_timeout(),
            Some(Duration::from_secs(10))
        );
    }

    #[mononoke::test]
    async fn test_same_target_shares_pools_and_prober() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable), (B, Server::ReadOnly)]);
        let options = mysql_options(
            registry(&fake, settings(Duration::from_secs(1))),
            ReadConnectionType::ReplicaOnly,
        );
        let target = target(&[A, B], DB);
        let first = create_oss_mysql_connections(&target, &options, "bookmarks", false).await?;
        // One inline round, which probed both candidates.
        assert_eq!(fake.probes().len(), 2);
        let second = create_oss_mysql_connections(&target, &options, "commit_graph", false).await?;
        assert_eq!(fake.probes().len(), 2);

        let registry = registry_of(&options);
        let pool_config = PoolConfig::default();
        let cluster = registry.cluster(&target, &pool_config).await?;
        assert!(Arc::ptr_eq(
            &cluster,
            &registry.cluster(&target, &pool_config).await?
        ));
        assert_eq!(pool_count(&cluster), 2);

        // One prober: a round per second, each probing both candidates.
        tokio::time::sleep(Duration::from_millis(5_500)).await;
        assert_eq!(fake.probes().len(), 2 * 6);

        // Other secret references make another cluster, with its own round.
        let other = OssMysqlTarget {
            user_secret: "literal:admin".to_string(),
            ..target.clone()
        };
        let probes = fake.probes().len();
        let _third = create_oss_mysql_connections(&other, &options, "bookmarks", false).await?;
        assert_eq!(fake.probes().len(), probes + 2);
        assert!(!Arc::ptr_eq(
            &cluster,
            &registry.cluster(&other, &pool_config).await?
        ));
        assert_eq!(fake.probes().len(), probes + 2);

        assert!(same_pool(
            &oss_pool(&first.write_connection),
            &oss_pool(&second.write_connection)
        ));
        assert!(same_pool(
            &oss_pool(&first.read_connection),
            &oss_pool(&second.read_connection)
        ));
        Ok(())
    }

    #[mononoke::test]
    async fn test_dropping_the_stores_stops_the_prober() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable), (B, Server::ReadOnly)]);
        let options = mysql_options(
            registry(&fake, settings(Duration::from_secs(1))),
            ReadConnectionType::ReplicaOnly,
        );
        let target = target(&[A, B], DB);
        let first = create_oss_mysql_connections(&target, &options, "bookmarks", false).await?;
        let second = create_oss_mysql_connections(&target, &options, "commit_graph", false).await?;
        let registry = registry_of(&options);
        let cluster = Arc::downgrade(&registry.cluster(&target, &PoolConfig::default()).await?);
        tokio::time::sleep(Duration::from_millis(2_500)).await;
        assert_eq!(fake.rounds().len(), 3);

        // One store left: the cluster lives on.
        drop(first);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(fake.rounds().len(), 5);
        assert!(cluster.upgrade().is_some());

        // No store left: the cluster is dropped, and its prober stops.
        drop(second);
        assert!(cluster.upgrade().is_none());
        let probes = fake.probes().len();
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(fake.probes().len(), probes);

        // The next store gets a new cluster, which probes again.
        let _third = create_oss_mysql_connections(&target, &options, "bookmarks", false).await?;
        assert_eq!(fake.probes().len(), probes + 2);
        assert_eq!(
            registry
                .clusters
                .lock()
                .unwrap()
                .values()
                .filter(|cell| cell.get().is_some_and(|c| c.strong_count() > 0))
                .count(),
            1
        );
        Ok(())
    }

    #[mononoke::test]
    async fn test_dead_clusters_leave_the_registry() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable)]);
        let registry = registry(&fake, settings(Duration::from_secs(1)));
        for user in ["a", "b", "c"] {
            let target = OssMysqlTarget {
                user_secret: format!("literal:{user}"),
                ..target(&[A], DB)
            };
            drop(registry.cluster(&target, &PoolConfig::default()).await?);
        }
        // Each lookup removes the clusters that died before it.
        assert_eq!(registry.clusters.lock().unwrap().len(), 1);
        let cluster = registry
            .cluster(&target(&[A], DB), &PoolConfig::default())
            .await?;
        assert_eq!(registry.clusters.lock().unwrap().len(), 1);
        drop(cluster);
        Ok(())
    }

    #[mononoke::test]
    async fn test_server_order_and_case_do_not_matter() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable), (B, Server::ReadOnly)]);
        let registry = registry(&fake, settings(Duration::from_secs(1)));
        let pool_config = PoolConfig::default();
        let cluster = registry.cluster(&target(&[A, B], DB), &pool_config).await?;
        let reordered = registry
            .cluster(&target(&[B, A, B], DB), &pool_config)
            .await?;
        assert!(Arc::ptr_eq(&cluster, &reordered));
        let key = |endpoints: &[(&str, u16)]| {
            ClusterKey::of(&OssMysqlTarget {
                endpoints: endpoints
                    .iter()
                    .map(|(host, port)| (host.to_string(), *port))
                    .collect(),
                ..target(&[], DB)
            })
        };
        assert_eq!(
            key(&[("DB1.example.com", 3306), ("db2", 3306)]),
            key(&[("db2", 3306), ("db1.EXAMPLE.com", 3306)])
        );
        assert_ne!(key(&[("db1", 3306)]), key(&[("db1", 3307)]));
        // One prober for both.
        assert_eq!(fake.probes().len(), 2);
        Ok(())
    }

    #[mononoke::test]
    fn test_debug_hides_literal_secrets() {
        let target = OssMysqlTarget {
            user_secret: "literal:mononoke".to_string(),
            password_secret: "literal:s3cret".to_string(),
            ..target(&[A], DB)
        };
        for debug in [
            format!("{target:?}"),
            format!("{:?}", ClusterKey::of(&target)),
        ] {
            assert!(!debug.contains("s3cret"), "{debug}");
            assert!(!debug.contains("literal:mononoke"), "{debug}");
            assert!(
                debug.contains(r#"password_secret: "literal:<redacted>""#),
                "{debug}"
            );
        }
        let target = OssMysqlTarget {
            user_secret: "env:MYSQL_USER".to_string(),
            password_secret: "file:/etc/mononoke/mysql.pw".to_string(),
            ..target
        };
        let debug = format!("{target:?}");
        assert!(
            debug.contains(r#"user_secret: "env:MYSQL_USER""#),
            "{debug}"
        );
        assert!(debug.contains("file:/etc/mononoke/mysql.pw"), "{debug}");
        assert!(debug.contains("127.0.0.1"), "{debug}");
        // A bare name or an unknown form may be a password without
        // literal:.
        let target = OssMysqlTarget {
            user_secret: "hunter2".to_string(),
            password_secret: "hunter:3".to_string(),
            ..target
        };
        for debug in [
            format!("{target:?}"),
            format!("{:?}", ClusterKey::of(&target)),
        ] {
            assert!(!debug.contains("hunter"), "{debug}");
        }
    }

    #[mononoke::test]
    async fn test_none_mode_follows_the_hosts_file_and_secrets() -> Result<()> {
        tokio::time::pause();
        let dir = tempfile::tempdir()?;
        let hosts = dir.path().join("hosts");
        let password_file = dir.path().join("password");
        write_file(&hosts, "127.0.0.1:13306\n", Some(1_000))?;
        write_file(&password_file, PASSWORD, None)?;
        let fake = FakeTransport::new(&[]);
        let none = OssFailoverSettings {
            primary_check: PrimaryCheck::None,
            ..settings(Duration::from_secs(1))
        };
        let options = mysql_options(registry(&fake, none), ReadConnectionType::ReplicaOnly);
        let target = OssMysqlTarget {
            password_secret: format!("file:{}", password_file.display()),
            ..hosts_file_target(hosts.clone(), &[C])
        };
        let connections = create_oss_mysql_connections(&target, &options, "a", false).await?;
        let primary = oss_pool(&connections.write_connection);
        let read = oss_pool(&connections.read_connection);
        for pool in [&primary, &read] {
            assert_eq!(endpoint(pool).as_deref(), Some("127.0.0.1:13306/mononoke"));
        }

        // The proxy moves: the pools follow within a probe interval.
        write_file(&hosts, "127.0.0.1:13307\n", Some(2_000))?;
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        for pool in [&primary, &read] {
            assert_eq!(endpoint(pool).as_deref(), Some("127.0.0.1:13307/mononoke"));
            assert_eq!(pool.generation(), 1);
        }
        // Two servers are not followed in none mode.
        write_file(&hosts, "127.0.0.1:13306\n127.0.0.1:13308\n", Some(3_000))?;
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        assert_eq!(
            endpoint(&primary).as_deref(),
            Some("127.0.0.1:13307/mononoke")
        );
        assert_eq!(primary.generation(), 1);

        // The password is rotated: the pools are rebuilt with it.
        write_file(&password_file, "n3w-s3cret\n", None)?;
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        let cluster = registry_of(&options)
            .cluster(&target, &PoolConfig::default())
            .await?;
        assert_eq!(cluster.credentials().password.expose(), "n3w-s3cret");
        for pool in [&primary, &read] {
            assert_eq!(pool.generation(), 2);
        }
        // Nothing changes when nothing changed.
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(primary.generation(), 2);
        // And there is still no probe.
        assert!(fake.probes().is_empty());
        Ok(())
    }

    #[mononoke::test]
    async fn test_none_mode_rejected_credentials_resolve_secrets_again() -> Result<()> {
        // A server that rejects every connection with 1045, as after a
        // password rotation.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let server = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                use tokio::io::AsyncWriteExt;
                let mut payload = vec![0xff];
                payload.extend_from_slice(&ER_ACCESS_DENIED_ERROR.to_le_bytes());
                payload.extend_from_slice(b"#28000Access denied");
                let mut packet = u32::try_from(payload.len())?.to_le_bytes()[..3].to_vec();
                packet.push(0);
                packet.extend_from_slice(&payload);
                stream.write_all(&packet).await?;
            }
            anyhow::Ok(())
        });
        let dir = tempfile::tempdir()?;
        let password_file = dir.path().join("password");
        write_file(&password_file, PASSWORD, None)?;
        // No refresh on an interval: only the rejection starts one.
        let none = OssFailoverSettings {
            primary_check: PrimaryCheck::None,
            ..settings(Duration::ZERO)
        };
        let fake = FakeTransport::new(&[]);
        let options = mysql_options(registry(&fake, none), ReadConnectionType::ReplicaOnly);
        let target = OssMysqlTarget {
            password_secret: format!("file:{}", password_file.display()),
            ..target(&[("127.0.0.1", port)], DB)
        };
        let connections = create_oss_mysql_connections(&target, &options, "a", false).await?;
        let primary = oss_pool(&connections.write_connection);
        write_file(&password_file, "n3w-s3cret", None)?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(primary.generation(), 0);

        let err = primary.get_conn().await.expect_err("the server rejects us");
        assert!(rejects_credentials(&err), "{err:?}");
        let started = Instant::now();
        while primary.generation() == 0 && started.elapsed() < Duration::from_secs(10) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(primary.generation(), 1);
        let cluster = registry_of(&options)
            .cluster(&target, &PoolConfig::default())
            .await?;
        assert_eq!(cluster.credentials().password.expose(), "n3w-s3cret");
        server.abort();
        Ok(())
    }

    #[mononoke::test]
    fn test_rejects_credentials() {
        let error = |kind| OssMysqlError {
            phase: sql_common::mysql::OssPhase::Checkout,
            stmt: sql_common::mysql::OssStmt::Unknown,
            endpoint: Arc::from("127.0.0.1:13306/mononoke"),
            generation: 0,
            kind,
        };
        let server = |code| {
            OssErrorKind::Mysql(MysqlError::Server(sql::mysql_async::ServerError {
                code,
                message: "fake".to_string(),
                state: "28000".to_string(),
            }))
        };
        assert!(rejects_credentials(&error(server(1045))));
        assert!(!rejects_credentials(&error(server(1040))));
        assert!(!rejects_credentials(&error(OssErrorKind::Timeout(
            Duration::from_secs(1)
        ))));
    }

    #[mononoke::test]
    async fn test_hosts_file_path_is_the_cluster_key() -> Result<()> {
        tokio::time::pause();
        let dir = tempfile::tempdir()?;
        let hosts = dir.path().join("hosts");
        std::fs::write(&hosts, "127.0.0.1:13306\n")?;
        let fake = FakeTransport::new(&[(A, Server::Writable)]);
        let registry = registry(&fake, settings(Duration::from_secs(1)));
        let pool_config = PoolConfig::default();
        // The fallback servers differ, but the hosts file is the same.
        let cluster = registry
            .cluster(&hosts_file_target(hosts.clone(), &[B]), &pool_config)
            .await?;
        let same = registry
            .cluster(&hosts_file_target(hosts.clone(), &[C]), &pool_config)
            .await?;
        assert!(Arc::ptr_eq(&cluster, &same));
        assert_eq!(cluster.name(), hosts.display().to_string());
        // The candidates are the file's.
        assert!(
            fake.probes()
                .iter()
                .all(|p| p.candidate == Candidate::new(A.0, A.1))
        );

        // Another file, or the same servers without a file, is another
        // cluster.
        let other = registry
            .cluster(
                &hosts_file_target(dir.path().join("other-hosts"), &[A]),
                &pool_config,
            )
            .await?;
        assert!(!Arc::ptr_eq(&cluster, &other));
        let plain = registry.cluster(&target(&[A], DB), &pool_config).await?;
        assert!(!Arc::ptr_eq(&cluster, &plain));
        assert!(!Arc::ptr_eq(&other, &plain));
        Ok(())
    }

    #[mononoke::test]
    async fn test_other_database_gets_other_pools() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable)]);
        let options = mysql_options(
            registry(&fake, settings(Duration::from_secs(1))),
            ReadConnectionType::ReplicaOnly,
        );
        let first = create_oss_mysql_connections(&target(&[A], DB), &options, "a", false).await?;
        let other =
            create_oss_mysql_connections(&target(&[A], "other"), &options, "a", false).await?;
        // The same cluster: no other round.
        assert_eq!(fake.probes().len(), 1);
        let cluster = registry_of(&options)
            .cluster(&target(&[A], DB), &PoolConfig::default())
            .await?;
        assert_eq!(pool_count(&cluster), 4);

        let primary = oss_pool(&first.write_connection);
        let other_primary = oss_pool(&other.write_connection);
        assert_eq!(
            endpoint(&primary).as_deref(),
            Some("127.0.0.1:13306/mononoke")
        );
        assert_eq!(
            endpoint(&other_primary).as_deref(),
            Some("127.0.0.1:13306/other")
        );
        assert_eq!(first.write_connection.shard_name, DB);
        assert_eq!(other.write_connection.shard_name, "other");
        assert!(!same_pool(&primary, &other_primary));
        Ok(())
    }

    #[mononoke::test]
    async fn test_role_mapping() -> Result<()> {
        tokio::time::pause();
        // No primary: the Primary pools are unavailable and the Read pools
        // use A, which tells them apart.
        let fake = FakeTransport::new(&[(A, Server::ReadOnly), (B, Server::Down)]);
        let is_primary = |connection: &Connection| endpoint(&oss_pool(connection)).is_none();
        let target = target(&[A, B], DB);

        let options = mysql_options(
            registry(&fake, settings(Duration::from_secs(1))),
            ReadConnectionType::ReplicaOnly,
        );
        let connections = create_oss_mysql_connections(&target, &options, "a", false).await?;
        assert!(is_primary(&connections.write_connection));
        assert!(!is_primary(&connections.read_connection));
        assert!(is_primary(&connections.read_master_connection));
        assert_eq!(
            endpoint(&oss_pool(&connections.read_connection)).as_deref(),
            Some("127.0.0.1:13306/mononoke")
        );
        match oss_pool(&connections.write_connection).state() {
            OssPoolState::Unavailable { reason, .. } => assert_eq!(&*reason, "no writable server"),
            state => panic!("{state:?}"),
        }

        // Read-only: writes go to the Read pool, whose sessions refuse them.
        let readonly = create_oss_mysql_connections(&target, &options, "a", true).await?;
        assert!(!is_primary(&readonly.write_connection));
        assert!(!is_primary(&readonly.read_connection));
        assert!(is_primary(&readonly.read_master_connection));

        // --mysql-master-only: reads go to the Primary pool.
        let master_only = mysql_options(
            registry(&fake, settings(Duration::from_secs(1))),
            ReadConnectionType::Master,
        );
        let connections = create_oss_mysql_connections(&target, &master_only, "a", false).await?;
        assert!(is_primary(&connections.write_connection));
        assert!(is_primary(&connections.read_connection));
        assert!(is_primary(&connections.read_master_connection));
        Ok(())
    }

    #[mononoke::test]
    async fn test_startup_without_primary() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Down), (B, Server::Down)]);
        let short_checkout = OssFailoverSettings {
            checkout_timeout: Duration::from_secs(3),
            ..settings(Duration::from_secs(1))
        };
        let options = mysql_options(
            registry(&fake, short_checkout),
            ReadConnectionType::ReplicaOnly,
        );
        let connections =
            create_oss_mysql_connections(&target(&[A, B], DB), &options, "a", false).await?;
        let primary = oss_pool(&connections.write_connection);
        let read = oss_pool(&connections.read_connection);
        assert_eq!(endpoint(&primary), None);
        assert_eq!(endpoint(&read), None);

        // Checkouts wait for a primary, up to the checkout timeout.
        let started = Instant::now();
        let err = primary.get_conn().await.expect_err("no primary");
        assert_elapsed(started, Duration::from_secs(3));
        assert!(format!("{:#}", anyhow::Error::from(err)).contains("no MySQL server answered"),);

        // B comes up as the primary: the pools follow within a fast round.
        fake.set(B, Server::Writable);
        tokio::time::sleep(Duration::from_millis(300)).await;
        for pool in [&primary, &read] {
            assert_eq!(endpoint(pool).as_deref(), Some("127.0.0.1:13307/mononoke"));
        }
        Ok(())
    }

    #[mononoke::test]
    async fn test_secret_errors() -> Result<()> {
        tokio::time::pause();
        let dir = tempfile::tempdir()?;
        let password_file = dir.path().join("password");
        let fake = FakeTransport::new(&[(A, Server::Writable)]);
        let options = mysql_options(
            registry(&fake, settings(Duration::from_secs(1))),
            ReadConnectionType::ReplicaOnly,
        );
        let target = OssMysqlTarget {
            password_secret: format!("file:{}", password_file.display()),
            ..target(&[A], DB)
        };
        let err = create_oss_mysql_connections(&target, &options, "a", false)
            .await
            .err()
            .expect("the secret is missing");
        assert!(
            format!("{err:#}").contains("cannot resolve the MySQL password secret"),
            "{err:#}"
        );
        assert!(fake.probes().is_empty());

        // The failure is not kept.
        std::fs::write(&password_file, PASSWORD)?;
        create_oss_mysql_connections(&target, &options, "a", false).await?;
        assert_eq!(fake.probes().len(), 1);
        Ok(())
    }

    #[mononoke::test]
    async fn test_modes_with_one_server() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable), (B, Server::ReadOnly)]);
        for mode in [PrimaryCheck::Writable, PrimaryCheck::None] {
            let mode_settings = OssFailoverSettings {
                primary_check: mode,
                ..settings(Duration::from_secs(1))
            };
            let options = mysql_options(
                registry(&fake, mode_settings),
                ReadConnectionType::ReplicaOnly,
            );
            let err = create_oss_mysql_connections(&target(&[A, B], DB), &options, "a", false)
                .await
                .err()
                .expect("two servers");
            assert!(
                format!("{err:#}").contains(&format!(
                    "--oss-mysql-primary-check {mode} needs exactly one MySQL server, \
                     but 2 are listed"
                )),
                "{err:#}"
            );
        }
        assert!(fake.probes().is_empty());

        // None: no prober, and the pools use the server as it is.
        let mode_settings = OssFailoverSettings {
            primary_check: PrimaryCheck::None,
            ..settings(Duration::from_secs(1))
        };
        let options = mysql_options(
            registry(&fake, mode_settings),
            ReadConnectionType::ReplicaOnly,
        );
        let connections =
            create_oss_mysql_connections(&target(&[B], DB), &options, "a", false).await?;
        for connection in [&connections.write_connection, &connections.read_connection] {
            assert_eq!(
                endpoint(&oss_pool(connection)).as_deref(),
                Some("127.0.0.1:13307/mononoke")
            );
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(fake.probes().is_empty());

        // Writable: probed, and the writable server is the primary.
        let mode_settings = OssFailoverSettings {
            primary_check: PrimaryCheck::Writable,
            ..settings(Duration::from_secs(1))
        };
        let options = mysql_options(
            registry(&fake, mode_settings),
            ReadConnectionType::ReplicaOnly,
        );
        let connections =
            create_oss_mysql_connections(&target(&[A], DB), &options, "a", false).await?;
        assert_eq!(
            endpoint(&oss_pool(&connections.write_connection)).as_deref(),
            Some("127.0.0.1:13306/mononoke")
        );
        assert_eq!(fake.probes().len(), 1);
        Ok(())
    }

    #[mononoke::test]
    async fn test_new_pools_start_from_the_current_servers() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable), (B, Server::ReadOnly)]);
        let options = mysql_options(
            registry(&fake, settings(Duration::from_secs(1))),
            ReadConnectionType::ReplicaOnly,
        );
        let db_target = target(&[A, B], DB);
        let first = create_oss_mysql_connections(&db_target, &options, "a", false).await?;
        let primary = oss_pool(&first.write_connection);
        assert_eq!(primary.generation(), 0);

        // Switchover from A to B.
        fake.set(A, Server::ReadOnly);
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        assert_eq!(endpoint(&primary), None);
        fake.set(B, Server::Writable);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            endpoint(&primary).as_deref(),
            Some("127.0.0.1:13307/mononoke")
        );
        assert_eq!(primary.generation(), 2);

        // A pool created now starts on B.
        let later =
            create_oss_mysql_connections(&target(&[A, B], "later"), &options, "a", false).await?;
        let later_primary = oss_pool(&later.write_connection);
        assert_eq!(
            endpoint(&later_primary).as_deref(),
            Some("127.0.0.1:13307/later")
        );
        assert_eq!(later_primary.generation(), 0);
        assert_eq!(
            endpoint(&oss_pool(&later.read_connection)).as_deref(),
            Some("127.0.0.1:13307/later")
        );
        Ok(())
    }

    #[mononoke::test]
    async fn test_mysql_opts() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable)]);
        let registry = registry(&fake, settings(Duration::from_secs(1)));
        let cluster = registry
            .cluster(&target(&[A], DB), &PoolConfig::default())
            .await?;
        let credentials = cluster.credentials();
        let identity = ServerIdentity {
            server_id: 1,
            hostname: "mac".to_string(),
            port: 13306,
        };
        let endpoint = PoolEndpoint {
            candidate: Candidate::new(A.0, A.1),
            identity: Some(identity.clone()),
        };

        let opts = cluster.mysql_opts(&credentials, DB, Role::Primary, &endpoint, 3);
        assert_eq!(opts.ip_or_hostname(), "127.0.0.1");
        assert_eq!(opts.tcp_port(), 13306);
        assert_eq!(opts.user(), Some("mononoke"));
        assert_eq!(opts.pass(), Some(PASSWORD));
        assert_eq!(opts.db_name(), Some(DB));
        assert!(!opts.prefer_socket());
        assert_eq!(opts.tcp_keepalive(), Some(10_000));
        // The fence, and a time limit for SELECTs a second longer than the
        // query time limit, so that the client gives up first.
        let server_limit = Some(Duration::from_secs(11));
        assert_eq!(
            opts.setup(),
            primary_pool_setup(PrimaryFence::Identity(&identity), 3, server_limit)
        );
        assert!(
            opts.setup()[0].ends_with("/*!50708 , SESSION max_execution_time = 11000 */"),
            "{:?}",
            opts.setup()
        );
        let pool_opts = opts.pool_opts();
        assert_eq!(
            pool_opts.constraints(),
            PoolConstraints::new(2, 100).unwrap()
        );
        assert_eq!(pool_opts.inactive_connection_ttl(), Duration::from_secs(4));
        assert_eq!(pool_opts.ttl_check_interval(), Duration::from_secs(1));
        assert_eq!(pool_opts.abs_conn_ttl(), Some(Duration::from_secs(60)));
        assert_eq!(
            pool_opts.abs_conn_ttl_jitter(),
            Some(Duration::from_secs(6))
        );
        assert!(!pool_opts.reset_connection());

        let opts = cluster.mysql_opts(&credentials, DB, Role::Read, &endpoint, 3);
        assert_eq!(opts.setup(), read_pool_setup(server_limit));

        // Small limits, and no query time limit.
        let pool_config = PoolConfig {
            per_key_limit: 1,
            conn_age_timeout_ms: 0,
            query_time_limit: Duration::ZERO,
            ..PoolConfig::default()
        };
        let cluster = registry
            .cluster(
                &OssMysqlTarget {
                    user_secret: "literal:other".to_string(),
                    ..target(&[A], DB)
                },
                &pool_config,
            )
            .await?;
        let opts = cluster.mysql_opts(&credentials, DB, Role::Primary, &endpoint, 0);
        let pool_opts = opts.pool_opts();
        assert_eq!(pool_opts.constraints(), PoolConstraints::new(1, 1).unwrap());
        assert_eq!(pool_opts.abs_conn_ttl(), None);
        assert_eq!(
            opts.setup(),
            primary_pool_setup(PrimaryFence::Identity(&identity), 0, None)
        );
        let opts = cluster.mysql_opts(&credentials, DB, Role::Read, &endpoint, 0);
        assert_eq!(opts.setup(), ["SET SESSION TRANSACTION READ ONLY"]);
        Ok(())
    }

    #[mononoke::test]
    async fn test_fence_per_mode() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable)]);
        let target = OssMysqlTarget {
            endpoints: vec![(A.0.to_string(), A.1)],
            ..target(&[A], DB)
        };
        let server_limit = Some(Duration::from_secs(11));
        // None mode sets nothing more than before, for SQL-aware proxies.
        for (mode, expected, expected_read) in [
            (
                PrimaryCheck::Writable,
                primary_pool_setup(PrimaryFence::Writable, 0, server_limit),
                read_pool_setup(server_limit),
            ),
            (PrimaryCheck::None, Vec::new(), read_pool_setup(None)),
        ] {
            let mode_settings = OssFailoverSettings {
                primary_check: mode,
                ..settings(Duration::from_secs(1))
            };
            let cluster = registry(&fake, mode_settings)
                .cluster(&target, &PoolConfig::default())
                .await?;
            let endpoint = cluster.lock().primary.clone().expect("a primary");
            assert_eq!(endpoint.identity, None, "{mode}");
            let opts = cluster.mysql_opts(&cluster.credentials(), DB, Role::Primary, &endpoint, 0);
            assert_eq!(opts.setup(), expected, "{mode}");
            let opts = cluster.mysql_opts(&cluster.credentials(), DB, Role::Read, &endpoint, 0);
            assert_eq!(opts.setup(), expected_read, "{mode}");
        }
        Ok(())
    }
}
