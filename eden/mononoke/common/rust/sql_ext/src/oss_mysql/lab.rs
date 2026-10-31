/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! The failover lab: scenarios against real MySQL servers, under concurrent
//! load of real `mononoke_queries!` statements.
//!
//! The tests are ignored, and do nothing unless `MONONOKE_OSS_MYSQL_LAB`
//! names the lab directory. It holds `lab.sh`, which runs three Percona
//! servers with GTID replication: a (13306, server_id 1) the primary, b
//! (13307, 2) and c (13305, 3) its replicas. After `lab.sh init`, every
//! scenario starts from `lab.sh reset`, so run them one at a time:
//!
//! ```text
//! MONONOKE_OSS_MYSQL_LAB=$L cargo test --release -p sql_ext lab_ -- \
//!     --ignored --test-threads=1 --nocapture
//! ```
//!
//! A scenario runs two or three registries, each standing for a Mononoke
//! process, changes the servers as an operator or a crash would, and then
//! checks that:
//! - every acknowledged write is on the final primary exactly once, and a
//!   write that failed with an error saying it had no effect is nowhere;
//! - no write ran again after a failure that left its outcome unknown;
//! - read_master never returned less than what was acknowledged before it
//!   started, beyond the staleness bound of the scenario;
//! - the clusters and their probers lived through the whole scenario;
//! - every connection of the mononoke user was TCP.
//!
//! The numbers, timelines and logs go to `$L/results/<scenario>`. Queries
//! run on worker threads, without the JustKnobs override of unit tests, so
//! they retry as in production; their failed attempts are logged through the
//! SQL error telemetry to `scuba.jsonl`.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use fbinit::FacebookInit;
use metadata::Metadata;
use mononoke_types::RepositoryId;
use serde_json::Value;
use serde_json::json;
use sql::mysql_async::Error as MysqlError;
use sql_common::mysql::OssErrorKind;
use sql_common::mysql::OssMysqlError;
use sql_common::mysql::OssPhase;
use sql_common::mysql::OssPool;
use sql_common::mysql::OssPoolState;
use sql_query_telemetry::SqlQueryTelemetry;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::task::AbortHandle;
use tokio::task::JoinHandle;

use super::Cluster;
use super::OssMysqlRegistry;
use super::OssMysqlTarget;
use super::Role;
use super::classify::Disposition;
use super::classify::ErrorScope;
use super::classify::classify;
use super::create_oss_mysql_connections;
use super::decide::PrimaryCheck;
use super::fence::FENCE_MARKER;
use super::fence::is_fence_rejection;
use crate::SqlConnections;
use crate::facebook::MysqlOptions;
use crate::facebook::ReadConnectionType;
use crate::mononoke_queries;
use crate::oss::OssFailoverSettings;
use crate::oss::PoolConfig;
use crate::oss::SharedConnectionPool;
use crate::oss::set_oss_transaction_time_limit;

const LAB_ENV: &str = "MONONOKE_OSS_MYSQL_LAB";
const DB: &str = "mononoke";
const FORWARDER_PORT: u16 = 13300;
/// The servers of the lab: name, port and server_id.
const SERVERS: [(&str, u16, u32); 3] = [("a", 13306, 1), ("b", 13307, 2), ("c", 13305, 3)];
/// The statement time limit, in and out of transactions, as the default
/// `--mysql-query-time-limit`.
const QUERY_TIME_LIMIT: Duration = Duration::from_secs(10);
/// How long the load runs before the first change.
const WARMUP: Duration = Duration::from_secs(4);

mononoke_queries! {
    // `op` is a RepositoryId so that the error telemetry names the operation.
    write LabInsert(uuid: String, seq: i64, op: RepositoryId) {
        none,
        "INSERT INTO lab_writes (uuid, seq, op, server_id, origin) VALUES ({uuid}, {seq}, {op}, @@server_id, 'w')"
    }
    write LabTxnInsert(uuid: String, seq: i64, op: RepositoryId) {
        none,
        "INSERT INTO lab_writes (uuid, seq, op, server_id, origin) VALUES ({uuid}, {seq}, {op}, @@server_id, 't')"
    }
    write LabTxnBump(uuid: String) {
        none,
        "UPDATE lab_txn SET n = n + 1, last_uuid = {uuid} WHERE id = 1"
    }
    read LabMaxSeq() -> (Option<i64>) {
        "SELECT MAX(seq) FROM lab_writes"
    }
    read LabCountSince(seq: i64) -> (i64) {
        "SELECT COUNT(*) FROM lab_writes WHERE seq >= {seq}"
    }
}

// Tracing: the events of the running scenario, prober decisions included.

struct LogSink {
    start: Instant,
    lines: Vec<String>,
}

static LOG_SINK: Mutex<Option<LogSink>> = Mutex::new(None);

/// Keeps INFO and above, and DEBUG of this module's parents (probe
/// failures).
struct LabSubscriber {
    next_span: AtomicU64,
}

impl tracing::Subscriber for LabSubscriber {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() <= tracing::Level::INFO
            || (*metadata.level() <= tracing::Level::DEBUG
                && metadata.target().contains("oss_mysql"))
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut text = String::new();
        event.record(&mut FieldText(&mut text));
        // Without JustKnobs, every telemetry sample warns that it has no
        // sample rate.
        if text.contains("Failed to set Scuba sample rate") {
            return;
        }
        let mut sink = LOG_SINK.lock().expect("lab log lock poisoned");
        if let Some(sink) = sink.as_mut() {
            let line = format!(
                "{:9.3} {:5} {} {}",
                sink.start.elapsed().as_secs_f64(),
                event.metadata().level(),
                event.metadata().target(),
                text
            );
            sink.lines.push(line);
        }
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

struct FieldText<'a>(&'a mut String);

impl tracing::field::Visit for FieldText<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        if field.name() == "message" {
            let _ = write!(self.0, "{value:?}");
        } else {
            let _ = write!(self.0, "{}={value:?}", field.name());
        }
    }
}

fn capture_tracing(start: Instant) {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        let _ = tracing::subscriber::set_global_default(LabSubscriber {
            next_span: AtomicU64::new(0),
        });
    });
    *LOG_SINK.lock().expect("lab log lock poisoned") = Some(LogSink {
        start,
        lines: Vec::new(),
    });
}

fn take_tracing() -> Vec<String> {
    LOG_SINK
        .lock()
        .expect("lab log lock poisoned")
        .take()
        .map_or_else(Vec::new, |sink| sink.lines)
}

// A TCP forwarder, standing for a stable DNS name or a proxy.

struct Forwarder {
    target: Arc<AtomicU16>,
    connections: Arc<Mutex<Vec<AbortHandle>>>,
    accept: JoinHandle<()>,
}

impl Forwarder {
    async fn start(port: u16, target: u16) -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", port))
            .await
            .with_context(|| format!("cannot listen on 127.0.0.1:{port}"))?;
        let target = Arc::new(AtomicU16::new(target));
        let connections: Arc<Mutex<Vec<AbortHandle>>> = Arc::new(Mutex::new(Vec::new()));
        let accept = tokio::spawn({
            let target = target.clone();
            let connections = connections.clone();
            async move {
                while let Ok((mut inbound, _)) = listener.accept().await {
                    let port = target.load(Ordering::SeqCst);
                    let forward = tokio::spawn(async move {
                        if let Ok(mut outbound) = TcpStream::connect(("127.0.0.1", port)).await {
                            let _ =
                                tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                        }
                    });
                    let mut connections = connections.lock().expect("forwarder lock poisoned");
                    connections.retain(|c| !c.is_finished());
                    connections.push(forward.abort_handle());
                }
            }
        });
        Ok(Self {
            target,
            connections,
            accept,
        })
    }

    /// Sends new connections to `port`. With `kill_existing`, also closes
    /// the connections forwarded so far.
    fn retarget(&self, port: u16, kill_existing: bool) {
        self.target.store(port, Ordering::SeqCst);
        if kill_existing {
            for connection in self
                .connections
                .lock()
                .expect("forwarder lock poisoned")
                .drain(..)
            {
                connection.abort();
            }
        }
    }
}

impl Drop for Forwarder {
    fn drop(&mut self) {
        self.accept.abort();
        self.retarget(0, true);
    }
}

// The load.

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum OpKind {
    Write,
    ReadMaster,
    Read,
    Txn,
    Rollback,
    /// I8's transaction, open across a switchover. It does not update the
    /// counter, so that it holds no lock the others wait for.
    TxnAcrossSwitch,
}

#[derive(Clone, Debug)]
struct Failure {
    /// Phase, statement, disposition and cause.
    label: String,
    phase: Option<OssPhase>,
    disposition: Option<Disposition>,
    message: String,
}

impl Failure {
    fn of(err: &anyhow::Error, scope: ErrorScope) -> Self {
        let class = classify(err, scope);
        let cause = err
            .chain()
            .find_map(|e| e.downcast_ref::<OssMysqlError>())
            .map(|e| match &e.kind {
                OssErrorKind::Mysql(e) => mysql_cause(e),
                OssErrorKind::Timeout(_) => "timeout".to_string(),
                OssErrorKind::Unavailable(_) => "unavailable".to_string(),
            })
            .or_else(|| {
                err.chain()
                    .find_map(|e| e.downcast_ref::<MysqlError>())
                    .map(mysql_cause)
            })
            .unwrap_or_else(|| "not MySQL".to_string());
        let label = match class {
            Some(c) => format!("{:?}/{:?} {:?} {cause}", c.phase, c.stmt, c.disposition),
            None => format!("unclassified {cause}"),
        };
        Self {
            label,
            phase: class.map(|c| c.phase),
            disposition: class.map(|c| c.disposition),
            message: format!("{err:#}"),
        }
    }
}

fn mysql_cause(err: &MysqlError) -> String {
    match err {
        MysqlError::Server(e) if is_fence_rejection(e) => format!("server {} fence", e.code),
        MysqlError::Server(e) => format!("server {}", e.code),
        MysqlError::Io(_) => "io".to_string(),
        MysqlError::Driver(e) => {
            let text = format!("{e:?}");
            format!(
                "driver {}",
                text.split(['(', ' ', '{']).next().unwrap_or_default()
            )
        }
        MysqlError::Url(_) => "url".to_string(),
        MysqlError::Other(_) => "other".to_string(),
    }
}

#[derive(Clone, Debug)]
struct Op {
    process: usize,
    kind: OpKind,
    op: i32,
    uuid: String,
    /// The sequence number written, or for ReadMaster the one read.
    seq: i64,
    start: f64,
    end: f64,
    result: Result<i64, Failure>,
    /// Failures of earlier whole-transaction attempts.
    earlier: Vec<Failure>,
}

#[derive(Clone, Copy, Debug)]
struct Load {
    writers: usize,
    master_readers: usize,
    readers: usize,
    txns: usize,
    pause: Duration,
}

impl Default for Load {
    fn default() -> Self {
        Self {
            writers: 4,
            master_readers: 2,
            readers: 1,
            txns: 1,
            pause: Duration::from_millis(20),
        }
    }
}

/// What the tasks of the load share.
struct Recorder {
    start: Instant,
    run: String,
    stop: AtomicBool,
    seq: AtomicI64,
    op: AtomicI32,
    ops: Mutex<Vec<Op>>,
    /// Every change of state of the processes' pools.
    transitions: Mutex<Vec<Transition>>,
}

/// A change of state of a pool: when, process, role, generation, state.
type Transition = (f64, String, Role, u64, String);

impl Recorder {
    fn t(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }

    fn next(&self) -> (i32, i64, String) {
        let op = self.op.fetch_add(1, Ordering::Relaxed);
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        (op, seq, format!("{}-{op}", self.run))
    }

    fn record(&self, op: Op) {
        self.ops.lock().expect("lab ops lock poisoned").push(op);
    }

    fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    /// When the Primary pool of process `name` first became unavailable
    /// for a reason containing `reason` after `since`.
    fn unavailable_since(&self, name: &str, reason: &str, since: f64) -> Option<f64> {
        self.transitions
            .lock()
            .expect("lab transitions lock poisoned")
            .iter()
            .find(|(t, process, role, _, text)| {
                *t >= since
                    && process == name
                    && *role == Role::Primary
                    && text.contains("unavailable")
                    && text.contains(reason)
            })
            .map(|(t, ..)| *t)
    }
}

async fn writer(
    rec: Arc<Recorder>,
    process: usize,
    conns: SqlConnections,
    tel: SqlQueryTelemetry,
    pause: Duration,
) {
    while !rec.stopped() {
        let (op, seq, uuid) = rec.next();
        let start = rec.t();
        let result = LabInsert::query(
            &conns.write_connection,
            tel.clone(),
            &uuid,
            &seq,
            &RepositoryId::new(op),
        )
        .await;
        let end = rec.t();
        rec.record(Op {
            process,
            kind: OpKind::Write,
            op,
            uuid,
            seq,
            start,
            end,
            result: result
                .map(|r| r.affected_rows() as i64)
                .map_err(|e| Failure::of(&e, ErrorScope::Query)),
            earlier: Vec::new(),
        });
        tokio::time::sleep(pause).await;
    }
}

async fn master_reader(
    rec: Arc<Recorder>,
    process: usize,
    conns: SqlConnections,
    tel: SqlQueryTelemetry,
    pause: Duration,
) {
    while !rec.stopped() {
        let op = rec.op.fetch_add(1, Ordering::Relaxed);
        let start = rec.t();
        let result = LabMaxSeq::query(&conns.read_master_connection, tel.clone()).await;
        let end = rec.t();
        let result = result
            .map(|rows| rows.first().and_then(|row| row.0).unwrap_or(0))
            .map_err(|e| Failure::of(&e, ErrorScope::Query));
        rec.record(Op {
            process,
            kind: OpKind::ReadMaster,
            op,
            uuid: String::new(),
            seq: *result.as_ref().unwrap_or(&-1),
            start,
            end,
            result,
            earlier: Vec::new(),
        });
        tokio::time::sleep(pause).await;
    }
}

async fn reader(
    rec: Arc<Recorder>,
    process: usize,
    conns: SqlConnections,
    tel: SqlQueryTelemetry,
    pause: Duration,
) {
    while !rec.stopped() {
        let op = rec.op.fetch_add(1, Ordering::Relaxed);
        let since = rec.seq.load(Ordering::Relaxed) - 50;
        let start = rec.t();
        let result = LabCountSince::query(&conns.read_connection, tel.clone(), &since).await;
        let end = rec.t();
        rec.record(Op {
            process,
            kind: OpKind::Read,
            op,
            uuid: String::new(),
            seq: since,
            start,
            end,
            result: result
                .map(|rows| rows.first().map_or(0, |row| row.0))
                .map_err(|e| Failure::of(&e, ErrorScope::Query)),
            earlier: Vec::new(),
        });
        tokio::time::sleep(pause * 2).await;
    }
}

/// One transaction: an insert, a counter update and a read, then a commit,
/// or a rollback.
async fn transaction(
    conns: &SqlConnections,
    tel: &SqlQueryTelemetry,
    uuid: &String,
    seq: i64,
    op: i32,
    rollback: bool,
) -> Result<()> {
    let txn = conns
        .write_connection
        .start_transaction(tel.clone())
        .await?;
    let (txn, _) =
        LabTxnInsert::query_with_transaction(txn, uuid, &seq, &RepositoryId::new(op)).await?;
    let (txn, _) = LabTxnBump::query_with_transaction(txn, uuid).await?;
    let (txn, _) = LabMaxSeq::query_with_transaction(txn).await?;
    if rollback {
        txn.rollback().await
    } else {
        txn.commit().await
    }
}

/// Runs `attempt` through `sql_ext::retry_transaction`, as the stores do.
/// Returns the final result with the failures of the earlier attempts.
async fn retry_transaction<F, Fut>(mut attempt: F) -> (Result<i64, Failure>, Vec<Failure>)
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let failures = Arc::new(Mutex::new(Vec::new()));
    let result = crate::retry_transaction("lab", |number| {
        let failures = failures.clone();
        let running = attempt(number);
        async move {
            let result = running.await;
            if let Err(err) = &result {
                failures
                    .lock()
                    .expect("lab failures lock poisoned")
                    .push(Failure::of(err, ErrorScope::Transaction));
            }
            result
        }
    })
    .await;
    let mut earlier = std::mem::take(&mut *failures.lock().expect("lab failures lock poisoned"));
    match result {
        Ok(()) => (Ok(1), earlier),
        // The failure of the last attempt is the one returned.
        Err(err) => {
            let last = earlier
                .pop()
                .unwrap_or_else(|| Failure::of(&err, ErrorScope::Transaction));
            (Err(last), earlier)
        }
    }
}

async fn transactor(
    rec: Arc<Recorder>,
    process: usize,
    conns: SqlConnections,
    tel: SqlQueryTelemetry,
    pause: Duration,
) {
    while !rec.stopped() {
        let (op, seq, uuid) = rec.next();
        // Every fifth transaction rolls back: a ROLLBACK must not hang on a
        // server that stopped answering either.
        let rollback = op % 5 == 0;
        let start = rec.t();
        let (result, earlier) =
            retry_transaction(|_| transaction(&conns, &tel, &uuid, seq, op, rollback)).await;
        let end = rec.t();
        rec.record(Op {
            process,
            kind: if rollback {
                OpKind::Rollback
            } else {
                OpKind::Txn
            },
            op,
            uuid,
            seq,
            start,
            end,
            result,
            earlier,
        });
        tokio::time::sleep(pause * 5).await;
    }
}

// Processes, the lab and its scenarios.

/// A registry with its stores' connections: one Mononoke process.
struct Process {
    name: String,
    options: MysqlOptions,
    target: OssMysqlTarget,
    conns: SqlConnections,
    cluster: Arc<Cluster>,
    primary: OssPool,
    read: OssPool,
}

impl Process {
    async fn new(name: &str, target: &OssMysqlTarget, pool_config: PoolConfig) -> Result<Self> {
        let settings = OssFailoverSettings {
            probe_interval: Duration::from_secs(1),
            primary_check: PrimaryCheck::Identity,
            secrets_dir: None,
            checkout_timeout: Duration::from_secs(10),
        };
        let options = MysqlOptions {
            pool: SharedConnectionPool(Arc::new(OssMysqlRegistry::new(settings))),
            pool_config,
            read_connection_type: ReadConnectionType::ReplicaOnly,
        };
        let conns = create_oss_mysql_connections(target, &options, "lab", false).await?;
        let cluster = options.pool.0.cluster(target, &pool_config).await?;
        Ok(Self {
            name: name.to_string(),
            options,
            target: target.clone(),
            conns,
            primary: cluster.pool(DB, Role::Primary),
            read: cluster.pool(DB, Role::Read),
            cluster,
        })
    }

    /// Whether the registry still has the cluster it started with, and its
    /// prober still runs.
    async fn lived_on(&self) -> Result<bool> {
        let cluster = self
            .options
            .pool
            .0
            .cluster(&self.target, &self.options.pool_config)
            .await?;
        let prober_runs = self
            .cluster
            .prober
            .lock()
            .expect("MySQL cluster lock poisoned")
            .as_ref()
            .is_some_and(|prober| !prober.is_finished());
        Ok(Arc::ptr_eq(&cluster, &self.cluster) && prober_runs)
    }
}

fn describe_state(state: &OssPoolState) -> String {
    match state {
        OssPoolState::Active {
            generation,
            endpoint,
            ..
        } => format!("gen {generation} active {endpoint}"),
        OssPoolState::Unavailable { generation, reason } => {
            format!("gen {generation} unavailable: {reason}")
        }
    }
}

/// Where the processes find the servers.
#[derive(Clone, Debug)]
enum Candidates {
    /// `hosts`, by server name.
    Hosts(&'static [&'static str]),
    /// One candidate: the forwarder, which starts on a.
    Forwarder,
    /// A hosts file listing these servers at first.
    HostsFile(&'static [&'static str]),
}

struct Config {
    candidates: Candidates,
    processes: usize,
    load: Load,
    pool_config: PoolConfig,
    /// SET GLOBAL max_connections on every server before the start.
    max_connections: Option<u32>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            candidates: Candidates::Hosts(&["a", "b"]),
            processes: 2,
            load: Load::default(),
            pool_config: PoolConfig::default(),
            max_connections: None,
        }
    }
}

/// How a scenario ended.
struct Outcome {
    /// The server that must hold every acknowledged write.
    final_primary: &'static str,
    /// The longest a read_master may lag behind acknowledged writes.
    staleness_bound: f64,
    /// When the server writes resume on became the primary, if it changed.
    promoted: Option<f64>,
    /// Checks of the scenario itself: name, passed, details.
    checks: Vec<(String, bool, String)>,
}

impl Outcome {
    fn new(final_primary: &'static str, staleness_bound: f64, promoted: Option<f64>) -> Self {
        Self {
            final_primary,
            staleness_bound,
            promoted,
            checks: Vec::new(),
        }
    }

    fn check(mut self, name: &str, ok: bool, detail: String) -> Self {
        self.checks.push((name.to_string(), ok, detail));
        self
    }
}

struct Lab {
    dir: PathBuf,
    results: PathBuf,
    start: Instant,
    wall_start: f64,
    events: Mutex<Vec<(f64, String)>>,
    marks: Mutex<BTreeMap<String, f64>>,
    rec: Arc<Recorder>,
    procs: Vec<Process>,
    forwarder: Option<Forwarder>,
    hosts_file: PathBuf,
    fb: FacebookInit,
}

impl Lab {
    fn t(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }

    fn note(&self, text: impl Into<String>) {
        let text = text.into();
        let t = self.t();
        println!("{t:8.3} {text}");
        self.events
            .lock()
            .expect("lab lock poisoned")
            .push((t, text));
    }

    fn mark(&self, name: &str) -> f64 {
        let t = self.t();
        self.note(format!("mark {name}"));
        self.marks
            .lock()
            .expect("lab lock poisoned")
            .insert(name.to_string(), t);
        t
    }

    async fn sleep(&self, secs: f64) {
        tokio::time::sleep(Duration::from_secs_f64(secs)).await;
    }

    /// Runs `lab.sh args` and returns what it printed.
    async fn sh(&self, args: &[&str]) -> Result<String> {
        let begin = self.t();
        let output = tokio::process::Command::new(self.dir.join("lab.sh"))
            .args(args)
            .kill_on_drop(true)
            .output()
            .await?;
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !output.status.success() {
            bail!(
                "lab.sh {} failed: {stdout} {}",
                args.join(" "),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        self.note(format!(
            "lab.sh {} ({:.3}s) {}",
            args.join(" "),
            self.t() - begin,
            stdout.replace('\n', " | ")
        ));
        Ok(stdout)
    }

    /// Runs `sql` as root on server `x`, quietly; fails after `limit`.
    async fn query(&self, x: &str, sql: &str, limit: Duration) -> Result<Vec<Vec<String>>> {
        let command = tokio::process::Command::new(self.dir.join("lab.sh"))
            .args(["sql", x, sql])
            .kill_on_drop(true)
            .output();
        let output = tokio::time::timeout(limit, command)
            .await
            .with_context(|| format!("no answer from {x}"))??;
        if !output.status.success() {
            bail!("{x}: {}", String::from_utf8_lossy(&output.stderr).trim());
        }
        Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(|line| line.split('\t').map(str::to_string).collect())
            .collect())
    }

    fn write_hosts_file(&self, servers: &[&str]) -> Result<()> {
        let contents: String = servers
            .iter()
            .map(|x| format!("127.0.0.1:{}\n", server_port(x)))
            .collect();
        let tmp = self.hosts_file.with_extension("tmp");
        std::fs::write(&tmp, &contents)?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
        std::fs::rename(&tmp, &self.hosts_file)?;
        self.note(format!(
            "hosts file: {}",
            contents.trim().replace('\n', ", ")
        ));
        Ok(())
    }

    /// The graceful switchover of the operator contract: demote `old`, wait
    /// until `new` caught up, wait `pause` seconds, promote `new`, repoint
    /// `old` to it. Returns when `new` became writable.
    async fn switchover(&self, old: &str, new: &str, pause: f64) -> Result<f64> {
        self.sh(&["demote", old]).await?;
        self.mark("demoted");
        self.sh(&["catchup", new, old]).await?;
        self.sleep(pause).await;
        self.sh(&["promote", new]).await?;
        let promoted = self.mark("promoted");
        self.sh(&["repoint", old, new]).await?;
        Ok(promoted)
    }
}

fn server_port(x: &str) -> u16 {
    SERVERS
        .iter()
        .find(|s| s.0 == x)
        .map(|s| s.1)
        .expect("unknown server")
}

fn server_id(x: &str) -> u32 {
    SERVERS
        .iter()
        .find(|s| s.0 == x)
        .map(|s| s.2)
        .expect("unknown server")
}

fn now_epoch() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time before 1970")
        .as_secs_f64()
}

/// Runs a scenario: resets the servers, starts the processes and their load,
/// runs `body`, stops the load and checks the results.
fn run_scenario(
    fb: FacebookInit,
    name: &str,
    config: Config,
    body: impl AsyncFnOnce(&Lab) -> Result<Outcome>,
) -> Result<()> {
    let Ok(dir) = std::env::var(LAB_ENV) else {
        eprintln!("{LAB_ENV} is not set: skipping the {name} failover lab");
        return Ok(());
    };
    let dir = PathBuf::from(dir);
    let results = dir.join("results").join(name);
    let _ = std::fs::remove_dir_all(&results);
    std::fs::create_dir_all(&results)?;
    // SAFETY: set before the runtime starts any thread that reads it.
    unsafe {
        std::env::set_var("SQL_TELEMETRY_SCUBA_FILE_PATH", results.join("scuba.jsonl"));
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    let start = Instant::now();
    capture_tracing(start);
    let result = runtime.block_on(async {
        set_oss_transaction_time_limit(QUERY_TIME_LIMIT);
        let tel = SqlQueryTelemetry::new(fb, Metadata::default(), None);
        let rec = Arc::new(Recorder {
            start,
            run: format!("{}{:x}", name, (now_epoch() * 1000.0) as u64),
            stop: AtomicBool::new(false),
            seq: AtomicI64::new(1),
            op: AtomicI32::new(1),
            ops: Mutex::new(Vec::new()),
            transitions: Mutex::new(Vec::new()),
        });
        let mut lab = Lab {
            hosts_file: dir.join("hosts"),
            dir: dir.clone(),
            results: results.clone(),
            start,
            wall_start: now_epoch() - start.elapsed().as_secs_f64(),
            events: Mutex::new(Vec::new()),
            marks: Mutex::new(BTreeMap::new()),
            rec: rec.clone(),
            procs: Vec::new(),
            forwarder: None,
            fb,
        };
        lab.sh(&["reset"]).await?;
        if let Some(max) = config.max_connections {
            for (x, _, _) in SERVERS {
                lab.sh(&["sql", x, &format!("SET GLOBAL max_connections = {max}")])
                    .await?;
            }
        }
        let password_secret = format!("file:{}", dir.join("secrets/mononoke.pw").display());
        let target = |endpoints: Vec<(String, u16)>, hosts_file: Option<PathBuf>| OssMysqlTarget {
            default_port: endpoints[0].1,
            endpoints,
            hosts_file,
            database: DB.to_string(),
            user_secret: "literal:mononoke".to_string(),
            password_secret: password_secret.clone(),
        };
        let hosts = |servers: &[&str]| -> Vec<(String, u16)> {
            servers
                .iter()
                .map(|x| ("127.0.0.1".to_string(), server_port(x)))
                .collect()
        };
        let target = match config.candidates {
            Candidates::Hosts(servers) => target(hosts(servers), None),
            Candidates::Forwarder => {
                lab.forwarder = Some(Forwarder::start(FORWARDER_PORT, server_port("a")).await?);
                target(vec![("127.0.0.1".to_string(), FORWARDER_PORT)], None)
            }
            Candidates::HostsFile(servers) => {
                lab.write_hosts_file(servers)?;
                target(hosts(servers), Some(lab.hosts_file.clone()))
            }
        };
        lab.note(format!("target {target:?}"));
        for p in 0..config.processes {
            let process = Process::new(&format!("P{}", p + 1), &target, config.pool_config).await?;
            lab.note(format!(
                "{}: primary {}, read {}",
                process.name,
                describe_state(&process.primary.state()),
                describe_state(&process.read.state())
            ));
            lab.procs.push(process);
        }
        let mut tasks = Vec::new();
        for (p, process) in lab.procs.iter().enumerate() {
            let load = config.load;
            let conns = &process.conns;
            for _ in 0..load.writers {
                tasks.push(tokio::spawn(writer(
                    rec.clone(),
                    p,
                    conns.clone(),
                    tel.clone(),
                    load.pause,
                )));
            }
            for _ in 0..load.master_readers {
                tasks.push(tokio::spawn(master_reader(
                    rec.clone(),
                    p,
                    conns.clone(),
                    tel.clone(),
                    load.pause,
                )));
            }
            for _ in 0..load.readers {
                tasks.push(tokio::spawn(reader(
                    rec.clone(),
                    p,
                    conns.clone(),
                    tel.clone(),
                    load.pause,
                )));
            }
            for _ in 0..load.txns {
                tasks.push(tokio::spawn(transactor(
                    rec.clone(),
                    p,
                    conns.clone(),
                    tel.clone(),
                    load.pause,
                )));
            }
        }
        let pools: Vec<(String, OssPool, OssPool)> = lab
            .procs
            .iter()
            .map(|p| (p.name.clone(), p.primary.clone(), p.read.clone()))
            .collect();
        let watcher = tokio::spawn(watch_pools(rec.clone(), pools));
        let sampler = tokio::spawn(sample_connections(dir.clone(), rec.clone()));

        lab.sleep(WARMUP.as_secs_f64()).await;
        let outcome = body(&lab).await;

        lab.note("stopping the load");
        rec.stop.store(true, Ordering::Relaxed);
        for task in tasks {
            if tokio::time::timeout(Duration::from_secs(90), task)
                .await
                .is_err()
            {
                lab.note("a load task did not stop within 90 s");
            }
        }
        lab.note("load stopped");
        watcher.await?;
        let samples = sampler.await?;
        let outcome = outcome?;
        let mut lived_on = true;
        for process in &lab.procs {
            lived_on &= process.lived_on().await?;
        }
        let report = analyse(&lab, &outcome, &samples, lived_on).await?;
        lab.forwarder.take();
        anyhow::Ok(report)
    });
    // Probers and pools stop with the runtime.
    runtime.shutdown_timeout(Duration::from_secs(5));
    let tracing = take_tracing();
    std::fs::write(results.join("tracing.log"), tracing.join("\n") + "\n")?;
    let report = result?;
    let failed: Vec<_> = report
        .checks
        .iter()
        .filter(|(_, ok, _)| !ok)
        .map(|(name, _, detail)| format!("{name}: {detail}"))
        .collect();
    assert!(failed.is_empty(), "{name} failed: {failed:#?}");
    Ok(())
}

/// Records every change of state of the processes' pools, until the load
/// stops.
async fn watch_pools(rec: Arc<Recorder>, pools: Vec<(String, OssPool, OssPool)>) {
    let mut last: HashMap<(String, Role), String> = HashMap::new();
    loop {
        for (name, primary, read) in &pools {
            for (role, pool) in [(Role::Primary, primary), (Role::Read, read)] {
                let state = pool.state();
                let text = describe_state(&state);
                let key = (name.clone(), role);
                if last.get(&key) != Some(&text) {
                    rec.transitions
                        .lock()
                        .expect("lab transitions lock poisoned")
                        .push((
                            rec.t(),
                            name.clone(),
                            role,
                            state.generation(),
                            text.clone(),
                        ));
                    last.insert(key, text);
                }
            }
        }
        if rec.stopped() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[derive(Clone, Debug)]
struct Sample {
    t: f64,
    server: &'static str,
    threads_connected: i64,
    /// Connections of the mononoke user, by CONNECTION_TYPE.
    mononoke: BTreeMap<String, i64>,
}

/// Samples the connections of every server twice a second, until the load
/// stops.
async fn sample_connections(dir: PathBuf, rec: Arc<Recorder>) -> Vec<Sample> {
    const SQL: &str = "SELECT 'threads', VARIABLE_VALUE FROM performance_schema.global_status \
         WHERE VARIABLE_NAME = 'Threads_connected' UNION ALL \
         SELECT IFNULL(CONNECTION_TYPE, 'NULL'), COUNT(*) FROM performance_schema.threads \
         WHERE PROCESSLIST_USER = 'mononoke' GROUP BY 1";
    let mut samples = Vec::new();
    while !rec.stopped() {
        let rounds = SERVERS.map(|(x, _, _)| {
            let dir = dir.clone();
            async move {
                let command = tokio::process::Command::new(dir.join("lab.sh"))
                    .args(["sql", x, SQL])
                    .kill_on_drop(true)
                    .output();
                let output = tokio::time::timeout(Duration::from_secs(2), command).await;
                (x, output)
            }
        });
        for (x, output) in futures::future::join_all(rounds).await {
            let t = rec.t();
            let Ok(Ok(output)) = output else { continue };
            if !output.status.success() {
                continue;
            }
            let mut sample = Sample {
                t,
                server: x,
                threads_connected: 0,
                mononoke: BTreeMap::new(),
            };
            for line in String::from_utf8_lossy(&output.stdout).lines() {
                let mut fields = line.split('\t');
                let (Some(kind), Some(count)) = (fields.next(), fields.next()) else {
                    continue;
                };
                let count = count.trim().parse().unwrap_or(-1);
                if kind == "threads" {
                    sample.threads_connected = count;
                } else {
                    sample.mononoke.insert(kind.to_string(), count);
                }
            }
            samples.push(sample);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    samples
}

// The checks.

struct Report {
    checks: Vec<(String, bool, String)>,
}

/// A row group of lab_writes on one server.
#[derive(Clone, Debug, Default)]
struct Rows {
    count: i64,
    server_ids: Vec<u32>,
}

async fn rows_on(lab: &Lab, x: &str) -> Result<HashMap<String, Rows>> {
    let lines = lab
        .query(
            x,
            "SELECT uuid, GROUP_CONCAT(DISTINCT server_id), COUNT(*) FROM mononoke.lab_writes GROUP BY uuid",
            Duration::from_secs(30),
        )
        .await?;
    Ok(lines
        .into_iter()
        .filter(|l| l.len() == 3)
        .map(|l| {
            (
                l[0].clone(),
                Rows {
                    count: l[2].parse().unwrap_or(-1),
                    server_ids: l[1].split(',').filter_map(|s| s.parse().ok()).collect(),
                },
            )
        })
        .collect())
}

/// What a failed attempt logged to the error telemetry says, from the text
/// of the error: phase and statement, cause, and whether the statement had
/// no effect ("safe"), an unknown one ("unknown"), or fails for good.
fn logged_effect(error: &str) -> (String, String, &'static str) {
    let tag = error
        .find("OSS MySQL ")
        .and_then(|i| {
            let rest = &error[i + "OSS MySQL ".len()..];
            rest.find(" error on").map(|end| rest[..end].to_string())
        })
        .unwrap_or_else(|| "untagged".to_string());
    let code = error.find("Server error: `ERROR ").and_then(|i| {
        let rest = &error[i..];
        let open = rest.find('(')?;
        let close = rest.find(')')?;
        rest[open + 1..close].parse::<u16>().ok()
    });
    let cause = if error.contains("timed out after") {
        "timeout".to_string()
    } else if error.contains("no usable MySQL primary") {
        "unavailable".to_string()
    } else if let Some(code) = code {
        if error.contains(FENCE_MARKER) {
            format!("server {code} fence")
        } else {
            format!("server {code}")
        }
    } else if error.contains("Input/output error") {
        "io".to_string()
    } else if error.contains("Driver error") {
        "driver".to_string()
    } else {
        "other".to_string()
    };
    let phase = tag.split('/').next().unwrap_or_default();
    let effect = match (phase, cause.as_str(), code) {
        ("Checkout", _, _) => "safe",
        (_, "unavailable", _) => "safe",
        (_, _, Some(1290 | 1836 | 4049 | 1213 | 1205 | 1047 | 3100 | 3101)) => "safe",
        (_, "timeout" | "io" | "driver", _) => "unknown",
        (_, _, Some(1053 | 1317 | 3169 | 1927 | 4031)) => "unknown",
        _ => "final",
    };
    (tag, cause, effect)
}

/// The failed attempts logged to the error telemetry: by operation, the
/// query name, attempt, whether it was retried, and the error.
fn scuba_attempts(path: &Path) -> Result<HashMap<i32, Vec<(String, i64, bool, String)>>> {
    let mut attempts: HashMap<i32, Vec<(String, i64, bool, String)>> = HashMap::new();
    let Ok(contents) = std::fs::read_to_string(path) else {
        return Ok(attempts);
    };
    // Samples are appended by concurrent writers: read them as a stream of
    // JSON values rather than lines.
    for sample in serde_json::Deserializer::from_str(&contents).into_iter::<Value>() {
        let sample = sample?;
        let int = |k: &str| sample["int"][k].as_i64();
        let normal = |k: &str| sample["normal"][k].as_str().unwrap_or_default().to_string();
        let Some(attempt) = int("attempt") else {
            continue;
        };
        let op = sample["normvector"]["repo_ids"]
            .as_array()
            .and_then(|ids| ids.first())
            .and_then(|id| id.as_str())
            .and_then(|id| id.parse().ok())
            .unwrap_or(0);
        attempts.entry(op).or_default().push((
            normal("query_name"),
            attempt,
            int("will_retry") == Some(1),
            normal("error"),
        ));
    }
    Ok(attempts)
}

/// Counts, for each uuid, the INSERT statements that reached a server, from
/// its general log.
fn general_log_inserts(path: &Path) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    let Ok(contents) = std::fs::read(path) else {
        return counts;
    };
    for line in String::from_utf8_lossy(&contents).lines() {
        if let Some(i) = line.find("INSERT INTO lab_writes") {
            let rest = &line[i..];
            if let Some(v) = rest.find("VALUES ('") {
                let uuid = &rest[v + "VALUES ('".len()..];
                if let Some(end) = uuid.find('\'') {
                    *counts.entry(uuid[..end].to_string()).or_insert(0) += 1;
                }
            }
        }
    }
    counts
}

/// A connection of the mononoke user to a server, from its general log.
#[derive(Debug)]
struct Connect {
    /// Seconds since the epoch.
    t: f64,
    /// Pools connect to the database, probes to none.
    pool: bool,
    /// "TCP/IP", "Socket", "SSL/TLS"...
    transport: String,
}

fn general_log_connects(path: &Path) -> Vec<Connect> {
    let Ok(contents) = std::fs::read(path) else {
        return Vec::new();
    };
    String::from_utf8_lossy(&contents)
        .lines()
        .filter(|line| line.contains("Connect\tmononoke@"))
        .filter_map(|line| {
            Some(Connect {
                t: parse_utc(line.split('\t').next()?)?,
                pool: line.contains(" on mononoke "),
                transport: line.rsplit(" using ").next()?.trim().to_string(),
            })
        })
        .collect()
}

/// Parses an ISO-8601 UTC time, "2026-10-31T14:04:45.685207Z".
fn parse_utc(text: &str) -> Option<f64> {
    let text = text.trim().strip_suffix('Z')?;
    let (date, time) = text.split_once('T')?;
    let mut d = date.split('-').map(|x| x.parse::<i64>());
    let (y, m, day) = (d.next()?.ok()?, d.next()?.ok()?, d.next()?.ok()?);
    let mut t = time.split(':');
    let (h, min, s) = (
        t.next()?.parse::<f64>().ok()?,
        t.next()?.parse::<f64>().ok()?,
        t.next()?.parse::<f64>().ok()?,
    );
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    Some(days as f64 * 86400.0 + h * 3600.0 + min * 60.0 + s)
}

fn percentile(values: &mut [f64], p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|a, b| a.total_cmp(b));
    values[((values.len() - 1) as f64 * p).round() as usize]
}

async fn analyse(
    lab: &Lab,
    outcome: &Outcome,
    samples: &[Sample],
    process_kept: bool,
) -> Result<Report> {
    let mut ops = lab.rec.ops.lock().expect("lab ops lock poisoned").clone();
    ops.sort_by(|a, b| a.end.total_cmp(&b.end));
    let transitions = lab
        .rec
        .transitions
        .lock()
        .expect("lab transitions lock poisoned")
        .clone();
    let mut checks: Vec<(String, bool, String)> = outcome.checks.clone();
    let mut numbers = serde_json::Map::new();

    // Rows on every server that answers.
    let final_primary = outcome.final_primary;
    let final_rows = rows_on(lab, final_primary).await?;
    let mut rows_by_server: BTreeMap<&str, HashMap<String, Rows>> = BTreeMap::new();
    for (x, _, _) in SERVERS {
        if x == final_primary {
            rows_by_server.insert(x, final_rows.clone());
        } else if let Ok(rows) = rows_on(lab, x).await {
            rows_by_server.insert(x, rows);
        }
    }
    let txn_counter: i64 = lab
        .query(
            final_primary,
            "SELECT n FROM mononoke.lab_txn WHERE id = 1",
            Duration::from_secs(10),
        )
        .await?
        .first()
        .and_then(|l| l.first())
        .and_then(|n| n.parse().ok())
        .unwrap_or(-1);

    // 1. Exactly once.
    let writes = || {
        ops.iter().filter(|o| {
            matches!(
                o.kind,
                OpKind::Write | OpKind::Txn | OpKind::Rollback | OpKind::TxnAcrossSwitch
            )
        })
    };
    let count_on = |rows: &HashMap<String, Rows>, uuid: &str| rows.get(uuid).map_or(0, |r| r.count);
    let mut lost = Vec::new();
    let mut duplicated = Vec::new();
    let mut acked = 0;
    for o in writes().filter(|o| o.result.is_ok() && o.kind != OpKind::Rollback) {
        acked += 1;
        match count_on(&final_rows, &o.uuid) {
            1 => {}
            0 => lost.push(o.uuid.clone()),
            _ => duplicated.push(o.uuid.clone()),
        }
    }
    let all_duplicates: Vec<_> = rows_by_server
        .iter()
        .flat_map(|(x, rows)| {
            rows.iter()
                .filter(|(_, r)| r.count > 1)
                .map(move |(uuid, r)| format!("{x}:{uuid}x{}", r.count))
        })
        .collect();
    checks.push((
        "acked writes on the final primary exactly once".to_string(),
        lost.is_empty() && duplicated.is_empty() && all_duplicates.is_empty(),
        format!(
            "{acked} acked; lost {lost:?}; duplicated {duplicated:?}; duplicates anywhere {all_duplicates:?}"
        ),
    ));
    // A failure that says the write had no effect, and a rollback: no row
    // anywhere.
    let mut applied_anyway = Vec::new();
    for o in writes() {
        let no_effect = o.kind == OpKind::Rollback
            || o.result.as_ref().is_err_and(|f| {
                matches!(
                    f.disposition,
                    Some(Disposition::Safe | Disposition::Permanent)
                )
            });
        if no_effect {
            for (x, rows) in &rows_by_server {
                if count_on(rows, &o.uuid) > 0 {
                    applied_anyway.push(format!(
                        "{x}:{} {:?} {:?}",
                        o.uuid,
                        o.kind,
                        o.result.as_ref().err().map(|f| &f.label)
                    ));
                }
            }
        }
    }
    checks.push((
        "writes that failed without effect, and rollbacks, left no row".to_string(),
        applied_anyway.is_empty(),
        format!("{applied_anyway:?}"),
    ));
    let txn_rows = final_rows
        .iter()
        .filter(|(uuid, _)| {
            ops.iter()
                .any(|o| o.kind == OpKind::Txn && &o.uuid == *uuid)
        })
        .count() as i64;
    checks.push((
        "transaction counter matches the transaction rows".to_string(),
        txn_counter == txn_rows,
        format!("lab_txn.n {txn_counter}, transaction rows {txn_rows}"),
    ));

    // 2. No write ran again after a failure with an unknown outcome.
    let scuba = scuba_attempts(&lab.results.join("scuba.jsonl"))?;
    let mut retried_unknown = Vec::new();
    let mut attempt_labels: BTreeMap<String, usize> = BTreeMap::new();
    for (op, attempts) in &scuba {
        for (query, attempt, will_retry, error) in attempts {
            let (tag, cause, effect) = logged_effect(error);
            *attempt_labels
                .entry(format!("{query} {tag} {cause} {effect} retry={will_retry}"))
                .or_default() += 1;
            if query == "LabInsert" && effect == "unknown" && *will_retry {
                retried_unknown.push(format!(
                    "op {op} attempt {attempt}: {}",
                    error.lines().next().unwrap_or_default()
                ));
            }
        }
    }
    for o in writes() {
        for f in &o.earlier {
            if f.phase == Some(OssPhase::Commit) && f.disposition == Some(Disposition::Ambiguous) {
                retried_unknown.push(format!("transaction {} retried after {}", o.uuid, f.label));
            }
        }
    }
    let ambiguous: Vec<&Op> = writes()
        .filter(|o| {
            o.result
                .as_ref()
                .is_err_and(|f| f.disposition == Some(Disposition::Ambiguous))
        })
        .collect();
    let mut general: BTreeMap<&str, HashMap<String, usize>> = BTreeMap::new();
    for (x, _, _) in SERVERS {
        general.insert(x, general_log_inserts(&lab.dir.join(x).join("general.log")));
    }
    let reached = |uuid: &str| {
        general
            .values()
            .map(|g| g.get(uuid).copied().unwrap_or(0))
            .sum::<usize>()
    };
    // An autocommit write that ended ambiguous was sent once more than the
    // attempts logged before it, at most.
    for o in &ambiguous {
        if o.kind != OpKind::Write {
            continue;
        }
        let logged = scuba.get(&o.op).map_or(0, |a| a.len());
        let sent = reached(&o.uuid);
        if sent > logged {
            retried_unknown.push(format!(
                "{} reached servers {sent} times for {logged} attempts",
                o.uuid
            ));
        }
    }
    checks.push((
        "no write ran again after a failure with an unknown outcome".to_string(),
        retried_unknown.is_empty(),
        format!(
            "{} writes ended with an unknown outcome ({} of them applied on {final_primary}); violations {retried_unknown:?}",
            ambiguous.len(),
            ambiguous.iter().filter(|o| count_on(&final_rows, &o.uuid) > 0).count()
        ),
    ));

    // 3. read_master lag.
    let mut acks: Vec<(f64, i64)> = writes()
        .filter(|o| o.result.is_ok() && o.kind != OpKind::Rollback)
        .map(|o| (o.end, o.seq))
        .collect();
    acks.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut prefix_max = Vec::with_capacity(acks.len());
    let mut max = 0;
    for (_, seq) in &acks {
        max = max.max(*seq);
        prefix_max.push(max);
    }
    let mut stale = 0;
    let mut worst = 0.0f64;
    let mut worst_read = String::new();
    let mut reads_master = 0;
    for o in ops.iter().filter(|o| o.kind == OpKind::ReadMaster) {
        let Ok(value) = o.result else { continue };
        reads_master += 1;
        let before = acks.partition_point(|(end, _)| *end < o.start);
        if before == 0 || prefix_max[before - 1] <= value {
            continue;
        }
        stale += 1;
        let oldest_missed = acks[..before]
            .iter()
            .filter(|(_, seq)| *seq > value)
            .map(|(end, _)| *end)
            .fold(f64::INFINITY, f64::min);
        let lag = o.start - oldest_missed;
        if lag > worst {
            worst = lag;
            worst_read = format!(
                "P{} read {value} at {:.3}, {} acked by then",
                o.process + 1,
                o.start,
                prefix_max[before - 1]
            );
        }
    }
    checks.push((
        format!(
            "read_master within {:.1}s of acknowledged writes",
            outcome.staleness_bound
        ),
        worst <= outcome.staleness_bound,
        format!("{stale} of {reads_master} reads stale, worst {worst:.3}s ({worst_read})"),
    ));

    // 4. The processes lived on.
    let mut regressions = Vec::new();
    let mut last_generation: HashMap<(String, Role), u64> = HashMap::new();
    for (t, name, role, generation, _) in &transitions {
        let last = last_generation.entry((name.clone(), *role)).or_insert(0);
        if generation < last {
            regressions.push(format!("{name} {role:?} at {t:.3}"));
        }
        *last = *generation;
    }
    checks.push((
        "processes kept their clusters and probers".to_string(),
        process_kept && regressions.is_empty(),
        format!(
            "clusters and probers kept: {process_kept}; generation regressions {regressions:?}"
        ),
    ));

    // 5. TCP only.
    let not_tcp: Vec<_> = samples
        .iter()
        .flat_map(|s| {
            s.mononoke
                .iter()
                .filter(|(kind, _)| kind.as_str() != "TCP/IP")
                .map(move |(kind, n)| format!("{} {}x{kind} at {:.1}", s.server, n, s.t))
        })
        .collect();
    let tcp_samples = samples.iter().filter(|s| !s.mononoke.is_empty()).count();
    // The general logs also see the connections of probes, too short-lived
    // for the samples.
    let mut transports: BTreeMap<String, usize> = BTreeMap::new();
    for (x, _, _) in SERVERS {
        for connect in general_log_connects(&lab.dir.join(x).join("general.log")) {
            let kind = if connect.pool { "pool" } else { "probe" };
            *transports
                .entry(format!("{x} {kind} {}", connect.transport))
                .or_default() += 1;
        }
    }
    let other_transports: Vec<_> = transports
        .iter()
        .filter(|(k, _)| !k.ends_with("TCP/IP"))
        .collect();
    checks.push((
        "every mononoke connection is TCP".to_string(),
        not_tcp.is_empty() && tcp_samples > 0 && other_transports.is_empty(),
        format!(
            "{tcp_samples} samples with mononoke connections, not TCP {not_tcp:?}; \
             connections in the general logs {transports:?}"
        ),
    ));

    // Numbers.
    let mut by_kind: BTreeMap<String, Value> = BTreeMap::new();
    let kinds = [
        OpKind::Write,
        OpKind::ReadMaster,
        OpKind::Read,
        OpKind::Txn,
        OpKind::Rollback,
        OpKind::TxnAcrossSwitch,
    ];
    for kind in kinds {
        let of_kind: Vec<&Op> = ops.iter().filter(|o| o.kind == kind).collect();
        let mut errors: BTreeMap<String, usize> = BTreeMap::new();
        let mut earlier: BTreeMap<String, usize> = BTreeMap::new();
        for o in &of_kind {
            if let Err(f) = &o.result {
                *errors.entry(f.label.clone()).or_default() += 1;
            }
            for f in &o.earlier {
                *earlier.entry(f.label.clone()).or_default() += 1;
            }
        }
        let mut latencies: Vec<f64> = of_kind.iter().map(|o| o.end - o.start).collect();
        let p99 = percentile(&mut latencies, 0.99);
        let max = latencies.last().copied().unwrap_or(0.0);
        by_kind.insert(
            format!("{kind:?}"),
            json!({
                "ops": of_kind.len(),
                "ok": of_kind.iter().filter(|o| o.result.is_ok()).count(),
                "errors": errors,
                "retried_transaction_attempts": earlier,
                "latency_p99_s": p99,
                "latency_max_s": max,
            }),
        );
    }
    numbers.insert("ops".to_string(), json!(by_kind));
    numbers.insert("failed_attempts_logged".to_string(), json!(attempt_labels));

    // Writes resume: the first acknowledged write after the promotion on
    // the new primary, and the longest gap between write acks.
    let ack_times: Vec<f64> = acks.iter().map(|(end, _)| *end).collect();
    let mut gaps: Vec<(f64, f64)> = ack_times
        .windows(2)
        .filter(|w| w[0] > WARMUP.as_secs_f64())
        .map(|w| (w[1] - w[0], w[0]))
        .collect();
    gaps.sort_by(|a, b| b.0.total_cmp(&a.0));
    numbers.insert(
        "longest_write_pauses_s".to_string(),
        json!(
            gaps.iter()
                .take(3)
                .map(|(gap, at)| format!("{gap:.3}s after {at:.3}"))
                .collect::<Vec<_>>()
        ),
    );
    if let Some(promoted) = outcome.promoted {
        let new_id = server_id(final_primary);
        let first_on_new = writes()
            .filter(|o| o.result.is_ok() && o.kind != OpKind::Rollback && o.end > promoted)
            .filter(|o| {
                final_rows
                    .get(&o.uuid)
                    .is_some_and(|r| r.server_ids.contains(&new_id))
            })
            .map(|o| o.end)
            .fold(f64::INFINITY, f64::min);
        numbers.insert(
            "writes_resume_after_promotion_s".to_string(),
            json!(first_on_new - promoted),
        );
    }

    // Generations and states.
    let mut generations: BTreeMap<String, Value> = BTreeMap::new();
    for p in &lab.procs {
        generations.insert(
            p.name.clone(),
            json!({
                "primary": p.primary.generation(),
                "read": p.read.generation(),
                "primary_state": describe_state(&p.primary.state()),
            }),
        );
    }
    numbers.insert("generations_at_end".to_string(), json!(generations));
    let mut unavailable: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (t, name, role, _, text) in &transitions {
        if let (Role::Primary, Some((_, reason))) = (role, text.split_once("unavailable: ")) {
            let reason: String = reason.chars().take(40).collect();
            unavailable
                .entry(name.clone())
                .or_default()
                .push(format!("{t:.3} {reason}"));
        }
    }
    numbers.insert("primary_marked_unavailable".to_string(), json!(unavailable));
    numbers.insert(
        "pool_transitions".to_string(),
        json!(
            transitions
                .iter()
                .map(|(t, name, role, _, text)| format!("{t:.3} {name} {role:?} {text}"))
                .collect::<Vec<_>>()
        ),
    );
    let mut peaks: BTreeMap<&str, (i64, i64)> = BTreeMap::new();
    for s in samples {
        let peak = peaks.entry(s.server).or_default();
        peak.0 = peak.0.max(s.threads_connected);
        peak.1 = peak.1.max(s.mononoke.values().sum());
    }
    numbers.insert(
        "peak_connections".to_string(),
        json!(
            peaks
                .iter()
                .map(|(x, (threads, mononoke))| (
                    x.to_string(),
                    json!({"threads_connected": threads, "mononoke": mononoke})
                ))
                .collect::<BTreeMap<_, _>>()
        ),
    );
    let rows_only_elsewhere: BTreeMap<&str, Vec<String>> = rows_by_server
        .iter()
        .filter(|(x, _)| **x != final_primary)
        .map(|(x, rows)| {
            let only: Vec<String> = rows
                .keys()
                .filter(|uuid| !final_rows.contains_key(*uuid))
                .map(|uuid| {
                    let outcome = ops
                        .iter()
                        .find(|o| &o.uuid == uuid)
                        .map(|o| match &o.result {
                            Ok(_) => "acked".to_string(),
                            Err(f) => f.label.clone(),
                        })
                        .unwrap_or_default();
                    format!("{uuid} ({outcome})")
                })
                .collect();
            (*x, only)
        })
        .collect();
    numbers.insert(
        "rows_missing_on_final_primary_by_server".to_string(),
        json!(rows_only_elsewhere),
    );
    numbers.insert(
        "marks".to_string(),
        json!(lab.marks.lock().expect("lab lock poisoned").clone()),
    );
    numbers.insert("wall_start_epoch".to_string(), json!(lab.wall_start));

    // Files.
    let events: Vec<String> = lab
        .events
        .lock()
        .expect("lab lock poisoned")
        .iter()
        .map(|(t, e)| format!("{t:8.3} {e}"))
        .collect();
    std::fs::write(lab.results.join("timeline.txt"), events.join("\n") + "\n")?;
    let mut op_lines = Vec::with_capacity(ops.len());
    for o in &ops {
        op_lines.push(format!(
            "{:8.3} {:8.3} P{} {:?} op={} seq={} {} {}",
            o.start,
            o.end,
            o.process + 1,
            o.kind,
            o.op,
            o.seq,
            o.uuid,
            match &o.result {
                Ok(v) => format!("ok {v}"),
                Err(f) => format!("ERR {} | {}", f.label, f.message),
            }
        ));
    }
    std::fs::write(lab.results.join("ops.txt"), op_lines.join("\n") + "\n")?;
    let sample_lines: Vec<String> = samples
        .iter()
        .map(|s| {
            format!(
                "{:8.3} {} threads={} mononoke={:?}",
                s.t, s.server, s.threads_connected, s.mononoke
            )
        })
        .collect();
    std::fs::write(
        lab.results.join("connections.txt"),
        sample_lines.join("\n") + "\n",
    )?;
    for (x, _, _) in SERVERS {
        let _ = std::fs::copy(
            lab.dir.join(x).join("err.log"),
            lab.results.join(format!("{x}-err.log")),
        );
        let _ = std::fs::copy(
            lab.dir.join(x).join("general.log"),
            lab.results.join(format!("{x}-general.log")),
        );
    }
    let summary = json!({
        "checks": checks
            .iter()
            .map(|(name, ok, detail)| json!({"check": name, "ok": ok, "detail": detail}))
            .collect::<Vec<_>>(),
        "numbers": numbers,
    });
    std::fs::write(
        lab.results.join("summary.json"),
        serde_json::to_string_pretty(&summary)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&summary["checks"])?);
    for key in [
        "writes_resume_after_promotion_s",
        "longest_write_pauses_s",
        "generations_at_end",
        "peak_connections",
    ] {
        if let Some(v) = numbers.get(key) {
            println!("{key}: {v}");
        }
    }
    Ok(Report { checks })
}

// The scenarios.

/// I1: graceful switchover a -> b with the operator contract's 2 s wait.
#[fbinit::test]
#[ignore]
fn lab_i1_graceful(fb: FacebookInit) -> Result<()> {
    run_scenario(fb, "I1", Config::default(), async |lab| {
        let promoted = lab.switchover("a", "b", 2.0).await?;
        lab.sleep(8.0).await;
        Ok(Outcome::new("b", 0.0, Some(promoted)))
    })
}

/// I2: fast switchover, with no wait between catch-up and promotion.
#[fbinit::test]
#[ignore]
fn lab_i2_fast_switch(fb: FacebookInit) -> Result<()> {
    run_scenario(fb, "I2", Config::default(), async |lab| {
        let promoted = lab.switchover("a", "b", 0.0).await?;
        lab.sleep(8.0).await;
        Ok(Outcome::new("b", 1.0, Some(promoted)))
    })
}

/// I3: kill -9 the primary, then promote b once it applied what it got.
#[fbinit::test]
#[ignore]
fn lab_i3_kill(fb: FacebookInit) -> Result<()> {
    run_scenario(fb, "I3", Config::default(), async |lab| {
        lab.sh(&["kill", "a"]).await?;
        lab.mark("killed");
        lab.sleep(1.0).await;
        lab.sh(&["wait-applied", "b"]).await?;
        lab.sh(&["promote", "b"]).await?;
        let promoted = lab.mark("promoted");
        lab.sleep(8.0).await;
        // Back read-only and without replication, to see what it committed.
        lab.sh(&["start", "a", "ro", "norepl"]).await?;
        lab.mark("a restarted");
        lab.sleep(3.0).await;
        Ok(Outcome::new("b", 1.0, Some(promoted)))
    })
}

/// I4: the primary stops answering (SIGSTOP); b is promoted; a comes back
/// still writable, which is a split brain, until the operator demotes it. A
/// transaction rolls back on a while it is stopped.
#[fbinit::test]
#[ignore]
fn lab_i4_black_hole(fb: FacebookInit) -> Result<()> {
    run_scenario(fb, "I4", Config::default(), async |lab| {
        let conns = lab.procs[0].conns.clone();
        let tel = SqlQueryTelemetry::new(lab.fb, Metadata::default(), None);
        let (op, seq, uuid) = lab.rec.next();
        let (opened_tx, opened) = tokio::sync::oneshot::channel();
        let paused = Arc::new(tokio::sync::Notify::new());
        let rec = lab.rec.clone();
        let rollback = tokio::spawn({
            let paused = paused.clone();
            async move {
                let start = rec.t();
                let txn = conns.write_connection.start_transaction(tel).await?;
                let (txn, _) =
                    LabTxnInsert::query_with_transaction(txn, &uuid, &seq, &RepositoryId::new(op))
                        .await?;
                let _ = opened_tx.send(());
                paused.notified().await;
                let rollback_start = rec.t();
                let result = txn.rollback().await;
                let end = rec.t();
                anyhow::Ok((
                    end - rollback_start,
                    Op {
                        process: 0,
                        kind: OpKind::Rollback,
                        op,
                        uuid,
                        seq,
                        start,
                        end,
                        result: result
                            .map(|()| 0)
                            .map_err(|e| Failure::of(&e, ErrorScope::Transaction)),
                        earlier: Vec::new(),
                    },
                ))
            }
        });
        opened.await?;
        lab.sh(&["pause", "a"]).await?;
        lab.mark("paused");
        paused.notify_one();
        lab.sleep(3.0).await;
        lab.sh(&["wait-applied", "b"]).await?;
        lab.sh(&["promote", "b"]).await?;
        let promoted = lab.mark("promoted");
        lab.sleep(12.0).await;
        lab.sh(&["cont", "a"]).await?;
        let resumed = lab.mark("a resumed, writable");
        lab.sleep(6.0).await;
        lab.sh(&["demote", "a"]).await?;
        lab.mark("a demoted");
        lab.sleep(8.0).await;
        let closed: Vec<_> = lab
            .procs
            .iter()
            .map(|p| lab.rec.unavailable_since(&p.name, "split brain", resumed))
            .collect();
        lab.note(format!("split brain declared at {closed:?}"));
        let (rollback_time, rollback) = rollback.await??;
        let detail = format!(
            "ROLLBACK took {rollback_time:.3}s: {:?}",
            rollback.result.as_ref().map_err(|f| &f.label)
        );
        lab.note(detail.clone());
        lab.rec.record(rollback);
        Ok(Outcome::new("b", 1.0, Some(promoted))
            .check(
                "split brain declared after a came back writable",
                closed.iter().all(Option::is_some),
                format!("{closed:?}, a resumed at {resumed:.3}"),
            )
            .check(
                "a ROLLBACK on the stopped primary ends at the statement time limit",
                rollback_time < QUERY_TIME_LIMIT.as_secs_f64() + 2.0,
                detail,
            ))
    })
}

/// I5: b becomes writable while a still is, then is demoted again.
#[fbinit::test]
#[ignore]
fn lab_i5_split_brain(fb: FacebookInit) -> Result<()> {
    run_scenario(fb, "I5", Config::default(), async |lab| {
        lab.sh(&["writable", "b"]).await?;
        let split = lab.mark("split");
        lab.sleep(8.0).await;
        lab.sh(&["demote", "b"]).await?;
        let resolved = lab.mark("resolved");
        lab.sleep(8.0).await;
        let closed: Vec<_> = lab
            .procs
            .iter()
            .map(|p| lab.rec.unavailable_since(&p.name, "split brain", split))
            .collect();
        lab.note(format!("split brain declared at {closed:?}"));
        let mut outcome = Outcome::new("a", 1.0, None).check(
            "split brain declared within a probe interval or two",
            closed.iter().all(|t| t.is_some_and(|t| t - split < 2.0)),
            format!("{closed:?}, split at {split:.3}"),
        );
        // Once every process failed closed, no row may be created anywhere
        // until the split ends, except by writes already in flight: allow
        // half a second for those.
        let latest = closed.iter().flatten().copied().fold(split, f64::max);
        for x in ["a", "b"] {
            let from = lab.wall_start + latest + 0.5;
            let to = lab.wall_start + resolved;
            let rows = lab
                .query(
                    x,
                    &format!(
                        "SELECT COUNT(*), IFNULL(SUM(server_id = 2), 0) FROM mononoke.lab_writes \
                         WHERE created > FROM_UNIXTIME({from:.6}) AND created < FROM_UNIXTIME({to:.6})"
                    ),
                    Duration::from_secs(10),
                )
                .await?;
            let counts = rows.first().cloned().unwrap_or_default();
            outcome = outcome.check(
                &format!("no row created on {x} while failed closed"),
                counts.iter().all(|n| n == "0"),
                format!("rows created, and of them by b: {counts:?}"),
            );
        }
        Ok(outcome)
    })
}

/// I6: one stable name, the forwarder, moved from a to b during a graceful
/// switchover; then a flap back to a that closes the forwarded connections.
#[fbinit::test]
#[ignore]
fn lab_i6_stable_name(fb: FacebookInit) -> Result<()> {
    let config = Config {
        candidates: Candidates::Forwarder,
        ..Config::default()
    };
    run_scenario(fb, "I6", config, async |lab| {
        let forwarder = lab.forwarder.as_ref().expect("I6 has a forwarder");
        lab.sh(&["demote", "a"]).await?;
        lab.mark("demoted");
        lab.sh(&["catchup", "b", "a"]).await?;
        lab.sleep(2.0).await;
        lab.sh(&["promote", "b"]).await?;
        forwarder.retarget(server_port("b"), false);
        let promoted = lab.mark("promoted and retargeted");
        lab.sh(&["repoint", "a", "b"]).await?;
        lab.sleep(6.0).await;
        forwarder.retarget(server_port("a"), true);
        lab.mark("flap to a, connections closed");
        lab.sleep(1.5).await;
        forwarder.retarget(server_port("b"), false);
        lab.mark("back to b");
        lab.sleep(8.0).await;
        Ok(Outcome::new("b", 0.0, Some(promoted)))
    })
}

/// I8: a transaction open across a graceful switchover.
#[fbinit::test]
#[ignore]
fn lab_i8_transaction(fb: FacebookInit) -> Result<()> {
    run_scenario(fb, "I8", Config::default(), async |lab| {
        let conns = lab.procs[0].conns.clone();
        let tel = SqlQueryTelemetry::new(lab.fb, Metadata::default(), None);
        let (op, seq, uuid) = lab.rec.next();
        let (opened_tx, opened) = tokio::sync::oneshot::channel();
        let demoted = Arc::new(tokio::sync::Notify::new());
        let rec = lab.rec.clone();
        let txn = tokio::spawn({
            let demoted = demoted.clone();
            let uuid = uuid.clone();
            async move {
                let mut opened_tx = Some(opened_tx);
                let start = rec.t();
                let (result, earlier) = retry_transaction(|attempt| {
                    let (conns, tel, uuid) = (conns.clone(), tel.clone(), uuid.clone());
                    let opened_tx = opened_tx.take();
                    let demoted = demoted.clone();
                    async move {
                        let txn = conns.write_connection.start_transaction(tel).await?;
                        let (txn, _) = LabTxnInsert::query_with_transaction(
                            txn,
                            &uuid,
                            &seq,
                            &RepositoryId::new(op),
                        )
                        .await?;
                        if let Some(opened_tx) = opened_tx {
                            let _ = opened_tx.send(attempt);
                            demoted.notified().await;
                        }
                        txn.commit().await
                    }
                })
                .await;
                Op {
                    process: 0,
                    kind: OpKind::TxnAcrossSwitch,
                    op,
                    uuid,
                    seq,
                    start,
                    end: rec.t(),
                    result,
                    earlier,
                }
            }
        });
        let attempt = opened.await?;
        lab.note(format!("transaction {uuid} open on attempt {attempt}"));
        lab.sh(&["demote", "a"]).await?;
        lab.mark("demoted");
        demoted.notify_one();
        lab.sh(&["catchup", "b", "a"]).await?;
        lab.sleep(2.0).await;
        lab.sh(&["promote", "b"]).await?;
        let promoted = lab.mark("promoted");
        lab.sh(&["repoint", "a", "b"]).await?;
        let txn = txn.await?;
        let earlier: Vec<_> = txn.earlier.iter().map(|f| f.label.clone()).collect();
        let detail = format!(
            "{uuid}: {:?} after failed attempts {earlier:?}",
            txn.result.as_ref().map_err(|f| &f.message)
        );
        lab.note(format!("transaction {detail}"));
        let ok = txn.result.is_ok()
            && earlier
                .first()
                .is_some_and(|f| f.starts_with("Commit/") && f.contains("server 1290"));
        lab.rec.record(txn);
        lab.sleep(6.0).await;
        Ok(Outcome::new("b", 0.0, Some(promoted)).check(
            "the open transaction's COMMIT got 1290 and its retry succeeded",
            ok,
            detail,
        ))
    })
}

/// I9: max_connections = 60 on every server, three processes with
/// 32-connection pools, and a graceful switchover under that load.
#[fbinit::test]
#[ignore]
fn lab_i9_budget(fb: FacebookInit) -> Result<()> {
    let config = Config {
        processes: 3,
        load: Load {
            writers: 14,
            master_readers: 4,
            readers: 2,
            txns: 1,
            pause: Duration::from_millis(5),
        },
        pool_config: PoolConfig {
            per_key_limit: 32,
            ..PoolConfig::default()
        },
        max_connections: Some(60),
        ..Config::default()
    };
    run_scenario(fb, "I9", config, async |lab| {
        lab.sleep(4.0).await;
        let promoted = lab.switchover("a", "b", 2.0).await?;
        lab.sleep(10.0).await;
        let mut outcome = Outcome::new("b", 0.0, Some(promoted));
        for x in ["a", "b"] {
            let status = lab
                .query(
                    x,
                    "SELECT VARIABLE_NAME, VARIABLE_VALUE FROM performance_schema.global_status \
                     WHERE VARIABLE_NAME IN ('Max_used_connections', 'Connection_errors_max_connections')",
                    Duration::from_secs(10),
                )
                .await?;
            lab.note(format!("{x}: {status:?}"));
            let max_used = status
                .iter()
                .find(|l| l.first().is_some_and(|n| n == "Max_used_connections"))
                .and_then(|l| l.get(1)?.parse::<i64>().ok())
                .unwrap_or(-1);
            // max_connections, plus the connection kept for an admin.
            outcome = outcome.check(
                &format!("{x} never had more than 61 connections"),
                (0..=61).contains(&max_used),
                format!("{status:?}"),
            );
        }
        Ok(outcome)
    })
}

/// I10: the password changes on the server first, and only 3 s later in the
/// `file:` secret; idle connections are closed meanwhile so that the pools
/// must connect again.
#[fbinit::test]
#[ignore]
fn lab_i10_rotation(fb: FacebookInit) -> Result<()> {
    run_scenario(fb, "I10", Config::default(), async |lab| {
        lab.sh(&["rotate-password", "a"]).await?;
        lab.mark("password changed on the server");
        for _ in 0..3 {
            lab.sh(&["kill-idle", "a"]).await?;
            lab.sleep(1.0).await;
        }
        lab.sh(&["commit-password"]).await?;
        let committed = lab.mark("password changed in the secret file");
        lab.sleep(3.0).await;
        lab.sh(&["kill-idle", "a"]).await?;
        lab.sleep(8.0).await;
        // Every pool is rebuilt with the new password.
        let rebuilt: Vec<_> = lab
            .rec
            .transitions
            .lock()
            .expect("lab transitions lock poisoned")
            .iter()
            .filter(|(t, ..)| *t >= committed)
            .map(|(t, name, role, generation, _)| {
                format!("{name} {role:?} gen {generation} at {t:.3}")
            })
            .collect();
        lab.note(format!(
            "pool changes after the secret changed: {rebuilt:?}"
        ));
        Ok(Outcome::new("a", 0.0, None).check(
            "every pool was rebuilt after the secret file changed",
            rebuilt.len() >= 2 * lab.procs.len(),
            format!("{rebuilt:?}"),
        ))
    })
}

/// I13: a hosts file that first lists a and b; c, a replica, is added while
/// the processes run, becomes the primary, and a is then removed.
#[fbinit::test]
#[ignore]
fn lab_i13_hosts_file(fb: FacebookInit) -> Result<()> {
    let config = Config {
        candidates: Candidates::HostsFile(&["a", "b"]),
        ..Config::default()
    };
    run_scenario(fb, "I13", config, async |lab| {
        lab.write_hosts_file(&["a", "b", "c"])?;
        lab.mark("c added");
        lab.sleep(3.0).await;
        let promoted = lab.switchover("a", "c", 2.0).await?;
        lab.sh(&["repoint", "b", "c"]).await?;
        lab.sleep(4.0).await;
        lab.write_hosts_file(&["b", "c"])?;
        let removed = lab.mark("a removed");
        lab.sleep(8.0).await;
        let connects = general_log_connects(&lab.dir.join("a").join("general.log"));
        let removed_epoch = lab.wall_start + removed;
        let after: Vec<_> = connects
            .iter()
            .filter(|c| c.t > removed_epoch + 1.5)
            .collect();
        let before = connects
            .iter()
            .filter(|c| c.t > removed_epoch - 4.0 && c.t <= removed_epoch)
            .count();
        Ok(Outcome::new("c", 0.0, Some(promoted)).check(
            "a is no longer contacted once removed from the hosts file",
            after.is_empty(),
            format!(
                "mononoke connections to a: {before} in the 4 s before its removal, {} from 1.5 s after: {after:?}",
                after.len()
            ),
        ))
    })
}
