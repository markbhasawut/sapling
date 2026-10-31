/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! The health report of a repo: what its metadata database looks like, and
//! what the in-memory stats say about its queries, blobstores and warm
//! bookmark cache, with the checks that judge them.
//!
//! The JSON form is schema 1: every key is always present, in the order of
//! the fields below, with `null` for what is not known. Callers outside the
//! admin group see `"<redacted>"` instead of server names, paths and
//! blobstore names.

use std::collections::BTreeMap;

use serde::Serialize;
use sql_ext::facebook::CandidateObservation;
use sql_ext::facebook::ClusterHealth;
use sql_ext::facebook::ClusterStatus;
use stats::memory_stats::Distribution;
use stats::memory_stats::Snapshot;
use stats::memory_stats::StatKind;

use super::rules::Check;
use super::rules::Status;
use super::rules::blobstore_check;
use super::rules::bookmark_check;
use super::rules::metadata_check;
use super::rules::overall;
use super::rules::server_check;
use super::rules::sql_check;

/// The version of the JSON form.
pub(crate) const SCHEMA: u32 = 1;

/// What callers outside the admin group see instead of a server or a path.
pub(crate) const REDACTED: &str = "<redacted>";

/// The version of this server.
pub(crate) const VERSION: &str = match option_env!("CARGO_PKG_VERSION") {
    Some(version) => version,
    None => "unknown",
};

/// The source revision this server was built from, if the build said.
pub(crate) const BUILD: Option<&str> = option_env!("MONONOKE_BUILD_REVISION");

const QUERIES: &str = "mononoke.sql_ext.queries";
const QUERY_ERRORS: &str = "mononoke.sql_ext.queries.errors.";
const CHECKOUT_PREFIX: &str = "sql.mysql_ffi.";
const CHECKOUT_SUFFIX: &str = ".get_connection_ms";
const BLOBSTORE_PREFIX: &str = "mononoke.blobstore.";
const BLOBSTORE_OPERATIONS: [&str; 6] = ["get", "put", "is_present", "copy", "unlink", "enumerate"];

/// The metadata database of a repo, as configured.
pub(crate) enum MetadataInput {
    /// SQLite files in the directory `path`.
    Local { path: String },
    /// Meta's MySQL.
    Remote,
    /// MySQL: the distinct clusters of the database entries, in the order
    /// of the config.
    OssRemote { clusters: Vec<ClusterInput> },
}

/// A MySQL cluster that database entries of a repo use.
pub(crate) struct ClusterInput {
    /// The entries, in the order of the config.
    pub used_for: Vec<&'static str>,
    /// Distinct, sorted.
    pub databases: Vec<String>,
    /// The cluster's name, or for one not in use, what it would be.
    pub name: String,
    /// `None` when no store of this process uses it.
    pub status: Option<ClusterStatus>,
}

#[derive(Debug, Serialize)]
pub(crate) struct HealthReport {
    pub schema: u32,
    pub repo: String,
    /// The worst status of `checks`.
    pub status: Status,
    /// Whether server names and paths are hidden.
    pub redacted: bool,
    pub server: ServerReport,
    pub checks: Vec<Check>,
    pub metadata_db: MetadataReport,
    /// `None` without the in-memory stats backend.
    pub sql: Option<SqlReport>,
    /// `None` without the backend, or without counted blobstores.
    pub blobstore: Option<BlobstoreReport>,
    /// `None` without the backend, or without a warm bookmark cache.
    pub bookmark_cache: Option<BookmarkCacheReport>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ServerReport {
    pub version: &'static str,
    pub build: Option<&'static str>,
    pub uptime_secs: Option<u64>,
    pub exiting: bool,
    pub metrics_available: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct MetadataReport {
    /// `local`, `oss_remote` or `remote`.
    pub kind: &'static str,
    /// The SQLite directory, for `local`.
    pub path: Option<String>,
    /// The clusters, for `oss_remote`.
    pub clusters: Vec<ClusterReport>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ClusterReport {
    pub name: String,
    pub used_for: Vec<&'static str>,
    pub databases: Vec<String>,
    pub in_use: bool,
    pub mode: Option<String>,
    pub health: Option<&'static str>,
    pub split_brain_clean_rounds: Option<u32>,
    pub primary: Option<String>,
    pub unavailable_reason: Option<String>,
    pub generation: Option<u64>,
    pub read: Option<String>,
    pub last_probe_age_ms: Option<u64>,
    pub candidates: Vec<CandidateReport>,
}

#[derive(Debug, Serialize)]
pub(crate) struct CandidateReport {
    pub endpoint: String,
    pub observation: &'static str,
}

/// The statements and transactions of the last minute.
#[derive(Debug, Serialize)]
pub(crate) struct SqlReport {
    pub window_secs: u64,
    pub statements: u64,
    pub transactions: u64,
    /// Of statements and transactions.
    pub failures: u64,
    /// `None` without statements or transactions.
    pub success_rate: Option<f64>,
    /// Of statements and transactions.
    pub retries: u64,
    pub statement_latency_ms: Option<Latency>,
    /// Of every connection pool.
    pub checkout_latency_ms: Option<Latency>,
    /// Failed attempts by error class, when there were any.
    pub errors_by_class: BTreeMap<String, u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Latency {
    pub p50: i64,
    pub p99: i64,
}

/// The operations of counted blobstores in the last minute.
#[derive(Debug, Serialize)]
pub(crate) struct BlobstoreReport {
    pub window_secs: u64,
    pub operations: u64,
    pub errors: u64,
    /// Errors by blobstore, when there were any. One redacted entry for
    /// callers outside the admin group.
    pub errors_by_store: BTreeMap<String, u64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BookmarkCacheReport {
    pub max_staleness_secs: i64,
}

/// The health report of `repo`. `snapshot` is `None` without the in-memory
/// stats backend; `admin` is whether the caller may see server names and
/// paths.
pub(crate) fn build_report(
    repo: &str,
    metadata: &MetadataInput,
    snapshot: Option<&Snapshot>,
    exiting: bool,
    admin: bool,
) -> HealthReport {
    let sql = snapshot.map(sql_report);
    let blobstore = snapshot
        .and_then(blobstore_report)
        .map(|blobstore| redact_stores(blobstore, admin));
    let bookmark_cache = snapshot.and_then(|snapshot| bookmark_report(snapshot, repo));

    let mut checks = vec![
        Check::new("server", server_check(exiting)),
        Check::new("metadata_db", metadata_check(metadata)),
    ];
    if let Some(sql) = &sql {
        checks.push(Check::new("sql_queries", sql_check(sql)));
    }
    if let Some(blobstore) = &blobstore {
        checks.push(Check::new("blobstore", blobstore_check(blobstore)));
    }
    if let Some(bookmark_cache) = &bookmark_cache {
        checks.push(Check::new("bookmark_cache", bookmark_check(bookmark_cache)));
    }

    HealthReport {
        schema: SCHEMA,
        repo: repo.to_string(),
        status: overall(&checks),
        redacted: !admin,
        server: ServerReport {
            version: VERSION,
            build: BUILD,
            uptime_secs: snapshot.map(|snapshot| snapshot.uptime.as_secs()),
            exiting,
            metrics_available: snapshot.is_some(),
        },
        checks,
        metadata_db: metadata_report(metadata, admin),
        sql,
        blobstore,
        bookmark_cache,
    }
}

fn metadata_report(metadata: &MetadataInput, admin: bool) -> MetadataReport {
    match metadata {
        MetadataInput::Local { path } => MetadataReport {
            kind: "local",
            path: Some(shown(admin, path)),
            clusters: Vec::new(),
        },
        MetadataInput::Remote => MetadataReport {
            kind: "remote",
            path: None,
            clusters: Vec::new(),
        },
        MetadataInput::OssRemote { clusters } => MetadataReport {
            kind: "oss_remote",
            path: None,
            clusters: clusters
                .iter()
                .map(|cluster| cluster_report(cluster, admin))
                .collect(),
        },
    }
}

fn cluster_report(cluster: &ClusterInput, admin: bool) -> ClusterReport {
    let status = cluster.status.as_ref();
    ClusterReport {
        name: shown(admin, &cluster.name),
        used_for: cluster.used_for.clone(),
        databases: cluster.databases.clone(),
        in_use: status.is_some(),
        mode: status.map(|status| status.mode.to_string()),
        health: status.map(|status| health_name(status.health)),
        split_brain_clean_rounds: status.and_then(|status| match status.health {
            ClusterHealth::SplitBrain { clean_rounds } => Some(clean_rounds),
            _ => None,
        }),
        primary: status
            .and_then(|status| status.primary.as_ref())
            .map(|primary| shown(admin, &primary.to_string())),
        // It may name servers.
        unavailable_reason: status
            .and_then(|status| status.unavailable_reason.clone())
            .filter(|_| admin),
        generation: status.map(|status| status.generation),
        read: status
            .and_then(|status| status.read.as_ref())
            .map(|read| shown(admin, &read.to_string())),
        last_probe_age_ms: status
            .and_then(|status| status.last_round_age)
            .map(|age| u64::try_from(age.as_millis()).unwrap_or(u64::MAX)),
        candidates: status.map_or_else(Vec::new, |status| {
            status
                .candidates
                .iter()
                .map(|candidate| CandidateReport {
                    endpoint: shown(admin, &candidate.candidate.to_string()),
                    observation: observation_name(candidate.observation),
                })
                .collect()
        }),
    }
}

fn shown(admin: bool, value: &str) -> String {
    if admin {
        value.to_string()
    } else {
        REDACTED.to_string()
    }
}

fn health_name(health: ClusterHealth) -> &'static str {
    match health {
        ClusterHealth::Unchecked => "unchecked",
        ClusterHealth::Unknown => "unknown",
        ClusterHealth::Active => "active",
        ClusterHealth::Pending => "pending",
        ClusterHealth::NoPrimary => "no_primary",
        ClusterHealth::SplitBrain { .. } => "split_brain",
    }
}

fn observation_name(observation: CandidateObservation) -> &'static str {
    match observation {
        CandidateObservation::Writable => "writable",
        CandidateObservation::ReadOnly => "read_only",
        CandidateObservation::Busy => "busy",
        CandidateObservation::Silent => "silent",
    }
}

/// The `mononoke.sql_ext.queries` stats of the window, with the checkout
/// latency of every pool.
fn sql_report(snapshot: &Snapshot) -> SqlReport {
    let sum = |name: &str| recent_sum(snapshot, &format!("{QUERIES}.{name}"));
    let statements = sum("statements");
    let transactions = sum("transactions");
    let failures = sum("statement_failures") + sum("transaction_failures");
    let queries = statements + transactions;
    let success_rate =
        (queries > 0).then(|| queries.saturating_sub(failures) as f64 / queries as f64);

    let statement_latency_ms = snapshot
        .get(&format!("{QUERIES}.statement_ms"))
        .and_then(|stat| stat.recent.distribution.as_ref())
        .and_then(latency);
    let mut checkout = Distribution::new();
    for stat in &snapshot.stats {
        if stat.name.starts_with(CHECKOUT_PREFIX)
            && stat.name.ends_with(CHECKOUT_SUFFIX)
            && let Some(distribution) = &stat.recent.distribution
        {
            checkout.merge(distribution);
        }
    }

    let errors_by_class = snapshot
        .stats
        .iter()
        .filter_map(|stat| {
            let class = stat.name.strip_prefix(QUERY_ERRORS)?;
            let count = non_negative(stat.recent.sum);
            (count > 0).then(|| (class.to_string(), count))
        })
        .collect();

    SqlReport {
        window_secs: snapshot.window.as_secs(),
        statements,
        transactions,
        failures,
        success_rate,
        retries: sum("statement_retries") + sum("transaction_retries"),
        statement_latency_ms,
        checkout_latency_ms: latency(&checkout),
        errors_by_class,
    }
}

/// The `mononoke.blobstore.{store}.{op}` and `{op}_err` stats of the
/// window, or `None` without any `{op}` stat.
fn blobstore_report(snapshot: &Snapshot) -> Option<BlobstoreReport> {
    let mut counted = false;
    let mut operations = 0;
    let mut errors = 0;
    let mut errors_by_store = BTreeMap::new();
    for stat in &snapshot.stats {
        let Some((store, op)) = stat
            .name
            .strip_prefix(BLOBSTORE_PREFIX)
            .and_then(|name| name.rsplit_once('.'))
            .filter(|(store, _)| !store.is_empty())
        else {
            continue;
        };
        let count = non_negative(stat.recent.sum);
        if BLOBSTORE_OPERATIONS.contains(&op) {
            counted = true;
            operations += count;
        } else if op
            .strip_suffix("_err")
            .is_some_and(|op| BLOBSTORE_OPERATIONS.contains(&op))
            && count > 0
        {
            errors += count;
            *errors_by_store.entry(store.to_string()).or_default() += count;
        }
    }
    counted.then(|| BlobstoreReport {
        window_secs: snapshot.window.as_secs(),
        operations,
        errors,
        errors_by_store,
    })
}

/// `blobstore`, with its stores merged into one redacted entry unless the
/// caller is an admin: a store's name may name a server or a bucket.
fn redact_stores(blobstore: BlobstoreReport, admin: bool) -> BlobstoreReport {
    if admin || blobstore.errors_by_store.is_empty() {
        return blobstore;
    }
    BlobstoreReport {
        errors_by_store: BTreeMap::from([(REDACTED.to_string(), blobstore.errors)]),
        ..blobstore
    }
}

/// How far behind the warm bookmark cache of `repo` is, once it said.
fn bookmark_report(snapshot: &Snapshot, repo: &str) -> Option<BookmarkCacheReport> {
    snapshot
        .get(&format!(
            "mononoke.warm_bookmarks_cache.{repo}.max_staleness_secs"
        ))
        .filter(|stat| stat.kind == StatKind::Singleton && stat.total.count > 0)
        .map(|stat| BookmarkCacheReport {
            max_staleness_secs: stat.total.sum,
        })
}

fn recent_sum(snapshot: &Snapshot, name: &str) -> u64 {
    snapshot
        .get(name)
        .map_or(0, |stat| non_negative(stat.recent.sum))
}

fn non_negative(sum: i64) -> u64 {
    u64::try_from(sum).unwrap_or(0)
}

/// The median and 99th percentile of `distribution`, unless it is empty.
fn latency(distribution: &Distribution) -> Option<Latency> {
    Some(Latency {
        p50: distribution.quantile(0.5)?,
        p99: distribution.quantile(0.99)?,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mononoke_macros::mononoke;
    use sql_ext::facebook::Candidate;
    use sql_ext::facebook::CandidateStatus;
    use sql_ext::facebook::PrimaryCheck;
    use stats::memory_stats::StatSnapshot;
    use stats::memory_stats::Window;

    use super::*;

    fn window(sum: i64, count: u64) -> Window {
        Window {
            sum,
            count,
            distribution: None,
        }
    }

    /// A timeseries with `sum` in the window and more since it was created.
    fn timeseries(name: &str, sum: i64) -> StatSnapshot {
        StatSnapshot {
            name: name.to_string(),
            kind: StatKind::Timeseries,
            total: window(sum + 1000, 1000),
            recent: window(sum, sum.unsigned_abs()),
        }
    }

    /// A quantile stat with `values` in the window, and more before.
    fn quantile(name: &str, values: &[i64]) -> StatSnapshot {
        let mut distribution = Distribution::new();
        for value in values {
            distribution.record(*value, 1);
        }
        let recent = Window {
            sum: values.iter().sum(),
            count: values.len() as u64,
            distribution: Some(distribution.clone()),
        };
        let mut old = distribution;
        old.record(1_000_000, 10);
        StatSnapshot {
            name: name.to_string(),
            kind: StatKind::Quantile,
            total: Window {
                sum: recent.sum + 10_000_000,
                count: recent.count + 10,
                distribution: Some(old),
            },
            recent,
        }
    }

    fn singleton(name: &str, value: i64) -> StatSnapshot {
        StatSnapshot {
            name: name.to_string(),
            kind: StatKind::Singleton,
            total: window(value, 1),
            recent: window(value, 1),
        }
    }

    fn snapshot(mut stats: Vec<StatSnapshot>) -> Snapshot {
        stats.sort_by(|a, b| a.name.cmp(&b.name));
        Snapshot {
            uptime: Duration::from_secs(5234),
            window: Duration::from_millis(59_500),
            dropped: 0,
            stats,
        }
    }

    /// The stats of a busy server.
    fn busy() -> Snapshot {
        snapshot(vec![
            timeseries("mononoke.sql_ext.queries.statements", 395),
            timeseries("mononoke.sql_ext.queries.statement_failures", 3),
            timeseries("mononoke.sql_ext.queries.statement_retries", 2),
            timeseries("mononoke.sql_ext.queries.transactions", 5),
            timeseries("mononoke.sql_ext.queries.transaction_failures", 1),
            timeseries("mononoke.sql_ext.queries.transaction_retries", 1),
            timeseries("mononoke.sql_ext.queries.errors.timeout", 3),
            timeseries("mononoke.sql_ext.queries.errors.connection", 1),
            timeseries("mononoke.sql_ext.queries.errors.lock", 0),
            quantile("mononoke.sql_ext.queries.statement_ms", &[2, 2, 2, 7]),
            quantile("sql.mysql_ffi.bookmarks.get_connection_ms", &[1, 1]),
            quantile("sql.mysql_ffi.filenodes.get_connection_ms", &[3, 5]),
            quantile("sql.mysql_ffi.filenodes.raw_query_ms", &[100]),
            timeseries("mononoke.blobstore.s3.get", 30),
            timeseries("mononoke.blobstore.s3.get_ok", 29),
            timeseries("mononoke.blobstore.s3.get_err", 1),
            timeseries("mononoke.blobstore.s3.put", 10),
            timeseries("mononoke.blobstore.s3.put_err", 0),
            singleton("mononoke.warm_bookmarks_cache.toy.max_staleness_secs", 3),
            singleton(
                "mononoke.warm_bookmarks_cache.other.max_staleness_secs",
                5000,
            ),
        ])
    }

    fn db(port: u16) -> Candidate {
        Candidate::new(format!("db{}.internal", port - 3305), 3306)
    }

    fn pending() -> ClusterStatus {
        ClusterStatus {
            id: 7,
            name: "db1.internal:3306, db2.internal:3306".to_string(),
            mode: PrimaryCheck::Identity,
            health: ClusterHealth::Pending,
            primary: Some(db(3306)),
            unavailable_reason: None,
            generation: 3,
            read: Some(db(3306)),
            last_round_age: Some(Duration::from_millis(180)),
            candidates: vec![
                CandidateStatus {
                    candidate: db(3306),
                    observation: CandidateObservation::Silent,
                },
                CandidateStatus {
                    candidate: db(3307),
                    observation: CandidateObservation::Writable,
                },
            ],
        }
    }

    fn oss_remote(first: ClusterStatus) -> MetadataInput {
        MetadataInput::OssRemote {
            clusters: vec![
                ClusterInput {
                    used_for: vec!["production", "bookmarks"],
                    databases: vec!["mononoke".to_string()],
                    name: first.name.clone(),
                    status: Some(first),
                },
                ClusterInput {
                    used_for: vec!["filenodes"],
                    databases: vec!["filenodes".to_string()],
                    name: "/etc/mononoke/mysql-hosts".to_string(),
                    status: None,
                },
            ],
        }
    }

    fn json(report: &HealthReport) -> String {
        serde_json::to_string(report).expect("a report serializes")
    }

    #[mononoke::test]
    fn test_admin_report() {
        let report = build_report("toy", &oss_remote(pending()), Some(&busy()), false, true);
        let server = format!(
            r#"{{"version":"{VERSION}","build":{},"uptime_secs":5234,"exiting":false,"metrics_available":true}}"#,
            serde_json::to_string(&BUILD).unwrap()
        );
        let expected = [
            r#"{"schema":1,"repo":"toy","status":"degraded","redacted":false,"server":"#,
            &server,
            r#","checks":["#,
            r#"{"name":"server","status":"ok","detail":"serving"},"#,
            r#"{"name":"metadata_db","status":"degraded","detail":"cluster 1 (identity): a new primary awaits confirmation; cluster 2: not in use"},"#,
            r#"{"name":"sql_queries","status":"ok","detail":"99.0% of 400 statements and transactions succeeded in the last 59 s"},"#,
            r#"{"name":"blobstore","status":"ok","detail":"1 of 40 blobstore operations failed in the last 59 s"},"#,
            r#"{"name":"bookmark_cache","status":"ok","detail":"the warm bookmark cache is 3 s behind"}],"#,
            r#""metadata_db":{"kind":"oss_remote","path":null,"clusters":["#,
            r#"{"name":"db1.internal:3306, db2.internal:3306","used_for":["production","bookmarks"],"databases":["mononoke"],"#,
            r#""in_use":true,"mode":"identity","health":"pending","split_brain_clean_rounds":null,"#,
            r#""primary":"db1.internal:3306","unavailable_reason":null,"generation":3,"read":"db1.internal:3306","#,
            r#""last_probe_age_ms":180,"candidates":[{"endpoint":"db1.internal:3306","observation":"silent"},"#,
            r#"{"endpoint":"db2.internal:3306","observation":"writable"}]},"#,
            r#"{"name":"/etc/mononoke/mysql-hosts","used_for":["filenodes"],"databases":["filenodes"],"#,
            r#""in_use":false,"mode":null,"health":null,"split_brain_clean_rounds":null,"primary":null,"#,
            r#""unavailable_reason":null,"generation":null,"read":null,"last_probe_age_ms":null,"candidates":[]}]},"#,
            r#""sql":{"window_secs":59,"statements":395,"transactions":5,"failures":4,"success_rate":0.99,"#,
            r#""retries":3,"statement_latency_ms":{"p50":2,"p99":7},"checkout_latency_ms":{"p50":1,"p99":5},"#,
            r#""errors_by_class":{"connection":1,"timeout":3}},"#,
            r#""blobstore":{"window_secs":59,"operations":40,"errors":1,"errors_by_store":{"s3":1}},"#,
            r#""bookmark_cache":{"max_staleness_secs":3}}"#,
        ]
        .concat();
        assert_eq!(json(&report), expected);
    }

    #[mononoke::test]
    fn test_redaction() {
        let no_primary = ClusterStatus {
            health: ClusterHealth::NoPrimary,
            primary: None,
            unavailable_reason: Some("db1.internal:3306 is read-only".to_string()),
            ..pending()
        };
        let metadata = oss_remote(no_primary);
        let admin = json(&build_report("toy", &metadata, Some(&busy()), false, true));
        let other = json(&build_report("toy", &metadata, Some(&busy()), false, false));
        for shown in [":3306", "db1.internal", "db2.internal", "/etc/mononoke"] {
            assert!(admin.contains(shown), "{shown} in {admin}");
            assert!(!other.contains(shown), "{shown} in {other}");
        }
        assert!(admin.contains(r#""unavailable_reason":"db1.internal:3306 is read-only""#));
        assert!(other.contains(r#""redacted":true"#));
        assert!(other.contains(
            r#""name":"<redacted>","used_for":["production","bookmarks"],"databases":["mononoke"],"in_use":true,"mode":"identity","health":"no_primary","split_brain_clean_rounds":null,"primary":null,"unavailable_reason":null,"generation":3,"read":"<redacted>","last_probe_age_ms":180,"candidates":[{"endpoint":"<redacted>","observation":"silent"},{"endpoint":"<redacted>","observation":"writable"}]"#
        ));
        // The details are the same for everyone.
        let detail = r#""detail":"cluster 1 (identity): no usable primary: no server is writable; cluster 2: not in use""#;
        assert!(admin.contains(detail));
        assert!(other.contains(detail));

        // A local SQLite directory.
        let local = MetadataInput::Local {
            path: "/var/mononoke/sqlite".to_string(),
        };
        let admin = json(&build_report("toy", &local, None, false, true));
        let other = json(&build_report("toy", &local, None, false, false));
        assert!(admin.contains(
            r#""metadata_db":{"kind":"local","path":"/var/mononoke/sqlite","clusters":[]}"#
        ));
        assert!(
            other.contains(r#""metadata_db":{"kind":"local","path":"<redacted>","clusters":[]}"#)
        );
    }

    #[mononoke::test]
    fn test_redacted_blobstore_names() {
        // S3 stores are named after their bucket.
        let failing = snapshot(vec![
            timeseries("mononoke.blobstore.s3.team-bucket.get", 30),
            timeseries("mononoke.blobstore.s3.team-bucket.get_err", 2),
            timeseries("mononoke.blobstore.sqlblob.sqlite.put", 10),
            timeseries("mononoke.blobstore.sqlblob.sqlite.put_err", 1),
        ]);
        let admin = json(&build_report(
            "toy",
            &MetadataInput::Remote,
            Some(&failing),
            false,
            true,
        ));
        let other = json(&build_report(
            "toy",
            &MetadataInput::Remote,
            Some(&failing),
            false,
            false,
        ));
        assert!(admin.contains(
            r#""blobstore":{"window_secs":59,"operations":40,"errors":3,"errors_by_store":{"s3.team-bucket":2,"sqlblob.sqlite":1}}"#
        ));
        assert!(other.contains(
            r#""blobstore":{"window_secs":59,"operations":40,"errors":3,"errors_by_store":{"<redacted>":3}}"#
        ));
        assert!(!other.contains("team-bucket"), "{other}");
    }

    #[mononoke::test]
    fn test_without_metrics() {
        let report = build_report("toy", &MetadataInput::Remote, None, true, true);
        let server = format!(
            r#""server":{{"version":"{VERSION}","build":{},"uptime_secs":null,"exiting":true,"metrics_available":false}}"#,
            serde_json::to_string(&BUILD).unwrap()
        );
        let expected = [
            r#"{"schema":1,"repo":"toy","status":"degraded","redacted":false,"#,
            &server,
            r#","checks":[{"name":"server","status":"degraded","detail":"the server is shutting down"},"#,
            r#"{"name":"metadata_db","status":"ok","detail":"remote database, not monitored by this server"}],"#,
            r#""metadata_db":{"kind":"remote","path":null,"clusters":[]},"#,
            r#""sql":null,"blobstore":null,"bookmark_cache":null}"#,
        ]
        .concat();
        assert_eq!(json(&report), expected);
    }

    #[mononoke::test]
    fn test_quiet_server() {
        // Nothing happened yet: no queries, no counted blobstore, and no
        // warm bookmark cache for this repo.
        let quiet = snapshot(vec![
            singleton("mononoke.warm_bookmarks_cache.other.max_staleness_secs", 0),
            StatSnapshot {
                name: "mononoke.warm_bookmarks_cache.toy.max_staleness_secs".to_string(),
                kind: StatKind::Singleton,
                total: window(0, 0),
                recent: window(0, 0),
            },
        ]);
        let report = build_report(
            "toy",
            &MetadataInput::Local {
                path: "/var/mononoke/sqlite".to_string(),
            },
            Some(&quiet),
            false,
            true,
        );
        assert_eq!(report.status, Status::Ok);
        let names: Vec<&str> = report.checks.iter().map(|check| check.name).collect();
        assert_eq!(names, ["server", "metadata_db", "sql_queries"]);
        let sql = report.sql.expect("metrics are available");
        assert_eq!(sql.statements + sql.transactions, 0);
        assert_eq!(sql.success_rate, None);
        assert_eq!(sql.statement_latency_ms, None);
        assert_eq!(sql.checkout_latency_ms, None);
        assert!(sql.errors_by_class.is_empty());
        assert!(report.blobstore.is_none());
        assert!(report.bookmark_cache.is_none());
        assert_eq!(report.server.uptime_secs, Some(5234));
    }

    #[mononoke::test]
    fn test_degraded_by_metrics() {
        let failing = snapshot(vec![
            timeseries("mononoke.sql_ext.queries.statements", 20),
            timeseries("mononoke.sql_ext.queries.statement_failures", 2),
            timeseries("mononoke.blobstore.sqlblob.shard.get", 10),
            timeseries("mononoke.blobstore.sqlblob.shard.get_err", 5),
            timeseries("mononoke.blobstore.cache.put", 10),
            singleton("mononoke.warm_bookmarks_cache.toy.max_staleness_secs", 901),
        ]);
        let report = build_report("toy", &MetadataInput::Remote, Some(&failing), false, false);
        assert_eq!(report.status, Status::Degraded);
        let statuses: Vec<(&str, Status)> = report
            .checks
            .iter()
            .map(|check| (check.name, check.status))
            .collect();
        assert_eq!(
            statuses,
            [
                ("server", Status::Ok),
                ("metadata_db", Status::Ok),
                ("sql_queries", Status::Degraded),
                ("blobstore", Status::Degraded),
                ("bookmark_cache", Status::Degraded),
            ]
        );
        let blobstore = report.blobstore.expect("counted blobstores");
        assert_eq!((blobstore.operations, blobstore.errors), (20, 5));
        // Not an admin: the stores are not named.
        assert_eq!(
            blobstore.errors_by_store,
            BTreeMap::from([(REDACTED.to_string(), 5)])
        );
    }
}
