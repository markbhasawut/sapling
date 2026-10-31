/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! How a health report judges what it shows. Details never name servers or
//! paths: they are shown to every caller.

use serde::Serialize;
use sql_ext::facebook::ClusterHealth;
use sql_ext::facebook::ClusterStatus;

use super::report::BlobstoreReport;
use super::report::BookmarkCacheReport;
use super::report::MetadataInput;
use super::report::SqlReport;

/// Fewer statements and transactions than this in the window are too few
/// to judge their success rate.
pub(crate) const MIN_QUERIES: u64 = 20;
/// The lowest share of statements and transactions that succeed before
/// the server is degraded.
pub(crate) const MIN_SUCCESS_RATE: f64 = 0.95;
/// Fewer blobstore operations than this in the window are too few to judge
/// their error rate.
pub(crate) const MIN_BLOBSTORE_OPERATIONS: u64 = 20;
/// The highest share of blobstore operations that fail before the server is
/// degraded.
pub(crate) const MAX_BLOBSTORE_ERROR_RATE: f64 = 0.05;
/// How far behind the warm bookmark cache may be before the server is
/// degraded.
pub(crate) const MAX_BOOKMARK_STALENESS_SECS: i64 = 900;

/// How healthy something is, from best to worst.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Status {
    Ok,
    Degraded,
    Down,
}

/// One aspect of the server, as judged.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Check {
    pub name: &'static str,
    pub status: Status,
    pub detail: String,
}

impl Check {
    pub(crate) fn new(name: &'static str, (status, detail): (Status, String)) -> Self {
        Self {
            name,
            status,
            detail,
        }
    }
}

pub(crate) fn server_check(exiting: bool) -> (Status, String) {
    if exiting {
        (Status::Degraded, "the server is shutting down".to_string())
    } else {
        (Status::Ok, "serving".to_string())
    }
}

/// The metadata database of the repo: for MySQL, the worst of its clusters,
/// numbered from 1 in the order of the config.
pub(crate) fn metadata_check(metadata: &MetadataInput) -> (Status, String) {
    match metadata {
        MetadataInput::Local { .. } => (Status::Ok, "local SQLite database".to_string()),
        MetadataInput::Remote => (
            Status::Ok,
            "remote database, not monitored by this server".to_string(),
        ),
        MetadataInput::OssRemote { clusters } => {
            let mut worst = Status::Ok;
            let mut details = Vec::with_capacity(clusters.len());
            for (index, cluster) in clusters.iter().enumerate() {
                let (status, detail) = cluster_check(cluster.status.as_ref());
                worst = worst.max(status);
                let number = index + 1;
                details.push(match &cluster.status {
                    Some(cluster) => format!("cluster {number} ({}): {detail}", cluster.mode),
                    None => format!("cluster {number}: {detail}"),
                });
            }
            (worst, details.join("; "))
        }
    }
}

/// A MySQL cluster, or `None` when no store of this process uses it.
pub(crate) fn cluster_check(cluster: Option<&ClusterStatus>) -> (Status, String) {
    let Some(cluster) = cluster else {
        return (Status::Ok, "not in use".to_string());
    };
    match cluster.health {
        ClusterHealth::Unchecked => (
            Status::Ok,
            "not probed (--oss-mysql-primary-check none)".to_string(),
        ),
        // The 3 is the prober's SPLIT_BRAIN_CLEAN_ROUNDS.
        ClusterHealth::SplitBrain { .. } => (
            Status::Down,
            "split brain: writes are refused until one server stays writable for 3 rounds"
                .to_string(),
        ),
        health if cluster.primary.is_none() => {
            let reason = match health {
                ClusterHealth::NoPrimary => "no server is writable",
                ClusterHealth::Pending => "a new primary awaits confirmation",
                ClusterHealth::Active => "no primary yet",
                ClusterHealth::Unknown
                | ClusterHealth::Unchecked
                | ClusterHealth::SplitBrain { .. } => "no server answered",
            };
            (Status::Down, format!("no usable primary: {reason}"))
        }
        ClusterHealth::Pending => (
            Status::Degraded,
            "a new primary awaits confirmation".to_string(),
        ),
        ClusterHealth::NoPrimary => (
            Status::Degraded,
            "the primary stopped answering or became read-only".to_string(),
        ),
        ClusterHealth::Unknown => (
            Status::Degraded,
            "no server answered the last probe round".to_string(),
        ),
        ClusterHealth::Active => (Status::Ok, "primary active".to_string()),
    }
}

pub(crate) fn sql_check(sql: &SqlReport) -> (Status, String) {
    let queries = sql.statements + sql.transactions;
    let window = sql.window_secs;
    match sql.success_rate {
        Some(rate) if queries >= MIN_QUERIES => {
            let detail = format!(
                "{:.1}% of {queries} statements and transactions succeeded in the last {window} s",
                rate * 100.0
            );
            if rate < MIN_SUCCESS_RATE {
                (Status::Degraded, detail)
            } else {
                (Status::Ok, detail)
            }
        }
        _ => (
            Status::Ok,
            format!(
                "{queries} statements and transactions in the last {window} s, too few to judge"
            ),
        ),
    }
}

pub(crate) fn blobstore_check(blobstore: &BlobstoreReport) -> (Status, String) {
    let operations = blobstore.operations;
    let errors = blobstore.errors;
    let window = blobstore.window_secs;
    if operations < MIN_BLOBSTORE_OPERATIONS {
        return (
            Status::Ok,
            format!("{operations} blobstore operations in the last {window} s, too few to judge"),
        );
    }
    let detail =
        format!("{errors} of {operations} blobstore operations failed in the last {window} s");
    if errors as f64 / operations as f64 > MAX_BLOBSTORE_ERROR_RATE {
        (Status::Degraded, detail)
    } else {
        (Status::Ok, detail)
    }
}

pub(crate) fn bookmark_check(bookmark_cache: &BookmarkCacheReport) -> (Status, String) {
    let staleness = bookmark_cache.max_staleness_secs;
    let detail = format!("the warm bookmark cache is {staleness} s behind");
    if staleness > MAX_BOOKMARK_STALENESS_SECS {
        (Status::Degraded, detail)
    } else {
        (Status::Ok, detail)
    }
}

/// The worst status of `checks`.
pub(crate) fn overall(checks: &[Check]) -> Status {
    checks
        .iter()
        .map(|check| check.status)
        .max()
        .unwrap_or(Status::Ok)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mononoke_macros::mononoke;
    use sql_ext::facebook::Candidate;
    use sql_ext::facebook::PrimaryCheck;

    use super::*;
    use crate::server_health::report::ClusterInput;

    fn cluster(health: ClusterHealth, primary: bool) -> ClusterStatus {
        let db1 = Candidate::new("db1.internal", 3306);
        ClusterStatus {
            id: 1,
            name: "db1.internal:3306, db2.internal:3306".to_string(),
            mode: PrimaryCheck::Identity,
            health,
            primary: primary.then(|| db1.clone()),
            unavailable_reason: (!primary).then(|| "no writable server".to_string()),
            generation: 1,
            read: Some(db1),
            last_round_age: Some(Duration::from_millis(100)),
            candidates: Vec::new(),
        }
    }

    fn check_of(health: ClusterHealth, primary: bool) -> (Status, String) {
        cluster_check(Some(&cluster(health, primary)))
    }

    fn judged(status: Status, detail: &str) -> (Status, String) {
        (status, detail.to_string())
    }

    #[mononoke::test]
    fn test_cluster_check() {
        assert_eq!(cluster_check(None), judged(Status::Ok, "not in use"));
        let unchecked = ClusterStatus {
            mode: PrimaryCheck::None,
            ..cluster(ClusterHealth::Unchecked, true)
        };
        assert_eq!(
            cluster_check(Some(&unchecked)),
            judged(Status::Ok, "not probed (--oss-mysql-primary-check none)")
        );
        let split_brain = judged(
            Status::Down,
            "split brain: writes are refused until one server stays writable for 3 rounds",
        );
        for primary in [false, true] {
            assert_eq!(
                check_of(ClusterHealth::SplitBrain { clean_rounds: 2 }, primary),
                split_brain
            );
        }

        // Without a usable primary, by what the last round found.
        for (health, reason) in [
            (ClusterHealth::NoPrimary, "no server is writable"),
            (ClusterHealth::Unknown, "no server answered"),
            (ClusterHealth::Pending, "a new primary awaits confirmation"),
            (ClusterHealth::Active, "no primary yet"),
        ] {
            assert_eq!(
                check_of(health, false),
                (Status::Down, format!("no usable primary: {reason}")),
                "{health:?}"
            );
        }

        // With one.
        assert_eq!(
            check_of(ClusterHealth::Pending, true),
            judged(Status::Degraded, "a new primary awaits confirmation")
        );
        assert_eq!(
            check_of(ClusterHealth::NoPrimary, true),
            judged(
                Status::Degraded,
                "the primary stopped answering or became read-only"
            )
        );
        assert_eq!(
            check_of(ClusterHealth::Unknown, true),
            judged(Status::Degraded, "no server answered the last probe round")
        );
        assert_eq!(
            check_of(ClusterHealth::Active, true),
            judged(Status::Ok, "primary active")
        );
    }

    #[mononoke::test]
    fn test_metadata_check() {
        assert_eq!(
            metadata_check(&MetadataInput::Local {
                path: "/var/mononoke/sqlite".to_string()
            }),
            judged(Status::Ok, "local SQLite database")
        );
        assert_eq!(
            metadata_check(&MetadataInput::Remote),
            judged(Status::Ok, "remote database, not monitored by this server")
        );

        // The worst cluster, and every cluster's text.
        let input = |status: Option<ClusterStatus>| ClusterInput {
            used_for: vec!["production"],
            databases: vec!["mononoke".to_string()],
            name: "db1.internal:3306, db2.internal:3306".to_string(),
            status,
        };
        let metadata = MetadataInput::OssRemote {
            clusters: vec![
                input(Some(cluster(ClusterHealth::Active, true))),
                input(Some(cluster(ClusterHealth::Pending, true))),
                input(None),
            ],
        };
        assert_eq!(
            metadata_check(&metadata),
            judged(
                Status::Degraded,
                "cluster 1 (identity): primary active; \
                 cluster 2 (identity): a new primary awaits confirmation; \
                 cluster 3: not in use"
            )
        );
        let metadata = MetadataInput::OssRemote {
            clusters: vec![
                input(Some(cluster(ClusterHealth::NoPrimary, false))),
                input(Some(cluster(ClusterHealth::Pending, true))),
            ],
        };
        assert_eq!(metadata_check(&metadata).0, Status::Down);
    }

    fn sql(statements: u64, transactions: u64, failures: u64) -> SqlReport {
        let queries = statements + transactions;
        SqlReport {
            window_secs: 60,
            statements,
            transactions,
            failures,
            success_rate: (queries > 0)
                .then(|| queries.saturating_sub(failures) as f64 / queries as f64),
            retries: 0,
            statement_latency_ms: None,
            checkout_latency_ms: None,
            errors_by_class: Default::default(),
        }
    }

    #[mononoke::test]
    fn test_sql_check() {
        assert_eq!(
            sql_check(&sql(0, 0, 0)),
            judged(
                Status::Ok,
                "0 statements and transactions in the last 60 s, too few to judge"
            )
        );
        // Every one failed, but there are too few to tell.
        assert_eq!(
            sql_check(&sql(19, 0, 19)),
            judged(
                Status::Ok,
                "19 statements and transactions in the last 60 s, too few to judge"
            )
        );
        assert_eq!(
            sql_check(&sql(12, 8, 2)),
            judged(
                Status::Degraded,
                "90.0% of 20 statements and transactions succeeded in the last 60 s"
            )
        );
        assert_eq!(
            sql_check(&sql(100, 0, 5)),
            judged(
                Status::Ok,
                "95.0% of 100 statements and transactions succeeded in the last 60 s"
            )
        );
        assert_eq!(sql_check(&sql(100, 0, 6)).0, Status::Degraded);
    }

    fn blobstore(operations: u64, errors: u64) -> BlobstoreReport {
        BlobstoreReport {
            window_secs: 60,
            operations,
            errors,
            errors_by_store: Default::default(),
        }
    }

    #[mononoke::test]
    fn test_blobstore_check() {
        assert_eq!(
            blobstore_check(&blobstore(19, 19)),
            judged(
                Status::Ok,
                "19 blobstore operations in the last 60 s, too few to judge"
            )
        );
        assert_eq!(
            blobstore_check(&blobstore(20, 2)),
            judged(
                Status::Degraded,
                "2 of 20 blobstore operations failed in the last 60 s"
            )
        );
        assert_eq!(
            blobstore_check(&blobstore(20, 1)),
            judged(
                Status::Ok,
                "1 of 20 blobstore operations failed in the last 60 s"
            )
        );
    }

    #[mononoke::test]
    fn test_bookmark_check() {
        let staleness =
            |max_staleness_secs| bookmark_check(&BookmarkCacheReport { max_staleness_secs });
        assert_eq!(
            staleness(900),
            judged(Status::Ok, "the warm bookmark cache is 900 s behind")
        );
        assert_eq!(
            staleness(901),
            judged(Status::Degraded, "the warm bookmark cache is 901 s behind")
        );
    }

    #[mononoke::test]
    fn test_server_check_and_overall() {
        assert_eq!(server_check(false), judged(Status::Ok, "serving"));
        assert_eq!(
            server_check(true),
            judged(Status::Degraded, "the server is shutting down")
        );

        let check = |status| Check {
            name: "server",
            status,
            detail: String::new(),
        };
        assert_eq!(overall(&[]), Status::Ok);
        assert_eq!(overall(&[check(Status::Ok), check(Status::Ok)]), Status::Ok);
        assert_eq!(
            overall(&[check(Status::Ok), check(Status::Degraded)]),
            Status::Degraded
        );
        assert_eq!(
            overall(&[
                check(Status::Down),
                check(Status::Degraded),
                check(Status::Ok)
            ]),
            Status::Down
        );
    }
}
