/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! The `mononoke.sql_ext.queries` stats of OSS builds, read back from the
//! in-memory stats backend. The backend must be installed before any stat
//! is used, so this runs in its own process, as a single test.

#![cfg(not(fbcode_build))]

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use anyhow::Result;
use anyhow::anyhow;
use fbinit::FacebookInit;
use metadata::Metadata;
use mononoke_macros::mononoke;
use sql::mysql_async::Error as MysqlError;
use sql::mysql_async::ServerError;
use sql_common::mysql::OssErrorKind;
use sql_common::mysql::OssMysqlError;
use sql_common::mysql::OssPhase;
use sql_common::mysql::OssStmt;
use sql_ext::_macro_internal::TelemetryGranularity;
use sql_ext::_macro_internal::query_with_retry_no_cache;
use sql_ext::SqlQueryTelemetry;
use stats::memory_stats;

fn oss_error(phase: OssPhase, kind: OssErrorKind) -> anyhow::Error {
    anyhow::Error::from(OssMysqlError {
        phase,
        stmt: OssStmt::Unknown,
        endpoint: Arc::from("127.0.0.1:13306"),
        generation: 0,
        kind,
    })
}

/// The sum of the stat `mononoke.sql_ext.queries.{name}` since it was
/// created, and how many values it got.
fn total(name: &str) -> (i64, u64) {
    let snapshot = memory_stats::snapshot().expect("the backend is installed");
    snapshot
        .get(&format!("mononoke.sql_ext.queries.{name}"))
        .map_or((0, 0), |stat| (stat.total.sum, stat.total.count))
}

#[mononoke::fbinit_test]
async fn test_queries_are_counted(fb: FacebookInit) -> Result<()> {
    assert!(memory_stats::install());
    tokio::time::pause();

    // A transaction that waits for a primary once, then commits.
    let value = sql_ext::retry_transaction("t", |attempt| async move {
        match attempt {
            1 => Err(oss_error(
                OssPhase::Checkout,
                OssErrorKind::Unavailable(Arc::from("test")),
            )),
            _ => Ok(attempt),
        }
    })
    .await?;
    assert_eq!(value, 2);
    assert_eq!(total("transactions").0, 1);
    assert_eq!(total("transaction_retries").0, 1);
    assert_eq!(total("transaction_failures").0, 0);
    assert_eq!(total("errors.no_primary").0, 1);

    // Transactions that fail for good: a duplicate key, and a failure
    // outside MySQL.
    let duplicate = || {
        oss_error(
            OssPhase::InTransaction,
            OssErrorKind::Mysql(MysqlError::Server(ServerError {
                code: 1062,
                message: "Duplicate entry".to_string(),
                state: "23000".to_string(),
            })),
        )
    };
    let result: Result<()> =
        sql_ext::retry_transaction("t", |_| async move { Err(duplicate()) }).await;
    assert!(result.is_err());
    let result: Result<()> = sql_ext::retry_transaction("t", |_| async { Err(anyhow!("x")) }).await;
    assert!(result.is_err());
    assert_eq!(total("transactions").0, 3);
    assert_eq!(total("transaction_retries").0, 1);
    assert_eq!(total("transaction_failures").0, 2);
    assert_eq!(total("errors.server").0, 1);
    assert_eq!(total("errors.non_mysql").0, 1);
    assert_eq!(total("statements"), (0, 0));

    // Autocommit statements: one that succeeds, and one that fails outside
    // MySQL, which is run once more.
    let telemetry = SqlQueryTelemetry::new(fb, Metadata::default(), None);
    let run = |fail: bool| {
        let telemetry = &telemetry;
        async move {
            let attempts = AtomicUsize::new(0);
            let result: Result<((), usize)> = query_with_retry_no_cache(
                |_| {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    async move { if fail { Err(anyhow!("row")) } else { Ok(()) } }
                },
                "shard",
                "Query",
                telemetry,
                TelemetryGranularity::Query,
                &[],
            )
            .await;
            (result.is_ok(), attempts.into_inner())
        }
    };
    assert_eq!(run(false).await, (true, 1));
    assert_eq!(run(true).await, (false, 2));
    assert_eq!(total("statements").0, 2);
    assert_eq!(total("statement_retries").0, 1);
    assert_eq!(total("statement_failures").0, 1);
    assert_eq!(total("errors.non_mysql").0, 3);
    // One latency per statement, retry waits included.
    let (_, latencies) = total("statement_ms");
    assert_eq!(latencies, 2);
    let snapshot = memory_stats::snapshot().expect("the backend is installed");
    let statement_ms = snapshot
        .get("mononoke.sql_ext.queries.statement_ms")
        .expect("a latency stat");
    assert_eq!(statement_ms.kind, memory_stats::StatKind::Quantile);
    assert!(statement_ms.total.sum >= 10_000, "{statement_ms:?}");
    Ok(())
}
