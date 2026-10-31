/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! How OSS builds retry failed SQL statements and transactions.
//!
//! An autocommit statement is run again only if the failure says it had no
//! effect, or if it reads: a write that may have been applied surfaces as an
//! error instead. Attempts after MySQL errors are spaced 1, 2 and 4 seconds
//! apart, plus jitter, which covers a failover the prober notices within a
//! second or two. A failure outside MySQL, for example in SQLite, is retried
//! once after 10 to 15 seconds, as before failover support.
//!
//! A whole transaction is run again on the same schedule unless its commit
//! may have been applied, for callers that run it through
//! [`run_transaction`]. A failure that is not retried any more, and was not
//! final in itself, is logged with whether it may have taken effect.
//!
//! Both count their runs, failures, retries and failed attempts by
//! [`error_class`], in the `mononoke.sql_ext.queries` stats, for SQLite and
//! MySQL alike.

use std::future::Future;
use std::time::Duration;

use anyhow::Result;
use futures_retry::backoff::ExponentialBackoff;
use futures_retry::backoff::Jitter;
use sql_common::mysql::OssPhase;
use sql_common::mysql::OssStmt;
use stats::prelude::*;
use tokio::time::Instant;

use super::classify::Disposition;
use super::classify::ErrorScope;
use super::classify::classify;
use super::classify::error_class;

define_stats! {
    prefix = "mononoke.sql_ext.queries";
    statements: timeseries(Sum),
    statement_failures: timeseries(Sum),
    statement_retries: timeseries(Sum),
    statement_ms: quantile_stat(Average, Count; P 50, P 95, P 99; Duration::from_secs(60)),
    transactions: timeseries(Sum),
    transaction_failures: timeseries(Sum),
    transaction_retries: timeseries(Sum),
    errors: dynamic_timeseries("errors.{}", (class: &'static str); Sum),
}

/// The wait before the first retry after a MySQL error. Each later wait
/// doubles.
pub const RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// The factor by which each wait grows.
const RETRY_BACKOFF_BASE: f64 = 2.0;

/// The most random delay added to each wait.
pub const RETRY_JITTER: Duration = Duration::from_secs(1);

/// The wait before the retry of a statement that failed outside MySQL, as
/// before failover support. SQLite waits up to 10 seconds for a lock before
/// it fails, so this gives a writer in another process time to finish.
pub const NON_MYSQL_RETRY_INTERVAL: Duration = Duration::from_secs(10);

/// The factor by which each wait grows after a failure outside MySQL.
const NON_MYSQL_RETRY_BACKOFF_BASE: f64 = 1.2;

/// The most random delay added to each wait after a failure outside MySQL.
pub const NON_MYSQL_RETRY_JITTER: Duration = Duration::from_secs(5);

/// Attempts of an autocommit statement, the first one included.
pub const AUTOCOMMIT_ATTEMPTS: usize = 4;

/// Attempts of a statement that failed outside MySQL, for example in SQLite
/// or while parsing a row: one retry, as before failover support.
pub const NON_MYSQL_ATTEMPTS: usize = 2;

/// Attempts of a whole transaction run with [`run_transaction`], the first
/// one included: as many as of an autocommit statement.
pub const TRANSACTION_ATTEMPTS: usize = AUTOCOMMIT_ATTEMPTS;

/// Whether to run again an autocommit statement whose attempt `attempt`
/// (counted from 1) failed with `err`.
pub fn should_retry_autocommit(attempt: usize, err: &anyhow::Error) -> bool {
    let attempts = match classify(err, ErrorScope::Query) {
        Some(class) if class.retry_autocommit() => AUTOCOMMIT_ATTEMPTS,
        Some(_) => return false,
        None => NON_MYSQL_ATTEMPTS,
    };
    attempt < attempts
}

/// Whether to run again a whole closure that failed with `err`, usually one
/// that runs a transaction. Nothing is committed before COMMIT, so only a
/// commit with an unknown outcome is never retried. An autocommit statement
/// that the closure runs outside its transaction keeps its own verdict: a
/// write that may have been applied is not run again.
pub fn should_retry_transaction(err: &anyhow::Error) -> bool {
    match classify(err, ErrorScope::Transaction) {
        Some(class) if class.phase == OssPhase::Execute => class.retry_autocommit(),
        Some(class) => class.retry_transaction(),
        None => false,
    }
}

/// What a failure that is not retried may have done, for logs: `None` when
/// a retry would have failed the same way, or it is not a MySQL failure.
pub fn unretried_outcome(err: &anyhow::Error, scope: ErrorScope) -> Option<&'static str> {
    let class = classify(err, scope)?;
    match class.disposition {
        Disposition::Permanent => None,
        Disposition::Safe => Some("it had no effect"),
        Disposition::Ambiguous => Some(match class.phase {
            OssPhase::Commit => "it may have been committed",
            OssPhase::InTransaction => "nothing was committed",
            OssPhase::Checkout | OssPhase::Execute if class.stmt == OssStmt::Read => {
                "it had no effect"
            }
            OssPhase::Checkout | OssPhase::Execute => "it may have been applied",
        }),
    }
}

/// The wait before running again a statement whose attempt `attempt`
/// (counted from 1) failed with `err`. The schedule depends on whether `err`
/// is a MySQL error; the wait grows with the attempt number either way.
fn retry_wait(attempt: usize, err: &anyhow::Error) -> Duration {
    let (interval, base, jitter) = match classify(err, ErrorScope::Query) {
        Some(_) => (RETRY_INTERVAL, RETRY_BACKOFF_BASE, RETRY_JITTER),
        None => (
            NON_MYSQL_RETRY_INTERVAL,
            NON_MYSQL_RETRY_BACKOFF_BASE,
            NON_MYSQL_RETRY_JITTER,
        ),
    };
    // The waits of futures_retry's exponential_backoff and jitter.
    Jitter::new(ExponentialBackoff::new(interval, base), jitter)
        .nth(attempt.saturating_sub(1))
        .unwrap_or(interval)
}

/// Runs a whole transaction with `do_transaction`, which gets the attempt
/// number, and runs it again while [`should_retry_transaction`] allows, up
/// to [`TRANSACTION_ATTEMPTS`] attempts in all, after the wait
/// [`retry_wait`] gives. A transaction open on a primary that is demoted
/// fails with 1290 and commits nothing: the next attempt waits for the new
/// primary. `name` names the transaction in logs.
pub async fn run_transaction<T, Fut>(
    name: &str,
    mut do_transaction: impl FnMut(usize) -> Fut,
) -> Result<T>
where
    Fut: Future<Output = Result<T>>,
{
    let mut attempt = 1;
    let result = loop {
        let err = match do_transaction(attempt).await {
            Ok(value) => break Ok(value),
            Err(err) => err,
        };
        STATS::errors.add_value(1, (error_class(&err, ErrorScope::Transaction),));
        if attempt >= TRANSACTION_ATTEMPTS || !should_retry_transaction(&err) {
            if let Some(outcome) = unretried_outcome(&err, ErrorScope::Transaction) {
                tracing::warn!(
                    "MySQL transaction {name} failed in attempt {attempt}, and is not run \
                     again ({outcome}): {err:#}"
                );
            }
            break Err(err);
        }
        tracing::info!(
            "MySQL transaction {name} failed in attempt {attempt}, and runs again: {err:#}"
        );
        STATS::transaction_retries.add_value(1);
        tokio::time::sleep(retry_wait(attempt, &err)).await;
        attempt += 1;
    };
    STATS::transactions.add_value(1);
    if result.is_err() {
        STATS::transaction_failures.add_value(1);
    }
    result
}

/// Runs an autocommit statement with `do_query`, which gets the attempt
/// number, and runs it again while [`should_retry_autocommit`] allows,
/// after the wait [`retry_wait`] gives. `on_error` sees every failed attempt,
/// with whether it will be retried. Returns the result with the number of
/// the attempt that gave it.
pub async fn run_autocommit<T, Fut>(
    do_query: impl Fn(usize) -> Fut + Send + Sync,
    mut on_error: impl FnMut(usize, &anyhow::Error, bool) + Send,
) -> Result<(T, usize)>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T>>,
{
    let start = Instant::now();
    let mut attempt = 1;
    let result = loop {
        let err = match do_query(attempt).await {
            Ok(value) => break Ok((value, attempt)),
            Err(err) => err,
        };
        STATS::errors.add_value(1, (error_class(&err, ErrorScope::Query),));
        let will_retry = should_retry_autocommit(attempt, &err);
        on_error(attempt, &err, will_retry);
        if !will_retry {
            break Err(err);
        }
        STATS::statement_retries.add_value(1);
        tokio::time::sleep(retry_wait(attempt, &err)).await;
        attempt += 1;
    };
    STATS::statements.add_value(1);
    if result.is_err() {
        STATS::statement_failures.add_value(1);
    }
    STATS::statement_ms.add_value(start.elapsed().as_millis() as i64);
    result
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use anyhow::anyhow;
    use fbinit::FacebookInit;
    use justknobs::test_helpers::JustKnobsInMemory;
    use justknobs::test_helpers::KnobVal;
    use justknobs::test_helpers::override_just_knobs;
    use maplit::hashmap;
    use metadata::Metadata;
    use mononoke_macros::mononoke;
    use sql::mysql_async::DriverError;
    use sql::mysql_async::Error as MysqlError;
    use sql::mysql_async::IoError;
    use sql::mysql_async::ServerError;
    use sql_common::mysql::OssErrorKind;
    use sql_common::mysql::OssMysqlError;
    use sql_common::mysql::OssStmt;
    use sql_query_telemetry::SqlQueryTelemetry;
    use tokio::time::Instant;

    use super::super::classify::Disposition;
    use super::super::fake_mysqld::FakeMysqld;
    use super::super::fake_mysqld::Reply;
    use super::*;
    use crate::mononoke_queries;
    use crate::mononoke_queries::query_with_retry_no_cache;
    use crate::should_retry_query;
    use crate::telemetry::TelemetryGranularity;

    /// Paused timers fire on whole milliseconds, so a wait may last up to that
    /// much longer.
    const SLACK: Duration = Duration::from_millis(2);

    mononoke_queries! {
        read FakeRead(id: u64) -> (u64) {
            "SELECT id FROM fake WHERE id = {id}"
        }
        write FakeWrite(values: (id: u64)) {
            none,
            "INSERT INTO fake (id) VALUES {values}"
        }
    }

    /// A way to fail, made fresh for every attempt.
    #[derive(Clone, Copy, Debug)]
    enum Failure {
        Server(u16),
        Io,
        Closed,
        AuthPlugin,
        Timeout,
        Unavailable,
    }

    fn mysql_error(failure: Failure) -> MysqlError {
        match failure {
            Failure::Server(code) => MysqlError::Server(ServerError {
                code,
                message: "test error".to_string(),
                state: "HY000".to_string(),
            }),
            Failure::Io => MysqlError::Io(IoError::Io(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "connection reset by peer",
            ))),
            Failure::Closed => MysqlError::Driver(DriverError::ConnectionClosed),
            Failure::AuthPlugin => MysqlError::Driver(DriverError::UnknownAuthPlugin {
                name: "auth_gssapi_client".to_string(),
            }),
            Failure::Timeout | Failure::Unavailable => {
                panic!("{failure:?} is not a mysql_async error")
            }
        }
    }

    /// The error as a pool or transaction returns it, under a context as
    /// callers add.
    fn tagged(phase: OssPhase, stmt: OssStmt, failure: Failure) -> anyhow::Error {
        let kind = match failure {
            Failure::Timeout => OssErrorKind::Timeout(Duration::from_secs(10)),
            Failure::Unavailable => OssErrorKind::Unavailable(Arc::from("no writable server")),
            failure => OssErrorKind::Mysql(mysql_error(failure)),
        };
        let err = OssMysqlError {
            phase,
            stmt,
            endpoint: Arc::from("127.0.0.1:13306/mononoke"),
            generation: 2,
            kind,
        };
        anyhow::Error::from(err.into_mysql()).context("while running query Fake")
    }

    /// What a run of [`run_autocommit`] did.
    struct Run {
        result: Result<(usize, usize)>,
        /// When each attempt started.
        attempts: Vec<Instant>,
        /// The attempts `on_error` saw, with whether they would be retried.
        logged: Vec<(usize, bool)>,
    }

    /// Runs a statement whose attempts fail with the errors `script` makes
    /// for them, and succeed when it makes none. A successful attempt
    /// returns its number.
    async fn run_script(script: impl Fn(usize) -> Option<anyhow::Error> + Send + Sync) -> Run {
        let attempts = Mutex::new(Vec::new());
        let mut logged = Vec::new();
        let result = run_autocommit(
            |attempt| {
                attempts.lock().unwrap().push(Instant::now());
                let result = match script(attempt) {
                    Some(err) => Err(err),
                    None => Ok(attempt),
                };
                async move { result }
            },
            |attempt, _err, will_retry| logged.push((attempt, will_retry)),
        )
        .await;
        Run {
            result,
            attempts: attempts.into_inner().unwrap(),
            logged,
        }
    }

    /// The number of attempts made for a statement that always fails with
    /// errors made by `make_error`.
    async fn attempts(make_error: impl Fn() -> anyhow::Error + Send + Sync) -> usize {
        let run = run_script(|_| Some(make_error())).await;
        assert!(run.result.is_err());
        run.attempts.len()
    }

    fn assert_gap(from: Instant, to: Instant, wait: Duration) {
        assert_gap_with_jitter(from, to, wait, RETRY_JITTER);
    }

    fn assert_gap_with_jitter(from: Instant, to: Instant, wait: Duration, jitter: Duration) {
        let gap = to - from;
        assert!(
            gap >= wait && gap <= wait + jitter + SLACK,
            "{gap:?} is not {wait:?} plus up to {jitter:?}"
        );
    }

    #[mononoke::test]
    async fn test_attempts_by_class() {
        tokio::time::pause();
        use Failure::*;
        use OssPhase::*;
        use OssStmt::*;
        let all = AUTOCOMMIT_ATTEMPTS;
        let cases: &[(OssPhase, &[Failure], usize, usize)] = &[
            // Checkouts never ran the statement.
            (Checkout, &[Io, Closed, Timeout, Unavailable], all, all),
            (Checkout, &[Server(1040), Server(1045)], all, all),
            (Checkout, &[Server(3032)], all, all),
            (Checkout, &[Server(1049), Server(1129)], 1, 1),
            (Checkout, &[AuthPlugin], 1, 1),
            // The server guarantees the statement had no effect.
            (Execute, &[Server(1290), Server(1836)], all, all),
            (Execute, &[Server(1213), Server(1205)], all, all),
            (Execute, &[Server(4049), Unavailable], all, all),
            // The statement may have been applied: only reads run again.
            (Execute, &[Io, Closed, Timeout, Server(1053)], all, 1),
            (Execute, &[Server(1317), Server(3169), Server(4031)], all, 1),
            // Retrying would fail the same way.
            (Execute, &[Server(1062), Server(1064), Server(1146)], 1, 1),
            (Execute, &[AuthPlugin], 1, 1),
        ];
        for &(phase, failures, reads, writes) in cases {
            for failure in failures {
                for (stmt, expected) in [(Read, reads), (Write, writes)] {
                    assert_eq!(
                        attempts(|| tagged(phase, stmt, *failure)).await,
                        expected,
                        "{phase:?}/{stmt:?}: {failure:?}"
                    );
                }
            }
        }
    }

    #[mononoke::test]
    async fn test_attempts_of_untagged_errors() {
        tokio::time::pause();
        // Errors from outside MySQL are retried once, as before.
        assert_eq!(
            attempts(|| anyhow!("Failed to parse column 0")).await,
            NON_MYSQL_ATTEMPTS
        );
        let sqlite = || anyhow::Error::from(io::Error::other("database is locked"));
        assert_eq!(attempts(sqlite).await, NON_MYSQL_ATTEMPTS);
        // A mysql_async error without a tag comes from an unknown statement,
        // so only the errors with no effect are retried.
        let untagged = |failure| move || anyhow::Error::from(mysql_error(failure));
        assert_eq!(attempts(untagged(Failure::Io)).await, 1);
        assert_eq!(
            attempts(untagged(Failure::Server(1290))).await,
            AUTOCOMMIT_ATTEMPTS
        );
        assert_eq!(attempts(untagged(Failure::Server(1062))).await, 1);
        // The attempts so far count against the limit of an error from
        // outside MySQL.
        let run = run_script(|attempt| {
            Some(match attempt {
                1 => tagged(OssPhase::Checkout, OssStmt::Unknown, Failure::Io),
                _ => anyhow!("Failed to parse column 0"),
            })
        })
        .await;
        assert_eq!(run.logged, [(1, true), (2, false)]);
    }

    /// When each attempt of a transaction run with [`run_transaction`]
    /// started, and its result: the number of the attempt that succeeded.
    /// Its attempts fail with the errors `script` makes for them.
    async fn run_transaction_script(
        script: impl Fn(usize) -> Option<anyhow::Error>,
    ) -> (Vec<Instant>, Result<usize>) {
        let mut attempts = Vec::new();
        let result = run_transaction("fake", |attempt| {
            attempts.push(Instant::now());
            let result = match script(attempt) {
                Some(err) => Err(err),
                None => Ok(attempt),
            };
            async move { result }
        })
        .await;
        (attempts, result)
    }

    #[mononoke::test]
    async fn test_transaction_attempts() {
        tokio::time::pause();
        use Failure::*;
        use OssPhase::*;
        let all = TRANSACTION_ATTEMPTS;
        let cases: &[(OssPhase, &[Failure], usize)] = &[
            // Nothing was committed.
            (Checkout, &[Io, Timeout, Unavailable, Server(1040)], all),
            (
                InTransaction,
                &[Server(1290), Server(1213), Io, Timeout],
                all,
            ),
            (Commit, &[Server(1290), Server(1836), Server(1213)], all),
            // The commit may have been applied.
            (Commit, &[Io, Closed, Timeout, Server(1053)], 1),
            // Retrying would fail the same way.
            (Checkout, &[Server(1049), AuthPlugin], 1),
            (InTransaction, &[Server(1062), Server(1064)], 1),
        ];
        for &(phase, failures, expected) in cases {
            for failure in failures {
                let (attempts, result) =
                    run_transaction_script(|_| Some(tagged(phase, OssStmt::Unknown, *failure)))
                        .await;
                assert!(result.is_err());
                assert_eq!(attempts.len(), expected, "{phase:?}: {failure:?}");
            }
        }
        // An autocommit write the closure ran keeps its own verdict.
        let (attempts, _) =
            run_transaction_script(|_| Some(tagged(Execute, OssStmt::Write, Io))).await;
        assert_eq!(attempts.len(), 1);
        // Errors from outside MySQL, such as SQLite's, are not retried.
        let (attempts, _) = run_transaction_script(|_| Some(anyhow!("database is locked"))).await;
        assert_eq!(attempts.len(), 1);

        // The primary is demoted while the transaction is open, and the
        // next attempt reaches the new one, on the autocommit schedule.
        let (attempts, result) = run_transaction_script(|attempt| {
            (attempt < 3).then(|| tagged(InTransaction, OssStmt::Unknown, Server(1290)))
        })
        .await;
        assert_eq!(result.unwrap(), 3);
        assert_gap(attempts[0], attempts[1], RETRY_INTERVAL);
        assert_gap(attempts[1], attempts[2], 2 * RETRY_INTERVAL);
    }

    #[mononoke::test]
    fn test_unretried_outcome() {
        use Failure::*;
        use OssPhase::*;
        let outcome = |phase, stmt, failure| {
            unretried_outcome(&tagged(phase, stmt, failure), ErrorScope::Transaction)
        };
        assert_eq!(
            outcome(Checkout, OssStmt::Unknown, Io),
            Some("it had no effect")
        );
        assert_eq!(
            outcome(Commit, OssStmt::Unknown, Server(1290)),
            Some("it had no effect")
        );
        assert_eq!(
            outcome(Commit, OssStmt::Unknown, Io),
            Some("it may have been committed")
        );
        assert_eq!(
            outcome(InTransaction, OssStmt::Unknown, Timeout),
            Some("nothing was committed")
        );
        assert_eq!(
            outcome(Execute, OssStmt::Write, Io),
            Some("it may have been applied")
        );
        assert_eq!(
            outcome(Execute, OssStmt::Read, Io),
            Some("it had no effect")
        );
        assert_eq!(outcome(Execute, OssStmt::Write, Server(1062)), None);
        assert_eq!(
            unretried_outcome(&anyhow!("database is locked"), ErrorScope::Query),
            None
        );
    }

    #[mononoke::test]
    async fn test_backoff() {
        tokio::time::pause();
        let run =
            run_script(|_| Some(tagged(OssPhase::Checkout, OssStmt::Write, Failure::Io))).await;
        let attempts = &run.attempts;
        assert_eq!(attempts.len(), AUTOCOMMIT_ATTEMPTS);
        assert_gap(attempts[0], attempts[1], RETRY_INTERVAL);
        assert_gap(attempts[1], attempts[2], 2 * RETRY_INTERVAL);
        assert_gap(attempts[2], attempts[3], 4 * RETRY_INTERVAL);
        assert_eq!(run.logged, [(1, true), (2, true), (3, true), (4, false)]);
        // The error of the last attempt is returned.
        let err = run.result.unwrap_err();
        assert_eq!(
            classify(&err, ErrorScope::Query).map(|c| c.disposition),
            Some(Disposition::Safe)
        );
    }

    #[mononoke::test]
    async fn test_backoff_outside_mysql() {
        tokio::time::pause();
        // An error from outside MySQL, such as SQLite's, waits as long as
        // before failover support: 10 seconds plus up to 5.
        let sqlite = || anyhow::Error::from(io::Error::other("database is locked"));
        for _ in 0..20 {
            let run = run_script(|_| Some(sqlite())).await;
            assert_eq!(run.attempts.len(), NON_MYSQL_ATTEMPTS);
            assert_gap_with_jitter(
                run.attempts[0],
                run.attempts[1],
                NON_MYSQL_RETRY_INTERVAL,
                NON_MYSQL_RETRY_JITTER,
            );
            assert_eq!(run.logged, [(1, true), (2, false)]);
        }
        let run =
            run_script(|attempt| (attempt < 2).then(|| anyhow!("Failed to parse column 0"))).await;
        assert_eq!(run.result.unwrap(), (2, 2));
        assert_gap_with_jitter(
            run.attempts[0],
            run.attempts[1],
            NON_MYSQL_RETRY_INTERVAL,
            NON_MYSQL_RETRY_JITTER,
        );

        // Each wait follows the error before it: a MySQL error after one from
        // outside MySQL waits its own schedule, at its attempt number.
        let run = run_script(|attempt| match attempt {
            1 => Some(sqlite()),
            2 => Some(tagged(OssPhase::Checkout, OssStmt::Write, Failure::Io)),
            _ => None,
        })
        .await;
        assert_eq!(run.result.unwrap(), (3, 3));
        assert_gap_with_jitter(
            run.attempts[0],
            run.attempts[1],
            NON_MYSQL_RETRY_INTERVAL,
            NON_MYSQL_RETRY_JITTER,
        );
        assert_gap(run.attempts[1], run.attempts[2], 2 * RETRY_INTERVAL);
    }

    #[mononoke::test]
    async fn test_success_after_retries() {
        tokio::time::pause();
        let run = run_script(|attempt| {
            (attempt < 3).then(|| tagged(OssPhase::Execute, OssStmt::Write, Failure::Server(1290)))
        })
        .await;
        assert_eq!(run.result.unwrap(), (3, 3));
        assert_eq!(run.logged, [(1, true), (2, true)]);

        // An ambiguous write is not retried, and logged as such.
        let run = run_script(|attempt| {
            (attempt < 3).then(|| tagged(OssPhase::Execute, OssStmt::Write, Failure::Timeout))
        })
        .await;
        assert!(run.result.is_err());
        assert_eq!(run.attempts.len(), 1);
        assert_eq!(run.logged, [(1, false)]);
    }

    #[mononoke::test]
    fn test_transaction_verdicts() {
        use Failure::*;
        use OssPhase::*;
        use OssStmt::*;
        let cases: &[(OssPhase, OssStmt, &[Failure], bool)] = &[
            // A transaction that could not start, or a statement in it that
            // failed for any transient reason: nothing was committed.
            (Checkout, Unknown, &[Io, Timeout, Unavailable], true),
            (Checkout, Unknown, &[Server(1040)], true),
            (Checkout, Unknown, &[Server(1049), AuthPlugin], false),
            (InTransaction, Unknown, &[Io, Closed, Timeout], true),
            (InTransaction, Unknown, &[Server(1053)], true),
            (InTransaction, Unknown, &[Server(1290)], true),
            (InTransaction, Unknown, &[Server(1213)], true),
            (InTransaction, Unknown, &[Server(1062)], false),
            // A commit that the server rejected was not applied; one with an
            // unknown outcome may have been.
            (Commit, Unknown, &[Server(1213), Server(1290)], true),
            (Commit, Unknown, &[Server(3101)], true),
            (Commit, Unknown, &[Io, Closed, Timeout], false),
            (Commit, Unknown, &[Server(1180), Server(1062)], false),
            // An autocommit statement run next to the transaction keeps its
            // own verdict.
            (Execute, Read, &[Io, Timeout, Server(1290)], true),
            (Execute, Write, &[Server(1290), Unavailable], true),
            (Execute, Write, &[Io, Closed, Timeout], false),
            (Execute, Read, &[Server(1062)], false),
        ];
        for &(phase, stmt, failures, expected) in cases {
            for failure in failures {
                let err = tagged(phase, stmt, *failure);
                assert_eq!(
                    should_retry_transaction(&err),
                    expected,
                    "{phase:?}/{stmt:?}: {failure:?}"
                );
                // The crate's entry point is the same function.
                assert_eq!(should_retry_query(&err), expected);
            }
        }
        // A mysql_async error without a tag comes from inside the
        // transaction.
        let untagged = anyhow::Error::from(mysql_error(Io)).context("while adding edges");
        assert!(should_retry_transaction(&untagged));
        let untagged = anyhow::Error::from(mysql_error(Server(1062)));
        assert!(!should_retry_transaction(&untagged));
        // Nothing else is retried, as before.
        let parse = anyhow!("Failed to parse column 0");
        assert!(!should_retry_transaction(&parse));
        let sqlite = anyhow::Error::from(io::Error::other("database is locked"));
        assert!(!should_retry_transaction(&sqlite.context("sqlite")));
    }

    fn telemetry(fb: FacebookInit) -> SqlQueryTelemetry {
        SqlQueryTelemetry::new(fb, Metadata::default(), None)
    }

    /// Runs [`query_with_retry_no_cache`] on a statement that always fails
    /// with errors made by `make_error`. Returns the attempt numbers it got.
    async fn query_attempts(
        fb: FacebookInit,
        make_error: impl Fn() -> anyhow::Error + Send + Sync,
    ) -> Vec<usize> {
        let attempts = Mutex::new(Vec::new());
        let result: Result<((), usize)> = query_with_retry_no_cache(
            |attempt| {
                attempts.lock().unwrap().push(attempt);
                let err = make_error();
                async move { Err(err) }
            },
            "fake_shard",
            "FakeQuery",
            &telemetry(fb),
            TelemetryGranularity::Query,
            &[],
        )
        .await;
        assert!(result.is_err());
        attempts.into_inner().unwrap()
    }

    #[mononoke::fbinit_test]
    async fn test_query_with_retry_no_cache(fb: FacebookInit) -> Result<()> {
        tokio::time::pause();
        let safe = || tagged(OssPhase::Execute, OssStmt::Write, Failure::Server(1290));
        let ambiguous = || tagged(OssPhase::Execute, OssStmt::Write, Failure::Timeout);
        assert_eq!(query_attempts(fb, safe).await, [1, 2, 3, 4]);
        assert_eq!(query_attempts(fb, ambiguous).await, [1]);
        let start = Instant::now();
        assert_eq!(query_attempts(fb, || anyhow!("row")).await, [1, 2]);
        assert!(start.elapsed() >= NON_MYSQL_RETRY_INTERVAL);

        // A success returns the attempt that gave it.
        let calls = AtomicUsize::new(0);
        let (value, attempt) = query_with_retry_no_cache(
            |attempt| {
                let err = (calls.fetch_add(1, Ordering::Relaxed) == 0).then(safe);
                async move {
                    match err {
                        Some(err) => Err(err),
                        None => Ok(attempt * 10),
                    }
                }
            },
            "fake_shard",
            "FakeQuery",
            &telemetry(fb),
            TelemetryGranularity::Query,
            &[],
        )
        .await?;
        assert_eq!((value, attempt), (20, 2));

        // The knob still turns retries off: one attempt, numbered 0.
        override_just_knobs(JustKnobsInMemory::new(hashmap! {
            "scm/mononoke:sql_disable_auto_retries".to_string() => KnobVal::Bool(true),
        }));
        assert_eq!(query_attempts(fb, safe).await, [0]);
        Ok(())
    }

    /// The class of an error, as a tuple to compare.
    fn class_of(
        err: &anyhow::Error,
        scope: ErrorScope,
    ) -> Option<(OssPhase, OssStmt, Disposition)> {
        classify(err, scope).map(|c| (c.phase, c.stmt, c.disposition))
    }

    /// Statements through `mononoke_queries!` reach a server that fails
    /// them, and their errors carry the statement kind to the retry policy.
    #[mononoke::fbinit_test]
    async fn test_statements_on_a_failing_server(fb: FacebookInit) -> Result<()> {
        tokio::time::pause();
        let tel = telemetry(fb);
        let on = |prefix: &'static str, reply| {
            move |query: &str| {
                if query.starts_with(prefix) {
                    reply
                } else {
                    Reply::Ok
                }
            }
        };

        // A read whose connection breaks runs again, on a new connection.
        let server = FakeMysqld::start(on("SELECT", Reply::Close)).await?;
        let err = FakeRead::query(&server.connection(), tel.clone(), &7)
            .await
            .unwrap_err();
        assert_eq!(server.queries().len(), AUTOCOMMIT_ATTEMPTS);
        assert_eq!(server.queries()[0], "SELECT id FROM fake WHERE id = 7");
        assert_eq!(
            class_of(&err, ErrorScope::Query),
            Some((OssPhase::Execute, OssStmt::Read, Disposition::Ambiguous))
        );

        // A write whose connection breaks does not.
        let server = FakeMysqld::start(on("INSERT", Reply::Close)).await?;
        let err = FakeWrite::query(&server.connection(), tel.clone(), &[(&7,)])
            .await
            .unwrap_err();
        assert_eq!(server.queries(), ["INSERT INTO fake (id) VALUES (7)"]);
        assert_eq!(
            class_of(&err, ErrorScope::Query),
            Some((OssPhase::Execute, OssStmt::Write, Disposition::Ambiguous))
        );

        // A write that a read-only server rejects runs again until it is
        // accepted.
        let rejected = AtomicUsize::new(0);
        let server = FakeMysqld::start(move |_: &str| {
            if rejected.fetch_add(1, Ordering::Relaxed) < 2 {
                Reply::Error(1290)
            } else {
                Reply::Ok
            }
        })
        .await?;
        let result = FakeWrite::query(&server.connection(), tel.clone(), &[(&7,)]).await?;
        assert_eq!(result.affected_rows(), 1);
        assert_eq!(server.queries().len(), 3);

        // A duplicate key is final.
        let server = FakeMysqld::start(on("INSERT", Reply::Error(1062))).await?;
        let err = FakeWrite::query(&server.connection(), tel.clone(), &[(&7,)])
            .await
            .unwrap_err();
        assert_eq!(server.queries().len(), 1);
        assert_eq!(
            class_of(&err, ErrorScope::Query),
            Some((OssPhase::Execute, OssStmt::Write, Disposition::Permanent))
        );
        Ok(())
    }

    /// Transactions through `sql_ext` fail with errors tagged with the step
    /// at which they failed, which decides whether to run them again.
    #[mononoke::fbinit_test]
    async fn test_transactions_on_a_failing_server(fb: FacebookInit) -> Result<()> {
        // Real time: the transaction statement timeout is process-wide, and
        // waiting for the fake server must not count against it.
        let tel = telemetry(fb);
        let on = |prefix: &'static str, reply| {
            move |query: &str| {
                if query.starts_with(prefix) {
                    reply
                } else {
                    Reply::Ok
                }
            }
        };
        let write = async |server: &FakeMysqld| -> Result<()> {
            let txn = server.connection().start_transaction(tel.clone()).await?;
            let (txn, _) = FakeWrite::query_with_transaction(txn, &[(&7,)]).await?;
            txn.commit().await
        };
        let read = async |server: &FakeMysqld| -> Result<()> {
            let txn = server.connection().start_transaction(tel.clone()).await?;
            let (txn, _) = FakeRead::query_with_transaction(txn, &7).await?;
            txn.commit().await
        };
        let class = |err: &anyhow::Error| class_of(err, ErrorScope::Transaction).unwrap();

        // The transaction could not start.
        let server = FakeMysqld::start(on("START TRANSACTION", Reply::Close)).await?;
        let err = write(&server).await.unwrap_err();
        assert_eq!(class(&err).0, OssPhase::Checkout);
        assert!(should_retry_query(&err));

        // A statement in it lost its connection, or was rejected.
        for (reply, retry) in [
            (Reply::Close, true),
            (Reply::Error(1213), true),
            (Reply::Error(1062), false),
        ] {
            let server = FakeMysqld::start(on("INSERT", reply)).await?;
            let err = write(&server).await.unwrap_err();
            assert_eq!(class(&err).0, OssPhase::InTransaction, "{reply:?}");
            assert_eq!(should_retry_query(&err), retry, "{reply:?}");
            assert_eq!(
                server.queries()[..2],
                ["START TRANSACTION", "INSERT INTO fake (id) VALUES (7)"]
            );
        }
        let server = FakeMysqld::start(on("SELECT", Reply::Close)).await?;
        let err = read(&server).await.unwrap_err();
        assert_eq!(class(&err).0, OssPhase::InTransaction);
        assert!(should_retry_query(&err));

        // The commit may have been applied, or was rejected.
        for (reply, retry) in [
            (Reply::Close, false),
            (Reply::Error(1290), true),
            (Reply::Error(1213), true),
        ] {
            let server = FakeMysqld::start(on("COMMIT", reply)).await?;
            let err = write(&server).await.unwrap_err();
            assert_eq!(class(&err).0, OssPhase::Commit, "{reply:?}");
            assert_eq!(should_retry_query(&err), retry, "{reply:?}");
            assert_eq!(server.queries()[2], "COMMIT");
        }

        // A transaction open on a primary that is demoted runs again, as a
        // whole, through `retry_transaction`.
        let rejected = AtomicUsize::new(0);
        let server = FakeMysqld::start(move |query: &str| {
            if query.starts_with("INSERT") && rejected.fetch_add(1, Ordering::Relaxed) == 0 {
                Reply::Error(1290)
            } else {
                Reply::Ok
            }
        })
        .await?;
        crate::retry_transaction("fake", |_| write(&server)).await?;
        // The first attempt's connection rolls back in the background.
        let count = |query: &str| server.queries().iter().filter(|q| *q == query).count();
        assert_eq!(count("START TRANSACTION"), 2);
        assert_eq!(count("INSERT INTO fake (id) VALUES (7)"), 2);
        assert_eq!(count("COMMIT"), 1);

        // All good.
        let server = FakeMysqld::start(|_: &str| Reply::Ok).await?;
        write(&server).await?;
        assert_eq!(
            server.queries(),
            [
                "START TRANSACTION",
                "INSERT INTO fake (id) VALUES (7)",
                "COMMIT"
            ]
        );
        Ok(())
    }
}
