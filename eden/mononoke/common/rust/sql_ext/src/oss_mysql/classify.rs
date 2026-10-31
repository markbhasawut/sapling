/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! Which failed OSS MySQL operations may be retried, and which errors should
//! make the prober look for a new primary.
//!
//! The verdict depends on the step at which the operation failed
//! ([`OssPhase`]), on whether its statement writes ([`OssStmt`]), and on the
//! error. A checkout failure never ran a statement of the caller. A
//! statement that fails with an I/O error or a timeout may or may not have
//! been applied, so a write is retried only as part of a whole transaction
//! that has not been committed.

use sql::mysql_async::DriverError;
use sql::mysql_async::Error as MysqlError;
use sql::mysql_async::ServerError;
use sql_common::mysql::OssErrorKind;
use sql_common::mysql::OssMysqlError;
use sql_common::mysql::OssPhase;
use sql_common::mysql::OssStmt;

use super::fence::is_fence_rejection;

// Server error codes, with MySQL's names. 1927 is MariaDB only, and Galera
// reuses 1047 for "WSREP has not yet prepared node".
const ER_CON_COUNT_ERROR: u16 = 1040;
pub(super) const ER_ACCESS_DENIED_ERROR: u16 = 1045;
const ER_UNKNOWN_COM_ERROR: u16 = 1047;
const ER_SERVER_SHUTDOWN: u16 = 1053;
const ER_TOO_MANY_USER_CONNECTIONS: u16 = 1203;
const ER_LOCK_WAIT_TIMEOUT: u16 = 1205;
const ER_LOCK_DEADLOCK: u16 = 1213;
const ER_OPTION_PREVENTS_STATEMENT: u16 = 1290;
const ER_QUERY_INTERRUPTED: u16 = 1317;
const ER_READ_ONLY_MODE: u16 = 1836;
const ER_CONNECTION_KILLED: u16 = 1927;
const ER_SERVER_OFFLINE_MODE: u16 = 3032;
const ER_RUN_HOOK_ERROR: u16 = 3100;
const ER_TRANSACTION_ROLLBACK_DURING_COMMIT: u16 = 3101;
const ER_SESSION_WAS_KILLED: u16 = 3169;
const ER_CLIENT_INTERACTION_TIMEOUT: u16 = 4031;
const ER_OPERATION_NOT_ALLOWED_ON_GR_SECONDARY: u16 = 4049;

/// Errors of a checkout that a later attempt may not get: the server is
/// busy, starting, stopping, offline, or not the primary, or the password
/// is being rotated.
const CHECKOUT_TRANSIENT: &[u16] = &[
    ER_CON_COUNT_ERROR,
    ER_TOO_MANY_USER_CONNECTIONS,
    ER_SERVER_SHUTDOWN,
    ER_SERVER_OFFLINE_MODE,
    ER_UNKNOWN_COM_ERROR,
    ER_ACCESS_DENIED_ERROR,
];

/// Errors with which the server guarantees that the statement, or the
/// commit, was not applied, and that a later attempt may not get.
const NOT_APPLIED: &[u16] = &[
    ER_OPTION_PREVENTS_STATEMENT,
    ER_READ_ONLY_MODE,
    ER_OPERATION_NOT_ALLOWED_ON_GR_SECONDARY,
    ER_LOCK_DEADLOCK,
    ER_LOCK_WAIT_TIMEOUT,
    ER_UNKNOWN_COM_ERROR,
    ER_RUN_HOOK_ERROR,
    ER_TRANSACTION_ROLLBACK_DURING_COMMIT,
];

/// Errors that interrupt a statement at an unknown point.
const INTERRUPTED: &[u16] = &[
    ER_SERVER_SHUTDOWN,
    ER_QUERY_INTERRUPTED,
    ER_SESSION_WAS_KILLED,
    ER_CONNECTION_KILLED,
    ER_CLIENT_INTERACTION_TIMEOUT,
];

/// Errors with which a server that is up turns a connection away because
/// it has none to spare.
pub(super) const OUT_OF_CONNECTIONS: &[u16] = &[ER_CON_COUNT_ERROR, ER_TOO_MANY_USER_CONNECTIONS];

/// Errors with which the server refuses a write because it is read-only.
const READ_ONLY: &[u16] = &[
    ER_OPTION_PREVENTS_STATEMENT,
    ER_READ_ONLY_MODE,
    ER_OPERATION_NOT_ALLOWED_ON_GR_SECONDARY,
];

/// Errors after which the primary may have changed.
const KICK: &[u16] = &[
    ER_OPTION_PREVENTS_STATEMENT,
    ER_READ_ONLY_MODE,
    ER_OPERATION_NOT_ALLOWED_ON_GR_SECONDARY,
    ER_SERVER_SHUTDOWN,
    ER_SERVER_OFFLINE_MODE,
    ER_UNKNOWN_COM_ERROR,
    ER_SESSION_WAS_KILLED,
    ER_CONNECTION_KILLED,
    ER_ACCESS_DENIED_ERROR,
];

/// Where an error that carries no [`OssMysqlError`] was raised.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorScope {
    /// An autocommit statement: the phase is taken as
    /// [`OssPhase::Execute`].
    Query,
    /// A transaction: the phase is taken as [`OssPhase::InTransaction`].
    Transaction,
}

/// What a failure means for a retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// Transient, and the operation had no effect: it never reached a
    /// usable server, or the server guarantees it was not applied.
    Safe,
    /// Transient, but the operation may or may not have taken effect.
    Ambiguous,
    /// A retry would fail the same way.
    Permanent,
}

/// The classification of a failed OSS MySQL operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ErrorClass {
    /// The step at which the operation failed.
    pub phase: OssPhase,
    /// The kind of statement that failed.
    pub stmt: OssStmt,
    /// What the failure means for a retry.
    pub disposition: Disposition,
}

impl ErrorClass {
    /// Whether to run an autocommit statement again. One that may have
    /// been applied is retried only if it reads.
    pub fn retry_autocommit(&self) -> bool {
        match self.disposition {
            Disposition::Safe => true,
            Disposition::Ambiguous => self.stmt == OssStmt::Read,
            Disposition::Permanent => false,
        }
    }

    /// Whether to run a whole transaction again. Nothing is committed
    /// before COMMIT, so only a commit with an unknown outcome is never
    /// retried.
    pub fn retry_transaction(&self) -> bool {
        match self.disposition {
            Disposition::Safe => true,
            Disposition::Ambiguous => self.phase != OssPhase::Commit,
            Disposition::Permanent => false,
        }
    }
}

/// Classifies `err` by the first [`OssMysqlError`] in its chain or, failing
/// that, the first `mysql_async::Error`, taken to come from `scope` with an
/// unknown statement. `None` when the chain has neither, for example for
/// SQLite or row parsing errors.
pub fn classify(err: &anyhow::Error, scope: ErrorScope) -> Option<ErrorClass> {
    if let Some(err) = err.chain().find_map(|e| e.downcast_ref::<OssMysqlError>()) {
        return Some(classify_oss_error(err));
    }
    let err = err.chain().find_map(|e| e.downcast_ref::<MysqlError>())?;
    let phase = match scope {
        ErrorScope::Query => OssPhase::Execute,
        ErrorScope::Transaction => OssPhase::InTransaction,
    };
    Some(ErrorClass {
        phase,
        stmt: OssStmt::Unknown,
        disposition: disposition(phase, Cause::of_mysql(err)),
    })
}

/// Classifies an error of an OSS MySQL pool or transaction.
pub fn classify_oss_error(err: &OssMysqlError) -> ErrorClass {
    ErrorClass {
        phase: err.phase,
        stmt: err.stmt,
        disposition: disposition(err.phase, Cause::of(&err.kind)),
    }
}

/// Whether a pool error should make the prober run a round now: the server
/// may have stopped being the primary, or gone away. A checkout that the
/// server turned away for lack of connections does not: the server is up,
/// and a round would only add probe connections to it.
///
/// Errors inside a transaction, its COMMIT included, never get here: they
/// are raised where there is no pool, so they cannot kick. The prober
/// learns of such a change from its next round. That is the periodic one
/// within the probe interval, or, with `--oss-mysql-probe-interval-ms 0`,
/// the one kicked by the next checkout that fails or waits for a primary;
/// at the latest a new connection, once the pooled ones reach their age
/// limit, fails its fence.
pub fn should_kick(err: &OssMysqlError) -> bool {
    match err.phase {
        OssPhase::Checkout => !is_out_of_connections(&err.kind),
        // Raised by `oss_tx_guard`, never by a pool: see above.
        OssPhase::InTransaction => false,
        OssPhase::Execute | OssPhase::Commit => match Cause::of(&err.kind) {
            Cause::Io | Cause::Timeout | Cause::Unavailable | Cause::Driver(_) => true,
            Cause::Server(err) => is_fence_rejection(err) || KICK.contains(&err.code),
            Cause::Other => false,
        },
    }
}

/// Whether a pool error is the server refusing a statement because it is
/// read-only: the server is up, and no longer the primary.
pub fn refuses_writes(err: &OssMysqlError) -> bool {
    matches!(
        Cause::of(&err.kind),
        Cause::Server(err) if READ_ONLY.contains(&err.code)
    )
}

/// Whether `kind` is a server turning a connection away because it has
/// none to spare.
fn is_out_of_connections(kind: &OssErrorKind) -> bool {
    matches!(
        Cause::of(kind),
        Cause::Server(err) if OUT_OF_CONNECTIONS.contains(&err.code)
    )
}

/// The error itself, without where it happened.
enum Cause<'a> {
    Io,
    Timeout,
    Unavailable,
    Driver(&'a DriverError),
    Server(&'a ServerError),
    /// URL errors and errors from outside mysql_async.
    Other,
}

impl<'a> Cause<'a> {
    fn of(kind: &'a OssErrorKind) -> Self {
        match kind {
            OssErrorKind::Mysql(err) => Cause::of_mysql(err),
            OssErrorKind::Timeout(_) => Cause::Timeout,
            OssErrorKind::Unavailable(_) => Cause::Unavailable,
        }
    }

    fn of_mysql(err: &'a MysqlError) -> Self {
        match err {
            MysqlError::Io(_) => Cause::Io,
            MysqlError::Driver(err) => Cause::Driver(err),
            MysqlError::Server(err) => Cause::Server(err),
            MysqlError::Url(_) | MysqlError::Other(_) => Cause::Other,
        }
    }
}

/// Whether a driver error means the connection broke, rather than a bad
/// statement, bad data or an unsupported configuration.
fn is_connection_error(err: &DriverError) -> bool {
    matches!(
        err,
        DriverError::ConnectionClosed
            | DriverError::PoolDisconnected
            | DriverError::PacketOutOfOrder
            | DriverError::UnexpectedPacket { .. }
            | DriverError::BadCompressedPacketHeader
    )
}

fn disposition(phase: OssPhase, cause: Cause<'_>) -> Disposition {
    use Disposition::*;
    match phase {
        OssPhase::Checkout => match cause {
            Cause::Io | Cause::Timeout | Cause::Unavailable => Safe,
            Cause::Driver(err) if is_connection_error(err) => Safe,
            Cause::Server(err) if is_fence_rejection(err) => Safe,
            Cause::Server(err) if CHECKOUT_TRANSIENT.contains(&err.code) => Safe,
            _ => Permanent,
        },
        OssPhase::Execute | OssPhase::InTransaction => match cause {
            Cause::Unavailable => Safe,
            Cause::Server(err) if NOT_APPLIED.contains(&err.code) => Safe,
            Cause::Io | Cause::Timeout => Ambiguous,
            Cause::Driver(err) if is_connection_error(err) => Ambiguous,
            Cause::Server(err) if INTERRUPTED.contains(&err.code) => Ambiguous,
            _ => Permanent,
        },
        OssPhase::Commit => match cause {
            Cause::Unavailable => Safe,
            Cause::Server(err) if NOT_APPLIED.contains(&err.code) => Safe,
            _ => Ambiguous,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Context;
    use anyhow::anyhow;
    use mononoke_macros::mononoke;
    use sql::mysql_async::IoError;
    use sql::mysql_async::UrlError;

    use super::Disposition::*;
    use super::*;

    const PHASES: [OssPhase; 4] = [
        OssPhase::Checkout,
        OssPhase::Execute,
        OssPhase::InTransaction,
        OssPhase::Commit,
    ];
    const STMTS: [OssStmt; 3] = [OssStmt::Read, OssStmt::Write, OssStmt::Unknown];

    /// A way to fail, made fresh for every use.
    #[derive(Clone, Copy, Debug)]
    enum Failure {
        Server(u16),
        Fence,
        Io,
        Driver(fn() -> DriverError),
        Url,
        Timeout,
        Unavailable,
    }

    fn server_error(code: u16, message: &str) -> MysqlError {
        MysqlError::Server(ServerError {
            code,
            message: message.to_string(),
            state: "HY000".to_string(),
        })
    }

    fn mysql_error(failure: Failure) -> MysqlError {
        match failure {
            Failure::Server(code) => server_error(code, "test error"),
            Failure::Fence => server_error(
                1231,
                "Variable 'sql_mode' can't be set to the value of 'MONONOKE_FENCE_G4'",
            ),
            Failure::Io => MysqlError::Io(IoError::Io(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "connection reset by peer",
            ))),
            Failure::Driver(make) => MysqlError::Driver(make()),
            Failure::Url => MysqlError::Url(UrlError::Invalid),
            Failure::Timeout | Failure::Unavailable => {
                panic!("{failure:?} is not a mysql_async error")
            }
        }
    }

    fn kind(failure: Failure) -> OssErrorKind {
        match failure {
            Failure::Timeout => OssErrorKind::Timeout(Duration::from_secs(10)),
            Failure::Unavailable => OssErrorKind::Unavailable(Arc::from("no writable server")),
            failure => OssErrorKind::Mysql(mysql_error(failure)),
        }
    }

    fn oss_error(phase: OssPhase, stmt: OssStmt, failure: Failure) -> OssMysqlError {
        OssMysqlError {
            phase,
            stmt,
            endpoint: Arc::from("127.0.0.1:13306"),
            generation: 3,
            kind: kind(failure),
        }
    }

    /// The error as the sql crate returns it: wrapped in a
    /// `mysql_async::Error`, in an `anyhow::Error`.
    fn tagged(phase: OssPhase, stmt: OssStmt, failure: Failure) -> anyhow::Error {
        anyhow::Error::from(oss_error(phase, stmt, failure).into_mysql())
    }

    fn disposition_of(phase: OssPhase, stmt: OssStmt, failure: Failure) -> Disposition {
        let class = classify(&tagged(phase, stmt, failure), ErrorScope::Query)
            .unwrap_or_else(|| panic!("{phase:?}/{failure:?} not classified"));
        assert_eq!(class.phase, phase);
        assert_eq!(class.stmt, stmt);
        class.disposition
    }

    /// Checks the disposition of `failures` in `phases`, for every kind of
    /// statement.
    fn check(phases: &[OssPhase], failures: &[Failure], expected: Disposition) {
        for phase in phases {
            for failure in failures {
                for stmt in STMTS {
                    assert_eq!(
                        disposition_of(*phase, stmt, *failure),
                        expected,
                        "{phase:?}/{stmt:?}: {failure:?}"
                    );
                }
            }
        }
    }

    fn servers(codes: &[u16]) -> Vec<Failure> {
        codes.iter().map(|code| Failure::Server(*code)).collect()
    }

    const CONNECTION_DRIVER_ERRORS: &[Failure] = &[
        Failure::Driver(|| DriverError::PoolDisconnected),
        Failure::Driver(|| DriverError::ConnectionClosed),
        Failure::Driver(|| DriverError::PacketOutOfOrder),
        Failure::Driver(|| DriverError::UnexpectedPacket {
            payload: vec![0xff],
        }),
    ];

    const OTHER_DRIVER_ERRORS: &[Failure] = &[
        Failure::Driver(|| DriverError::UnknownAuthPlugin {
            name: "auth_gssapi_client".to_string(),
        }),
        Failure::Driver(|| DriverError::MysqlOldPasswordDisabled),
        Failure::Driver(|| DriverError::MixedParams),
        Failure::Driver(|| DriverError::NestedTransaction),
        Failure::Driver(|| DriverError::PacketTooLarge),
    ];

    /// Errors that no phase retries.
    const ANY_PERMANENT: &[u16] = &[1062, 1064, 1146, 1054, 1792, 3024, 1969, 1366, 1406];

    #[mononoke::test]
    fn test_checkout() {
        let checkout = &[OssPhase::Checkout];
        check(
            checkout,
            &[
                Failure::Io,
                Failure::Timeout,
                Failure::Unavailable,
                Failure::Fence,
            ],
            Safe,
        );
        check(checkout, CONNECTION_DRIVER_ERRORS, Safe);
        check(
            checkout,
            &servers(&[1040, 1203, 1053, 3032, 1047, 1045]),
            Safe,
        );
        check(checkout, &servers(&[1129, 1044, 1049]), Permanent);
        check(checkout, OTHER_DRIVER_ERRORS, Permanent);
        check(checkout, &[Failure::Url], Permanent);
        check(checkout, &servers(ANY_PERMANENT), Permanent);
        // 1231 without the marker is an ordinary error.
        check(checkout, &servers(&[1231]), Permanent);
        // Errors only a statement gets.
        check(checkout, &servers(&[1290, 1213, 1317]), Permanent);
    }

    #[mononoke::test]
    fn test_execute_and_in_transaction() {
        let phases = &[OssPhase::Execute, OssPhase::InTransaction];
        // The server guarantees no effect.
        check(
            phases,
            &servers(&[1290, 1836, 4049, 1213, 1205, 1047, 3100, 3101]),
            Safe,
        );
        // Outcome unknown.
        check(phases, &[Failure::Io, Failure::Timeout], Ambiguous);
        check(phases, CONNECTION_DRIVER_ERRORS, Ambiguous);
        check(phases, &servers(&[1053, 1317, 3169, 1927, 4031]), Ambiguous);
        // Retrying would fail again.
        check(phases, &servers(ANY_PERMANENT), Permanent);
        check(phases, OTHER_DRIVER_ERRORS, Permanent);
        check(phases, &[Failure::Url, Failure::Fence], Permanent);
        check(phases, &servers(&[1040, 1045, 1129, 1231]), Permanent);
        let row_error = Failure::Driver(|| DriverError::FromValue {
            value: sql::mysql_async::Value::NULL,
        });
        check(phases, &[row_error], Permanent);
    }

    #[mononoke::test]
    fn test_commit() {
        let commit = &[OssPhase::Commit];
        // Definitely not committed.
        check(
            commit,
            &servers(&[1213, 1205, 1290, 1836, 4049, 3100, 3101, 1047]),
            Safe,
        );
        // Maybe committed.
        check(
            commit,
            &[Failure::Io, Failure::Timeout, Failure::Url],
            Ambiguous,
        );
        check(commit, CONNECTION_DRIVER_ERRORS, Ambiguous);
        check(commit, OTHER_DRIVER_ERRORS, Ambiguous);
        check(
            commit,
            &servers(&[1180, 1053, 1317, 3169, 1927, 4031, 1062, 1064, 3024]),
            Ambiguous,
        );
    }

    #[mononoke::test]
    fn test_verdicts() {
        let class = |phase, stmt, disposition| ErrorClass {
            phase,
            stmt,
            disposition,
        };
        for phase in PHASES {
            for stmt in STMTS {
                let safe = class(phase, stmt, Safe);
                assert!(safe.retry_autocommit(), "{safe:?}");
                assert!(safe.retry_transaction(), "{safe:?}");
                let permanent = class(phase, stmt, Permanent);
                assert!(!permanent.retry_autocommit(), "{permanent:?}");
                assert!(!permanent.retry_transaction(), "{permanent:?}");
                // An ambiguous autocommit statement is retried only if it
                // reads; an ambiguous transaction unless it was committing.
                let ambiguous = class(phase, stmt, Ambiguous);
                assert_eq!(
                    ambiguous.retry_autocommit(),
                    stmt == OssStmt::Read,
                    "{ambiguous:?}"
                );
                assert_eq!(
                    ambiguous.retry_transaction(),
                    phase != OssPhase::Commit,
                    "{ambiguous:?}"
                );
            }
        }
    }

    #[mononoke::test]
    fn test_verdicts_of_table_rows() {
        let classify_query = |phase, stmt, failure| {
            classify(&tagged(phase, stmt, failure), ErrorScope::Query).unwrap()
        };
        // A read that timed out is retried, a write is not, and either is
        // retried as part of a transaction.
        let read = classify_query(OssPhase::Execute, OssStmt::Read, Failure::Timeout);
        let write = classify_query(OssPhase::Execute, OssStmt::Write, Failure::Timeout);
        assert!(read.retry_autocommit());
        assert!(!write.retry_autocommit());
        assert!(write.retry_transaction());
        // A write rejected by a read-only server is retried.
        let write = classify_query(OssPhase::Execute, OssStmt::Write, Failure::Server(1290));
        assert!(write.retry_autocommit());
        // A commit that lost its connection is never retried; one rejected
        // by a read-only server is.
        let commit = classify_query(OssPhase::Commit, OssStmt::Unknown, Failure::Io);
        assert!(!commit.retry_transaction());
        let commit = classify_query(OssPhase::Commit, OssStmt::Unknown, Failure::Server(1290));
        assert!(commit.retry_transaction());
        // A statement inside a transaction that lost its connection: the
        // transaction is retried.
        let statement = classify_query(OssPhase::InTransaction, OssStmt::Unknown, Failure::Io);
        assert!(statement.retry_transaction());
        // Checkout failures are retried everywhere.
        let checkout = classify_query(OssPhase::Checkout, OssStmt::Unknown, Failure::Unavailable);
        assert!(checkout.retry_autocommit());
        assert!(checkout.retry_transaction());
        // Duplicate keys never are.
        let duplicate = classify_query(OssPhase::Execute, OssStmt::Write, Failure::Server(1062));
        assert!(!duplicate.retry_autocommit());
        assert!(!duplicate.retry_transaction());
    }

    #[mononoke::test]
    fn test_chains() {
        let expected = ErrorClass {
            phase: OssPhase::Execute,
            stmt: OssStmt::Write,
            disposition: Safe,
        };
        let error = || oss_error(OssPhase::Execute, OssStmt::Write, Failure::Server(1290));

        // Directly, wrapped in a mysql_async::Error, and under contexts.
        let direct = anyhow::Error::from(error());
        assert_eq!(classify(&direct, ErrorScope::Query), Some(expected));
        let wrapped = anyhow::Error::from(error().into_mysql());
        assert_eq!(classify(&wrapped, ErrorScope::Query), Some(expected));
        let with_context = anyhow::Error::from(error().into_mysql())
            .context("while running query Foo")
            .context("while adding bookmarks");
        assert_eq!(classify(&with_context, ErrorScope::Query), Some(expected));
        let result: Result<(), MysqlError> = Err(error().into_mysql());
        let with_context = result.context("while running query Foo").unwrap_err();
        assert_eq!(
            classify(&with_context, ErrorScope::Transaction),
            Some(expected)
        );

        // The tag wins over the scope, and over the mysql_async::Error that
        // wraps it and comes first in the chain.
        let tagged_commit = anyhow::Error::from(
            oss_error(OssPhase::Commit, OssStmt::Unknown, Failure::Io).into_mysql(),
        )
        .context("commit");
        assert_eq!(
            classify(&tagged_commit, ErrorScope::Query),
            Some(ErrorClass {
                phase: OssPhase::Commit,
                stmt: OssStmt::Unknown,
                disposition: Ambiguous,
            })
        );
    }

    #[mononoke::test]
    fn test_untagged_errors() {
        // A bare mysql_async error is an Execute error of an unknown
        // statement in a query, and an InTransaction error in a transaction.
        let io = || anyhow::Error::from(mysql_error(Failure::Io)).context("query Foo");
        let query = classify(&io(), ErrorScope::Query).unwrap();
        assert_eq!(
            query,
            ErrorClass {
                phase: OssPhase::Execute,
                stmt: OssStmt::Unknown,
                disposition: Ambiguous,
            }
        );
        assert!(!query.retry_autocommit());
        let transaction = classify(&io(), ErrorScope::Transaction).unwrap();
        assert_eq!(transaction.phase, OssPhase::InTransaction);
        assert!(transaction.retry_transaction());

        let read_only = anyhow::Error::from(mysql_error(Failure::Server(1836)));
        let class = classify(&read_only, ErrorScope::Query).unwrap();
        assert_eq!(class.disposition, Safe);
        assert!(class.retry_autocommit());

        // A mysql_async error that wraps something else is permanent.
        let other = anyhow::Error::from(MysqlError::Other(Box::new(io::Error::other("custom"))));
        assert_eq!(
            classify(&other, ErrorScope::Query).map(|c| c.disposition),
            Some(Permanent)
        );
    }

    #[mononoke::test]
    fn test_no_mysql_error() {
        assert_eq!(
            classify(&anyhow!("row parse error"), ErrorScope::Query),
            None
        );
        let sqlite = anyhow::Error::from(io::Error::other("database is locked")).context("sqlite");
        assert_eq!(classify(&sqlite, ErrorScope::Transaction), None);
    }

    #[mononoke::test]
    fn test_kick() {
        let kicks = |phase, failure| should_kick(&oss_error(phase, OssStmt::Write, failure));
        // Any checkout error...
        for failure in [
            Failure::Io,
            Failure::Timeout,
            Failure::Unavailable,
            Failure::Fence,
            Failure::Server(1049),
            Failure::Server(1045),
            Failure::Url,
        ] {
            assert!(kicks(OssPhase::Checkout, failure), "{failure:?}");
        }
        // ...but a server out of connections, which a round would only load
        // further.
        for code in [1040, 1203] {
            assert!(!kicks(OssPhase::Checkout, Failure::Server(code)), "{code}");
        }
        for phase in [OssPhase::Execute, OssPhase::Commit] {
            for failure in [
                Failure::Io,
                Failure::Timeout,
                Failure::Driver(|| DriverError::ConnectionClosed),
                Failure::Fence,
            ] {
                assert!(kicks(phase, failure), "{phase:?}: {failure:?}");
            }
            for code in [1290, 1836, 4049, 1053, 3032, 1047, 3169, 1927, 1045] {
                assert!(kicks(phase, Failure::Server(code)), "{phase:?}: {code}");
            }
            for code in [1213, 1205, 1062, 1064, 1146, 1231, 1317] {
                assert!(!kicks(phase, Failure::Server(code)), "{phase:?}: {code}");
            }
            assert!(!kicks(phase, Failure::Url), "{phase:?}");
        }
        // Nothing inside a transaction.
        for failure in [Failure::Io, Failure::Timeout, Failure::Server(1290)] {
            assert!(!kicks(OssPhase::InTransaction, failure), "{failure:?}");
        }
    }

    #[mononoke::test]
    fn test_refuses_writes() {
        let refuses =
            |failure| refuses_writes(&oss_error(OssPhase::Execute, OssStmt::Write, failure));
        for code in [1290, 1836, 4049] {
            assert!(refuses(Failure::Server(code)), "{code}");
        }
        for failure in [
            Failure::Server(1040),
            Failure::Server(1213),
            Failure::Server(1792),
            Failure::Fence,
            Failure::Io,
            Failure::Timeout,
            Failure::Unavailable,
        ] {
            assert!(!refuses(failure), "{failure:?}");
        }
    }
}
