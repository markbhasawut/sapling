/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! Setup statements of the OSS MySQL pools.
//!
//! Every new connection of a Primary pool runs a fence: one statement that
//! fails unless the server it reached is writable and, in identity mode, is
//! the server the pool was built for. A failing fence sets `sql_mode` to an
//! invalid value carrying a marker, so the server rejects it with error 1231
//! and the checkout fails; the marker tells that error apart.
//!
//! mysql_async runs a pool's setup statements in reverse order, so each pool
//! has at most one.
//!
//! That statement also sets `max_execution_time`, the time limit the server
//! applies to SELECT statements, a little above the pools' query time limit,
//! so that the client gives up first. A read the client gave up on is not
//! stopped by closing its connection: the server notices only when it next
//! sends rows, which may be at the very end. Only MySQL and Percona Server
//! have the variable, so it is set in an executable comment for MySQL 5.7.8
//! or later (`/*!50708 ... */`), which MariaDB skips, as it takes versions
//! 50700 to 99999 in such comments for MySQL ones. MariaDB reads get no
//! server-side limit.

use std::time::Duration;

use sql::mysql_async::ServerError;

use super::decide::ServerIdentity;

/// Start of the invalid `sql_mode` value a failing fence sets. The pool
/// generation follows it.
pub const FENCE_MARKER: &str = "MONONOKE_FENCE_G";

/// The setup statement of Read pools: they never write.
pub const READ_POOL_SETUP: &str = "SET SESSION TRANSACTION READ ONLY";

/// Error code of a failing fence (ER_WRONG_VALUE_FOR_VAR).
const ER_WRONG_VALUE_FOR_VAR: u16 = 1231;

/// What new connections of a Primary pool check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrimaryFence<'a> {
    /// The server is writable and is this one (identity mode).
    Identity(&'a ServerIdentity),
    /// The server is writable (writable mode).
    Writable,
    /// Nothing (none mode).
    None,
}

/// The setup statements of a Primary pool of the given generation. SELECTs
/// get `select_time_limit`, if any, as their server-side time limit, except
/// with [`PrimaryFence::None`], which sets nothing.
pub fn primary_pool_setup(
    fence: PrimaryFence<'_>,
    generation: u64,
    select_time_limit: Option<Duration>,
) -> Vec<String> {
    let identity_checks = match fence {
        PrimaryFence::None => return Vec::new(),
        PrimaryFence::Writable => String::new(),
        PrimaryFence::Identity(identity) => format!(
            " AND @@global.server_id = {} AND @@port = {} AND @@hostname = {}",
            identity.server_id,
            identity.port,
            sql_string(&identity.hostname),
        ),
    };
    // CAST makes the check work with both 0/1 (MySQL, Percona) and the
    // OFF/ON/NO_LOCK/NO_LOCK_NO_ADMIN enum of MariaDB's read_only.
    // super_read_only forces read_only on, so it needs no check of its own.
    // A SET fails as a whole, so a failing fence sets no time limit either.
    let time_limit = match select_time_limit {
        Some(limit) => format!(
            " /*!50708 , SESSION max_execution_time = {} */",
            max_execution_time(limit)
        ),
        None => String::new(),
    };
    vec![format!(
        "SET SESSION sql_mode = IF(\
         CAST(@@global.read_only AS CHAR) IN ('0','OFF') \
         AND CAST(@@global.innodb_read_only AS CHAR) IN ('0','OFF'){identity_checks}, \
         @@session.sql_mode, '{FENCE_MARKER}{generation}'){time_limit}"
    )]
}

/// The setup statements of a Read pool, whose SELECTs get
/// `select_time_limit`, if any, as their server-side time limit.
pub fn read_pool_setup(select_time_limit: Option<Duration>) -> Vec<String> {
    match select_time_limit {
        None => vec![READ_POOL_SETUP.to_string()],
        // `SET TRANSACTION READ ONLY` cannot be followed by other variables,
        // so MySQL gets the `transaction_read_only` variable (5.7.20 and
        // later) instead. MariaDB, which has it only since 11.1, keeps the
        // statement, in a comment only it runs (`/*M!...*/`).
        Some(limit) => vec![format!(
            "SET SESSION /*M!100000 TRANSACTION READ ONLY */ \
             /*!50720 transaction_read_only = ON, SESSION max_execution_time = {} */",
            max_execution_time(limit)
        )],
    }
}

/// `limit` as a value of `max_execution_time`: whole milliseconds, where 0
/// would mean no limit.
fn max_execution_time(limit: Duration) -> u32 {
    u32::try_from(limit.as_millis()).unwrap_or(u32::MAX).max(1)
}

/// Whether `err` is a failing fence.
pub fn is_fence_rejection(err: &ServerError) -> bool {
    err.code == ER_WRONG_VALUE_FOR_VAR && err.message.contains(FENCE_MARKER)
}

/// `value` as a quoted SQL string. A quote is doubled, which works whatever
/// the session's sql_mode. A backslash is doubled too: under
/// NO_BACKSLASH_ESCAPES that makes the string differ from `value`, so the
/// fence fails closed.
fn sql_string(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('\'');
    for c in value.chars() {
        match c {
            '\'' => quoted.push_str("''"),
            '\\' => quoted.push_str("\\\\"),
            c => quoted.push(c),
        }
    }
    quoted.push('\'');
    quoted
}

#[cfg(test)]
mod tests {
    use mononoke_macros::mononoke;

    use super::*;

    fn identity(hostname: &str) -> ServerIdentity {
        ServerIdentity {
            server_id: 2,
            hostname: hostname.to_string(),
            port: 13307,
        }
    }

    #[mononoke::test]
    fn test_identity_fence() {
        let identity = identity("mac.local");
        assert_eq!(
            primary_pool_setup(PrimaryFence::Identity(&identity), 7, None),
            vec![
                "SET SESSION sql_mode = IF(\
                 CAST(@@global.read_only AS CHAR) IN ('0','OFF') \
                 AND CAST(@@global.innodb_read_only AS CHAR) IN ('0','OFF') \
                 AND @@global.server_id = 2 AND @@port = 13307 AND @@hostname = 'mac.local', \
                 @@session.sql_mode, 'MONONOKE_FENCE_G7')"
                    .to_string()
            ]
        );
    }

    #[mononoke::test]
    fn test_writable_fence() {
        assert_eq!(
            primary_pool_setup(PrimaryFence::Writable, 12, None),
            vec![
                "SET SESSION sql_mode = IF(\
                 CAST(@@global.read_only AS CHAR) IN ('0','OFF') \
                 AND CAST(@@global.innodb_read_only AS CHAR) IN ('0','OFF'), \
                 @@session.sql_mode, 'MONONOKE_FENCE_G12')"
                    .to_string()
            ]
        );
    }

    #[mononoke::test]
    fn test_no_fence() {
        assert_eq!(
            primary_pool_setup(PrimaryFence::None, 3, Some(Duration::from_secs(10))),
            Vec::<String>::new()
        );
    }

    #[mononoke::test]
    fn test_read_pool_setup() {
        assert_eq!(
            read_pool_setup(None),
            vec!["SET SESSION TRANSACTION READ ONLY".to_string()]
        );
    }

    #[mononoke::test]
    fn test_select_time_limit() {
        // Merged into the one statement, in a comment MariaDB skips.
        let identity = identity("mac.local");
        assert_eq!(
            primary_pool_setup(
                PrimaryFence::Identity(&identity),
                7,
                Some(Duration::from_secs(10))
            ),
            vec![
                "SET SESSION sql_mode = IF(\
                 CAST(@@global.read_only AS CHAR) IN ('0','OFF') \
                 AND CAST(@@global.innodb_read_only AS CHAR) IN ('0','OFF') \
                 AND @@global.server_id = 2 AND @@port = 13307 AND @@hostname = 'mac.local', \
                 @@session.sql_mode, 'MONONOKE_FENCE_G7') \
                 /*!50708 , SESSION max_execution_time = 10000 */"
                    .to_string()
            ]
        );
        assert_eq!(
            primary_pool_setup(PrimaryFence::Writable, 1, Some(Duration::from_millis(1500))),
            vec![
                "SET SESSION sql_mode = IF(\
                 CAST(@@global.read_only AS CHAR) IN ('0','OFF') \
                 AND CAST(@@global.innodb_read_only AS CHAR) IN ('0','OFF'), \
                 @@session.sql_mode, 'MONONOKE_FENCE_G1') \
                 /*!50708 , SESSION max_execution_time = 1500 */"
                    .to_string()
            ]
        );
        assert_eq!(
            read_pool_setup(Some(Duration::from_secs(10))),
            vec![
                "SET SESSION /*M!100000 TRANSACTION READ ONLY */ \
                 /*!50720 transaction_read_only = ON, SESSION max_execution_time = 10000 */"
                    .to_string()
            ]
        );
        // Whole milliseconds, at least one (0 would mean no limit), and at
        // most what the variable holds.
        assert_eq!(max_execution_time(Duration::from_micros(1500)), 1);
        assert_eq!(max_execution_time(Duration::from_micros(10)), 1);
        assert_eq!(max_execution_time(Duration::from_secs(1 << 40)), u32::MAX);
    }

    #[mononoke::test]
    fn test_hostname_escaping() {
        assert_eq!(sql_string("db1"), "'db1'");
        assert_eq!(sql_string(""), "''");
        assert_eq!(sql_string("it's"), "'it''s'");
        assert_eq!(sql_string(r"a\b"), r"'a\\b'");
        assert_eq!(sql_string(r"\'"), r"'\\'''");
        assert_eq!(sql_string("x' OR '1'='1"), "'x'' OR ''1''=''1'");

        let identity = identity(r"o'brien\host");
        let setup = primary_pool_setup(PrimaryFence::Identity(&identity), 1, None);
        assert!(
            setup[0].contains(r"AND @@hostname = 'o''brien\\host', "),
            "{}",
            setup[0]
        );
    }

    #[mononoke::test]
    fn test_fence_rejection() {
        let error = |code: u16, message: &str| ServerError {
            code,
            message: message.to_string(),
            state: "42000".to_string(),
        };
        assert!(is_fence_rejection(&error(
            1231,
            "Variable 'sql_mode' can't be set to the value of 'MONONOKE_FENCE_G7'"
        )));
        // The same error without the marker is an ordinary one.
        assert!(!is_fence_rejection(&error(
            1231,
            "Variable 'sql_mode' can't be set to the value of 'BOGUS'"
        )));
        // The marker with another code is not a fence either.
        assert!(!is_fence_rejection(&error(
            1064,
            "near 'MONONOKE_FENCE_G7'"
        )));
    }
}
