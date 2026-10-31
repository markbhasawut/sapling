/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! Candidate server syntax shared by the `hosts` config list and hosts files.
//!
//! metaconfig_parser parses `hosts` with the same grammar
//! (`convert/storage.rs`); sql_ext cannot depend on metaconfig, so keep the
//! two in sync.

use std::collections::HashSet;
use std::net::Ipv6Addr;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;

/// Parse one candidate server: "host", "host:port", "[v6]" or "[v6]:port".
/// The port defaults to `default_port`. IPv6 hosts are returned without
/// brackets.
pub fn parse_oss_mysql_endpoint(entry: &str, default_port: u16) -> Result<(String, u16)> {
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
    Ok((host.to_string(), port))
}

/// Parse the contents of a hosts file: one candidate server per line, in
/// the forms accepted by [`parse_oss_mysql_endpoint`]. Blank lines and lines
/// starting with '#' are ignored. Fails on an invalid line, a duplicate
/// server, or when no server is listed.
pub fn parse_oss_mysql_hosts_file(contents: &str, default_port: u16) -> Result<Vec<(String, u16)>> {
    let mut seen = HashSet::new();
    let mut endpoints = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line_number = index + 1;
        let (host, port) = parse_oss_mysql_endpoint(line, default_port)
            .with_context(|| format!("line {line_number}: invalid entry '{line}'"))?;
        if !seen.insert((host.to_ascii_lowercase(), port)) {
            bail!("line {line_number}: duplicate entry '{line}'");
        }
        endpoints.push((host, port));
    }
    if endpoints.is_empty() {
        bail!("no servers listed");
    }
    Ok(endpoints)
}

#[cfg(test)]
mod tests {
    use mononoke_macros::mononoke;

    use super::*;

    fn endpoint(host: &str, port: u16) -> (String, u16) {
        (host.to_string(), port)
    }

    fn endpoint_error(entry: &str) -> String {
        format!(
            "{:#}",
            parse_oss_mysql_endpoint(entry, 3306).expect_err("parsing should fail")
        )
    }

    fn hosts_file_error(contents: &str) -> String {
        format!(
            "{:#}",
            parse_oss_mysql_hosts_file(contents, 3306).expect_err("parsing should fail")
        )
    }

    // The endpoint tests mirror the `hosts` tests in metaconfig_parser
    // (convert/storage.rs), so that both accept the same forms.

    #[mononoke::test]
    fn test_endpoint_forms() -> Result<()> {
        assert_eq!(
            parse_oss_mysql_endpoint("db1", 3306)?,
            endpoint("db1", 3306)
        );
        assert_eq!(
            parse_oss_mysql_endpoint("db2:3307", 3306)?,
            endpoint("db2", 3307)
        );
        assert_eq!(
            parse_oss_mysql_endpoint(" db-3.example.com:13306 ", 3306)?,
            endpoint("db-3.example.com", 13306)
        );
        assert_eq!(
            parse_oss_mysql_endpoint("10.0.0.1", 3306)?,
            endpoint("10.0.0.1", 3306)
        );
        assert_eq!(
            parse_oss_mysql_endpoint("10.0.0.2:65535", 3306)?,
            endpoint("10.0.0.2", 65535)
        );
        assert_eq!(
            parse_oss_mysql_endpoint("my_db:1", 3306)?,
            endpoint("my_db", 1)
        );
        Ok(())
    }

    #[mononoke::test]
    fn test_endpoint_ipv6() -> Result<()> {
        assert_eq!(
            parse_oss_mysql_endpoint("[::1]:13307", 13306)?,
            endpoint("::1", 13307)
        );
        assert_eq!(
            parse_oss_mysql_endpoint("[fe80::1]", 13306)?,
            endpoint("fe80::1", 13306)
        );
        assert_eq!(
            parse_oss_mysql_endpoint("[2001:db8::2]:3306", 13306)?,
            endpoint("2001:db8::2", 3306)
        );

        let err = endpoint_error("::1");
        assert!(
            err.contains("must be in brackets, as in [::1]:3306"),
            "{err}"
        );
        let err = endpoint_error("fe80::1:3306");
        assert!(err.contains("must be in brackets"), "{err}");
        let err = endpoint_error("[::1");
        assert!(err.contains("missing ']'"), "{err}");
        let err = endpoint_error("[::1]3306");
        assert!(err.contains("expected ':port'"), "{err}");
        let err = endpoint_error("[db1]:3306");
        assert!(err.contains("not an IPv6 address"), "{err}");
        let err = endpoint_error("[]:3306");
        assert!(err.contains("not an IPv6 address"), "{err}");
        Ok(())
    }

    #[mononoke::test]
    fn test_endpoint_port_bounds() -> Result<()> {
        assert_eq!(parse_oss_mysql_endpoint("db1:1", 3306)?, endpoint("db1", 1));
        assert_eq!(
            parse_oss_mysql_endpoint("db2:65535", 3306)?,
            endpoint("db2", 65535)
        );
        assert_eq!(
            parse_oss_mysql_endpoint("db3:003306", 3306)?,
            endpoint("db3", 3306)
        );
        for entry in [
            "db1:0",
            "db1:65536",
            "db1:99999999999",
            "[::1]:0",
            "[::1]:70000",
        ] {
            let err = endpoint_error(entry);
            assert!(err.contains("out of range 1..=65535"), "{entry}: {err}");
        }
        for entry in [
            "db1:",
            "db1:+3306",
            "db1:-1",
            "db1:abc",
            "db1: 3306",
            "[::1]:",
        ] {
            let err = endpoint_error(entry);
            assert!(err.contains("is not a valid port"), "{entry}: {err}");
        }
        Ok(())
    }

    #[mononoke::test]
    fn test_endpoint_invalid_hosts() {
        for entry in ["", "  ", ":3306"] {
            let err = endpoint_error(entry);
            assert!(err.contains("empty host"), "{entry}: {err}");
        }
        for entry in ["db 1:3306", "db1/x", "db1,db2", "db1,db2:3306"] {
            let err = endpoint_error(entry);
            assert!(err.contains("not a valid host name"), "{entry}: {err}");
        }
        for entry in ["a:1:2", "db1:3306,db2:3306"] {
            let err = endpoint_error(entry);
            assert!(err.contains("is not a valid port"), "{entry}: {err}");
        }
    }

    #[mononoke::test]
    fn test_hosts_file() -> Result<()> {
        let contents = "\
# Mononoke MySQL candidates
127.0.0.1:13306

  # the replica
  127.0.0.1:13307\r
db3
[::1]:13308
";
        assert_eq!(
            parse_oss_mysql_hosts_file(contents, 3306)?,
            vec![
                endpoint("127.0.0.1", 13306),
                endpoint("127.0.0.1", 13307),
                endpoint("db3", 3306),
                endpoint("::1", 13308),
            ]
        );
        // No trailing newline.
        assert_eq!(
            parse_oss_mysql_hosts_file("db1", 13306)?,
            vec![endpoint("db1", 13306)]
        );
        Ok(())
    }

    #[mononoke::test]
    fn test_hosts_file_empty() {
        for contents in ["", "\n\n", "# nothing yet\n", "  \n# a\n\t\n"] {
            let err = hosts_file_error(contents);
            assert!(err.contains("no servers listed"), "{contents:?}: {err}");
        }
    }

    #[mononoke::test]
    fn test_hosts_file_invalid_line() {
        let err = hosts_file_error("db1\n\ndb2:0\n");
        assert!(err.contains("line 3: invalid entry 'db2:0'"), "{err}");
        assert!(err.contains("out of range"), "{err}");

        // Only whole-line comments are allowed.
        let err = hosts_file_error("db1:3306 # primary\n");
        assert!(err.contains("line 1: invalid entry"), "{err}");

        let err = hosts_file_error("db1, db2\n");
        assert!(err.contains("line 1: invalid entry 'db1, db2'"), "{err}");
    }

    #[mononoke::test]
    fn test_hosts_file_duplicates() {
        // Duplicates are detected after the default port is applied, and
        // host names are compared case-insensitively.
        let err = hosts_file_error("db1\ndb2\ndb1:3306\n");
        assert!(err.contains("line 3: duplicate entry 'db1:3306'"), "{err}");
        let err = hosts_file_error("DB1:3306\ndb1\n");
        assert!(err.contains("line 2: duplicate entry 'db1'"), "{err}");
        let err = hosts_file_error("[::1]\n[::1]:3306\n");
        assert!(err.contains("line 2: duplicate entry"), "{err}");

        // The same host on different ports is not a duplicate.
        assert_eq!(
            parse_oss_mysql_hosts_file("db1:3306\ndb1:3307\n", 3306).unwrap(),
            vec![endpoint("db1", 3306), endpoint("db1", 3307)]
        );
    }
}
