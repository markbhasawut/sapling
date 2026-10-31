/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! Fake servers for the prober and registry tests.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use anyhow::anyhow;
use async_trait::async_trait;
use sql::mysql_async::Error as MysqlError;
use sql::mysql_async::ServerError;
use sql_common::mysql::OssPool;
use sql_common::mysql::OssPoolState;
use tokio::time::Instant;

use super::OssMysqlRegistry;
use super::OssMysqlTarget;
use super::decide::Candidate;
use super::decide::PrimaryCheck;
use super::decide::ProbeAnswer;
use super::decide::ServerIdentity;
use super::probe::ProbeTransport;
use super::secrets::Credentials;
use crate::oss::OssFailoverSettings;

pub const A: (&str, u16) = ("127.0.0.1", 13306);
pub const B: (&str, u16) = ("127.0.0.1", 13307);
pub const C: (&str, u16) = ("127.0.0.1", 13308);

/// The password the fake servers accept at first.
pub const PASSWORD: &str = "s3cret";

/// Paused timers fire on whole milliseconds, so a wait may last up to that
/// much longer.
pub const SLACK: Duration = Duration::from_millis(2);

/// How a fake server answers probes.
#[derive(Clone, Copy, Debug)]
pub enum Server {
    Writable,
    ReadOnly,
    /// Refuses connections.
    Down,
    /// Never answers.
    Silent,
    /// Up, with no connection to spare: turns probes away with 1040.
    Busy,
}

/// A probe the fake servers received.
#[derive(Clone, Debug)]
pub struct Probe {
    pub at: Instant,
    pub candidate: Candidate,
    pub password: String,
}

/// Fake servers, one per candidate, that all accept one password. The
/// server behind candidate A, B or C has server_id 1, 2 or 3.
pub struct FakeTransport {
    servers: Mutex<HashMap<Candidate, Server>>,
    password: Mutex<String>,
    probes: Mutex<Vec<Probe>>,
}

impl FakeTransport {
    pub fn new(servers: &[((&str, u16), Server)]) -> Arc<Self> {
        let fake = Arc::new(Self {
            servers: Mutex::new(HashMap::new()),
            password: Mutex::new(PASSWORD.to_string()),
            probes: Mutex::new(Vec::new()),
        });
        for (candidate, server) in servers {
            fake.set(*candidate, *server);
        }
        fake
    }

    pub fn set(&self, (host, port): (&str, u16), server: Server) {
        self.servers
            .lock()
            .unwrap()
            .insert(Candidate::new(host, port), server);
    }

    pub fn set_password(&self, password: &str) {
        *self.password.lock().unwrap() = password.to_string();
    }

    pub fn probes(&self) -> Vec<Probe> {
        self.probes.lock().unwrap().clone()
    }

    /// When each round started, in order: the probes of a round all start
    /// at the same (paused) time.
    pub fn rounds(&self) -> Vec<Instant> {
        let mut rounds: Vec<Instant> = self.probes().iter().map(|p| p.at).collect();
        rounds.dedup();
        rounds
    }

    /// The rounds that started after `after`.
    pub fn rounds_after(&self, after: Instant) -> Vec<Instant> {
        self.rounds().into_iter().filter(|at| *at > after).collect()
    }
}

#[async_trait]
impl ProbeTransport for FakeTransport {
    async fn probe(&self, candidate: &Candidate, credentials: &Credentials) -> Result<ProbeAnswer> {
        self.probes.lock().unwrap().push(Probe {
            at: Instant::now(),
            candidate: candidate.clone(),
            password: credentials.password.expose().to_string(),
        });
        let server = self.servers.lock().unwrap().get(candidate).copied();
        let read_only = match server {
            Some(Server::Writable) => "OFF",
            Some(Server::ReadOnly) => "ON",
            Some(Server::Silent) => std::future::pending().await,
            Some(Server::Busy) => {
                return Err(MysqlError::Server(ServerError {
                    code: 1040,
                    message: "Too many connections".to_string(),
                    state: "08004".to_string(),
                })
                .into());
            }
            Some(Server::Down) | None => return Err(anyhow!("Connection refused (os error 61)")),
        };
        if credentials.password.expose() != *self.password.lock().unwrap() {
            return Err(MysqlError::Server(ServerError {
                code: 1045,
                message: "Access denied for user 'mononoke'@'127.0.0.1' (using password: YES)"
                    .to_string(),
                state: "28000".to_string(),
            })
            .into());
        }
        Ok(ProbeAnswer {
            identity: ServerIdentity {
                server_id: u64::from(candidate.port - 13305),
                hostname: "mac".to_string(),
                port: candidate.port,
            },
            server_uuid: Some(format!("uuid-{}", candidate.port)),
            version: Some("9.7.0-1".to_string()),
            read_only: Some(read_only.to_string()),
            super_read_only: Some(read_only.to_string()),
            innodb_read_only: Some("OFF".to_string()),
            offline_mode: Some("OFF".to_string()),
        })
    }
}

pub fn settings(probe_interval: Duration) -> OssFailoverSettings {
    OssFailoverSettings {
        probe_interval,
        primary_check: PrimaryCheck::Identity,
        secrets_dir: None,
        checkout_timeout: Duration::from_secs(10),
    }
}

pub fn registry(fake: &Arc<FakeTransport>, settings: OssFailoverSettings) -> OssMysqlRegistry {
    OssMysqlRegistry::with_transport(settings, fake.clone())
}

pub fn target(endpoints: &[(&str, u16)], database: &str) -> OssMysqlTarget {
    OssMysqlTarget {
        endpoints: endpoints
            .iter()
            .map(|(host, port)| (host.to_string(), *port))
            .collect(),
        hosts_file: None,
        default_port: 3306,
        database: database.to_string(),
        user_secret: "literal:mononoke".to_string(),
        password_secret: format!("literal:{PASSWORD}"),
    }
}

pub fn hosts_file_target(path: PathBuf, fallback: &[(&str, u16)]) -> OssMysqlTarget {
    OssMysqlTarget {
        hosts_file: Some(path),
        ..target(fallback, "mononoke")
    }
}

/// Where `pool` points, as "host:port/database", or `None` while it is
/// unavailable.
pub fn endpoint(pool: &OssPool) -> Option<String> {
    match pool.state() {
        OssPoolState::Active { endpoint, .. } => Some(endpoint.to_string()),
        OssPoolState::Unavailable { .. } => None,
    }
}

/// Whether `a` and `b` are handles of the same pool. Changes the state of
/// `a`, so use it last.
pub fn same_pool(a: &OssPool, b: &OssPool) -> bool {
    let before = b.generation();
    a.mark_unavailable("checking which pool this is");
    b.generation() != before
}

/// Asserts that `expected` passed since `started`, give or take [`SLACK`].
pub fn assert_elapsed(started: Instant, expected: Duration) {
    let elapsed = Instant::now() - started;
    assert!(
        elapsed >= expected && elapsed <= expected + SLACK,
        "{elapsed:?} passed where {expected:?} was expected"
    );
}
