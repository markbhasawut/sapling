/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! The prober: one task per cluster that finds the MySQL primary.
//!
//! Every round reads the hosts file again if it changed, probes every
//! candidate over a fresh connection, passes what they answered to
//! [`decide`], and applies the actions to the cluster's pools.
//!
//! Rounds run every probe interval. They run every 250 ms instead while
//! there is no settled primary and for 10 s after a kick, for at most 60 s
//! in a row (with no limit when the probe interval is zero). A pool error
//! that may mean the primary changed, or a caller that waits for a primary,
//! kicks the prober into a round at once. However many kicks arrive, at most
//! one round starts every 100 ms.
//!
//! A round does not wait for every probe to end. Candidates other than the
//! primary's get a decision budget, shorter than the probe timeout: one that
//! has not answered by then counts as silent in this round, and its probe
//! answers the next one. So a candidate that never answers delays neither the
//! fast rounds nor a switch.
//!
//! A server with no connection to spare (1040, 1203) is up, and counts as
//! busy rather than silent: see [`Observation::busy`]. Pools that fill a
//! server's connections then do not get purged for it, only to fill them
//! again. A busy primary whose pools had a write refused because it is
//! read-only counts as answering that it is read-only: the pools found out
//! what its probe could not.
//!
//! Without a prober (`--oss-mysql-primary-check none`), a [`Refresher`]
//! keeps the pools up to date with the hosts file and the secrets.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use anyhow::Result;
use anyhow::anyhow;
use async_trait::async_trait;
use futures::FutureExt;
use futures::future::join_all;
use sql::mysql_async::Conn;
use sql::mysql_async::Error as MysqlError;
use sql::mysql_async::OptsBuilder;
use sql::mysql_async::prelude::Queryable;
use stats::prelude::*;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::Cluster;
use super::classify::ER_ACCESS_DENIED_ERROR;
use super::classify::OUT_OF_CONNECTIONS;
use super::decide::Action;
use super::decide::Candidate;
use super::decide::ClusterState;
use super::decide::Health;
use super::decide::Observation;
use super::decide::ProbeAnswer;
use super::decide::Target;
use super::decide::decide;
use super::hosts_file::CandidateList;
use super::hosts_file::Refresh;
use super::pool_connects_to;
use super::secrets::Credentials;

define_stats! {
    prefix = "mononoke.sql_ext.oss_mysql";
    probe_rounds: timeseries(Sum),
    probe_failures: timeseries(Sum),
    primary_switches: timeseries(Sum),
    primary_unavailable: timeseries(Sum),
    split_brain_rounds: timeseries(Sum),
}

/// What a probe reads. It needs no privilege, and a variable the server
/// does not have is simply missing from the answer.
pub const PROBE_QUERY: &str = "SHOW GLOBAL VARIABLES WHERE Variable_name IN \
     ('hostname', 'port', 'server_id', 'server_uuid', 'read_only', 'super_read_only', \
     'innodb_read_only', 'offline_mode', 'version')";

/// Minimum time between the starts of two rounds.
pub const MIN_ROUND_INTERVAL: Duration = Duration::from_millis(100);

/// Time between rounds in fast mode.
pub const FAST_PROBE_INTERVAL: Duration = Duration::from_millis(250);

/// How long a kick keeps the prober in fast mode.
pub const KICK_FAST_PERIOD: Duration = Duration::from_secs(10);

/// Longest run of fast rounds. The prober then uses the probe interval
/// until it would leave fast mode anyway.
pub const MAX_FAST_PERIOD: Duration = Duration::from_secs(60);

/// How long a round in fast mode waits for candidates other than the
/// primary's: two fast intervals.
pub const FAST_DECISION_BUDGET: Duration = Duration::from_millis(500);

/// Minimum time between two warnings about rejected credentials, about
/// secrets that cannot be resolved, or about a primary with no connection
/// to spare.
const ACCESS_DENIED_WARNING_INTERVAL: Duration = Duration::from_secs(60);

/// A probe of one candidate, which may outlive the round that started it.
type ProbeTask = JoinHandle<Result<ProbeAnswer>>;

/// How the prober reaches a candidate. Tests replace it.
#[async_trait]
pub trait ProbeTransport: Send + Sync + 'static {
    /// Reads the server variables of [`PROBE_QUERY`] from `candidate`. The
    /// prober bounds the time it takes.
    async fn probe(&self, candidate: &Candidate, credentials: &Credentials) -> Result<ProbeAnswer>;
}

/// Probes over a fresh connection, without a pool, that never switches to
/// the server's local socket.
pub struct MysqlProbeTransport;

#[async_trait]
impl ProbeTransport for MysqlProbeTransport {
    async fn probe(&self, candidate: &Candidate, credentials: &Credentials) -> Result<ProbeAnswer> {
        let opts = OptsBuilder::default()
            .ip_or_hostname(candidate.host.clone())
            .tcp_port(candidate.port)
            .user(Some(credentials.user.expose()))
            .pass(Some(credentials.password.expose()))
            .prefer_socket(false);
        let mut conn = Conn::new(opts).await?;
        let rows: Result<Vec<(String, Option<String>)>, MysqlError> = conn.query(PROBE_QUERY).await;
        // The answer does not wait for the server to see the connection close.
        tokio::spawn(async move {
            let _ = conn.disconnect().await;
        });
        ProbeAnswer::from_variables(
            rows?
                .into_iter()
                .filter_map(|(name, value)| Some((name, value?))),
        )
    }
}

/// Whether a probe failed because the server rejected the credentials.
pub fn is_access_denied(err: &anyhow::Error) -> bool {
    err.chain().any(|e| {
        matches!(
            e.downcast_ref::<MysqlError>(),
            Some(MysqlError::Server(err)) if err.code == ER_ACCESS_DENIED_ERROR
        )
    })
}

/// Whether a probe failed because the server had no connection to spare.
pub fn is_busy(err: &anyhow::Error) -> bool {
    err.chain().any(|e| {
        matches!(
            e.downcast_ref::<MysqlError>(),
            Some(MysqlError::Server(err)) if OUT_OF_CONNECTIONS.contains(&err.code)
        )
    })
}

/// When rounds are due.
#[derive(Debug)]
pub struct Cadence {
    /// The probe interval. Zero means that while the state is settled,
    /// rounds run only on kicks.
    steady: Duration,
    /// The end of the fast period that the last kick started.
    kicked_until: Option<Instant>,
    /// The start of the current run of fast rounds.
    fast_since: Option<Instant>,
}

impl Cadence {
    pub fn new(steady: Duration) -> Self {
        Self {
            steady,
            kicked_until: None,
            fast_since: None,
        }
    }

    /// Records that a round started at `now` served a kick.
    pub fn kicked(&mut self, now: Instant) {
        self.kicked_until = Some(now + KICK_FAST_PERIOD);
    }

    /// Whether rounds are fast at `now`: the state is unsettled, or a kick
    /// came less than [`KICK_FAST_PERIOD`] ago.
    pub fn wants_fast(&self, now: Instant, unsettled: bool) -> bool {
        unsettled || self.kicked_until.is_some_and(|until| now < until)
    }

    /// When the round after one that started at `last` is due, as seen at
    /// `now`, or `None` if only a kick starts it. `unsettled` is whether the
    /// state after the last round wants fast probes.
    pub fn next_round(&mut self, last: Instant, now: Instant, unsettled: bool) -> Option<Instant> {
        if self.steady.is_zero() {
            // Settled rounds run only on kicks. Unsettled ones run at the
            // fast interval for as long as that lasts, as there is no slower
            // interval to fall back to: only a round can find the primary,
            // and callers that wait for one kick only when they start to.
            self.fast_since = None;
            return unsettled.then(|| last + FAST_PROBE_INTERVAL);
        }
        let wants_fast = self.wants_fast(now, unsettled);
        let fast = if wants_fast {
            let since = *self.fast_since.get_or_insert(now);
            now.saturating_duration_since(since) < MAX_FAST_PERIOD
        } else {
            self.fast_since = None;
            false
        };
        let interval = if fast {
            FAST_PROBE_INTERVAL.min(self.steady)
        } else {
            self.steady
        };
        Some(last + interval)
    }
}

/// Finds the primary of one cluster.
pub(super) struct Prober {
    /// The cluster probed. The prober stops when it is dropped.
    cluster: Weak<Cluster>,
    transport: Arc<dyn ProbeTransport>,
    candidates: CandidateList,
    state: ClusterState,
    probe_timeout: Duration,
    cadence: Cadence,
    /// Probes that did not end within the decision budget of their round,
    /// by candidate. They answer the next round, so that a candidate is
    /// never probed twice at once.
    in_flight: HashMap<Candidate, ProbeTask>,
    /// When rejected credentials were last warned about.
    access_denied_warning: Option<Instant>,
    /// When a busy primary was last warned about.
    busy_warning: Option<Instant>,
    /// Whether a round has run.
    probed: bool,
}

impl Drop for Prober {
    fn drop(&mut self) {
        for probe in self.in_flight.values() {
            probe.abort();
        }
    }
}

impl Prober {
    pub(super) fn new(
        cluster: Weak<Cluster>,
        transport: Arc<dyn ProbeTransport>,
        candidates: CandidateList,
        probe_timeout: Duration,
        probe_interval: Duration,
    ) -> Self {
        Self {
            cluster,
            transport,
            candidates,
            state: ClusterState::new(),
            probe_timeout,
            cadence: Cadence::new(probe_interval),
            in_flight: HashMap::new(),
            access_denied_warning: None,
            busy_warning: None,
            probed: false,
        }
    }

    /// Runs rounds until the cluster is dropped. `last_round` is when the
    /// round run at the creation of the cluster started.
    pub(super) async fn run(mut self, kick: Arc<Notify>, mut last_round: Instant) {
        loop {
            let next =
                self.cadence
                    .next_round(last_round, Instant::now(), self.state.wants_fast_probes());
            let mut kicked = match next {
                Some(next) => tokio::select! {
                    _ = tokio::time::sleep_until(next) => false,
                    _ = kick.notified() => true,
                },
                None => {
                    kick.notified().await;
                    true
                }
            };
            tokio::time::sleep_until(last_round + MIN_ROUND_INTERVAL).await;
            // Kicks that arrived meanwhile are served by this round.
            kicked |= kick.notified().now_or_never().is_some();

            let Some(cluster) = self.cluster.upgrade() else {
                return;
            };
            let start = Instant::now();
            if kicked {
                self.cadence.kicked(start);
            }
            self.round(&cluster).await;
            last_round = start;
        }
    }

    /// Runs one round: probes every candidate, decides, and applies the
    /// decision to the cluster's pools.
    pub(super) async fn round(&mut self, cluster: &Cluster) {
        let start = Instant::now();
        if self.candidates.refresh(start) == Refresh::Changed {
            cluster.check_candidate_count(self.candidates.candidates().len());
        }
        let candidates = self.candidates.candidates().to_vec();
        let credentials = cluster.credentials();
        // Probes of candidates that are no longer listed are of no use.
        self.in_flight.retain(|candidate, probe| {
            let listed = candidates.contains(candidate);
            if !listed {
                probe.abort();
            }
            listed
        });
        let mut probes: Vec<ProbeTask> = candidates
            .iter()
            .map(|candidate| match self.in_flight.remove(candidate) {
                Some(probe) => probe,
                None => self.start_probe(candidate, &credentials),
            })
            .collect();
        let budget = self.decision_budget(start);
        let deadlines: Vec<Option<Instant>> = candidates
            .iter()
            .map(|candidate| (!self.waits_for(candidate)).then_some(start + budget))
            .collect();
        // `None` for probes still running at their deadline.
        let results = join_all(probes.iter_mut().zip(&deadlines).map(
            |(probe, deadline)| async move {
                match deadline {
                    Some(deadline) => tokio::time::timeout_at(*deadline, probe).await.ok(),
                    None => Some(probe.await),
                }
            },
        ))
        .await;
        STATS::probe_rounds.add_value(1);

        let mut access_denied = false;
        let mut failures = Vec::new();
        let mut observations = Vec::with_capacity(candidates.len());
        let write_refusals = cluster.take_write_refusals();
        for ((candidate, probe), result) in candidates.into_iter().zip(probes).zip(results) {
            let result = match result {
                Some(Ok(result)) => result,
                Some(Err(err)) => Err(anyhow!("the probe task failed: {err}")),
                None => {
                    self.in_flight.insert(candidate.clone(), probe);
                    Err(anyhow!("no answer within {budget:?}"))
                }
            };
            let (answer, busy) = match result {
                Ok(answer) => (Some(answer), false),
                Err(err) => {
                    STATS::probe_failures.add_value(1);
                    access_denied |= is_access_denied(&err);
                    tracing::debug!(
                        "MySQL cluster {}: probe of {candidate} failed: {err:#}",
                        cluster.name()
                    );
                    failures.push(format!("{candidate}: {err:#}"));
                    if !is_busy(&err) {
                        (None, false)
                    } else if let Some(answer) =
                        self.read_only_by_pools(&candidate, &write_refusals)
                    {
                        (Some(answer), false)
                    } else {
                        (None, true)
                    }
                }
            };
            observations.push(Observation {
                candidate,
                answer,
                busy,
            });
        }
        if let Some(primary) = self.active_primary()
            && observations
                .iter()
                .any(|o| o.busy && o.candidate == primary.candidate)
        {
            let candidate = primary.candidate.clone();
            self.warn_busy(cluster, &candidate);
        }

        let (state, actions) = decide(&self.state, &observations, cluster.mode(), Instant::now());

        // Credentials are read again when the server rejects them and before
        // a switch, keeping the last good ones when that fails.
        let switching = actions
            .iter()
            .any(|action| matches!(action, Action::SwitchPrimary(_)));
        let new_credentials = if access_denied || switching {
            cluster.resolve_credentials()
        } else {
            None
        };
        if access_denied && new_credentials.is_none() {
            self.warn_access_denied(cluster);
        }

        self.log_round(cluster, &state, &actions, &observations, &failures);
        cluster.apply(&actions, new_credentials, unavailable_reason(&state));
        self.state = state;
        cluster.record_round(&self.state, &observations, start);
        self.probed = true;
    }

    /// Starts a probe of `candidate`, bounded by the probe timeout.
    fn start_probe(&self, candidate: &Candidate, credentials: &Credentials) -> ProbeTask {
        let transport = self.transport.clone();
        let candidate = candidate.clone();
        let credentials = credentials.clone();
        let probe_timeout = self.probe_timeout;
        tokio::spawn(async move {
            match tokio::time::timeout(probe_timeout, transport.probe(&candidate, &credentials))
                .await
            {
                Ok(result) => result,
                Err(_) => Err(anyhow!("timed out after {probe_timeout:?}")),
            }
        })
    }

    /// The server the Primary pools point at, if they are active.
    fn active_primary(&self) -> Option<&Target> {
        self.state
            .primary
            .as_ref()
            .filter(|_| self.state.primary_active)
    }

    /// What the Primary pools tell of `candidate` when its probe found it
    /// busy: if they point at it, and one of them had a statement refused
    /// since the last round because the server is read-only, an answer that
    /// the primary is read-only. `write_refusals` are the endpoints of the
    /// pools that had one.
    fn read_only_by_pools(
        &self,
        candidate: &Candidate,
        write_refusals: &HashSet<Arc<str>>,
    ) -> Option<ProbeAnswer> {
        let primary = self
            .active_primary()
            .filter(|primary| primary.candidate == *candidate)?;
        write_refusals
            .iter()
            .any(|endpoint| pool_connects_to(endpoint, candidate))
            .then(|| ProbeAnswer::read_only(primary.identity.clone()))
    }

    /// Whether a round waits for the probe of `candidate` to end, rather
    /// than for the decision budget: the candidate the Primary pools use, so
    /// that a slow primary is not taken for a silent one, and that of a
    /// writer awaiting confirmation, so that a slow one is ever confirmed.
    /// A primary that already went a round without answering, while another
    /// server is writable, gets the budget like the others: a black-holed
    /// primary would otherwise hold up each round of the confirmation for
    /// the whole probe timeout.
    fn waits_for(&self, candidate: &Candidate) -> bool {
        let pending = self.state.pending.as_ref().map(|(target, _)| target);
        let primary = self
            .active_primary()
            .filter(|_| pending.is_none() || self.state.primary_silent_rounds == 0);
        primary
            .into_iter()
            .chain(pending)
            .any(|target| target.candidate == *candidate)
    }

    /// How long a round that starts at `now` waits for the other
    /// candidates: [`FAST_DECISION_BUDGET`] in fast mode, and the probe
    /// interval otherwise, so that a candidate that never answers does not
    /// hold up the round a kick asks for either. At most the probe timeout.
    fn decision_budget(&self, now: Instant) -> Duration {
        let budget = if self.cadence.wants_fast(now, self.state.wants_fast_probes()) {
            FAST_DECISION_BUDGET
        } else {
            self.cadence.steady.max(FAST_DECISION_BUDGET)
        };
        budget.min(self.probe_timeout)
    }

    fn warn_access_denied(&mut self, cluster: &Cluster) {
        let now = Instant::now();
        let warn = self.access_denied_warning.is_none_or(|last| {
            now.saturating_duration_since(last) >= ACCESS_DENIED_WARNING_INTERVAL
        });
        if warn {
            self.access_denied_warning = Some(now);
            tracing::warn!(
                "MySQL cluster {}: the server rejected the MySQL user or password, \
                 and the secrets give no new ones",
                cluster.name()
            );
        }
    }

    fn warn_busy(&mut self, cluster: &Cluster, candidate: &Candidate) {
        let now = Instant::now();
        let warn = self.busy_warning.is_none_or(|last| {
            now.saturating_duration_since(last) >= ACCESS_DENIED_WARNING_INTERVAL
        });
        if warn {
            self.busy_warning = Some(now);
            tracing::warn!(
                "MySQL cluster {}: the primary {candidate} has no connection to spare for \
                 probes (too many connections); its pools are kept",
                cluster.name()
            );
        }
    }

    fn log_round(
        &self,
        cluster: &Cluster,
        next: &ClusterState,
        actions: &[Action],
        observations: &[Observation],
        failures: &[String],
    ) {
        let name = cluster.name();
        for action in actions {
            match action {
                Action::SwitchPrimary(target) => {
                    STATS::primary_switches.add_value(1);
                    let answer = observations
                        .iter()
                        .find(|o| o.candidate == target.candidate)
                        .and_then(|o| o.answer.as_ref());
                    tracing::info!(
                        "MySQL cluster {name}: the primary is now {target} \
                         (server_uuid {}, version {})",
                        answer
                            .and_then(|a| a.server_uuid.as_deref())
                            .unwrap_or("unknown"),
                        answer
                            .and_then(|a| a.version.as_deref())
                            .unwrap_or("unknown"),
                    );
                }
                Action::ReplaceRead(target) => {
                    tracing::info!("MySQL cluster {name}: reads now go to {target}");
                }
                Action::MarkPrimaryUnavailable(reason) => {
                    STATS::primary_unavailable.add_value(1);
                    tracing::warn!(
                        "MySQL cluster {name}: no usable primary ({reason}); \
                         writes wait for one"
                    );
                }
            }
        }

        if next.writers.len() >= 2 {
            STATS::split_brain_rounds.add_value(1);
            tracing::error!(
                "MySQL cluster {name}: split brain: {} servers are writable: {}",
                next.writers.len(),
                next.writers
                    .iter()
                    .map(|w| w.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            return;
        }
        // Changes are logged, and the outcome of the first round.
        if self.probed
            && std::mem::discriminant(&next.health) == std::mem::discriminant(&self.state.health)
        {
            return;
        }
        match next.health {
            Health::Unknown => tracing::warn!(
                "MySQL cluster {name}: no server answered: {}",
                failures.join("; ")
            ),
            Health::NoPrimary => {
                // Unless the Primary pools were just marked unavailable,
                // which is logged above.
                if !actions
                    .iter()
                    .any(|action| matches!(action, Action::MarkPrimaryUnavailable(_)))
                {
                    tracing::warn!("MySQL cluster {name}: no writable server");
                }
            }
            Health::Pending => {
                if let Some((target, _)) = &next.pending {
                    tracing::info!(
                        "MySQL cluster {name}: {target} is writable; \
                         switching to it once confirmed"
                    );
                }
            }
            Health::Active => {
                // Unless it just switched, which is logged above.
                if !actions
                    .iter()
                    .any(|action| matches!(action, Action::SwitchPrimary(_)))
                    && let Some(primary) = &next.primary
                {
                    tracing::info!("MySQL cluster {name}: the primary is {primary} again");
                }
            }
            Health::SplitBrain { clean_rounds } => tracing::info!(
                "MySQL cluster {name}: at most one writable server for {clean_rounds} \
                 round(s); the Primary pools stay unavailable for now"
            ),
        }
    }
}

/// Keeps the pools of a cluster without a prober (none mode) up to date.
/// Every probe interval, and when a pool reports that the server rejected
/// the credentials, it reads the hosts file again if it changed, and the
/// secrets. When the single candidate or the credentials changed, it points
/// the pools at them.
pub(super) struct Refresher {
    /// The cluster refreshed. The refresher stops when it is dropped.
    cluster: Weak<Cluster>,
    candidates: CandidateList,
    /// Zero means refreshes run only on kicks.
    interval: Duration,
    /// When a problem with the secrets was last warned about.
    warning: Option<Instant>,
}

impl Refresher {
    pub(super) fn new(
        cluster: Weak<Cluster>,
        candidates: CandidateList,
        interval: Duration,
    ) -> Self {
        Self {
            cluster,
            candidates,
            interval,
            warning: None,
        }
    }

    /// Refreshes until the cluster is dropped. Kicks come from pools whose
    /// server rejected the credentials.
    pub(super) async fn run(mut self, kick: Arc<Notify>) {
        let mut last = Instant::now();
        loop {
            let kicked = if self.interval.is_zero() {
                kick.notified().await;
                true
            } else {
                tokio::select! {
                    _ = tokio::time::sleep_until(last + self.interval) => false,
                    _ = kick.notified() => true,
                }
            };
            tokio::time::sleep_until(last + MIN_ROUND_INTERVAL).await;
            let kicked = kicked | kick.notified().now_or_never().is_some();
            let Some(cluster) = self.cluster.upgrade() else {
                return;
            };
            last = Instant::now();
            self.refresh(&cluster, kicked);
        }
    }

    /// Refreshes once. `rejected` is whether a pool reported rejected
    /// credentials since the last refresh.
    pub(super) fn refresh(&mut self, cluster: &Cluster, rejected: bool) {
        let now = Instant::now();
        let candidate = match self.candidates.refresh(now) {
            Refresh::Changed => match self.candidates.candidates() {
                [candidate] => Some(candidate.clone()),
                // Warned about; the pools stay where they are.
                candidates => {
                    cluster.check_candidate_count(candidates.len());
                    None
                }
            },
            Refresh::Unchanged | Refresh::KeptLastGood { .. } => None,
        };
        let credentials = match cluster.changed_credentials() {
            Ok(credentials) => {
                if rejected && credentials.is_none() {
                    self.warn(|| {
                        format!(
                            "MySQL cluster {}: the server rejected the MySQL user or password, \
                             and the secrets give no new ones",
                            cluster.name()
                        )
                    });
                }
                credentials
            }
            Err(err) => {
                self.warn(|| {
                    format!(
                        "MySQL cluster {}: keeping the current credentials: {err:#}",
                        cluster.name()
                    )
                });
                None
            }
        };
        if candidate.is_some() || credentials.is_some() {
            cluster.retarget(candidate, credentials);
        }
    }

    fn warn(&mut self, message: impl FnOnce() -> String) {
        let now = Instant::now();
        if self.warning.is_none_or(|last| {
            now.saturating_duration_since(last) >= ACCESS_DENIED_WARNING_INTERVAL
        }) {
            self.warning = Some(now);
            tracing::warn!("{}", message());
        }
    }
}

/// Why the Primary pools are unavailable after a round, if they are.
fn unavailable_reason(state: &ClusterState) -> Option<String> {
    if state.primary_active {
        return None;
    }
    Some(match state.health {
        Health::Unknown => "no MySQL server answered".to_string(),
        Health::NoPrimary => "no writable server".to_string(),
        Health::Pending => "waiting to confirm the new primary".to_string(),
        Health::SplitBrain { .. } => "split brain".to_string(),
        Health::Active => "no primary yet".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::time::SystemTime;

    use mononoke_macros::mononoke;

    use super::*;
    use crate::oss::OssFailoverSettings;
    use crate::oss::PoolConfig;
    use crate::oss_mysql::OssMysqlTarget;
    use crate::oss_mysql::Role;
    use crate::oss_mysql::fake::*;

    const STEADY: Duration = Duration::from_secs(1);

    fn at(t0: Instant, seconds: f64) -> Instant {
        t0 + Duration::from_secs_f64(seconds)
    }

    /// The rounds that started from `from` to `to`, inclusive.
    fn between(rounds: &[Instant], from: Instant, to: Instant) -> Vec<Instant> {
        rounds
            .iter()
            .copied()
            .filter(|t| (from..=to).contains(t))
            .collect()
    }

    /// Asserts that consecutive rounds are `expected` apart.
    fn assert_gaps(rounds: &[Instant], expected: Duration) {
        assert!(rounds.len() >= 2, "too few rounds: {rounds:?}");
        for pair in rounds.windows(2) {
            let gap = pair[1] - pair[0];
            assert!(
                gap >= expected && gap <= expected + SLACK,
                "a gap of {gap:?} where {expected:?} was expected"
            );
        }
    }

    fn candidate((host, port): (&str, u16)) -> Candidate {
        Candidate::new(host, port)
    }

    fn write_secret(path: &Path, value: &str) -> Result<()> {
        fs::write(path, value)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }

    /// Writes a hosts file with the given modification time, in seconds
    /// since the epoch.
    fn write_hosts(path: &Path, contents: &str, modified: u64) -> Result<()> {
        fs::write(path, contents)?;
        fs::File::options()
            .write(true)
            .open(path)?
            .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(modified))?;
        Ok(())
    }

    #[mononoke::test]
    fn test_cadence() {
        let t0 = Instant::now();
        let s = |seconds| at(t0, seconds);

        let mut cadence = Cadence::new(STEADY);
        // Settled: the probe interval.
        assert_eq!(cadence.next_round(s(0.0), s(0.0), false), Some(s(1.0)));
        // Unsettled: fast, for at most 60 s in a row.
        assert_eq!(cadence.next_round(s(1.0), s(1.0), true), Some(s(1.25)));
        assert_eq!(cadence.next_round(s(60.75), s(60.75), true), Some(s(61.0)));
        assert_eq!(cadence.next_round(s(61.0), s(61.0), true), Some(s(62.0)));
        assert_eq!(cadence.next_round(s(62.0), s(62.0), true), Some(s(63.0)));
        // Settled again: the next unsettled run is fast again.
        assert_eq!(cadence.next_round(s(63.0), s(63.0), false), Some(s(64.0)));
        assert_eq!(cadence.next_round(s(64.0), s(64.0), true), Some(s(64.25)));

        // A kick: fast for 10 s, even when settled.
        let mut cadence = Cadence::new(STEADY);
        cadence.kicked(s(100.0));
        assert_eq!(
            cadence.next_round(s(100.0), s(100.0), false),
            Some(s(100.25))
        );
        assert_eq!(
            cadence.next_round(s(109.75), s(109.75), false),
            Some(s(110.0))
        );
        assert_eq!(
            cadence.next_round(s(110.0), s(110.0), false),
            Some(s(111.0))
        );

        // A probe interval shorter than the fast one is kept.
        let mut cadence = Cadence::new(Duration::from_millis(100));
        assert_eq!(cadence.next_round(s(0.0), s(0.0), true), Some(s(0.1)));

        // Zero: kicks only while settled, even right after a kick...
        let mut cadence = Cadence::new(Duration::ZERO);
        cadence.kicked(s(0.0));
        assert_eq!(cadence.next_round(s(0.0), s(0.0), false), None);
        // ...but fast while unsettled, for as long as that lasts: there is
        // no slower interval to fall back to.
        assert_eq!(cadence.next_round(s(0.0), s(0.0), true), Some(s(0.25)));
        assert_eq!(
            cadence.next_round(s(100.0), s(100.0), true),
            Some(s(100.25))
        );
        assert_eq!(cadence.next_round(s(101.0), s(101.0), false), None);
    }

    #[mononoke::test]
    async fn test_rounds_follow_the_cadence() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable), (B, Server::ReadOnly)]);
        let registry = registry(&fake, settings(STEADY));
        let cluster = registry
            .cluster(&target(&[A, B], "mononoke"), &PoolConfig::default())
            .await?;
        let primary = cluster.pool("mononoke", Role::Primary);
        let t0 = Instant::now();
        let s = |seconds| at(t0, seconds);

        // Settled: one round per second, the first one inline.
        tokio::time::sleep(Duration::from_millis(10_500)).await;
        let rounds = fake.rounds();
        assert_eq!(rounds.first(), Some(&t0));
        assert_eq!(rounds.len(), 11, "{rounds:?}");
        assert_gaps(&rounds, STEADY);

        // A is demoted and nothing is writable: rounds every 250 ms, for 60 s.
        fake.set(A, Server::ReadOnly);
        tokio::time::sleep_until(s(85.5)).await;
        assert_eq!(endpoint(&primary), None);
        let rounds = fake.rounds();
        assert_gaps(&between(&rounds, s(11.0), s(70.0)), FAST_PROBE_INTERVAL);
        // Then one per second, although there is still no primary.
        assert_gaps(&between(&rounds, s(72.5), s(85.5)), STEADY);

        // B is promoted: the next round switches, and the cluster is settled.
        fake.set(B, Server::Writable);
        tokio::time::sleep_until(s(100.5)).await;
        assert_eq!(
            endpoint(&primary).as_deref(),
            Some("127.0.0.1:13307/mononoke")
        );
        assert_gaps(&between(&fake.rounds(), s(86.5), s(100.5)), STEADY);

        // A kick: a round at once, then rounds every 250 ms for 10 s.
        cluster.kick.notify_one();
        tokio::time::sleep(Duration::from_millis(1)).await;
        let rounds = fake.rounds();
        let kicked = *rounds.last().unwrap();
        assert!(kicked >= s(100.5), "{kicked:?}");
        tokio::time::sleep_until(s(120.5)).await;
        let rounds = fake.rounds();
        assert_gaps(
            &between(&rounds, kicked, kicked + Duration::from_millis(9_900)),
            FAST_PROBE_INTERVAL,
        );
        assert_gaps(
            &between(&rounds, kicked + Duration::from_secs(11), s(120.5)),
            STEADY,
        );
        Ok(())
    }

    #[mononoke::test]
    async fn test_kicks_are_coalesced() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable)]);
        let registry = registry(&fake, settings(STEADY));
        let cluster = registry
            .cluster(&target(&[A], "mononoke"), &PoolConfig::default())
            .await?;
        let start = Instant::now();
        assert_eq!(fake.rounds(), vec![start]);

        for _ in 0..1000 {
            cluster.kick.notify_one();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let elapsed = Instant::now() - start;
        let rounds = fake.rounds();
        for pair in rounds.windows(2) {
            assert!(pair[1] - pair[0] >= MIN_ROUND_INTERVAL, "{rounds:?}");
        }
        // One round per 100 ms while the kicks last.
        let per_100ms = u32::try_from(elapsed.as_millis() / 100)?;
        let kicked = u32::try_from(fake.rounds_after(start).len())?;
        assert!(
            (per_100ms - 1..=per_100ms + 1).contains(&kicked),
            "{kicked} rounds in {elapsed:?}"
        );
        Ok(())
    }

    #[mononoke::test]
    async fn test_waiting_checkout_kicks() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::ReadOnly)]);
        let settings = OssFailoverSettings {
            checkout_timeout: Duration::from_secs(2),
            // Rounds every hour once the fast period is over.
            ..settings(Duration::from_secs(3_600))
        };
        let registry = registry(&fake, settings);
        let cluster = registry
            .cluster(&target(&[A], "mononoke"), &PoolConfig::default())
            .await?;
        let primary = cluster.pool("mononoke", Role::Primary);
        assert_eq!(endpoint(&primary), None);
        tokio::time::sleep(MAX_FAST_PERIOD + Duration::from_secs(10)).await;
        let rounds = fake.rounds().len();
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(fake.rounds().len(), rounds);

        // A is promoted. A checkout that finds no primary kicks the prober
        // as it starts to wait, rather than when it fails at its timeout,
        // and the round finds A.
        fake.set(A, Server::Writable);
        let started = Instant::now();
        let checkout = tokio::spawn({
            let primary = primary.clone();
            async move { primary.get_conn().await }
        });
        tokio::time::sleep(MIN_ROUND_INTERVAL / 2).await;
        let new_rounds = fake.rounds()[rounds..].to_vec();
        assert_eq!(new_rounds, vec![started]);
        assert_eq!(
            endpoint(&primary).as_deref(),
            Some("127.0.0.1:13306/mononoke")
        );
        checkout.abort();
        Ok(())
    }

    #[mononoke::test]
    async fn test_zero_interval_probes_while_unsettled() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::ReadOnly)]);
        // Rounds only on kicks while settled.
        let registry = registry(&fake, settings(Duration::ZERO));
        let cluster = registry
            .cluster(&target(&[A], "mononoke"), &PoolConfig::default())
            .await?;
        let primary = cluster.pool("mononoke", Role::Primary);
        // No primary: fast rounds, with no end, though nothing kicks.
        tokio::time::sleep(Duration::from_secs(120)).await;
        let rounds = fake.rounds();
        assert_gaps(&rounds, FAST_PROBE_INTERVAL);
        assert!(rounds.len() > 400, "{}", rounds.len());

        // A is promoted: the next round finds it, and rounds stop.
        fake.set(A, Server::Writable);
        tokio::time::sleep(FAST_PROBE_INTERVAL).await;
        assert_eq!(
            endpoint(&primary).as_deref(),
            Some("127.0.0.1:13306/mononoke")
        );
        let rounds = fake.rounds().len();
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(fake.rounds().len(), rounds);
        Ok(())
    }

    #[mononoke::test]
    async fn test_access_denied_resolves_secrets_again() -> Result<()> {
        tokio::time::pause();
        let dir = tempfile::tempdir()?;
        let password_file = dir.path().join("password");
        write_secret(&password_file, PASSWORD)?;
        let fake = FakeTransport::new(&[(A, Server::Writable)]);
        let registry = registry(&fake, settings(STEADY));
        let target = OssMysqlTarget {
            password_secret: format!("file:{}", password_file.display()),
            ..target(&[A], "mononoke")
        };
        let cluster = registry.cluster(&target, &PoolConfig::default()).await?;
        let primary = cluster.pool("mononoke", Role::Primary);
        let read = cluster.pool("mononoke", Role::Read);
        assert_eq!((primary.generation(), read.generation()), (0, 0));

        // The password changes on the server only. Probes are rejected, and
        // the secret still gives the old password: the pools stay.
        fake.set_password("n3w-s3cret");
        let rotated = Instant::now();
        tokio::time::sleep(Duration::from_secs(3)).await;
        let probes = fake.probes();
        let rejected: Vec<_> = probes.iter().filter(|p| p.at > rotated).collect();
        assert!(rejected.len() >= 3, "{rejected:?}");
        assert!(rejected.iter().all(|p| p.password == PASSWORD));
        assert_eq!((primary.generation(), read.generation()), (0, 0));

        // The secret gets the new password: the next rejected probe reads it
        // again, and the pools are rebuilt with it.
        write_secret(&password_file, "n3w-s3cret\n")?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(cluster.credentials().password.expose(), "n3w-s3cret");
        assert_eq!((primary.generation(), read.generation()), (1, 1));
        assert_eq!(
            endpoint(&primary).as_deref(),
            Some("127.0.0.1:13306/mononoke")
        );
        assert_eq!(fake.probes().last().unwrap().password, "n3w-s3cret");
        Ok(())
    }

    #[mononoke::test]
    async fn test_switch_keeps_credentials_when_secrets_fail() -> Result<()> {
        tokio::time::pause();
        let dir = tempfile::tempdir()?;
        let password_file = dir.path().join("password");
        write_secret(&password_file, PASSWORD)?;
        let fake = FakeTransport::new(&[(A, Server::Writable), (B, Server::ReadOnly)]);
        let registry = registry(&fake, settings(STEADY));
        let target = OssMysqlTarget {
            password_secret: format!("file:{}", password_file.display()),
            ..target(&[A, B], "mononoke")
        };
        let cluster = registry.cluster(&target, &PoolConfig::default()).await?;
        let primary = cluster.pool("mononoke", Role::Primary);

        // The secret cannot be read when the primary moves to B: the switch
        // goes ahead with the credentials in use.
        fs::remove_file(&password_file)?;
        fake.set(A, Server::ReadOnly);
        fake.set(B, Server::Writable);
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        assert_eq!(
            endpoint(&primary).as_deref(),
            Some("127.0.0.1:13307/mononoke")
        );
        assert_eq!(cluster.credentials().password.expose(), PASSWORD);
        assert!(fake.probes().iter().all(|p| p.password == PASSWORD));
        Ok(())
    }

    #[mononoke::test]
    async fn test_hosts_file_change() -> Result<()> {
        tokio::time::pause();
        let dir = tempfile::tempdir()?;
        let hosts = dir.path().join("hosts");
        write_hosts(&hosts, "# the primary\n127.0.0.1:13306\n", 1_000)?;
        // B is writable too, but not listed yet.
        let fake = FakeTransport::new(&[(A, Server::Writable), (B, Server::Writable)]);
        let registry = registry(&fake, settings(STEADY));
        let cluster = registry
            .cluster(
                &hosts_file_target(hosts.clone(), &[C]),
                &PoolConfig::default(),
            )
            .await?;
        let primary = cluster.pool("mononoke", Role::Primary);
        let read = cluster.pool("mononoke", Role::Read);
        tokio::time::sleep(Duration::from_millis(2_500)).await;
        assert_eq!(
            endpoint(&primary).as_deref(),
            Some("127.0.0.1:13306/mononoke")
        );
        assert!(fake.probes().iter().all(|p| p.candidate == candidate(A)));

        // The operator moved the primary to B, and lists only B. A, still
        // writable, is no longer probed, and the pools move to B.
        write_hosts(&hosts, "127.0.0.1:13307\n", 2_000)?;
        let edited = Instant::now();
        tokio::time::sleep(Duration::from_secs(3)).await;
        let probes: Vec<_> = fake
            .probes()
            .into_iter()
            .filter(|p| p.at > edited)
            .collect();
        assert!(!probes.is_empty());
        assert!(
            probes.iter().all(|p| p.candidate == candidate(B)),
            "{probes:?}"
        );
        for pool in [&primary, &read] {
            assert_eq!(endpoint(pool).as_deref(), Some("127.0.0.1:13307/mononoke"));
        }

        // A bad edit keeps the last good list.
        write_hosts(&hosts, "127.0.0.1:99999\n", 3_000)?;
        let broken = Instant::now();
        tokio::time::sleep(Duration::from_secs(3)).await;
        let probes: Vec<_> = fake
            .probes()
            .into_iter()
            .filter(|p| p.at > broken)
            .collect();
        assert!(!probes.is_empty());
        assert!(probes.iter().all(|p| p.candidate == candidate(B)));
        assert_eq!(
            endpoint(&primary).as_deref(),
            Some("127.0.0.1:13307/mononoke")
        );
        Ok(())
    }

    #[mononoke::test]
    async fn test_silent_candidate_times_out() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Silent), (B, Server::Writable)]);
        let registry = registry(&fake, settings(STEADY));
        let started = Instant::now();
        let cluster = registry
            .cluster(&target(&[A, B], "mononoke"), &PoolConfig::default())
            .await?;
        // The inline round is in fast mode, as nothing is known yet: it
        // waits for A only for the decision budget.
        assert_elapsed(started, FAST_DECISION_BUDGET);
        assert_eq!(
            endpoint(&cluster.pool("mononoke", Role::Primary)).as_deref(),
            Some("127.0.0.1:13307/mononoke")
        );
        // Settled rounds wait for the probe interval at most. A is probed
        // again only once its probe timed out: --mysql-conn-open-timeout
        // plus one second.
        tokio::time::sleep(Duration::from_secs(20)).await;
        let rounds = fake.rounds();
        assert_eq!(rounds.len(), 21, "{rounds:?}");
        assert_gaps(&rounds, STEADY);
        let probes_of_a: Vec<Instant> = fake
            .probes()
            .iter()
            .filter(|p| p.candidate == candidate(A))
            .map(|p| p.at)
            .collect();
        assert!(probes_of_a.len() >= 4, "{probes_of_a:?}");
        for pair in probes_of_a.windows(2) {
            let gap = pair[1] - pair[0];
            assert!(
                gap >= Duration::from_secs(4) && gap <= Duration::from_secs(5) + SLACK,
                "{probes_of_a:?}"
            );
        }
        Ok(())
    }

    #[mononoke::test]
    async fn test_silent_candidate_does_not_delay_a_switch() -> Result<()> {
        tokio::time::pause();
        // C never answers, like a host whose packets are dropped.
        let fake = FakeTransport::new(&[
            (A, Server::Writable),
            (B, Server::ReadOnly),
            (C, Server::Silent),
        ]);
        let registry = registry(&fake, settings(STEADY));
        let t0 = Instant::now();
        let cluster = registry
            .cluster(&target(&[A, B, C], "mononoke"), &PoolConfig::default())
            .await?;
        let primary = cluster.pool("mononoke", Role::Primary);
        tokio::time::sleep_until(at(t0, 10.3)).await;
        assert_eq!(
            endpoint(&primary).as_deref(),
            Some("127.0.0.1:13306/mononoke")
        );

        // A crashes and B is promoted at once; the failing writes kick the
        // prober. A did not answer, so B must be seen in two rounds.
        fake.set(A, Server::Down);
        fake.set(B, Server::Writable);
        let crashed = Instant::now();
        cluster.kick.notify_one();
        while endpoint(&primary).as_deref() != Some("127.0.0.1:13307/mononoke") {
            assert!(
                Instant::now() - crashed < Duration::from_secs(10),
                "no switch to B"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // The round under way when the kick came ends within the probe
        // interval; then two fast rounds.
        let switched = Instant::now() - crashed;
        assert!(
            switched <= STEADY + 2 * FAST_DECISION_BUDGET + SLACK,
            "the switch took {switched:?}"
        );

        // C was never probed twice at once.
        let probes_of_c: Vec<Instant> = fake
            .probes()
            .iter()
            .filter(|p| p.candidate == candidate(C))
            .map(|p| p.at)
            .collect();
        for pair in probes_of_c.windows(2) {
            assert!(
                pair[1] - pair[0] >= Duration::from_secs(4),
                "{probes_of_c:?}"
            );
        }
        Ok(())
    }

    #[mononoke::test]
    async fn test_black_holed_primary_does_not_delay_the_confirmation() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable), (B, Server::ReadOnly)]);
        let registry = registry(&fake, settings(STEADY));
        let t0 = Instant::now();
        let cluster = registry
            .cluster(&target(&[A, B], "mononoke"), &PoolConfig::default())
            .await?;
        let primary = cluster.pool("mononoke", Role::Primary);
        tokio::time::sleep_until(at(t0, 10.5)).await;

        // A is stopped (it accepts connections and never answers), and B is
        // promoted. The next round waits the probe timeout for A, as it may
        // only be slow, and sees B writable once. A did not answer, so B
        // must be seen in a second round, which need not wait for A again.
        fake.set(A, Server::Silent);
        fake.set(B, Server::Writable);
        let stopped = Instant::now();
        let probe_timeout = Duration::from_millis(PoolConfig::default().conn_open_timeout_ms)
            + Duration::from_secs(1);
        while endpoint(&primary).as_deref() != Some("127.0.0.1:13307/mononoke") {
            assert!(
                Instant::now() - stopped < Duration::from_secs(20),
                "no switch to B"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // The round under way ends within the probe interval, the next one
        // at A's probe timeout, and the one that switches within the budget.
        let switched = Instant::now() - stopped;
        assert!(
            switched <= STEADY + probe_timeout + FAST_DECISION_BUDGET + SLACK,
            "the switch took {switched:?}"
        );
        Ok(())
    }

    #[mononoke::test]
    async fn test_busy_primary_keeps_its_pools() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable), (B, Server::ReadOnly)]);
        let registry = registry(&fake, settings(STEADY));
        let cluster = registry
            .cluster(&target(&[A, B], "mononoke"), &PoolConfig::default())
            .await?;
        let primary = cluster.pool("mononoke", Role::Primary);
        let rounds = fake.rounds().len();

        // The pools of every process take all of A's connections, so its
        // probes are turned away more often than not, in fast rounds.
        for round in 0..40 {
            let server = if round % 4 == 3 {
                Server::Writable
            } else {
                Server::Busy
            };
            fake.set(A, server);
            cluster.kick.notify_one();
            tokio::time::sleep(FAST_PROBE_INTERVAL).await;
        }
        assert!(fake.rounds().len() >= rounds + 30, "{:?}", fake.rounds());
        // A is up: its pools are neither purged nor rebuilt.
        assert_eq!(primary.generation(), 0);
        assert_eq!(
            endpoint(&primary).as_deref(),
            Some("127.0.0.1:13306/mononoke")
        );

        // A is demoted while busy: a write of its pools is refused. The next
        // round takes A to be read-only, and the pools wait for a primary.
        fake.set(A, Server::Busy);
        cluster
            .write_refusals
            .lock()
            .unwrap()
            .insert(Arc::from("127.0.0.1:13306/mononoke"));
        cluster.kick.notify_one();
        tokio::time::sleep(MIN_ROUND_INTERVAL).await;
        assert_eq!(endpoint(&primary), None);
        assert_eq!(primary.generation(), 1);
        // B is promoted.
        fake.set(B, Server::Writable);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            endpoint(&primary).as_deref(),
            Some("127.0.0.1:13307/mononoke")
        );

        // A refusal by the pools of another server tells nothing of B.
        let primary_generation = primary.generation();
        fake.set(B, Server::Busy);
        cluster
            .write_refusals
            .lock()
            .unwrap()
            .insert(Arc::from("127.0.0.1:13306/mononoke"));
        cluster.kick.notify_one();
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(primary.generation(), primary_generation);
        Ok(())
    }

    #[mononoke::test]
    fn test_busy_detection() {
        let error = |code: u16| {
            anyhow::Error::from(MysqlError::Server(sql::mysql_async::ServerError {
                code,
                message: "error".to_string(),
                state: "HY000".to_string(),
            }))
        };
        assert!(is_busy(&error(1040)));
        assert!(is_busy(&error(1203).context("probe of 127.0.0.1:13306")));
        assert!(!is_busy(&error(1045)));
        assert!(!is_busy(&error(1053)));
        assert!(!is_busy(&anyhow!("Connection refused (os error 61)")));
    }

    #[mononoke::test]
    async fn test_mysql_transport_connection_refused() -> Result<()> {
        let credentials = Credentials::resolve("literal:mononoke", "literal:s3cret", None)?;
        // Nothing listens on port 1.
        let closed = Candidate::new("127.0.0.1", 1);
        let probe = MysqlProbeTransport.probe(&closed, &credentials);
        let err = tokio::time::timeout(Duration::from_secs(10), probe)
            .await?
            .expect_err("nothing listens on port 1");
        assert!(!is_access_denied(&err), "{err:#}");
        Ok(())
    }

    #[mononoke::test]
    fn test_access_denied_detection() {
        let denied = anyhow::Error::from(MysqlError::Server(sql::mysql_async::ServerError {
            code: 1045,
            message: "Access denied".to_string(),
            state: "28000".to_string(),
        }));
        assert!(is_access_denied(&denied));
        assert!(is_access_denied(
            &denied.context("probe of 127.0.0.1:13306")
        ));
        let other = anyhow::Error::from(MysqlError::Server(sql::mysql_async::ServerError {
            code: 1040,
            message: "Too many connections".to_string(),
            state: "08004".to_string(),
        }));
        assert!(!is_access_denied(&other));
        assert!(!is_access_denied(&anyhow!("connection refused")));
    }
}
