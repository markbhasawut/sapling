/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! Choosing the MySQL primary from one round of probes.
//!
//! The primary is the unique writable server among the candidates. Mononoke
//! never elects one: it follows the `read_only` flags that the operator or
//! the failover tool sets, and makes the Primary pools unavailable when the
//! picture is ambiguous.
//!
//! [`decide`] is pure. The prober passes in the state after the previous
//! round and what every candidate answered in this one, and applies the
//! [`Action`]s it returns. The candidates are an input of every round, so
//! the list may change between rounds.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use tokio::time::Instant;

/// Clean rounds in a row needed to leave split brain: see
/// [`Health::SplitBrain`].
pub const SPLIT_BRAIN_CLEAN_ROUNDS: u32 = 3;

/// Rounds in a row that must see the same new writable server before
/// switching to it, when the previous primary did not answer. Also the
/// rounds in a row the candidate of the Primary pools may stay silent before
/// they are moved or made unavailable.
pub const CRASH_CONFIRM_ROUNDS: u32 = 2;

/// Minimum time between two switches of the primary.
pub const MIN_SWITCH_INTERVAL: Duration = Duration::from_secs(1);

/// How the primary is recognised and fenced (`--oss-mysql-primary-check`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum PrimaryCheck {
    /// The primary is the unique writable server, told apart by server_id,
    /// hostname and port. New Primary-pool connections check that they
    /// reach that server and that it is still writable. Works with any
    /// number of candidates.
    #[default]
    Identity,
    /// The primary is whatever writable server the single candidate
    /// reaches; identities are ignored. For Galera, PXC or multi-primary
    /// Group Replication behind a single-writer balancer.
    Writable,
    /// No prober and no fence: the single candidate is the primary. For
    /// SQL-aware proxies such as ProxySQL or MaxScale.
    None,
}

impl FromStr for PrimaryCheck {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "identity" => Ok(PrimaryCheck::Identity),
            "writable" => Ok(PrimaryCheck::Writable),
            "none" => Ok(PrimaryCheck::None),
            _ => bail!("unknown primary check '{s}': expected identity, writable or none"),
        }
    }
}

impl fmt::Display for PrimaryCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            PrimaryCheck::Identity => "identity",
            PrimaryCheck::Writable => "writable",
            PrimaryCheck::None => "none",
        })
    }
}

/// A candidate server as configured: a host name or IP address, and a port.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Candidate {
    /// Host name or IP address (IPv6 without brackets).
    pub host: String,
    /// TCP port.
    pub port: u16,
}

impl Candidate {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }
}

impl From<(String, u16)> for Candidate {
    fn from((host, port): (String, u16)) -> Self {
        Self { host, port }
    }
}

impl fmt::Display for Candidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// What tells MySQL servers apart. Candidates that reach the same server,
/// such as a name and its IP address, see the same identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServerIdentity {
    /// `@@server_id`.
    pub server_id: u64,
    /// `@@hostname`.
    pub hostname: String,
    /// `@@port`.
    pub port: u16,
}

impl fmt::Display for ServerIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "server_id {} on {}:{}",
            self.server_id, self.hostname, self.port
        )
    }
}

/// The server variables one probe read, from
/// `SHOW GLOBAL VARIABLES WHERE Variable_name IN (..)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProbeAnswer {
    /// The server that answered.
    pub identity: ServerIdentity,
    /// `@@server_uuid`, for logs. MariaDB has none.
    pub server_uuid: Option<String>,
    /// `@@version`, for logs.
    pub version: Option<String>,
    /// `@@read_only`: 0/1 or OFF/ON, or on MariaDB also NO_LOCK and
    /// NO_LOCK_NO_ADMIN.
    pub read_only: Option<String>,
    /// `@@super_read_only`, for logs: it forces `read_only` on.
    pub super_read_only: Option<String>,
    /// `@@innodb_read_only`.
    pub innodb_read_only: Option<String>,
    /// `@@offline_mode`. MariaDB has none.
    pub offline_mode: Option<String>,
}

impl ProbeAnswer {
    /// Builds an answer from (variable name, value) rows. Variables the
    /// server does not have are absent; hostname, port and server_id are
    /// required.
    pub fn from_variables<I, K, V>(rows: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let mut variables: HashMap<String, String> = rows
            .into_iter()
            .map(|(name, value)| (name.as_ref().to_ascii_lowercase(), value.into()))
            .collect();
        let mut take = |name: &str| variables.remove(name);
        let required = |value: Option<String>, name: &str| {
            value
                .filter(|value| !value.is_empty())
                .ok_or_else(|| anyhow!("the server did not report its {name}"))
        };
        let hostname = required(take("hostname"), "hostname")?;
        let port = required(take("port"), "port")?;
        let server_id = required(take("server_id"), "server_id")?;
        Ok(ProbeAnswer {
            identity: ServerIdentity {
                server_id: server_id
                    .trim()
                    .parse()
                    .with_context(|| format!("invalid server_id '{server_id}'"))?,
                hostname,
                port: port
                    .trim()
                    .parse()
                    .with_context(|| format!("invalid port '{port}'"))?,
            },
            server_uuid: take("server_uuid"),
            version: take("version"),
            read_only: take("read_only"),
            super_read_only: take("super_read_only"),
            innodb_read_only: take("innodb_read_only"),
            offline_mode: take("offline_mode"),
        })
    }

    /// Whether the server accepts writes from a user without admin
    /// privileges. A missing `read_only` or `innodb_read_only` counts as
    /// read-only.
    pub fn writable(&self) -> bool {
        is_off(self.read_only.as_deref())
            && is_off(self.innodb_read_only.as_deref())
            && !is_on(self.offline_mode.as_deref())
    }

    /// An answer that `identity` is read-only, for a server known to be
    /// read-only although its probe failed.
    pub fn read_only(identity: ServerIdentity) -> Self {
        ProbeAnswer {
            identity,
            server_uuid: None,
            version: None,
            read_only: Some("ON".to_string()),
            super_read_only: None,
            innodb_read_only: None,
            offline_mode: None,
        }
    }
}

fn is_off(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        let value = value.trim();
        value == "0" || value.eq_ignore_ascii_case("OFF")
    })
}

fn is_on(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        let value = value.trim();
        value == "1" || value.eq_ignore_ascii_case("ON")
    })
}

/// The result of probing one candidate in a round.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observation {
    /// The candidate probed.
    pub candidate: Candidate,
    /// What it answered, or `None` if the probe failed or timed out.
    pub answer: Option<ProbeAnswer>,
    /// Whether the probe failed because the server had no connection to
    /// spare (1040, 1203): a server is up behind the candidate, but what it
    /// would have answered is unknown. False when there is an answer.
    pub busy: bool,
}

/// A server that pools point at: the candidate they connect to, and the
/// server it reached when probed.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Target {
    /// Where the pools connect.
    pub candidate: Candidate,
    /// The server the candidate reached.
    pub identity: ServerIdentity,
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.candidate, self.identity)
    }
}

/// The verdict of the last round.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Health {
    /// No candidate answered, or no round has run yet. The pools are left
    /// as they are.
    Unknown,
    /// Exactly one writable server, and the Primary pools use it. Their own
    /// candidate may have been silent for fewer than
    /// [`CRASH_CONFIRM_ROUNDS`] rounds while the server answered through
    /// another. It may also be busy (see [`Observation::busy`]), with no
    /// other server writable: the server is up, and taken to be as it was.
    Active,
    /// Exactly one writable server that the Primary pools do not use yet:
    /// it awaits a second round or the switch rate limit.
    Pending,
    /// Candidates answered, but none is writable. When the primary itself
    /// was silent, the Primary pools are kept for up to
    /// [`CRASH_CONFIRM_ROUNDS`] rounds, as a slow primary looks the same.
    NoPrimary,
    /// Two or more writable servers were seen. The Primary pools stay
    /// unavailable until [`SPLIT_BRAIN_CLEAN_ROUNDS`] clean rounds in a row:
    /// rounds in which every server of the split answers and at most one,
    /// the same one each time, is writable. A server of the split that goes
    /// silent may still be writable, so rounds in which one is silent,
    /// like rounds with no answer, neither count nor reset. A server leaves
    /// the split when it answers read-only or its candidate is no longer
    /// listed.
    SplitBrain {
        /// Clean rounds in a row so far.
        clean_rounds: u32,
    },
}

/// What one cluster's prober knows after a round.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterState {
    /// The verdict of the last round.
    pub health: Health,
    /// The server the Primary pools point at while `primary_active`;
    /// otherwise the last primary, if any.
    pub primary: Option<Target>,
    /// Whether the Primary pools point at `primary`. When false they are
    /// unavailable.
    pub primary_active: bool,
    /// The server the Read pools point at, if any.
    pub read: Option<Target>,
    /// The distinct writable servers seen in the last round.
    pub writers: Vec<Target>,
    /// A writable server the primary has not switched to yet, and in how
    /// many rounds in a row it was seen.
    pub pending: Option<(Target, u32)>,
    /// When the primary last switched.
    pub last_switch: Option<Instant>,
    /// Rounds in a row in which the candidate the Primary pools use did not
    /// answer, while they were active. Rounds in which no candidate
    /// answered leave it as it is, and a round in which it was busy ends
    /// the run: the server is up.
    pub primary_silent_rounds: u32,
    /// In split brain: the servers of the split. They are every server seen
    /// writable since it began, less those that answered read-only or whose
    /// candidate left the list since. Empty otherwise.
    pub split_writers: Vec<Target>,
    /// In split brain: the writable server of the last clean round, if it
    /// had one.
    pub clean_writer: Option<Target>,
}

impl ClusterState {
    /// The state before the first round: nothing known, and the Primary
    /// pools unavailable.
    pub fn new() -> Self {
        Self {
            health: Health::Unknown,
            primary: None,
            primary_active: false,
            read: None,
            writers: Vec::new(),
            pending: None,
            last_switch: None,
            primary_silent_rounds: 0,
            split_writers: Vec::new(),
            clean_writer: None,
        }
    }

    /// Whether the prober should use its fast cadence.
    pub fn wants_fast_probes(&self) -> bool {
        self.health != Health::Active
    }
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::new()
    }
}

/// What the prober must do after a round.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Point the Primary pools at this server, fenced for it.
    SwitchPrimary(Target),
    /// Point the Read pools at this server.
    ReplaceRead(Target),
    /// Make the Primary pools unavailable, for the given reason.
    MarkPrimaryUnavailable(String),
}

/// Decides what to do after a round of probes.
///
/// `observations` lists every current candidate once, in configured order,
/// with what it answered. A candidate missing from it, for example one
/// removed from the hosts file, counts as not answering, and pools that
/// point at it are moved to a listed candidate.
///
/// The returned state assumes the actions are applied. `PrimaryCheck::None`
/// has no prober, so it changes nothing.
pub fn decide(
    prev: &ClusterState,
    observations: &[Observation],
    mode: PrimaryCheck,
    now: Instant,
) -> (ClusterState, Vec<Action>) {
    let mut next = prev.clone();
    let mut actions = Vec::new();
    if mode == PrimaryCheck::None {
        return (next, actions);
    }

    let round = Round {
        observations,
        answered: observations
            .iter()
            .filter_map(|o| o.answer.as_ref().map(|answer| (&o.candidate, answer)))
            .collect(),
        mode,
    };
    let writers = round.writers(prev.primary.as_ref());
    next.writers = writers.clone();
    let split_brain = match prev.health {
        Health::SplitBrain { clean_rounds } => Some(clean_rounds),
        _ => None,
    };

    if round.answered.is_empty() {
        // Nothing is known: keep the pools. Connections that hit I/O errors
        // are discarded by the pools themselves. A busy primary is up, so
        // it is not silent, and a settled verdict about it stands.
        next.pending = None;
        let primary_busy = round.primary_busy(prev);
        if primary_busy {
            next.primary_silent_rounds = 0;
        }
        if split_brain.is_none() && !(primary_busy && prev.health == Health::Active) {
            next.health = Health::Unknown;
        }
    } else {
        next.primary_silent_rounds = round.primary_silent_rounds(prev);
        if writers.len() >= 2 {
            next.health = Health::SplitBrain { clean_rounds: 0 };
            next.pending = None;
            next.split_writers = round.split_writers(&prev.split_writers, &writers);
            next.clean_writer = None;
            let reason = format!(
                "split brain: {} writable servers: {}",
                writers.len(),
                writers
                    .iter()
                    .map(|w| w.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            mark_primary_unavailable(&mut next, &mut actions, reason);
        } else if let Some(clean_rounds) = split_brain {
            next.pending = None;
            next.split_writers = round.split_writers(&prev.split_writers, &writers);
            if next.split_writers.iter().all(|w| round.answered_by(w)) {
                let writer = writers.into_iter().next();
                let same_writer = match (&writer, &prev.clean_writer) {
                    (Some(writer), Some(last)) => round.same_server(writer, last),
                    (None, None) => true,
                    _ => false,
                };
                let clean_rounds = if same_writer { clean_rounds + 1 } else { 1 };
                if clean_rounds < SPLIT_BRAIN_CLEAN_ROUNDS {
                    next.health = Health::SplitBrain { clean_rounds };
                    next.clean_writer = writer;
                } else {
                    // Left through the usual checks: leaving split brain
                    // confirms nothing about the writer.
                    next.split_writers.clear();
                    next.clean_writer = None;
                    round.settle(prev, &mut next, &mut actions, writer, now);
                }
            }
            // Otherwise a server of the split did not answer, and it may
            // still be writable: like a round with no answer, this one
            // neither counts nor resets.
        } else {
            round.settle(
                prev,
                &mut next,
                &mut actions,
                writers.into_iter().next(),
                now,
            );
        }
    }

    round.update_read_target(&mut next, &mut actions);
    (next, actions)
}

fn mark_primary_unavailable(next: &mut ClusterState, actions: &mut Vec<Action>, reason: String) {
    if next.primary_active {
        next.primary_active = false;
        next.primary_silent_rounds = 0;
        actions.push(Action::MarkPrimaryUnavailable(reason));
    }
}

struct Round<'a> {
    observations: &'a [Observation],
    answered: Vec<(&'a Candidate, &'a ProbeAnswer)>,
    mode: PrimaryCheck,
}

impl Round<'_> {
    fn listed(&self, candidate: &Candidate) -> bool {
        self.observations.iter().any(|o| &o.candidate == candidate)
    }

    /// Whether `a` and `b` are the same server: the same identity, or in
    /// writable mode the same candidate.
    fn same_server(&self, a: &Target, b: &Target) -> bool {
        match self.mode {
            PrimaryCheck::Identity => a.identity == b.identity,
            PrimaryCheck::Writable | PrimaryCheck::None => a.candidate == b.candidate,
        }
    }

    /// Whether pools that point at `a` would also point at `b`.
    fn same_target(&self, a: &Target, b: &Target) -> bool {
        match self.mode {
            PrimaryCheck::Identity => a == b,
            PrimaryCheck::Writable | PrimaryCheck::None => a.candidate == b.candidate,
        }
    }

    /// The distinct writable servers, in candidate order. When several
    /// candidates reach the same server, the one `primary` uses is kept,
    /// else the first.
    fn writers(&self, primary: Option<&Target>) -> Vec<Target> {
        let mut writers: Vec<Target> = Vec::new();
        for (candidate, answer) in &self.answered {
            if !answer.writable() {
                continue;
            }
            let target = Target {
                candidate: (*candidate).clone(),
                identity: answer.identity.clone(),
            };
            match writers.iter_mut().find(|w| self.same_server(w, &target)) {
                Some(seen) => {
                    if primary.is_some_and(|p| p.candidate == target.candidate) {
                        *seen = target;
                    }
                }
                None => writers.push(target),
            }
        }
        writers
    }

    /// Whether the probe of `candidate` found its server busy.
    fn busy(&self, candidate: &Candidate) -> bool {
        self.observations
            .iter()
            .any(|o| o.busy && &o.candidate == candidate)
    }

    /// Whether the Primary pools are active and their candidate was busy in
    /// this round.
    fn primary_busy(&self, prev: &ClusterState) -> bool {
        prev.primary_active
            && prev
                .primary
                .as_ref()
                .is_some_and(|primary| self.busy(&primary.candidate))
    }

    /// Whether `server` answered in this round, through any candidate.
    fn answered_by(&self, server: &Target) -> bool {
        self.answered
            .iter()
            .any(|(candidate, answer)| match self.mode {
                PrimaryCheck::Identity => answer.identity == server.identity,
                PrimaryCheck::Writable | PrimaryCheck::None => **candidate == server.candidate,
            })
    }

    /// Whether the candidate of `server` itself answered in this round, as
    /// writable and, in identity mode, as that server: whether new
    /// connections of pools that point at `server` pass the fence.
    fn reaches(&self, server: &Target) -> bool {
        self.answered.iter().any(|(candidate, answer)| {
            **candidate == server.candidate
                && answer.writable()
                && (self.mode != PrimaryCheck::Identity || answer.identity == server.identity)
        })
    }

    /// The new value of [`ClusterState::primary_silent_rounds`], for a round
    /// in which some candidate answered.
    fn primary_silent_rounds(&self, prev: &ClusterState) -> u32 {
        match &prev.primary {
            Some(primary)
                if prev.primary_active
                    && self.listed(&primary.candidate)
                    && !self.busy(&primary.candidate)
                    && !self
                        .answered
                        .iter()
                        .any(|(candidate, _)| **candidate == primary.candidate) =>
            {
                prev.primary_silent_rounds + 1
            }
            _ => 0,
        }
    }

    /// The servers of a split brain after this round, from those of the
    /// last round and this round's `writers`. A server seen again keeps the
    /// candidate it answered through last.
    fn split_writers(&self, remembered: &[Target], writers: &[Target]) -> Vec<Target> {
        let mut split = remembered.to_vec();
        for writer in writers {
            match split.iter_mut().find(|w| self.same_server(w, writer)) {
                Some(seen) => *seen = writer.clone(),
                None => split.push(writer.clone()),
            }
        }
        // A server that answered and is not among the writers is read-only.
        // One that is silent and no longer listed was removed by the
        // operator, who knows that it is gone.
        split.retain(|w| {
            writers.iter().any(|writer| self.same_server(w, writer))
                || (!self.answered_by(w) && self.listed(&w.candidate))
        });
        split
    }

    /// Decides a round outside split brain, given its only writable server,
    /// if any.
    fn settle(
        &self,
        prev: &ClusterState,
        next: &mut ClusterState,
        actions: &mut Vec<Action>,
        writer: Option<Target>,
        now: Instant,
    ) {
        match writer {
            Some(writer) => self.single_writer(prev, next, actions, writer, now),
            None => {
                next.pending = None;
                // A busy primary is up but has no connection to spare, as
                // when its pools hold them all. Nothing else is writable, so
                // it is taken to be as it was: purging its pools would only
                // free connections for them to take again, round after
                // round.
                if self.primary_busy(prev) {
                    if prev.health != Health::Active {
                        next.health = Health::NoPrimary;
                    }
                    return;
                }
                next.health = Health::NoPrimary;
                // A primary that answered is known not to be writable any
                // more, and one whose candidate left the list is gone. One
                // that did not answer may only be slow to, for example when
                // its handshakes queue under load: keep its pools for a few
                // rounds, as for a crash, rather than purge them at once.
                let silent = prev.primary.as_ref().is_some_and(|p| !self.answered_by(p));
                if !(silent && (1..CRASH_CONFIRM_ROUNDS).contains(&next.primary_silent_rounds)) {
                    mark_primary_unavailable(next, actions, "no writable server".to_string());
                }
            }
        }
    }

    fn single_writer(
        &self,
        prev: &ClusterState,
        next: &mut ClusterState,
        actions: &mut Vec<Action>,
        writer: Target,
        now: Instant,
    ) {
        if prev.primary_active
            && let Some(primary) = &prev.primary
            && self.same_server(primary, &writer)
        {
            // The writer is the primary. Keep the pools if their own
            // candidate reaches it, or is busy: a server is up behind it,
            // with no connection to spare. If that candidate is silent while
            // the primary answers through another, keep them for a few
            // rounds, as a name may fail to resolve now and then. If it
            // reaches another server, or a read-only one, the fence rejects
            // every new connection: move the pools to the writer's candidate
            // at once.
            if self.reaches(primary)
                || self.busy(&primary.candidate)
                || (1..CRASH_CONFIRM_ROUNDS).contains(&next.primary_silent_rounds)
            {
                next.health = Health::Active;
                next.pending = None;
                return;
            }
        }

        let rounds = match &prev.pending {
            Some((pending, rounds)) if self.same_target(pending, &writer) => rounds + 1,
            _ => 1,
        };
        // A previous primary that answered is known not to be writable any
        // more. One that did not answer may have crashed, or may be cut off
        // from us only: wait for a second round before trusting the new
        // writer.
        let previous_answered = prev.primary.as_ref().is_none_or(|p| self.answered_by(p));
        let confirmed = previous_answered || rounds >= CRASH_CONFIRM_ROUNDS;
        let allowed = prev
            .last_switch
            .is_none_or(|last| now.saturating_duration_since(last) >= MIN_SWITCH_INTERVAL);
        if !(confirmed && allowed) {
            next.health = Health::Pending;
            next.pending = Some((writer, rounds));
            return;
        }

        actions.push(Action::SwitchPrimary(writer.clone()));
        if !next
            .read
            .as_ref()
            .is_some_and(|read| self.same_target(read, &writer))
        {
            actions.push(Action::ReplaceRead(writer.clone()));
            next.read = Some(writer.clone());
        }
        next.health = Health::Active;
        next.primary = Some(writer);
        next.primary_active = true;
        next.primary_silent_rounds = 0;
        next.pending = None;
        next.last_switch = Some(now);
    }

    /// The Read pools follow the primary. Before any primary was seen they
    /// use the first candidate that answers, and move when it stops
    /// answering. Pools that point at a candidate no longer listed move to
    /// the primary, or else to the first candidate that answers.
    fn update_read_target(&self, next: &mut ClusterState, actions: &mut Vec<Action>) {
        if let Some(read) = &next.read {
            let answers = self
                .answered
                .iter()
                .any(|(candidate, _)| **candidate == read.candidate);
            if self.listed(&read.candidate) && (next.primary.is_some() || answers) {
                return;
            }
        }
        let choice = match &next.primary {
            Some(primary) if self.listed(&primary.candidate) => Some(primary.clone()),
            _ => self.answered.first().map(|(candidate, answer)| Target {
                candidate: (*candidate).clone(),
                identity: answer.identity.clone(),
            }),
        };
        if let Some(choice) = choice
            && next.read.as_ref() != Some(&choice)
        {
            actions.push(Action::ReplaceRead(choice.clone()));
            next.read = Some(choice);
        }
    }
}

#[cfg(test)]
mod tests {
    use mononoke_macros::mononoke;

    use super::*;

    const A: (&str, u16) = ("127.0.0.1", 13306);
    const B: (&str, u16) = ("127.0.0.1", 13307);
    const C: (&str, u16) = ("127.0.0.1", 13308);

    fn candidate((host, port): (&str, u16)) -> Candidate {
        Candidate::new(host, port)
    }

    fn identity(server_id: u64, port: u16) -> ServerIdentity {
        ServerIdentity {
            server_id,
            hostname: "mac".to_string(),
            port,
        }
    }

    /// The identity behind candidate A, B or C: server_id 1, 2 or 3.
    fn identity_of((_, port): (&str, u16)) -> ServerIdentity {
        identity(u64::from(port - 13305), port)
    }

    fn target(c: (&str, u16)) -> Target {
        Target {
            candidate: candidate(c),
            identity: identity_of(c),
        }
    }

    fn answer(identity: ServerIdentity, read_only: &str) -> ProbeAnswer {
        ProbeAnswer {
            identity,
            server_uuid: None,
            version: Some("9.7.0-1".to_string()),
            read_only: Some(read_only.to_string()),
            super_read_only: Some(read_only.to_string()),
            innodb_read_only: Some("OFF".to_string()),
            offline_mode: Some("OFF".to_string()),
        }
    }

    fn writable(c: (&str, u16)) -> Observation {
        Observation {
            candidate: candidate(c),
            answer: Some(answer(identity_of(c), "OFF")),
            busy: false,
        }
    }

    fn read_only(c: (&str, u16)) -> Observation {
        Observation {
            candidate: candidate(c),
            answer: Some(answer(identity_of(c), "ON")),
            busy: false,
        }
    }

    fn silent(c: (&str, u16)) -> Observation {
        Observation {
            candidate: candidate(c),
            answer: None,
            busy: false,
        }
    }

    /// A server that turned the probe away with 1040 or 1203.
    fn busy(c: (&str, u16)) -> Observation {
        Observation {
            candidate: candidate(c),
            answer: None,
            busy: true,
        }
    }

    /// Runs rounds one second apart, so that the switch rate limit does not
    /// get in the way unless a test wants it to.
    struct Prober {
        state: ClusterState,
        mode: PrimaryCheck,
        now: Instant,
    }

    impl Prober {
        fn new(mode: PrimaryCheck) -> Self {
            Self {
                state: ClusterState::new(),
                mode,
                now: Instant::now(),
            }
        }

        /// A prober whose primary is A, after one round.
        fn active_on_a(mode: PrimaryCheck) -> Self {
            let mut prober = Prober::new(mode);
            prober.round(&[writable(A), read_only(B)]);
            assert_eq!(prober.state.health, Health::Active);
            assert_eq!(prober.state.primary, Some(target(A)));
            prober
        }

        fn round(&mut self, observations: &[Observation]) -> Vec<Action> {
            self.round_after(Duration::from_secs(1), observations)
        }

        fn round_after(&mut self, elapsed: Duration, observations: &[Observation]) -> Vec<Action> {
            self.now += elapsed;
            let (state, actions) = decide(&self.state, observations, self.mode, self.now);
            self.state = state;
            actions
        }
    }

    fn switch(c: (&str, u16)) -> Action {
        Action::SwitchPrimary(target(c))
    }

    fn replace_read(c: (&str, u16)) -> Action {
        Action::ReplaceRead(target(c))
    }

    fn is_mark_unavailable(actions: &[Action]) -> bool {
        matches!(actions, [Action::MarkPrimaryUnavailable(_)])
    }

    #[mononoke::test]
    fn test_first_round_switches_at_once() {
        let mut prober = Prober::new(PrimaryCheck::Identity);
        assert_eq!(prober.state.health, Health::Unknown);
        assert!(!prober.state.primary_active);
        let actions = prober.round(&[read_only(B), writable(A)]);
        assert_eq!(actions, vec![switch(A), replace_read(A)]);
        assert_eq!(prober.state.health, Health::Active);
        assert!(prober.state.primary_active);
        assert_eq!(prober.state.read, Some(target(A)));
        assert!(!prober.state.wants_fast_probes());
    }

    #[mononoke::test]
    fn test_steady_state() {
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        let before = prober.state.clone();
        for _ in 0..5 {
            assert_eq!(prober.round(&[writable(A), read_only(B)]), vec![]);
            assert_eq!(prober.state.health, Health::Active);
        }
        assert_eq!(prober.state, before);
        // A replica that stops answering changes nothing.
        assert_eq!(prober.round(&[writable(A), silent(B)]), vec![]);
        assert_eq!(prober.state.health, Health::Active);
    }

    #[mononoke::test]
    fn test_graceful_switchover() {
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        // The operator demotes A: no writable server.
        let actions = prober.round(&[read_only(A), read_only(B)]);
        assert!(is_mark_unavailable(&actions), "{actions:?}");
        assert_eq!(prober.state.health, Health::NoPrimary);
        assert!(!prober.state.primary_active);
        assert!(prober.state.wants_fast_probes());
        // Plain reads stay on the last primary.
        assert_eq!(prober.state.read, Some(target(A)));
        // A second round changes nothing more.
        assert_eq!(prober.round(&[read_only(A), read_only(B)]), vec![]);
        // B is promoted. A answered, so the switch is immediate.
        let actions = prober.round(&[read_only(A), writable(B)]);
        assert_eq!(actions, vec![switch(B), replace_read(B)]);
        assert_eq!(prober.state.health, Health::Active);
        assert_eq!(prober.state.primary, Some(target(B)));
        assert!(prober.state.primary_active);
    }

    #[mononoke::test]
    fn test_direct_swap_in_one_round() {
        // Demotion and promotion between two rounds.
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        let actions = prober.round(&[read_only(A), writable(B)]);
        assert_eq!(actions, vec![switch(B), replace_read(B)]);
    }

    #[mononoke::test]
    fn test_crash_debounce() {
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        // A crashes and B is promoted before the next round. A did not
        // answer, so B must be seen twice.
        let actions = prober.round(&[silent(A), writable(B)]);
        assert_eq!(actions, vec![]);
        assert_eq!(prober.state.health, Health::Pending);
        assert_eq!(prober.state.pending, Some((target(B), 1)));
        // The Primary pools still point at A meanwhile.
        assert!(prober.state.primary_active);
        assert!(prober.state.wants_fast_probes());
        let actions = prober.round_after(Duration::from_millis(250), &[silent(A), writable(B)]);
        assert_eq!(actions, vec![switch(B), replace_read(B)]);
        assert_eq!(prober.state.health, Health::Active);
        assert_eq!(prober.state.pending, None);
    }

    #[mononoke::test]
    fn test_crash_then_promotion() {
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        // A crashes; B is still read-only. A may only be slow, so its pools
        // are kept for one round...
        assert_eq!(prober.round(&[silent(A), read_only(B)]), vec![]);
        assert_eq!(prober.state.health, Health::NoPrimary);
        assert!(prober.state.primary_active);
        assert!(prober.state.wants_fast_probes());
        // ...and made unavailable in the second.
        let actions = prober.round(&[silent(A), read_only(B)]);
        assert!(is_mark_unavailable(&actions), "{actions:?}");
        assert_eq!(prober.state.health, Health::NoPrimary);
        assert!(!prober.state.primary_active);
        // B is promoted: two rounds, since A still does not answer.
        assert_eq!(prober.round(&[silent(A), writable(B)]), vec![]);
        assert_eq!(prober.state.health, Health::Pending);
        assert_eq!(
            prober.round(&[silent(A), writable(B)]),
            vec![switch(B), replace_read(B)]
        );
    }

    #[mononoke::test]
    fn test_debounce_needs_consecutive_rounds() {
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        assert_eq!(
            prober.round(&[silent(A), writable(B), read_only(C)]),
            vec![]
        );
        // A different writer starts the count again.
        assert_eq!(
            prober.round(&[silent(A), read_only(B), writable(C)]),
            vec![]
        );
        assert_eq!(prober.state.pending, Some((target(C), 1)));
        // So does a round in which nothing answered.
        assert_eq!(prober.round(&[silent(A), silent(B), silent(C)]), vec![]);
        assert_eq!(prober.state.pending, None);
        assert_eq!(
            prober.round(&[silent(A), read_only(B), writable(C)]),
            vec![]
        );
        assert_eq!(
            prober.round(&[silent(A), read_only(B), writable(C)]),
            vec![switch(C), replace_read(C)]
        );
    }

    #[mononoke::test]
    fn test_unknown_keeps_pools() {
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        for _ in 0..3 {
            assert_eq!(prober.round(&[silent(A), silent(B)]), vec![]);
            assert_eq!(prober.state.health, Health::Unknown);
            assert!(prober.state.primary_active);
            assert_eq!(prober.state.primary, Some(target(A)));
            assert!(prober.state.wants_fast_probes());
        }
        // A comes back as it was.
        assert_eq!(prober.round(&[writable(A), silent(B)]), vec![]);
        assert_eq!(prober.state.health, Health::Active);
    }

    #[mononoke::test]
    fn test_no_answer_at_startup() {
        let mut prober = Prober::new(PrimaryCheck::Identity);
        assert_eq!(prober.round(&[silent(A), silent(B)]), vec![]);
        assert_eq!(prober.state, ClusterState::new());
    }

    #[mononoke::test]
    fn test_unknown_after_no_primary() {
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        assert!(is_mark_unavailable(
            &prober.round(&[read_only(A), read_only(B)])
        ));
        // Nothing answers, then A is writable again and answers: it is
        // reactivated with a new pool.
        assert_eq!(prober.round(&[silent(A), silent(B)]), vec![]);
        assert!(!prober.state.primary_active);
        assert_eq!(prober.round(&[writable(A), read_only(B)]), vec![switch(A)]);
        assert!(prober.state.primary_active);
    }

    #[mononoke::test]
    fn test_no_primary_after_unknown_purges() {
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        assert_eq!(prober.round(&[silent(A), silent(B)]), vec![]);
        assert!(prober.state.primary_active);
        // A round with no answer does not count as a silent round of A.
        assert_eq!(prober.round(&[silent(A), read_only(B)]), vec![]);
        assert!(prober.state.primary_active);
        assert_eq!(prober.round(&[silent(A), silent(B)]), vec![]);
        assert!(is_mark_unavailable(
            &prober.round(&[silent(A), read_only(B)])
        ));
        assert!(!prober.state.primary_active);
    }

    #[mononoke::test]
    fn test_silent_primary_does_not_flap() {
        // Probes of a healthy but loaded A time out now and then, its
        // handshakes queued, while the replica answers. The pools are kept
        // throughout.
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        for _ in 0..3 {
            assert_eq!(prober.round(&[silent(A), read_only(B)]), vec![]);
            assert!(prober.state.primary_active);
            assert_eq!(prober.round(&[writable(A), read_only(B)]), vec![]);
            assert_eq!(prober.state.health, Health::Active);
            assert_eq!(prober.state.primary_silent_rounds, 0);
        }
        // A primary that answers read-only is made unavailable at once.
        assert!(is_mark_unavailable(
            &prober.round(&[read_only(A), read_only(B)])
        ));
    }

    #[mononoke::test]
    fn test_busy_primary_keeps_its_pools() {
        // The pools of every process take all of A's connections, so its
        // probes get 1040 now and then while the replica answers. A is up:
        // its pools are kept, and the cluster stays settled.
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        for _ in 0..5 {
            assert_eq!(prober.round(&[busy(A), read_only(B)]), vec![]);
            assert_eq!(prober.state.health, Health::Active);
            assert!(prober.state.primary_active);
            assert_eq!(prober.state.primary_silent_rounds, 0);
            assert!(!prober.state.wants_fast_probes());
            assert_eq!(prober.round(&[writable(A), read_only(B)]), vec![]);
        }
        for _ in 0..5 {
            assert_eq!(prober.round(&[busy(A), read_only(B)]), vec![]);
        }
        assert_eq!(prober.state.health, Health::Active);
        // Busy rounds end a run of silent ones.
        assert_eq!(prober.round(&[silent(A), read_only(B)]), vec![]);
        assert_eq!(prober.round(&[busy(A), read_only(B)]), vec![]);
        assert_eq!(prober.round(&[silent(A), read_only(B)]), vec![]);
        assert!(prober.state.primary_active);
        assert_eq!(prober.state.health, Health::NoPrimary);
        // So do they when nothing else answers: with one candidate, say.
        assert_eq!(prober.round(&[busy(A), silent(B)]), vec![]);
        assert_eq!(prober.state.primary_silent_rounds, 0);
        assert!(prober.state.primary_active);
        assert_eq!(prober.round(&[writable(A), read_only(B)]), vec![]);
        assert_eq!(prober.state.health, Health::Active);
        assert_eq!(prober.round(&[busy(A), silent(B)]), vec![]);
        assert_eq!(prober.state.health, Health::Active);
        // A primary that answers read-only is still made unavailable at once.
        assert!(is_mark_unavailable(
            &prober.round(&[read_only(A), read_only(B)])
        ));
    }

    #[mononoke::test]
    fn test_busy_primary_and_a_new_writer() {
        // A, busy, may still be writable: B is switched to as after a crash.
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        assert_eq!(prober.round(&[busy(A), writable(B)]), vec![]);
        assert_eq!(prober.state.health, Health::Pending);
        assert!(prober.state.primary_active);
        assert_eq!(
            prober.round(&[busy(A), writable(B)]),
            vec![switch(B), replace_read(B)]
        );
        // A server of a split brain that is busy may still be writable.
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        assert!(is_mark_unavailable(
            &prober.round(&[writable(A), writable(B)])
        ));
        for _ in 0..5 {
            assert_eq!(prober.round(&[writable(A), busy(B)]), vec![]);
            assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 0 });
        }
        // The primary answers through another candidate while its own is
        // busy: the pools stay.
        let mut cluster = PrimaryByName::new();
        for _ in 0..5 {
            assert_eq!(cluster.round(busy(PrimaryByName::BY_NAME)), vec![]);
            assert_eq!(cluster.prober.state.health, Health::Active);
            assert_eq!(
                cluster.prober.state.primary,
                Some(PrimaryByName::primary(PrimaryByName::BY_NAME))
            );
        }
    }

    #[mononoke::test]
    fn test_split_brain() {
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        let actions = prober.round(&[writable(A), writable(B)]);
        match actions.as_slice() {
            [Action::MarkPrimaryUnavailable(reason)] => {
                assert!(
                    reason.contains("split brain: 2 writable servers"),
                    "{reason}"
                );
                assert!(reason.contains("127.0.0.1:13306"), "{reason}");
                assert!(reason.contains("127.0.0.1:13307"), "{reason}");
            }
            _ => panic!("unexpected actions {actions:?}"),
        }
        assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 0 });
        assert_eq!(prober.state.writers, vec![target(A), target(B)]);
        assert!(!prober.state.primary_active);
        assert!(prober.state.wants_fast_probes());

        // Still split: nothing more to do.
        assert_eq!(prober.round(&[writable(A), writable(B)]), vec![]);
        assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 0 });

        // The operator demotes B. Two clean rounds are not enough.
        assert_eq!(prober.round(&[writable(A), read_only(B)]), vec![]);
        assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 1 });
        // A round with no answer neither counts nor resets.
        assert_eq!(prober.round(&[silent(A), silent(B)]), vec![]);
        assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 1 });
        assert_eq!(prober.round(&[writable(A), read_only(B)]), vec![]);
        assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 2 });
        assert!(!prober.state.primary_active);
        // The third clean round switches back to A.
        assert_eq!(prober.round(&[writable(A), read_only(B)]), vec![switch(A)]);
        assert_eq!(prober.state.health, Health::Active);
        assert!(prober.state.primary_active);
    }

    #[mononoke::test]
    fn test_split_brain_restarts_count() {
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        assert!(is_mark_unavailable(
            &prober.round(&[writable(A), writable(B)])
        ));
        assert_eq!(prober.round(&[writable(A), read_only(B)]), vec![]);
        assert_eq!(prober.round(&[writable(A), read_only(B)]), vec![]);
        // Split again just before the third clean round.
        assert_eq!(prober.round(&[writable(A), writable(B)]), vec![]);
        assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 0 });
        // Rounds with no writable server count as clean, and leaving split
        // brain without one gives NoPrimary.
        assert_eq!(prober.round(&[read_only(A), read_only(B)]), vec![]);
        assert_eq!(prober.round(&[read_only(A), read_only(B)]), vec![]);
        assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 2 });
        assert_eq!(prober.round(&[read_only(A), read_only(B)]), vec![]);
        assert_eq!(prober.state.health, Health::NoPrimary);
        // B is promoted; A answered, so the switch is immediate.
        assert_eq!(
            prober.round(&[read_only(A), writable(B)]),
            vec![switch(B), replace_read(B)]
        );
    }

    #[mononoke::test]
    fn test_split_brain_with_no_primary_yet() {
        let mut prober = Prober::new(PrimaryCheck::Identity);
        // The pools are not active, so there is nothing to mark.
        assert_eq!(
            prober.round(&[writable(A), writable(B)]),
            vec![replace_read(A)]
        );
        assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 0 });
        assert_eq!(prober.state.primary, None);
    }

    #[mononoke::test]
    fn test_split_brain_with_a_silent_writer() {
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        assert!(is_mark_unavailable(
            &prober.round(&[writable(A), writable(B)])
        ));
        assert_eq!(prober.state.split_writers, vec![target(A), target(B)]);
        // B is cut off from this process only, and may still take writes:
        // the split goes on.
        for _ in 0..5 {
            assert_eq!(prober.round(&[writable(A), silent(B)]), vec![]);
            assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 0 });
            assert!(!prober.state.primary_active);
        }
        // B answers read-only: it leaves the split, and the rounds count.
        assert_eq!(prober.round(&[writable(A), read_only(B)]), vec![]);
        assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 1 });
        assert_eq!(prober.state.split_writers, vec![target(A)]);
        // Once it has left, its silence does not matter.
        assert_eq!(prober.round(&[writable(A), silent(B)]), vec![]);
        assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 2 });
        // The third clean round switches back to A, which answered.
        assert_eq!(prober.round(&[writable(A), read_only(B)]), vec![switch(A)]);
        assert_eq!(prober.state.health, Health::Active);
        assert!(prober.state.primary_active);
        assert_eq!(prober.state.split_writers, vec![]);
        assert_eq!(prober.state.clean_writer, None);
    }

    #[mononoke::test]
    fn test_split_brain_has_no_tie_break() {
        // Each server is silent in turn, and writable whenever it answers.
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        assert!(is_mark_unavailable(
            &prober.round(&[writable(A), writable(B)])
        ));
        for _ in 0..3 {
            assert_eq!(prober.round(&[silent(A), writable(B)]), vec![]);
            assert_eq!(prober.round(&[writable(A), silent(B)]), vec![]);
        }
        assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 0 });
        assert!(!prober.state.primary_active);
        assert_eq!(prober.state.primary, Some(target(A)));
        assert_eq!(prober.state.split_writers, vec![target(A), target(B)]);
    }

    #[mononoke::test]
    fn test_split_brain_writer_removed_from_list() {
        // B is dead for good, and the operator removes it from the hosts
        // file: it leaves the split.
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        prober.round(&[writable(A), writable(B)]);
        assert_eq!(prober.round(&[writable(A)]), vec![]);
        assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 1 });
        assert_eq!(prober.state.split_writers, vec![target(A)]);
        assert_eq!(prober.round(&[writable(A)]), vec![]);
        assert_eq!(prober.round(&[writable(A)]), vec![switch(A)]);
        assert_eq!(prober.state.health, Health::Active);
    }

    #[mononoke::test]
    fn test_split_brain_count_restarts_when_the_writer_changes() {
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        prober.round(&[writable(A), writable(B)]);
        assert_eq!(prober.round(&[writable(A), read_only(B)]), vec![]);
        assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 1 });
        assert_eq!(prober.state.clean_writer, Some(target(A)));
        // Now B is the only writable server: it joins the split again, A
        // leaves it, and the count starts again.
        assert_eq!(prober.round(&[read_only(A), writable(B)]), vec![]);
        assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 1 });
        assert_eq!(prober.state.split_writers, vec![target(B)]);
        assert_eq!(prober.state.clean_writer, Some(target(B)));
        assert_eq!(prober.round(&[read_only(A), writable(B)]), vec![]);
        assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 2 });
        // Leaving split brain takes the usual path: A answered, so the
        // switch to B is immediate.
        assert_eq!(
            prober.round(&[read_only(A), writable(B)]),
            vec![switch(B), replace_read(B)]
        );
        assert_eq!(prober.state.health, Health::Active);
    }

    #[mononoke::test]
    fn test_leaving_split_brain_confirms_nothing() {
        // C and B were the split. The last primary, A, is silent when the
        // split ends with B writable: B must be seen twice, as after a crash.
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        assert!(is_mark_unavailable(&prober.round(&[
            read_only(A),
            writable(B),
            writable(C)
        ])));
        for _ in 0..2 {
            assert_eq!(
                prober.round(&[silent(A), writable(B), read_only(C)]),
                vec![]
            );
        }
        assert_eq!(prober.state.health, Health::SplitBrain { clean_rounds: 2 });
        assert_eq!(
            prober.round(&[silent(A), writable(B), read_only(C)]),
            vec![]
        );
        assert_eq!(prober.state.health, Health::Pending);
        assert_eq!(prober.state.pending, Some((target(B), 1)));
        assert_eq!(
            prober.round(&[silent(A), writable(B), read_only(C)]),
            vec![switch(B), replace_read(B)]
        );
    }

    #[mononoke::test]
    fn test_identity_change_behind_candidate() {
        // One stable name in front of the servers.
        let name = ("db.example.com", 3306);
        let server = |server_id: u64, read_only: &str| Observation {
            candidate: candidate(name),
            answer: Some(answer(identity(server_id, 3306), read_only)),
            busy: false,
        };
        let mut prober = Prober::new(PrimaryCheck::Identity);
        prober.round(&[server(1, "OFF")]);
        assert_eq!(prober.state.primary.as_ref().unwrap().identity.server_id, 1);

        // The old primary is demoted.
        assert!(is_mark_unavailable(&prober.round(&[server(1, "ON")])));
        // The name now points at a new writable server. The old one is not
        // seen any more, so the new one must be seen twice.
        assert_eq!(prober.round(&[server(2, "OFF")]), vec![]);
        assert_eq!(prober.state.health, Health::Pending);
        let new_primary = Target {
            candidate: candidate(name),
            identity: identity(2, 3306),
        };
        assert_eq!(
            prober.round(&[server(2, "OFF")]),
            vec![
                Action::SwitchPrimary(new_primary.clone()),
                Action::ReplaceRead(new_primary.clone()),
            ]
        );
        assert_eq!(prober.state.primary, Some(new_primary));
    }

    #[mononoke::test]
    fn test_identity_change_without_demotion() {
        // The name moves to a new writable server while the old one stays
        // writable but out of sight: switch after two rounds.
        let name = ("db.example.com", 3306);
        let server = |server_id: u64| Observation {
            candidate: candidate(name),
            answer: Some(answer(identity(server_id, 3306), "OFF")),
            busy: false,
        };
        let mut prober = Prober::new(PrimaryCheck::Identity);
        prober.round(&[server(1)]);
        assert_eq!(prober.round(&[server(2)]), vec![]);
        assert!(prober.state.primary_active);
        let actions = prober.round(&[server(2)]);
        assert_eq!(actions.len(), 2, "{actions:?}");
        assert_eq!(prober.state.primary.as_ref().unwrap().identity.server_id, 2);
    }

    #[mononoke::test]
    fn test_name_and_ip_deduplicated() {
        // "db1" and "10.0.0.1" reach the same server.
        let by_name = ("db1", 3306);
        let by_ip = ("10.0.0.1", 3306);
        let replica = ("10.0.0.2", 3306);
        let observe = |c: (&str, u16), server_id: u64, read_only: &str| Observation {
            candidate: candidate(c),
            answer: Some(answer(identity(server_id, 3306), read_only)),
            busy: false,
        };
        let primary_by = |c: (&str, u16)| Target {
            candidate: candidate(c),
            identity: identity(1, 3306),
        };

        let mut prober = Prober::new(PrimaryCheck::Identity);
        let actions = prober.round(&[
            observe(by_name, 1, "OFF"),
            observe(by_ip, 1, "OFF"),
            observe(replica, 2, "ON"),
        ]);
        // One writer, through the first candidate: no split brain.
        assert_eq!(
            actions,
            vec![
                Action::SwitchPrimary(primary_by(by_name)),
                Action::ReplaceRead(primary_by(by_name)),
            ]
        );
        assert_eq!(prober.state.writers, vec![primary_by(by_name)]);

        // The name fails to resolve in one round; the server is still seen
        // through its IP address, so nothing changes.
        assert_eq!(
            prober.round(&[
                silent(by_name),
                observe(by_ip, 1, "OFF"),
                observe(replica, 2, "ON"),
            ]),
            vec![]
        );
        assert_eq!(prober.state.health, Health::Active);
        assert_eq!(prober.state.primary, Some(primary_by(by_name)));

        // When both answer, the candidate in use is kept even though it is
        // not listed first.
        let mut prober = Prober::new(PrimaryCheck::Identity);
        prober.round(&[silent(by_name), observe(by_ip, 1, "OFF")]);
        assert_eq!(prober.state.primary, Some(primary_by(by_ip)));
        assert_eq!(
            prober.round(&[observe(by_name, 1, "OFF"), observe(by_ip, 1, "OFF")]),
            vec![]
        );
        assert_eq!(prober.state.writers, vec![primary_by(by_ip)]);
    }

    /// Candidates "db1" and "10.0.0.1" that reach the primary, server 1,
    /// and "10.0.0.2" that reaches the replica, server 2, with the pools on
    /// "db1".
    struct PrimaryByName {
        prober: Prober,
    }

    impl PrimaryByName {
        const BY_NAME: (&'static str, u16) = ("db1", 3306);
        const BY_IP: (&'static str, u16) = ("10.0.0.1", 3306);
        const REPLICA: (&'static str, u16) = ("10.0.0.2", 3306);

        fn new() -> Self {
            let mut prober = Prober::new(PrimaryCheck::Identity);
            prober.round(&[
                Self::observe(Self::BY_NAME, 1, "OFF"),
                Self::observe(Self::BY_IP, 1, "OFF"),
                Self::observe(Self::REPLICA, 2, "ON"),
            ]);
            assert_eq!(prober.state.primary, Some(Self::primary(Self::BY_NAME)));
            Self { prober }
        }

        fn observe(c: (&str, u16), server_id: u64, read_only: &str) -> Observation {
            Observation {
                candidate: candidate(c),
                answer: Some(answer(identity(server_id, 3306), read_only)),
                busy: false,
            }
        }

        fn primary(c: (&str, u16)) -> Target {
            Target {
                candidate: candidate(c),
                identity: identity(1, 3306),
            }
        }

        /// A round in which "db1" answers with `by_name`, and the others as
        /// before.
        fn round(&mut self, by_name: Observation) -> Vec<Action> {
            self.prober.round(&[
                by_name,
                Self::observe(Self::BY_IP, 1, "OFF"),
                Self::observe(Self::REPLICA, 2, "ON"),
            ])
        }
    }

    #[mononoke::test]
    fn test_primary_candidate_reaches_another_server() {
        // The name now points at the replica. New connections of the pools
        // would reach it and fail the fence: they move to the IP address at
        // once.
        let mut cluster = PrimaryByName::new();
        let by_ip = PrimaryByName::primary(PrimaryByName::BY_IP);
        let actions = cluster.round(PrimaryByName::observe(PrimaryByName::BY_NAME, 2, "ON"));
        assert_eq!(
            actions,
            vec![
                Action::SwitchPrimary(by_ip.clone()),
                Action::ReplaceRead(by_ip.clone()),
            ]
        );
        assert_eq!(cluster.prober.state.health, Health::Active);
        assert_eq!(cluster.prober.state.primary, Some(by_ip));

        // The same when the name reaches the primary, but it is read-only
        // through it: only the IP address sees it writable.
        let mut cluster = PrimaryByName::new();
        let actions = cluster.round(PrimaryByName::observe(PrimaryByName::BY_NAME, 1, "ON"));
        assert!(
            matches!(&actions[..], [Action::SwitchPrimary(t), Action::ReplaceRead(_)] if t.candidate == candidate(PrimaryByName::BY_IP)),
            "{actions:?}"
        );
    }

    #[mononoke::test]
    fn test_primary_candidate_stays_silent() {
        // The name no longer resolves. One round is a blip...
        let mut cluster = PrimaryByName::new();
        assert_eq!(cluster.round(silent(PrimaryByName::BY_NAME)), vec![]);
        assert_eq!(cluster.prober.state.health, Health::Active);
        // ...and the name resolving again ends it.
        assert_eq!(
            cluster.round(PrimaryByName::observe(PrimaryByName::BY_NAME, 1, "OFF")),
            vec![]
        );
        assert_eq!(cluster.prober.state.primary_silent_rounds, 0);
        assert_eq!(cluster.round(silent(PrimaryByName::BY_NAME)), vec![]);
        // A second round in a row moves the pools to the IP address.
        let by_ip = PrimaryByName::primary(PrimaryByName::BY_IP);
        assert_eq!(
            cluster.round(silent(PrimaryByName::BY_NAME)),
            vec![
                Action::SwitchPrimary(by_ip.clone()),
                Action::ReplaceRead(by_ip.clone()),
            ]
        );
        assert_eq!(cluster.prober.state.health, Health::Active);
        assert_eq!(cluster.prober.state.primary, Some(by_ip));
        // And they stay there.
        for _ in 0..3 {
            assert_eq!(cluster.round(silent(PrimaryByName::BY_NAME)), vec![]);
        }
    }

    #[mononoke::test]
    fn test_read_only_values() {
        let with = |read_only: &str, innodb_read_only: Option<&str>, offline_mode: Option<&str>| {
            ProbeAnswer {
                read_only: Some(read_only.to_string()),
                innodb_read_only: innodb_read_only.map(str::to_string),
                offline_mode: offline_mode.map(str::to_string),
                ..answer(identity(1, 3306), "OFF")
            }
            .writable()
        };
        // MySQL and Percona: 0/1 from @@ and OFF/ON from SHOW VARIABLES.
        assert!(with("0", Some("0"), Some("0")));
        assert!(with("OFF", Some("OFF"), Some("OFF")));
        assert!(with("off", Some("Off"), Some("off")));
        assert!(!with("1", Some("0"), Some("0")));
        assert!(!with("ON", Some("OFF"), Some("OFF")));
        // MariaDB: an enum, and no offline_mode.
        assert!(with("OFF", Some("OFF"), None));
        assert!(!with("ON", Some("OFF"), None));
        assert!(!with("NO_LOCK", Some("OFF"), None));
        assert!(!with("NO_LOCK_NO_ADMIN", Some("OFF"), None));
        // innodb_read_only.
        assert!(!with("OFF", Some("ON"), Some("OFF")));
        assert!(!with("OFF", Some("1"), Some("OFF")));
        // offline_mode.
        assert!(!with("OFF", Some("OFF"), Some("ON")));
        assert!(!with("OFF", Some("OFF"), Some("1")));
        // Missing flags count as read-only.
        assert!(!with("OFF", None, Some("OFF")));
        assert!(
            !ProbeAnswer {
                read_only: None,
                ..answer(identity(1, 3306), "OFF")
            }
            .writable()
        );
        // Anything else is not writable either.
        assert!(!with("", Some("OFF"), Some("OFF")));
        assert!(!with("2", Some("OFF"), Some("OFF")));
    }

    #[mononoke::test]
    fn test_read_only_values_drive_decisions() {
        for value in ["1", "ON", "NO_LOCK", "NO_LOCK_NO_ADMIN"] {
            let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
            let demoted = Observation {
                candidate: candidate(A),
                answer: Some(answer(identity_of(A), value)),
                busy: false,
            };
            assert!(
                is_mark_unavailable(&prober.round(&[demoted, read_only(B)])),
                "{value}"
            );
        }
        // A server in offline mode is not a primary.
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        let offline = Observation {
            candidate: candidate(A),
            answer: Some(ProbeAnswer {
                offline_mode: Some("ON".to_string()),
                ..answer(identity_of(A), "OFF")
            }),
            busy: false,
        };
        assert!(is_mark_unavailable(&prober.round(&[offline, read_only(B)])));
    }

    #[mononoke::test]
    fn test_from_variables() -> Result<()> {
        let rows = vec![
            ("hostname", "mac.local"),
            ("innodb_read_only", "OFF"),
            ("offline_mode", "OFF"),
            ("port", "13306"),
            ("read_only", "OFF"),
            ("server_id", "1"),
            ("server_uuid", "3e11fa47-71ca-11e1-9e33-c80aa9429562"),
            ("super_read_only", "OFF"),
            ("version", "9.7.0-1"),
            ("unrelated", "x"),
        ];
        let answer = ProbeAnswer::from_variables(rows)?;
        assert_eq!(
            answer.identity,
            ServerIdentity {
                server_id: 1,
                hostname: "mac.local".to_string(),
                port: 13306,
            }
        );
        assert_eq!(
            answer.server_uuid.as_deref(),
            Some("3e11fa47-71ca-11e1-9e33-c80aa9429562")
        );
        assert!(answer.writable());

        // MariaDB: no server_uuid, super_read_only or offline_mode.
        let answer = ProbeAnswer::from_variables(vec![
            ("Hostname".to_string(), "maria".to_string()),
            ("port".to_string(), "3306".to_string()),
            ("server_id".to_string(), "4294967295".to_string()),
            ("read_only".to_string(), "NO_LOCK_NO_ADMIN".to_string()),
            ("innodb_read_only".to_string(), "OFF".to_string()),
        ])?;
        assert_eq!(answer.identity.server_id, 4294967295);
        assert_eq!(answer.server_uuid, None);
        assert_eq!(answer.offline_mode, None);
        assert!(!answer.writable());

        let missing = |name: &str| {
            let rows = [("hostname", "h"), ("port", "1"), ("server_id", "1")]
                .into_iter()
                .filter(|(n, _)| *n != name);
            format!("{:#}", ProbeAnswer::from_variables(rows).unwrap_err())
        };
        assert!(missing("hostname").contains("hostname"));
        assert!(missing("port").contains("port"));
        assert!(missing("server_id").contains("server_id"));
        let err =
            ProbeAnswer::from_variables([("hostname", "h"), ("port", "x"), ("server_id", "1")])
                .unwrap_err();
        assert!(format!("{err:#}").contains("invalid port 'x'"), "{err:#}");
        let err =
            ProbeAnswer::from_variables([("hostname", ""), ("port", "1"), ("server_id", "1")])
                .unwrap_err();
        assert!(format!("{err:#}").contains("hostname"), "{err:#}");
        Ok(())
    }

    #[mononoke::test]
    fn test_writable_mode_ignores_identity() {
        // A balancer in front of a multi-writer cluster: each probe may land
        // on a different node.
        let balancer = ("galera-lb", 3306);
        let node = |server_id: u64| Observation {
            candidate: candidate(balancer),
            answer: Some(answer(identity(server_id, 3306), "OFF")),
            busy: false,
        };
        let mut prober = Prober::new(PrimaryCheck::Writable);
        let actions = prober.round(&[node(1)]);
        assert_eq!(actions.len(), 2, "{actions:?}");
        assert_eq!(prober.state.health, Health::Active);
        for server_id in [2, 3, 1, 2] {
            assert_eq!(prober.round(&[node(server_id)]), vec![]);
            assert_eq!(prober.state.health, Health::Active);
        }
        // A node that turns read-only still makes the primary unavailable.
        let read_only_node = Observation {
            candidate: candidate(balancer),
            answer: Some(answer(identity(3, 3306), "ON")),
            busy: false,
        };
        assert!(is_mark_unavailable(&prober.round(&[read_only_node])));
        // And a writable answer brings it back at once, whatever the node.
        let actions = prober.round(&[node(2)]);
        assert!(
            matches!(actions.as_slice(), [Action::SwitchPrimary(t)] if t.identity.server_id == 2),
            "{actions:?}"
        );
    }

    #[mononoke::test]
    fn test_none_mode_does_nothing() {
        let mut prober = Prober::new(PrimaryCheck::None);
        assert_eq!(prober.round(&[writable(A)]), vec![]);
        assert_eq!(prober.state, ClusterState::new());
    }

    #[mononoke::test]
    fn test_switch_rate_limit() {
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        // A is demoted and B promoted within 300 ms of the first switch.
        let actions = prober.round_after(Duration::ZERO, &[read_only(A), writable(B)]);
        assert_eq!(actions, vec![]);
        assert_eq!(prober.state.health, Health::Pending);
        let actions = prober.round_after(Duration::from_millis(300), &[read_only(A), writable(B)]);
        assert_eq!(actions, vec![]);
        // One second after the last switch.
        let actions = prober.round_after(Duration::from_millis(700), &[read_only(A), writable(B)]);
        assert_eq!(actions, vec![switch(B), replace_read(B)]);
        // And again: B is replaced by C 500 ms later.
        let actions = prober.round_after(Duration::from_millis(500), &[read_only(B), writable(C)]);
        assert_eq!(actions, vec![]);
        let actions = prober.round_after(Duration::from_millis(500), &[read_only(B), writable(C)]);
        assert_eq!(actions, vec![switch(C), replace_read(C)]);
    }

    #[mononoke::test]
    fn test_read_target_before_any_primary() {
        let mut prober = Prober::new(PrimaryCheck::Identity);
        // No primary yet: the first candidate that answers.
        assert_eq!(
            prober.round(&[silent(A), read_only(B), read_only(C)]),
            vec![replace_read(B)]
        );
        assert_eq!(prober.state.health, Health::NoPrimary);
        // Kept while it answers.
        assert_eq!(
            prober.round(&[read_only(A), read_only(B), read_only(C)]),
            vec![]
        );
        // Moved when it stops answering.
        assert_eq!(
            prober.round(&[read_only(A), silent(B), read_only(C)]),
            vec![replace_read(A)]
        );
        // Kept when nothing answers.
        assert_eq!(prober.round(&[silent(A), silent(B), silent(C)]), vec![]);
        assert_eq!(prober.state.read, Some(target(A)));
        // A primary takes the Read pools along.
        assert_eq!(
            prober.round(&[read_only(A), read_only(B), writable(C)]),
            vec![switch(C), replace_read(C)]
        );
        // After that they stay on the last primary, even when it is gone.
        for _ in 0..CRASH_CONFIRM_ROUNDS - 1 {
            assert_eq!(
                prober.round(&[read_only(A), read_only(B), silent(C)]),
                vec![]
            );
        }
        assert!(is_mark_unavailable(&prober.round(&[
            read_only(A),
            read_only(B),
            silent(C)
        ])));
        assert_eq!(prober.state.read, Some(target(C)));
    }

    #[mononoke::test]
    fn test_read_target_same_server_no_replace() {
        // After NoPrimary, the old primary comes back: its Read pools are
        // already in place.
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        prober.round(&[read_only(A), read_only(B)]);
        assert_eq!(prober.round(&[writable(A), read_only(B)]), vec![switch(A)]);
        assert_eq!(prober.state.read, Some(target(A)));
    }

    #[mononoke::test]
    fn test_primary_candidate_removed() {
        // "db1" and "10.0.0.1" reach the same server; the hosts file drops
        // "db1", which the pools use.
        let by_name = ("db1", 3306);
        let by_ip = ("10.0.0.1", 3306);
        let observe = |c: (&str, u16)| Observation {
            candidate: candidate(c),
            answer: Some(answer(identity(1, 3306), "OFF")),
            busy: false,
        };
        let primary_by = |c: (&str, u16)| Target {
            candidate: candidate(c),
            identity: identity(1, 3306),
        };
        let mut prober = Prober::new(PrimaryCheck::Identity);
        prober.round(&[observe(by_name), observe(by_ip)]);
        assert_eq!(prober.state.primary, Some(primary_by(by_name)));
        // The server answered through the remaining candidate, so the pools
        // move there at once.
        assert_eq!(
            prober.round(&[observe(by_ip)]),
            vec![
                Action::SwitchPrimary(primary_by(by_ip)),
                Action::ReplaceRead(primary_by(by_ip)),
            ]
        );

        // Removing the primary's only candidate makes the Primary pools
        // unavailable at once: unlike a silent primary, it is not waited
        // for.
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        let actions = prober.round(&[read_only(B), read_only(C)]);
        assert_eq!(
            actions[0],
            Action::MarkPrimaryUnavailable("no writable server".to_string())
        );
        // The Read pools leave the removed candidate too.
        assert_eq!(actions[1..], [replace_read(B)]);
        assert_eq!(prober.state.read, Some(target(B)));

        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        // With a writer among the rest, it needs two rounds, as for a crash.
        let actions = prober.round(&[writable(B), read_only(C)]);
        assert_eq!(actions, vec![replace_read(B)]);
        assert_eq!(prober.state.health, Health::Pending);
        assert_eq!(prober.round(&[writable(B), read_only(C)]), vec![switch(B)]);
        // With nothing reachable, the pools are kept.
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        assert_eq!(prober.round(&[silent(B)]), vec![]);
        assert!(prober.state.primary_active);
        assert_eq!(prober.state.health, Health::Unknown);
    }

    #[mononoke::test]
    fn test_candidate_added() {
        let mut prober = Prober::active_on_a(PrimaryCheck::Identity);
        // A new replica in the hosts file changes nothing...
        assert_eq!(
            prober.round(&[writable(A), read_only(B), read_only(C)]),
            vec![]
        );
        // ...until it is the primary.
        assert_eq!(
            prober.round(&[read_only(A), read_only(B), writable(C)]),
            vec![switch(C), replace_read(C)]
        );
    }

    #[mononoke::test]
    fn test_primary_check_from_str() -> Result<()> {
        for check in [
            PrimaryCheck::Identity,
            PrimaryCheck::Writable,
            PrimaryCheck::None,
        ] {
            assert_eq!(check.to_string().parse::<PrimaryCheck>()?, check);
        }
        assert_eq!(PrimaryCheck::default(), PrimaryCheck::Identity);
        let err = "Identity".parse::<PrimaryCheck>().unwrap_err();
        assert!(
            err.to_string()
                .contains("expected identity, writable or none")
        );
        Ok(())
    }

    #[mononoke::test]
    fn test_candidate_display() {
        assert_eq!(Candidate::new("db1", 3306).to_string(), "db1:3306");
        assert_eq!(Candidate::new("::1", 3306).to_string(), "[::1]:3306");
        assert_eq!(
            target(A).to_string(),
            "127.0.0.1:13306 (server_id 1 on mac:13306)"
        );
    }
}
