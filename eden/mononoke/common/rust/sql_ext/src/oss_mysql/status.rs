/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! What the clusters of this process look like, for health reports.
//!
//! Every cluster is listed here from its creation until its last store
//! drops it. Its status combines where its pools point with what its last
//! probe round found.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::time::Instant;

use super::Cluster;
use super::ClusterKey;
use super::OssMysqlTarget;
use super::decide::Candidate;
use super::decide::ClusterState;
use super::decide::Health;
use super::decide::Observation;
use super::decide::PrimaryCheck;

/// The clusters of this process, in the order they were created. Entries
/// of dropped clusters are removed when the list is next used.
static LIVE_CLUSTERS: Mutex<Vec<Weak<Cluster>>> = Mutex::new(Vec::new());

static NEXT_CLUSTER_ID: AtomicU64 = AtomicU64::new(1);

/// How a cluster's prober last judged it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterHealth {
    /// `--oss-mysql-primary-check none`: no prober; the single candidate is
    /// used as it is.
    Unchecked,
    /// No candidate answered, or no round has ended yet.
    Unknown,
    /// Exactly one writable server, and the Primary pools use it.
    Active,
    /// Exactly one writable server, which the Primary pools do not use yet.
    Pending,
    /// Candidates answered, but none is writable.
    NoPrimary,
    /// Two or more servers were seen writable: writes are refused until
    /// one stays the only writable server for a few rounds.
    SplitBrain {
        /// Clean rounds in a row so far.
        clean_rounds: u32,
    },
}

/// What a candidate answered in the last round.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CandidateObservation {
    Writable,
    ReadOnly,
    /// Up, with no connection to spare.
    Busy,
    /// No answer: down, unreachable, or too slow.
    Silent,
}

/// A candidate of the last round, and what it answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CandidateStatus {
    pub candidate: Candidate,
    pub observation: CandidateObservation,
}

/// The state of a cluster, as a health report shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterStatus {
    /// Unique in this process, for grouping. Not shown.
    pub id: u64,
    /// The hosts file path, or the candidates as `host:port, ...`.
    pub name: String,
    pub mode: PrimaryCheck,
    pub health: ClusterHealth,
    /// Where the Primary pools point, or `None` while they are unavailable.
    pub primary: Option<Candidate>,
    /// Why they are unavailable. May name servers.
    pub unavailable_reason: Option<String>,
    /// Times the Primary pools were repointed or made unavailable since the
    /// cluster was created.
    pub generation: u64,
    /// Where the Read pools point, or `None` while they are unavailable.
    pub read: Option<Candidate>,
    /// Time since the start of the last probe round; `None` in none mode.
    pub last_round_age: Option<Duration>,
    /// Every candidate of the last round, in configured order; empty in
    /// none mode.
    pub candidates: Vec<CandidateStatus>,
}

/// What a cluster's last probe round found.
#[derive(Clone, Debug)]
pub(super) struct RoundStatus {
    /// When the round started.
    at: Instant,
    health: ClusterHealth,
    candidates: Vec<CandidateStatus>,
}

/// The status of the cluster that serves `target`, if a store uses one.
/// When several registries have one, the most recently probed.
pub fn oss_mysql_cluster_status(target: &OssMysqlTarget) -> Option<ClusterStatus> {
    let key = ClusterKey::of(target);
    live_clusters()
        .into_iter()
        .filter(|cluster| cluster.key == key)
        .max_by_key(|cluster| (cluster.last_round_start(), cluster.id))
        .map(|cluster| cluster.status())
}

/// Every cluster in use in this process, by `id`.
pub fn oss_mysql_cluster_statuses() -> Vec<ClusterStatus> {
    let mut statuses: Vec<ClusterStatus> = live_clusters()
        .iter()
        .map(|cluster| cluster.status())
        .collect();
    statuses.sort_by_key(|status| status.id);
    statuses
}

pub(super) fn next_cluster_id() -> u64 {
    NEXT_CLUSTER_ID.fetch_add(1, Ordering::Relaxed)
}

/// Lists a new cluster.
pub(super) fn register(cluster: &Arc<Cluster>) {
    let mut clusters = LIVE_CLUSTERS.lock().expect("MySQL cluster list poisoned");
    clusters.retain(|cluster| cluster.strong_count() > 0);
    clusters.push(Arc::downgrade(cluster));
}

/// The clusters in use.
fn live_clusters() -> Vec<Arc<Cluster>> {
    let mut clusters = LIVE_CLUSTERS.lock().expect("MySQL cluster list poisoned");
    clusters.retain(|cluster| cluster.strong_count() > 0);
    clusters.iter().filter_map(Weak::upgrade).collect()
}

impl Cluster {
    /// Keeps what a probe round that started `at` found, for
    /// [`Cluster::status`].
    pub(crate) fn record_round(
        &self,
        state: &ClusterState,
        observations: &[Observation],
        at: Instant,
    ) {
        let health = match state.health {
            Health::Unknown => ClusterHealth::Unknown,
            Health::Active => ClusterHealth::Active,
            Health::Pending => ClusterHealth::Pending,
            Health::NoPrimary => ClusterHealth::NoPrimary,
            Health::SplitBrain { clean_rounds } => ClusterHealth::SplitBrain { clean_rounds },
        };
        let candidates = observations
            .iter()
            .map(|observation| CandidateStatus {
                candidate: observation.candidate.clone(),
                observation: match &observation.answer {
                    Some(answer) if answer.writable() => CandidateObservation::Writable,
                    Some(_) => CandidateObservation::ReadOnly,
                    None if observation.busy => CandidateObservation::Busy,
                    None => CandidateObservation::Silent,
                },
            })
            .collect();
        *self.round.lock().expect("MySQL cluster lock poisoned") = Some(RoundStatus {
            at,
            health,
            candidates,
        });
    }

    /// The state of the cluster: where its pools point, and what its last
    /// probe round found.
    pub(crate) fn status(&self) -> ClusterStatus {
        let (primary, unavailable_reason, generation, read) = {
            let shared = self.lock();
            let (primary, unavailable_reason) = match &shared.primary {
                Ok(endpoint) => (Some(endpoint.candidate.clone()), None),
                Err(reason) => (None, Some(reason.clone())),
            };
            let read = shared
                .read
                .as_ref()
                .ok()
                .map(|endpoint| endpoint.candidate.clone());
            (primary, unavailable_reason, shared.primary_generation, read)
        };
        let round = self
            .round
            .lock()
            .expect("MySQL cluster lock poisoned")
            .clone();
        let (health, last_round_age, candidates) = match (self.mode, round) {
            (PrimaryCheck::None, _) => (ClusterHealth::Unchecked, None, Vec::new()),
            (_, Some(round)) => (
                round.health,
                Some(Instant::now().saturating_duration_since(round.at)),
                round.candidates,
            ),
            (_, None) => (ClusterHealth::Unknown, None, Vec::new()),
        };
        ClusterStatus {
            id: self.id,
            name: self.name.clone(),
            mode: self.mode,
            health,
            primary,
            unavailable_reason,
            generation,
            read,
            last_round_age,
            candidates,
        }
    }

    /// When the last probe round started, if one ended.
    fn last_round_start(&self) -> Option<Instant> {
        self.round
            .lock()
            .expect("MySQL cluster lock poisoned")
            .as_ref()
            .map(|round| round.at)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use mononoke_macros::mononoke;

    use super::super::fake::*;
    use super::*;
    use crate::oss::OssFailoverSettings;
    use crate::oss::PoolConfig;

    /// Probes only when kicked, so that tests run rounds one at a time.
    const RARELY: Duration = Duration::from_secs(3_600);

    /// Long enough for a kicked round to end, and shorter than the fast
    /// interval that follows it.
    const ONE_ROUND: Duration = Duration::from_millis(200);

    fn candidate((host, port): (&str, u16)) -> Candidate {
        Candidate::new(host, port)
    }

    fn observed(servers: &[((&str, u16), CandidateObservation)]) -> Vec<CandidateStatus> {
        servers
            .iter()
            .map(|(server, observation)| CandidateStatus {
                candidate: candidate(*server),
                observation: *observation,
            })
            .collect()
    }

    #[mononoke::test]
    async fn test_active() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable), (B, Server::ReadOnly)]);
        let registry = registry(&fake, settings(RARELY));
        let cluster = registry
            .cluster(&target(&[A, B], "mononoke"), &PoolConfig::default())
            .await?;
        let probed = Instant::now();
        tokio::time::sleep(Duration::from_millis(30)).await;
        let status = cluster.status();
        assert_eq!(status.id, cluster.id);
        assert_eq!(status.name, "127.0.0.1:13306, 127.0.0.1:13307");
        assert_eq!(status.mode, PrimaryCheck::Identity);
        assert_eq!(status.health, ClusterHealth::Active);
        assert_eq!(status.primary, Some(candidate(A)));
        assert_eq!(status.unavailable_reason, None);
        assert_eq!(status.generation, 1);
        assert_eq!(status.read, Some(candidate(A)));
        assert_eq!(status.last_round_age, Some(probed.elapsed()));
        assert_eq!(
            status.candidates,
            observed(&[
                (A, CandidateObservation::Writable),
                (B, CandidateObservation::ReadOnly),
            ])
        );
        Ok(())
    }

    #[mononoke::test]
    async fn test_split_brain() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable), (B, Server::Writable)]);
        let registry = registry(&fake, settings(RARELY));
        let cluster = registry
            .cluster(&target(&[A, B], "mononoke"), &PoolConfig::default())
            .await?;
        let status = cluster.status();
        assert_eq!(status.health, ClusterHealth::SplitBrain { clean_rounds: 0 });
        assert_eq!(status.primary, None);
        assert!(status.unavailable_reason.is_some());
        assert_eq!(
            status.candidates,
            observed(&[
                (A, CandidateObservation::Writable),
                (B, CandidateObservation::Writable),
            ])
        );
        Ok(())
    }

    #[mononoke::test]
    async fn test_no_primary() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::ReadOnly), (B, Server::ReadOnly)]);
        let registry = registry(&fake, settings(RARELY));
        let cluster = registry
            .cluster(&target(&[A, B], "mononoke"), &PoolConfig::default())
            .await?;
        let status = cluster.status();
        assert_eq!(status.health, ClusterHealth::NoPrimary);
        assert_eq!(status.primary, None);
        assert_eq!(
            status.unavailable_reason.as_deref(),
            Some("no writable server")
        );
        // Never repointed: the pools were unavailable from the start.
        assert_eq!(status.generation, 0);
        assert_eq!(status.read, Some(candidate(A)));
        assert_eq!(
            status.candidates,
            observed(&[
                (A, CandidateObservation::ReadOnly),
                (B, CandidateObservation::ReadOnly),
            ])
        );
        Ok(())
    }

    #[mononoke::test]
    async fn test_pending_then_switched() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable), (B, Server::ReadOnly)]);
        let registry = registry(&fake, settings(RARELY));
        let cluster = registry
            .cluster(&target(&[A, B], "mononoke"), &PoolConfig::default())
            .await?;
        assert_eq!(cluster.status().generation, 1);

        // A crashes and B is promoted: one round sees B writable while A is
        // silent, which is not enough to switch.
        fake.set(A, Server::Down);
        fake.set(B, Server::Writable);
        cluster.kick.notify_one();
        tokio::time::sleep(ONE_ROUND).await;
        let status = cluster.status();
        assert_eq!(status.health, ClusterHealth::Pending);
        assert_eq!(status.primary, Some(candidate(A)));
        assert_eq!(status.generation, 1);
        assert_eq!(
            status.candidates,
            observed(&[
                (A, CandidateObservation::Silent),
                (B, CandidateObservation::Writable),
            ])
        );

        // The next round switches.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let status = cluster.status();
        assert_eq!(status.health, ClusterHealth::Active);
        assert_eq!(status.primary, Some(candidate(B)));
        assert_eq!(status.generation, 2);
        Ok(())
    }

    #[mononoke::test]
    async fn test_busy_and_silent() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[
            (A, Server::Writable),
            (B, Server::Busy),
            (C, Server::Silent),
        ]);
        let registry = registry(&fake, settings(RARELY));
        let cluster = registry
            .cluster(&target(&[A, B, C], "mononoke"), &PoolConfig::default())
            .await?;
        let status = cluster.status();
        assert_eq!(status.health, ClusterHealth::Active);
        assert_eq!(
            status.candidates,
            observed(&[
                (A, CandidateObservation::Writable),
                (B, CandidateObservation::Busy),
                (C, CandidateObservation::Silent),
            ])
        );
        Ok(())
    }

    #[mononoke::test]
    async fn test_none_mode() -> Result<()> {
        tokio::time::pause();
        let fake = FakeTransport::new(&[(A, Server::Writable)]);
        let none = OssFailoverSettings {
            primary_check: PrimaryCheck::None,
            ..settings(RARELY)
        };
        let registry = registry(&fake, none);
        let cluster = registry
            .cluster(&target(&[A], "mononoke"), &PoolConfig::default())
            .await?;
        let status = cluster.status();
        assert_eq!(status.mode, PrimaryCheck::None);
        assert_eq!(status.health, ClusterHealth::Unchecked);
        assert_eq!(status.primary, Some(candidate(A)));
        assert_eq!(status.read, Some(candidate(A)));
        assert_eq!(status.generation, 0);
        assert_eq!(status.last_round_age, None);
        assert_eq!(status.candidates, Vec::new());
        Ok(())
    }

    #[mononoke::test]
    async fn test_lookup_by_target() -> Result<()> {
        tokio::time::pause();
        // A port no other test uses: the list of clusters is process-wide.
        const UNIQUE: (&str, u16) = ("127.0.0.1", 13399);
        let fake = FakeTransport::new(&[(UNIQUE, Server::Writable)]);
        let target = target(&[UNIQUE], "mononoke");
        assert_eq!(oss_mysql_cluster_status(&target), None);

        let registry = registry(&fake, settings(RARELY));
        let cluster = registry.cluster(&target, &PoolConfig::default()).await?;
        let status = oss_mysql_cluster_status(&target).expect("the cluster is in use");
        assert_eq!(status.id, cluster.id);
        assert_eq!(status.primary, Some(candidate(UNIQUE)));
        // Every database of the same servers is served by the cluster.
        let other_database = OssMysqlTarget {
            database: "other".to_string(),
            ..target.clone()
        };
        assert_eq!(
            oss_mysql_cluster_status(&other_database).map(|status| status.id),
            Some(cluster.id)
        );
        assert!(
            oss_mysql_cluster_statuses()
                .iter()
                .any(|status| status.id == cluster.id)
        );

        // Once its last store drops it, it is gone.
        let id = cluster.id;
        drop(cluster);
        assert_eq!(oss_mysql_cluster_status(&target), None);
        assert!(
            oss_mysql_cluster_statuses()
                .iter()
                .all(|status| status.id != id)
        );
        Ok(())
    }
}
