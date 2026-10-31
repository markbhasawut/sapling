/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! Server health for OSS builds: `GET /:repo/server_health`, a JSON report
//! of what this server knows about its own health and that of the repo's
//! metadata database, and the Prometheus export of the in-memory stats that
//! the HTTP layer serves on `/metrics`.
//!
//! The route is authorized as every per-repo route is. The report is 200
//! whatever the health; only members of the ACL admin group see server
//! names and paths. Clients treat a server without the route, such as one
//! built in fbcode, as one with nothing to report.

mod prometheus;
mod report;
mod rules;

use bytes::Bytes;
use gotham::state::FromState;
use gotham::state::State;
use gotham_derive::StateData;
use gotham_derive::StaticResponseExtender;
use gotham_ext::error::HttpError;
use gotham_ext::middleware::request_context::RequestContext;
use gotham_ext::response::BytesBody;
use metaconfig_types::MetadataDatabaseConfig;
use metaconfig_types::OssRemoteDatabaseConfig;
use metaconfig_types::OssRemoteMetadataDatabaseConfig;
use mononoke_api::Repo;
use mononoke_api_hg::HgRepoContext;
pub use prometheus::PROMETHEUS_CONTENT_TYPE;
pub use prometheus::render_prometheus;
use serde::Deserialize;
use sql_construct::facebook::oss_mysql_target;
use sql_ext::facebook::Candidate;
use sql_ext::facebook::ClusterStatus;
use sql_ext::facebook::OssMysqlTarget;
use sql_ext::facebook::oss_mysql_cluster_status;
use stats::memory_stats;

use self::report::ClusterInput;
use self::report::MetadataInput;
use self::report::build_report;
use crate::context::ServerContext;
use crate::handlers::HandlerInfo;
use crate::handlers::SaplingRemoteApiMethod;
use crate::utils::get_repo;

#[derive(Debug, Deserialize, StateData, StaticResponseExtender)]
pub struct ServerHealthParams {
    repo: String,
}

pub async fn server_health(state: &mut State) -> Result<BytesBody<Bytes>, HttpError> {
    let params = ServerHealthParams::take_from(state);

    state.put(HandlerInfo::new(
        &params.repo,
        SaplingRemoteApiMethod::ServerHealth,
    ));

    let sctx = ServerContext::borrow_from(state);
    let rctx = RequestContext::borrow_from(state).clone();
    let hg_repo_ctx: HgRepoContext<Repo> = get_repo(sctx, &rctx, &params.repo, None).await?;
    let admin = sctx.is_admin(rctx.ctx.metadata().identities()).await;

    let metadata = metadata_input(
        &hg_repo_ctx.repo_ctx().config().storage_config.metadata,
        oss_mysql_cluster_status,
    );
    let snapshot = memory_stats::snapshot();
    let report = build_report(
        &params.repo,
        &metadata,
        snapshot.as_ref(),
        sctx.will_exit(),
        admin,
    );

    let json = serde_json::to_vec(&report).map_err(|e| {
        HttpError::e500(anyhow::Error::from(e).context("converting the server health to JSON"))
    })?;
    Ok(BytesBody::new(json.into(), mime::APPLICATION_JSON))
}

/// The metadata database of a repo, with the status of every MySQL cluster
/// that `cluster_status` finds for its database entries.
fn metadata_input(
    config: &MetadataDatabaseConfig,
    cluster_status: impl Fn(&OssMysqlTarget) -> Option<ClusterStatus>,
) -> MetadataInput {
    match config {
        MetadataDatabaseConfig::Local(local) => MetadataInput::Local {
            path: local.path.display().to_string(),
        },
        MetadataDatabaseConfig::Remote(_) => MetadataInput::Remote,
        MetadataDatabaseConfig::OssRemote(remote) => MetadataInput::OssRemote {
            clusters: oss_clusters(remote, cluster_status),
        },
    }
}

/// The distinct clusters of the database entries of `remote`, in the order
/// of the entries: one per cluster in use, and one per name for the others.
fn oss_clusters(
    remote: &OssRemoteMetadataDatabaseConfig,
    cluster_status: impl Fn(&OssMysqlTarget) -> Option<ClusterStatus>,
) -> Vec<ClusterInput> {
    let entries: [(&'static str, Option<&OssRemoteDatabaseConfig>); 10] = [
        ("production", Some(&remote.production)),
        ("commit_graph", Some(&remote.commit_graph)),
        ("bookmarks", Some(&remote.bookmarks)),
        ("bonsai_hg_mapping", Some(&remote.bonsai_hg_mapping)),
        ("filenodes", Some(&remote.filenodes)),
        ("mutation", Some(&remote.mutation)),
        ("sparse_profiles", Some(&remote.sparse_profiles)),
        ("bonsai_blob_mapping", remote.bonsai_blob_mapping.as_ref()),
        ("deletion_log", remote.deletion_log.as_ref()),
        (
            "commit_derived_data_mapping",
            remote.commit_derived_data_mapping.as_ref(),
        ),
    ];
    let mut clusters: Vec<ClusterInput> = Vec::new();
    for (entry, config) in entries {
        let Some(config) = config else {
            continue;
        };
        // An entry whose target is invalid has no store, so no cluster.
        let status = oss_mysql_target(config)
            .ok()
            .and_then(|target| cluster_status(&target));
        let name = match &status {
            Some(status) => status.name.clone(),
            None => configured_name(config),
        };
        let same = |cluster: &&mut ClusterInput| match (&cluster.status, &status) {
            (Some(cluster), Some(status)) => cluster.id == status.id,
            (None, None) => cluster.name == name,
            _ => false,
        };
        match clusters.iter_mut().find(same) {
            Some(cluster) => {
                cluster.used_for.push(entry);
                if !cluster.databases.contains(&config.database) {
                    cluster.databases.push(config.database.clone());
                    cluster.databases.sort();
                }
            }
            None => clusters.push(ClusterInput {
                used_for: vec![entry],
                databases: vec![config.database.clone()],
                name,
                status,
            }),
        }
    }
    clusters
}

/// The name a cluster of `config` gets: its hosts file, or its servers.
fn configured_name(config: &OssRemoteDatabaseConfig) -> String {
    match &config.hosts_file {
        Some(path) => path.display().to_string(),
        None => config
            .endpoints
            .iter()
            .map(|endpoint| Candidate::new(endpoint.host.as_str(), endpoint.port).to_string())
            .collect::<Vec<_>>()
            .join(", "),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use metaconfig_types::LocalDatabaseConfig;
    use metaconfig_types::OssDbEndpoint;
    use mononoke_macros::mononoke;
    use sql_ext::facebook::ClusterHealth;
    use sql_ext::facebook::PrimaryCheck;

    use super::*;

    fn entry(hosts: &[(&str, u16)], database: &str) -> OssRemoteDatabaseConfig {
        OssRemoteDatabaseConfig {
            host: hosts[0].0.to_string(),
            port: hosts[0].1 as i16,
            database: database.to_string(),
            user_secret: "literal:mononoke".to_string(),
            password_secret: "literal:secret".to_string(),
            endpoints: hosts
                .iter()
                .map(|(host, port)| OssDbEndpoint {
                    host: host.to_string(),
                    port: *port,
                })
                .collect(),
            hosts_file: None,
        }
    }

    fn remote() -> OssRemoteMetadataDatabaseConfig {
        let main = entry(
            &[("db1.internal", 3306), ("db2.internal", 3306)],
            "mononoke",
        );
        OssRemoteMetadataDatabaseConfig {
            production: main.clone(),
            commit_graph: entry(&[("db2.internal", 3306), ("db1.internal", 3306)], "graph"),
            bookmarks: main.clone(),
            bonsai_hg_mapping: main.clone(),
            filenodes: OssRemoteDatabaseConfig {
                hosts_file: Some(PathBuf::from("/etc/mononoke/filenodes-hosts")),
                ..entry(&[("fn1.internal", 3306)], "filenodes")
            },
            mutation: main.clone(),
            sparse_profiles: main,
            bonsai_blob_mapping: None,
            deletion_log: Some(entry(&[("::1", 3306)], "deletion_log")),
            commit_derived_data_mapping: Some(OssRemoteDatabaseConfig {
                port: 0,
                ..entry(
                    &[("db1.internal", 3306), ("db2.internal", 3306)],
                    "mononoke",
                )
            }),
        }
    }

    fn summary(clusters: &[ClusterInput]) -> Vec<(String, Vec<&'static str>, Vec<String>, bool)> {
        clusters
            .iter()
            .map(|cluster| {
                (
                    cluster.name.clone(),
                    cluster.used_for.clone(),
                    cluster.databases.clone(),
                    cluster.status.is_some(),
                )
            })
            .collect()
    }

    #[mononoke::test]
    fn test_clusters_not_in_use() {
        let clusters = oss_clusters(&remote(), |_| None);
        let names = |names: &[&str]| names.iter().map(|name| name.to_string()).collect();
        assert_eq!(
            summary(&clusters),
            vec![
                (
                    "db1.internal:3306, db2.internal:3306".to_string(),
                    vec![
                        "production",
                        "bookmarks",
                        "bonsai_hg_mapping",
                        "mutation",
                        "sparse_profiles",
                        "commit_derived_data_mapping",
                    ],
                    names(&["mononoke"]),
                    false,
                ),
                (
                    "db2.internal:3306, db1.internal:3306".to_string(),
                    vec!["commit_graph"],
                    names(&["graph"]),
                    false,
                ),
                (
                    "/etc/mononoke/filenodes-hosts".to_string(),
                    vec!["filenodes"],
                    names(&["filenodes"]),
                    false,
                ),
                (
                    "[::1]:3306".to_string(),
                    vec!["deletion_log"],
                    names(&["deletion_log"]),
                    false,
                ),
            ]
        );
    }

    #[mononoke::test]
    fn test_clusters_in_use() {
        // The servers of the main entries and of the commit graph, listed in
        // another order, are one cluster, as the registry keys it.
        let status = |target: &OssMysqlTarget| {
            let mut endpoints = target.endpoints.clone();
            endpoints.sort();
            (endpoints
                == [
                    ("db1.internal".to_string(), 3306),
                    ("db2.internal".to_string(), 3306),
                ])
            .then(|| ClusterStatus {
                id: 4,
                name: "db1.internal:3306, db2.internal:3306".to_string(),
                mode: PrimaryCheck::Identity,
                health: ClusterHealth::Active,
                primary: Some(Candidate::new("db1.internal", 3306)),
                unavailable_reason: None,
                generation: 1,
                read: Some(Candidate::new("db1.internal", 3306)),
                last_round_age: None,
                candidates: Vec::new(),
            })
        };
        let clusters = oss_clusters(&remote(), status);
        let used_for: Vec<(Vec<&'static str>, Vec<String>, bool)> = clusters
            .iter()
            .map(|cluster| {
                (
                    cluster.used_for.clone(),
                    cluster.databases.clone(),
                    cluster.status.is_some(),
                )
            })
            .collect();
        assert_eq!(
            used_for,
            vec![
                (
                    vec![
                        "production",
                        "commit_graph",
                        "bookmarks",
                        "bonsai_hg_mapping",
                        "mutation",
                        "sparse_profiles",
                    ],
                    vec!["graph".to_string(), "mononoke".to_string()],
                    true,
                ),
                (vec!["filenodes"], vec!["filenodes".to_string()], false),
                (
                    vec!["deletion_log"],
                    vec!["deletion_log".to_string()],
                    false
                ),
                // Port 0 is invalid: no store, no cluster.
                (
                    vec!["commit_derived_data_mapping"],
                    vec!["mononoke".to_string()],
                    false,
                ),
            ]
        );
    }

    #[mononoke::test]
    fn test_local() {
        let local = MetadataDatabaseConfig::Local(LocalDatabaseConfig {
            path: PathBuf::from("/var/mononoke/sqlite"),
        });
        match metadata_input(&local, |_| None) {
            MetadataInput::Local { path } => assert_eq!(path, "/var/mononoke/sqlite"),
            _ => panic!("not local"),
        }
    }
}
