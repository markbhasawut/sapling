/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-commit-cloud.sql. Mononoke creates tables
-- only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names.
-- Applying it again changes nothing.
--
-- The tables follow the mysql() texts in src/sql/*_ops.rs, which differ from
-- the SQLite ones:
-- * `heads`, `snapshots`, `remotebookmarks` and `workspacebookmarks` name the
--   commit column `node` (SQLite: `commit`); `checkoutlocations` keeps
--   `commit`. CloudChangesetId binds as 40 hex characters, so these columns
--   are VARBINARY(40); SQLite's VARBINARY(32) is not enforced there.
-- * `history.timestamp` and `versions.timestamp` are SQL timestamps: the
--   queries write them with FROM_UNIXTIME() and current_timestamp and read
--   them with UNIX_TIMESTAMP(). `checkoutlocations.timestamp` is a Timestamp
--   (i64 nanoseconds) and stays BIGINT.
-- * Meta's commit cloud database calls the workspace bookmarks table
--   `bookmarks`. In open-source builds commit cloud shares a database with
--   Mononoke's own `bookmarks` table, so the queries use SQLite's name,
--   `workspacebookmarks` (src/sql/local_bookmarks_ops.rs).
--
-- Repo, workspace, host, path and bookmark names are compared byte for byte.
-- History blobs are serialized head and bookmark lists, which outgrow BLOB's
-- 64 KiB on large workspaces, so they are MEDIUMBLOB. Versions are u64.

-- bookmarks
-- Contains the set of bookmarks for each workspace
CREATE TABLE IF NOT EXISTS `workspacebookmarks` (
  `reponame` VARBINARY(255) NOT NULL,
  `workspace` VARBINARY(255) NOT NULL,
  `name` VARBINARY(512) NOT NULL,
  `node` VARBINARY(40) NOT NULL,
  PRIMARY KEY (`reponame`, `workspace`, `name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- checkout locations
-- Contains the checkout locations
CREATE TABLE IF NOT EXISTS `checkoutlocations` (
  `reponame` VARBINARY(255) NOT NULL,
  `workspace` VARBINARY(255) NOT NULL,
  `hostname` VARBINARY(255) NOT NULL,
  `commit` VARBINARY(40) NOT NULL,
  `checkout_path` VARBINARY(255) NOT NULL,
  `shared_path` VARBINARY(255) NOT NULL,
  `timestamp` BIGINT NOT NULL,
  `unixname` VARBINARY(255) NOT NULL,
  PRIMARY KEY (`reponame`, `workspace`, `hostname`, `checkout_path`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- heads
-- Contains the set of heads for each workspace
-- `author_date` is unix seconds, signed so pre-1970 dates stay in the past, and nullable
-- because NULL means "not yet backfilled" -- a sentinel of 0 would be indistinguishable
-- from a genuine epoch-era author date.
CREATE TABLE IF NOT EXISTS `heads` (
  `reponame` VARBINARY(255) NOT NULL,
  `workspace` VARBINARY(255) NOT NULL,
  `node` VARBINARY(40) NOT NULL,
  `author_date` BIGINT DEFAULT NULL,
  `seq` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`seq`),
  UNIQUE KEY `reponame_workspace_node` (`reponame`, `workspace`, `node`),
  KEY `reponame_commit` (`reponame`, `node`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- history
-- Creates the table to store historical version of timeline
CREATE TABLE IF NOT EXISTS `history` (
  `reponame` VARBINARY(255) NOT NULL,
  `workspace` VARBINARY(255) NOT NULL,
  `version` BIGINT UNSIGNED NOT NULL,
  `timestamp` TIMESTAMP NOT NULL,
  `heads` MEDIUMBLOB NOT NULL,
  `bookmarks` MEDIUMBLOB NOT NULL,
  `remotebookmarks` MEDIUMBLOB NOT NULL,
  PRIMARY KEY (`reponame`, `workspace`, `version`),
  KEY `reponame_workspace_timestamp` (`reponame`, `workspace`, `timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- remotebookmarks
-- Contains the set of remotebookmarks for each workspace
CREATE TABLE IF NOT EXISTS `remotebookmarks` (
  `reponame` VARBINARY(255) NOT NULL,
  `workspace` VARBINARY(255) NOT NULL,
  `remote` VARBINARY(255) NOT NULL,
  `name` VARBINARY(512) NOT NULL,
  `node` VARBINARY(40) NOT NULL,
  PRIMARY KEY (`reponame`, `workspace`, `remote`, `name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- snapshots
-- Contains the set of snapshots for each workspace
CREATE TABLE IF NOT EXISTS `snapshots` (
  `reponame` VARBINARY(255) NOT NULL,
  `workspace` VARBINARY(255) NOT NULL,
  `node` VARBINARY(40) NOT NULL,
  `seq` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`seq`),
  UNIQUE KEY `reponame_workspace_node` (`reponame`, `workspace`, `node`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- versions
-- Contains the latest version number of a workspace.
CREATE TABLE IF NOT EXISTS `versions` (
  `reponame` VARBINARY(255) NOT NULL,
  `workspace` VARBINARY(255) NOT NULL,
  `version` BIGINT UNSIGNED NOT NULL,
  `timestamp` TIMESTAMP NULL,
  `archived` BOOLEAN DEFAULT FALSE,
  PRIMARY KEY (`reponame`, `workspace`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
