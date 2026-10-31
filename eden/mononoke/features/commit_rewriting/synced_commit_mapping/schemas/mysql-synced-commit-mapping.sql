/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-synced-commit-mapping.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
-- See the SQLite file for what each table means.
--
-- Types follow src/sql.rs: changeset ids are 32-byte Blake2 hashes,
-- CommitSyncConfigVersion is a string compared byte for byte, and
-- SyncedCommitSourceRepo is 'small' or 'large' (an enum in Meta's schema).
-- Rust never reads the mapping_id columns.

CREATE TABLE IF NOT EXISTS `synced_commit_mapping` (
  `mapping_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `small_repo_id` INT NOT NULL,
  `small_bcs_id` BINARY(32) NOT NULL,
  `large_repo_id` INT NOT NULL,
  `large_bcs_id` BINARY(32) NOT NULL,
  `sync_map_version_name` VARBINARY(255),
  `source_repo` VARBINARY(32),
  PRIMARY KEY (`mapping_id`),
  UNIQUE KEY `small_repo_id_large_repo_id_large_bcs_id` (`small_repo_id`, `large_repo_id`, `large_bcs_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `synced_working_copy_equivalence` (
  `mapping_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `small_repo_id` INT NOT NULL,
  `small_bcs_id` BINARY(32),
  `large_repo_id` INT NOT NULL,
  `large_bcs_id` BINARY(32) NOT NULL,
  `sync_map_version_name` VARBINARY(255),
  PRIMARY KEY (`mapping_id`),
  UNIQUE KEY `large_repo_id_small_repo_id_large_bcs_id` (`large_repo_id`, `small_repo_id`, `large_bcs_id`),
  -- Small bcs id can map to multiple large bcs ids
  KEY `synced_working_copy_equivalence_small_bcs_key` (`large_repo_id`, `small_repo_id`, `small_bcs_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `version_for_large_repo_commit` (
  `mapping_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `large_repo_id` INT NOT NULL,
  `large_bcs_id` BINARY(32) NOT NULL,
  `sync_map_version_name` VARBINARY(255) NOT NULL,
  PRIMARY KEY (`mapping_id`),
  UNIQUE KEY `large_repo_id_large_bcs_id` (`large_repo_id`, `large_bcs_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
