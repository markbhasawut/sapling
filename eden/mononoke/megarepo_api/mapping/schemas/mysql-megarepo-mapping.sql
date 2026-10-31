/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-megarepo-mapping.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
--
-- Types follow src/lib.rs: changeset ids are 32-byte Blake2 hashes; source
-- names, bookmarks and SyncConfigVersion are strings compared byte for byte.
-- Rust never reads mapping_id.

CREATE TABLE IF NOT EXISTS `megarepo_changeset_mapping` (
  `mapping_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `source_name` VARBINARY(255) NOT NULL,
  `target_repo_id` INT NOT NULL,
  `target_bookmark` VARBINARY(512) NOT NULL,
  `source_bcs_id` BINARY(32) NOT NULL,
  `target_bcs_id` BINARY(32) NOT NULL,
  `sync_config_version` VARBINARY(255),
  PRIMARY KEY (`mapping_id`),
  UNIQUE KEY `target_repo_id_target_bookmark_target_bcs_id` (`target_repo_id`, `target_bookmark`, `target_bcs_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
