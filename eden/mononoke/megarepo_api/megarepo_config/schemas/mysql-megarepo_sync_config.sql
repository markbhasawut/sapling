/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-megarepo_sync_config.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
--
-- Types follow src/db/store.rs: RowId is u64; bookmarks and versions are
-- strings compared byte for byte. `serialized_config` is a whole serialized
-- sync config read back as a String; a megarepo with many sources outgrows
-- BLOB's 64 KiB, so it is MEDIUMBLOB.

CREATE TABLE IF NOT EXISTS `megarepo_sync_config` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `repo_id` INT NOT NULL,
  `bookmark` VARBINARY(512) NOT NULL,
  `version` VARBINARY(512) NOT NULL,
  `serialized_config` MEDIUMBLOB NOT NULL,
  PRIMARY KEY (`id`),
  KEY `megarepo_sync_config_target_and_version` (`repo_id`, `bookmark`, `version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
