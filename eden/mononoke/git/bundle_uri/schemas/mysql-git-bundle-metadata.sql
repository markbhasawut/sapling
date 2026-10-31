/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-git-bundle-metadata.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
--
-- Types follow src/sql.rs: bundle list numbers, positions and generation
-- start timestamps are u64; handles and fingerprints are Strings compared
-- byte for byte (SQLite's `VARCHAT(255)` is a typo it accepts).

CREATE TABLE IF NOT EXISTS `git_bundles` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `repo_id` INT NOT NULL,
  `bundle_handle` VARBINARY(255) NOT NULL,
  `bundle_list` BIGINT UNSIGNED NOT NULL,
  `in_bundle_list_order` BIGINT UNSIGNED NOT NULL,
  `bundle_fingerprint` VARBINARY(255) NOT NULL,
  `generation_start_timestamp` BIGINT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `repo_id_bundle_list_in_bundle_list_order` (`repo_id`, `bundle_list`, `in_bundle_list_order`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
