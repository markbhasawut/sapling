/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-restricted-paths-manifest-ids.sql. Mononoke
-- creates tables only on SQLite: apply this file to the database that the
-- repo's `oss_remote` metadata config names. Applying it again changes
-- nothing.
--
-- Types follow src/manifest_id_store.rs: ids are u64, the manifest type is a
-- short name ('hg', 'fsnode', ...) and the manifest id a hash of at most 32
-- bytes, both compared byte for byte; paths are raw bytes (only their hashes
-- are indexed).

CREATE TABLE IF NOT EXISTS `restricted_paths_manifest_ids` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `repo_id` INT UNSIGNED NOT NULL,
  `manifest_type` VARBINARY(32) NOT NULL,
  `manifest_id` VARBINARY(32) NOT NULL,
  `path` VARBINARY(4096) NOT NULL,
  `path_hash` VARBINARY(32) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `repo_id_manifest_type_manifest_id_path_hash` (`repo_id`, `manifest_type`, `manifest_id`, `path_hash`),
  UNIQUE KEY `repo_id_id` (`repo_id`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
