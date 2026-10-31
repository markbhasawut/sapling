/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-mutable-renames.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
--
-- Types follow src/lib.rs: changeset and unode ids are 32-byte Blake2
-- hashes, path hashes are Vec<u8> of at most 32 bytes, paths are raw bytes
-- (VARBINARY(4096), as in SQLite; only their hashes are indexed) and is_tree
-- is a 0/1 i8.

CREATE TABLE IF NOT EXISTS `mutable_renames` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `repo_id` INT UNSIGNED NOT NULL,
  `dst_cs_id` VARBINARY(32) NOT NULL,
  `dst_path_hash` VARBINARY(32) NOT NULL,
  `src_cs_id` VARBINARY(32) NOT NULL,
  `src_path_hash` VARBINARY(32) NOT NULL,
  `src_unode_id` VARBINARY(32) NOT NULL,
  `is_tree` BOOLEAN NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `repo_id_dst_path_hash_dst_cs_id` (`repo_id`, `dst_path_hash`, `dst_cs_id`),
  KEY `mutable_renames_dst_cs_id` (`repo_id`, `dst_cs_id`),
  KEY `mutable_renames_dst_path_hash` (`dst_path_hash`),
  KEY `mutable_renames_src_path_hash` (`src_path_hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `mutable_renames_paths` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `path_hash` VARBINARY(32) NOT NULL,
  `path` VARBINARY(4096) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `path_hash` (`path_hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
