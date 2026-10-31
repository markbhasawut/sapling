/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-bonsai-git-mapping.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
-- bcs_id is a 32-byte Blake2 hash and git_sha1 a 20-byte SHA-1 (src/sql.rs).

CREATE TABLE IF NOT EXISTS `bonsai_git_mapping` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `repo_id` INT NOT NULL,
  `bcs_id` BINARY(32) NOT NULL,
  `git_sha1` BINARY(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `repo_id_bcs_id` (`repo_id`, `bcs_id`),
  UNIQUE KEY `repo_id_git_sha1` (`repo_id`, `git_sha1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
