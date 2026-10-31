/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-hg-mutations.sql. Mononoke creates tables
-- only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes
-- nothing.
--
-- Types follow src/store.rs: changeset ids are 20-byte hg hashes, counts and
-- sequence numbers are u64, `timestamp` is i64 seconds and `tz` is i32.
-- Operation names and users are strings compared byte for byte; `extra` is a
-- small JSON list of key/value pairs read back as a String.

CREATE TABLE IF NOT EXISTS `hg_mutation_changesets` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `repo_id` INT UNSIGNED NOT NULL,
  `changeset_id` BINARY(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `repo_id_changeset_id` (`repo_id`, `changeset_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `hg_mutation_info` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `repo_id` INT UNSIGNED NOT NULL,
  `successor` BINARY(20) NOT NULL,
  `pred_count` BIGINT UNSIGNED NOT NULL,
  `split_count` BIGINT UNSIGNED NOT NULL,
  `op` VARBINARY(32) NOT NULL,
  `user` VARBINARY(512) NOT NULL,
  `timestamp` BIGINT NOT NULL,
  `tz` INT NOT NULL,
  `extra` BLOB, -- JSON '[["key","value"],...]',
  PRIMARY KEY (`id`),
  UNIQUE KEY `repo_id_successor` (`repo_id`, `successor`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `hg_mutation_preds` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `repo_id` INT UNSIGNED NOT NULL,
  `successor` BINARY(20) NOT NULL,
  `seq` BIGINT UNSIGNED NOT NULL,
  `predecessor` BINARY(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `repo_id_successor_seq` (`repo_id`, `successor`, `seq`),
  KEY `hg_mutation_preds_repo_id_predecessor` (`repo_id`, `predecessor`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `hg_mutation_splits` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `repo_id` INT UNSIGNED NOT NULL,
  `successor` BINARY(20) NOT NULL,
  `seq` BIGINT UNSIGNED NOT NULL,
  `split_successor` BINARY(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `repo_id_successor_seq` (`repo_id`, `successor`, `seq`),
  KEY `hg_mutation_splits_repo_id_split_successor` (`repo_id`, `split_successor`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
