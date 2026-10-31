/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-git-repositories-source-of-truth.sql.
-- Mononoke creates tables only on SQLite: apply this file to the database
-- that the repo's `oss_remote` metadata config names. Applying it again
-- changes nothing.
--
-- Types follow src/store.rs and src/types.rs: RowId is u64, RepositoryId is
-- i32, repo names and sources of truth are strings compared byte for byte,
-- mutation_id is i64 and repo_id_sequence hands out i64 ids.
--
-- InnoDB enforces the foreign key from source_of_truth (SQLite does not), so
-- source_of_truth_type must list every GitSourceOfTruth value, 'reserved'
-- included: without it, reserving a repo fails with error 1452. The seed
-- updates rows in place instead of REPLACE INTO, whose delete step the foreign
-- key would refuse once repos reference a value; it selects from a derived
-- table because ON DUPLICATE KEY UPDATE ... VALUES() is deprecated.

CREATE TABLE IF NOT EXISTS `source_of_truth_type` (
  `source_of_truth` VARBINARY(20) NOT NULL,
  `sequence` INT NOT NULL,
  PRIMARY KEY (`source_of_truth`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

INSERT INTO `source_of_truth_type` (`source_of_truth`, `sequence`)
  SELECT * FROM (
    SELECT 'mononoke' AS `source_of_truth`, 1 AS `sequence`
    UNION ALL SELECT 'metagit', 2
    UNION ALL SELECT 'locked', 3
    UNION ALL SELECT 'reserved', 4
  ) AS `seed`
  ON DUPLICATE KEY UPDATE `sequence` = `seed`.`sequence`;

CREATE TABLE IF NOT EXISTS `git_repositories_source_of_truth` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `repo_id` INT NOT NULL,
  `repo_name` VARBINARY(255) NOT NULL,
  `source_of_truth` VARBINARY(20) NOT NULL DEFAULT 'locked',
  `mutation_id` BIGINT DEFAULT NULL,
  `storage_config` VARBINARY(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `repo_id_idx` (`repo_id`),
  UNIQUE KEY `repo_name_idx` (`repo_name`),
  KEY `source_of_truth_idx` (`source_of_truth`),
  KEY `mutation_id_idx` (`mutation_id`),
  FOREIGN KEY (`source_of_truth`) REFERENCES `source_of_truth_type` (`source_of_truth`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Allocates repo ids. Nothing ever deletes from this table, so the counter only
-- goes up -- unlike `MAX(repo_id) + 1` over `git_repositories_source_of_truth`,
-- whose ceiling falls back when `cleanup_repos` removes rows and hands live ids
-- out a second time (S709055). Seeded once above every id that has ever existed.
CREATE TABLE IF NOT EXISTS `repo_id_sequence` (
  `repo_id` BIGINT NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`repo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
