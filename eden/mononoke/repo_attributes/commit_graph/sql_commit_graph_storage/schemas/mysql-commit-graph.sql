/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-commit-graph.sql. Mononoke creates tables
-- only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
--
-- Types follow src/lib.rs: ids and the parent and ancestor columns that hold
-- ids are u64, generations and depths are u64, counts and parent numbers are
-- usize (small; INT UNSIGNED), and cs_id is a 32-byte Blake2 hash.
--
-- Index names: the id-range queries force `repo_id_id`, which SQLite lacks
-- (SQLite plans those scans on its rowid). It is (repo_id, id).

CREATE TABLE IF NOT EXISTS `commit_graph_edges` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `repo_id` INT NOT NULL,
  `cs_id` VARBINARY(32) NOT NULL,
  `gen` BIGINT UNSIGNED NOT NULL,
  `subtree_source_gen` BIGINT UNSIGNED NOT NULL,
  `skip_tree_depth` BIGINT UNSIGNED NOT NULL,
  `p1_linear_depth` BIGINT UNSIGNED NOT NULL,
  `subtree_source_depth` BIGINT UNSIGNED NOT NULL,
  `parent_count` INT UNSIGNED NOT NULL,
  `subtree_source_count` INT UNSIGNED NOT NULL,
  `p1_parent` BIGINT UNSIGNED NULL,
  `merge_ancestor` BIGINT UNSIGNED NULL,
  `skip_tree_parent` BIGINT UNSIGNED NULL,
  `skip_tree_skew_ancestor` BIGINT UNSIGNED NULL,
  `p1_linear_skew_ancestor` BIGINT UNSIGNED NULL,
  `subtree_or_merge_ancestor` BIGINT UNSIGNED NULL,
  `subtree_source_parent` BIGINT UNSIGNED NULL,
  `subtree_source_skew_ancestor` BIGINT UNSIGNED NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `repo_id_cs_id` (`repo_id`, `cs_id`),
  KEY `repo_id_id` (`repo_id`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `commit_graph_merge_parents` (
  `id` BIGINT UNSIGNED NOT NULL,
  `parent_num` INT UNSIGNED NOT NULL,
  `parent` BIGINT UNSIGNED NOT NULL,
  `repo_id` INT NOT NULL,
  PRIMARY KEY (`id`, `parent_num`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `commit_graph_subtree_sources` (
  `id` BIGINT UNSIGNED NOT NULL,
  `subtree_source_num` INT UNSIGNED NOT NULL,
  `subtree_source` BIGINT UNSIGNED NOT NULL,
  `repo_id` INT NOT NULL,
  PRIMARY KEY (`id`, `subtree_source_num`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
