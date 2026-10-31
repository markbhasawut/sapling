/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-pushrebase-mutation-mapping.sql. Mononoke
-- creates tables only on SQLite: apply this file to the database that the
-- repo's `oss_remote` metadata config names. Applying it again changes
-- nothing. Both changeset ids are 32-byte Blake2 hashes (src/sql_queries.rs).

CREATE TABLE IF NOT EXISTS `pushrebase_mutation_mapping` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `repo_id` INT NOT NULL,
  `predecessor_bcs_id` BINARY(32) NOT NULL,
  `successor_bcs_id` BINARY(32) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `pushrebase_mutation_mapping_repo_successor_key` (`repo_id`, `successor_bcs_id`),
  -- Production already carries this index as `repo_predecessor_key`; it is
  -- declared here so SQLite-backed tests plan the forward lookup the same way.
  KEY `pushrebase_mutation_mapping_repo_predecessor_key` (`repo_id`, `predecessor_bcs_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
