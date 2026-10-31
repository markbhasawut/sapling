/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-bonsai-globalrev-mapping.sql. Mononoke
-- creates tables only on SQLite: apply this file to the database that the
-- repo's `oss_remote` metadata config names. Applying it again
-- changes nothing. Globalrev binds as u64 (mononoke_types/src/sql_types.rs).

CREATE TABLE IF NOT EXISTS `bonsai_globalrev_mapping` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `repo_id` INT NOT NULL,
  `bcs_id` BINARY(32) NOT NULL,
  `globalrev` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `repo_id_bcs_id` (`repo_id`, `bcs_id`),
  UNIQUE KEY `repo_id_globalrev` (`repo_id`, `globalrev`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
