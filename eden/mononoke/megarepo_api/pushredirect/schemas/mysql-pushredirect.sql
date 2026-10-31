/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-pushredirect.sql. Mononoke creates tables
-- only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
-- RowId is u64 and both flags are bool (src/store.rs, src/types.rs).

CREATE TABLE IF NOT EXISTS `pushredirect` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `repo_id` INT NOT NULL,
  `draft_push` BOOLEAN NOT NULL DEFAULT FALSE,
  `public_push` BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY (`id`),
  UNIQUE KEY `pushredirect_repo_id` (`repo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
