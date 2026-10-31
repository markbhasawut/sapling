/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-long_running_request_dependencies.sql.
-- Mononoke creates tables only on SQLite: apply this file to the database
-- that the repo's `oss_remote` metadata config names. Applying it again
-- changes nothing. Both columns hold RowId, a u64 `long_running_request_queue`
-- id (src/types.rs).

CREATE TABLE IF NOT EXISTS `long_running_request_dependencies` (
  `request_id` BIGINT UNSIGNED NOT NULL,
  `depends_on_request_id` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`request_id`, `depends_on_request_id`),
  KEY `idx_depends_on` (`depends_on_request_id`),
  KEY `idx_request_id` (`request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
