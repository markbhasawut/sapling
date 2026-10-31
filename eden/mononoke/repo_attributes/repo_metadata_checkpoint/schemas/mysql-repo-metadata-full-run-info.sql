/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-repo-metadata-full-run-info.sql. Mononoke
-- creates tables only on SQLite: apply this file to the database that the
-- repo's `oss_remote` metadata config names. Applying it again changes
-- nothing. The timestamp is a Timestamp, i64 nanoseconds (src/sql.rs).
--
-- Table for tracking when the last full mode run completed for each repo.
-- This is separate from the per-bookmark checkpoint table because full mode
-- runs are a repo-level operation, not a per-bookmark operation.
CREATE TABLE IF NOT EXISTS `repo_metadata_full_run_info` (
  `repo_id` INT UNSIGNED NOT NULL, -- The ID of the repo
  `last_full_run_timestamp` BIGINT NOT NULL, -- Timestamp of last successful full run
  PRIMARY KEY (`repo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
