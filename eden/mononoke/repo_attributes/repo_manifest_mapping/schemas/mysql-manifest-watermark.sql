/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-manifest-watermark.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
-- Branch names bind as bytes (src/types.rs); log ids and last_landed_at are
-- i64.
--
-- Per-manifest-BRANCH tailer watermark: the last processed bookmark_update_log
-- id for each manifest branch, so the tailer resumes per branch. Keyed by
-- (repo_id, manifest_branch) so correctness rests only on per-branch log-id
-- monotonicity -- robust to a future per-repo-per-bookmark transaction model
-- where ids are monotonic per branch but not per repo.
CREATE TABLE IF NOT EXISTS `manifest_watermark` (
  `repo_id` INT NOT NULL,
  `manifest_branch` VARBINARY(255) NOT NULL,
  `log_id` BIGINT NOT NULL,
  -- Unix seconds of the branch's last non-service bookmark move or named land; NULL = never.
  -- Writers only ever raise it, so stale readers and replays cannot rewind it.
  `last_landed_at` BIGINT NULL,
  PRIMARY KEY (`repo_id`, `manifest_branch`),
  -- Serves `GetReadCursor` (`ORDER BY log_id DESC LIMIT 1`) without a filesort.
  KEY `read_cursor_idx` (`repo_id`, `log_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
