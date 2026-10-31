/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-repo-metadata-info.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
-- Bookmark names are Strings compared byte for byte, changeset_id is a
-- 32-byte Blake2 hash and the timestamp is a Timestamp, i64 nanoseconds
-- (src/sql.rs).
--
-- Table for maintaining information about the metadata logged per repo per bookmark.
-- This will reflect the last successful bonsai changeset that was logged for a given
-- repo and bookmark
CREATE TABLE IF NOT EXISTS `repo_metadata_info` (
  `repo_id` INT UNSIGNED NOT NULL, -- The ID of the repo for which this entry exists
  `bookmark_name` VARBINARY(512) NOT NULL, -- The name of the bookmark
  `changeset_id` VARBINARY(32) NOT NULL, -- The changeset ID that was processed successfully for the given bookmark and repo
  `last_updated_timestamp` BIGINT NOT NULL, -- The timestamp of the last successful update
  PRIMARY KEY (`repo_id`, `bookmark_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
