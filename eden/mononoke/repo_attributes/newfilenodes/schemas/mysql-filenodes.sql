/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-filenodes.sql. Mononoke creates tables only
-- on SQLite: apply this file to the database that the repo's `oss_remote`
-- metadata config names. Applying it again changes nothing.
--
-- Types follow src/reader.rs and src/writer.rs: filenode and parent ids are
-- 20-byte hg hashes, linknode is a 20-byte HgChangesetId (VARBINARY(32) as in
-- SQLite), path hashes are PathHashBytes of at most 32 bytes, paths are raw
-- bytes and is_tree / has_copyinfo are 0/1 i8.

CREATE TABLE IF NOT EXISTS `filenodes` (
  `repo_id` INT UNSIGNED NOT NULL,
  `path_hash` VARBINARY(32) NOT NULL,
  `is_tree` BOOLEAN NOT NULL,
  `filenode` BINARY(20) NOT NULL,
  `linknode` VARBINARY(32) NOT NULL,
  `p1` BINARY(20),
  `p2` BINARY(20),
  `has_copyinfo` BOOLEAN NOT NULL,
  PRIMARY KEY (`repo_id`, `path_hash`, `is_tree`, `filenode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `fixedcopyinfo` (
  `repo_id` INT UNSIGNED NOT NULL,
  `topath_hash` VARBINARY(32) NOT NULL,
  `tonode` BINARY(20) NOT NULL,
  `is_tree` BOOLEAN NOT NULL,
  `frompath_hash` VARBINARY(32) NOT NULL,
  `fromnode` BINARY(20) NOT NULL,
  PRIMARY KEY (`repo_id`, `topath_hash`, `tonode`, `is_tree`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `paths` (
  `repo_id` INT UNSIGNED NOT NULL,
  `path_hash` VARBINARY(32) NOT NULL,
  `path` VARBINARY(4096) NOT NULL,
  PRIMARY KEY (`repo_id`, `path_hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
