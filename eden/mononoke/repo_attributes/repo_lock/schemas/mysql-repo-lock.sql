/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-repo-lock.sql. Mononoke creates tables only
-- on SQLite: apply this file to the database that the repo's `oss_remote`
-- metadata config names. Applying it again changes nothing. The state is a
-- u8 and the reason an optional &str (src/lib.rs). SQLite's INTEGER PRIMARY
-- KEY is a rowid alias; Rust always supplies repo_id, so no AUTO_INCREMENT.

CREATE TABLE IF NOT EXISTS `repo_lock` (
  `repo_id` INT NOT NULL,
  `state` TINYINT UNSIGNED NOT NULL,
  `reason` VARBINARY(255),
  PRIMARY KEY (`repo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
