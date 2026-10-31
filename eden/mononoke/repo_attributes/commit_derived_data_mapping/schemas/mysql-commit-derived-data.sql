/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-commit-derived-data.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
-- Types follow src/lib.rs: cs_id is a 32-byte Blake2 hash, the type and
-- version are i32, and the derived value is a Vec<u8> id (32 bytes at most).

CREATE TABLE IF NOT EXISTS `commit_derived_data` (
  `repo_id` INT NOT NULL,
  `cs_id` VARBINARY(32) NOT NULL,
  `derived_data_type` INT NOT NULL,
  `derived_data_version` INT NOT NULL,
  `derived_value` VARBINARY(32) NOT NULL,
  PRIMARY KEY (`repo_id`, `cs_id`, `derived_data_type`, `derived_data_version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
