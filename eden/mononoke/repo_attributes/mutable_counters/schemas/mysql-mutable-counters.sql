/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-mutable-counters.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
-- Counter names are &str compared byte for byte and values are i64
-- (src/lib.rs). The key is 644 bytes, well under InnoDB's 3072.

CREATE TABLE IF NOT EXISTS `mutable_counters` (
  `repo_id` INT UNSIGNED NOT NULL,
  `name` VARBINARY(640) NOT NULL,
  `value` BIGINT NOT NULL,
  PRIMARY KEY (`repo_id`, `name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
