/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-sparse-profiles-sizes.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
-- cs_id is a 32-byte Blake2 hash, profile names are Strings compared byte
-- for byte and sizes are u64 (src/lib.rs).

CREATE TABLE IF NOT EXISTS `sparse_profiles_sizes` (
  `cs_id` VARBINARY(32) NOT NULL,
  `profile_name` VARBINARY(512) NOT NULL,
  `size` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`cs_id`, `profile_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
