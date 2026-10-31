/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-phases.sql. Mononoke creates tables only on
-- SQLite: apply this file to the database that the repo's `oss_remote`
-- metadata config names. Applying it again changes nothing.
-- cs_id is a 32-byte Blake2 hash and the phase is 'Public' or 'Draft',
-- compared byte for byte (src/sql_phases.rs; Meta's schema uses an enum).
-- SelectPhases forces the PRIMARY index.

CREATE TABLE IF NOT EXISTS `phases` (
  `repo_id` INT NOT NULL,
  `cs_id` VARBINARY(32) NOT NULL,
  `phase` VARBINARY(32) NOT NULL,
  PRIMARY KEY (`repo_id`, `cs_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
