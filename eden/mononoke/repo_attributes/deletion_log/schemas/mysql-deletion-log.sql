/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-deletion-log.sql. Mononoke creates tables
-- only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
--
-- Types follow src/lib.rs: cs_id is a 32-byte Blake2 hash, blob keys,
-- reasons and stages are Strings compared byte for byte, and Timestamp is i64
-- nanoseconds. `stage` is an enum in Meta's schema; its values (planned,
-- staged, deleted, cancelled) fit in 10 bytes.

CREATE TABLE IF NOT EXISTS `deletion_plan` (
  `repo_id` INT NOT NULL,
  `cs_id` VARBINARY(32) NOT NULL,
  `blob_key` VARBINARY(255) NOT NULL,
  `reason` VARBINARY(64) NOT NULL,
  `stage` VARBINARY(10) NOT NULL,
  `timestamp` BIGINT NOT NULL,
  PRIMARY KEY (`repo_id`, `cs_id`, `blob_key`, `reason`),
  KEY `deletion_plan_repo_id_reason` (`repo_id`, `reason`),
  KEY `deletion_plan_reason` (`reason`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
