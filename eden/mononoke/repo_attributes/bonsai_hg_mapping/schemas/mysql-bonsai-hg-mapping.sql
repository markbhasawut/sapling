/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-bonsai-hg-mapping.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it
-- again changes nothing. hg_cs_id is a 20-byte hg hash and bcs_id a 32-byte
-- Blake2 hash (src/lib.rs).

CREATE TABLE IF NOT EXISTS `bonsai_hg_mapping` (
  `repo_id` INT NOT NULL,
  `hg_cs_id` BINARY(20) NOT NULL,
  `bcs_id` BINARY(32) NOT NULL,
  PRIMARY KEY (`repo_id`, `bcs_id`),
  UNIQUE KEY `repo_id_hg_cs_id` (`repo_id`, `hg_cs_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
