/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-bonsai-blob-mapping.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it
-- again changes nothing. Changeset ids are 32-byte Blake2 hashes and blob
-- keys are Strings compared byte for byte (src/lib.rs); 255 bytes matches the
-- other blobstore key columns.

CREATE TABLE IF NOT EXISTS `bonsai_blob_mapping` (
  `repo_id` INT NOT NULL,
  `cs_id` VARBINARY(32) NOT NULL,
  `blob_key` VARBINARY(255) NOT NULL,
  PRIMARY KEY (`repo_id`, `cs_id`, `blob_key`),
  KEY `bonsai_blob_mapping_blob_key` (`repo_id`, `blob_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
