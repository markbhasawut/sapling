/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-bonsai-tag-mapping.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again
-- changes nothing.
--
-- Table for maintaining mapping between git annotated tags
-- and the bonsai changeset stored in Mononoke representing the metadata
-- associated with the tag. Tag names are Strings compared byte for byte (Git
-- ref names are case sensitive), changeset_id is a 32-byte Blake2 hash,
-- tag_hash a 20-byte SHA-1 and target_is_tag a bool (src/sql.rs).
CREATE TABLE IF NOT EXISTS `bonsai_tag_mapping` (
  `repo_id` INT UNSIGNED NOT NULL, -- The ID of the repo for which this tag exists
  `tag_name` VARBINARY(512) NOT NULL, -- The name of the tag
  `changeset_id` VARBINARY(32) NOT NULL, -- The Id of the mapped changeset
  `tag_hash` VARBINARY(20) DEFAULT 0x0000000000000000000000000000000000000000, -- The Git hash of the tag object
  `target_is_tag` BOOLEAN DEFAULT FALSE, -- Flag determining if the target of the tag is another tag instead of a commit
  PRIMARY KEY (`repo_id`, `tag_name`),
  KEY `bonsai_tag_mapping_hashes` (`tag_hash`, `repo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
