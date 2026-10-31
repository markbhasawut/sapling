/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-ephemeral-blobstore.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
--
-- Types follow the Rust bindings (src/bubble.rs, src/store.rs,
-- src/commit_graph.rs): BubbleId is a non-zero u64, Timestamp is i64
-- nanoseconds, ExpiryStatus is 0 or 1, generations are u64, parent indexes
-- are u32 and changeset ids are 32-byte Blake2 hashes. Labels and owner
-- identities are compared byte for byte.

CREATE TABLE IF NOT EXISTS `ephemeral_bubbles` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `created_at` BIGINT NOT NULL,
  `expires_at` BIGINT NOT NULL,
  `expired` BOOLEAN NOT NULL DEFAULT FALSE,
  `owner_identity` VARBINARY(255),
  PRIMARY KEY (`id`),
  KEY `ephemeral_bubbles_expires` (`expires_at`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `ephemeral_bubble_changeset_mapping` (
  `repo_id` INT NOT NULL,
  `cs_id` BINARY(32) NOT NULL,
  `bubble_id` BIGINT UNSIGNED NOT NULL,
  `gen` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`repo_id`, `bubble_id`, `cs_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `ephemeral_bubble_changeset_parents` (
  `repo_id` INT NOT NULL,
  `bubble_id` BIGINT UNSIGNED NOT NULL,
  `cs_id` BINARY(32) NOT NULL,
  `parent_index` INT UNSIGNED NOT NULL,
  `parent_cs_id` BINARY(32) NOT NULL,
  PRIMARY KEY (`repo_id`, `bubble_id`, `cs_id`, `parent_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- `label` is part of the primary key, so MySQL needs it NOT NULL (SQLite
-- accepts the nullable declaration). Rust never binds a NULL label.
CREATE TABLE IF NOT EXISTS `ephemeral_bubble_labels` (
  `bubble_id` BIGINT UNSIGNED NOT NULL,
  `label` VARBINARY(255) NOT NULL,
  PRIMARY KEY (`bubble_id`, `label`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
