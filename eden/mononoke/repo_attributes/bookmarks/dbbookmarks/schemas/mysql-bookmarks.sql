/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-bookmarks.sql. Mononoke creates tables only
-- on SQLite: apply this file to the database that the repo's `oss_remote`
-- metadata config names. Applying it again changes nothing.
--
-- Types follow src/store.rs, src/transaction.rs and bookmarks_types:
-- bookmark names, kinds, categories and reasons bind as byte strings and are
-- compared byte for byte (Meta's schema uses enums for kinds and reasons),
-- changeset ids are 32-byte Blake2 hashes, log ids are u64 and Timestamp is
-- i64 nanoseconds. bookmarks_update_log.id is assigned by Mononoke, not by
-- AUTO_INCREMENT.
--
-- Index names: subscription.rs forces `repo_id_log_id`, which is the
-- UNIQUE (repo_id, log_id) constraint that SQLite leaves unnamed, and
-- store.rs forces `repo_id_hg_kind_category`, which SQLite lacks.

CREATE TABLE IF NOT EXISTS `bookmarks` (
  `repo_id` INT UNSIGNED NOT NULL,
  `name` VARBINARY(512) NOT NULL,
  `changeset_id` VARBINARY(32) NOT NULL,
  -- this column is named 'hg_kind' for historical reasons, but applies for non-Mercurial uses (e.g. phase calculations)
  `hg_kind` VARBINARY(32) NOT NULL DEFAULT 'pull_default', -- enum is used in Meta's schema
  -- Distinguish between different categories of bookmarks:
  -- branch, tag or note
  `category` VARBINARY(32) NOT NULL DEFAULT 'branch',
  `log_id` BIGINT UNSIGNED NULL,
  PRIMARY KEY (`repo_id`, `name`, `category`),
  UNIQUE KEY `repo_id_log_id` (`repo_id`, `log_id`),
  KEY `bookmarks_repo_id_hg_kind` (`repo_id`, `hg_kind`),
  KEY `repo_id_hg_kind_category` (`repo_id`, `hg_kind`, `category`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `bookmarks_update_log` (
  `id` BIGINT UNSIGNED NOT NULL,
  `repo_id` INT UNSIGNED NOT NULL,
  `name` VARBINARY(512) NOT NULL,
  `from_changeset_id` VARBINARY(32),
  `to_changeset_id` VARBINARY(32),
  `reason` VARBINARY(32) NOT NULL, -- enum is used in Meta's schema
  `timestamp` BIGINT NOT NULL,
  `category` VARBINARY(32) NOT NULL DEFAULT 'branch',
  PRIMARY KEY (`repo_id`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
