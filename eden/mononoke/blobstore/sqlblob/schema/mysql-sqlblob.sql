/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-sqlblob.sql. Mononoke creates tables only
-- on SQLite: apply this file to every shard database of the SQL blobstore.
-- Applying it again changes nothing.
--
-- Types follow src/store.rs: blobstore keys and chunk ids are &str compared
-- byte for byte (keys are a repo prefix, a type tag and a hash, well under 255
-- bytes), creation times are i64, chunk counts and numbers are u32,
-- generations and value lengths are u64. A chunk holds up to CHUNK_SIZE
-- (1 MiB, src/lib.rs) bytes, which needs MEDIUMBLOB (BLOB stops at 64 KiB).
-- InsertGeneration inserts into `chunk_generation` without a column list, so
-- its column order must not change.

CREATE TABLE IF NOT EXISTS `data` (
  `id` VARBINARY(255) NOT NULL,
  `creation_time` BIGINT NOT NULL,
  `chunk_id` VARBINARY(255) NOT NULL,
  `chunk_count` INT UNSIGNED NOT NULL,
  `chunking_method` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `chunk` (
  `id` VARBINARY(255) NOT NULL,
  `creation_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `chunk_num` INT UNSIGNED NOT NULL,
  `value` MEDIUMBLOB NOT NULL,
  PRIMARY KEY (`id`, `chunk_num`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `chunk_generation` (
  `id` VARBINARY(255) NOT NULL,
  `last_seen_generation` BIGINT UNSIGNED NOT NULL,
  `value_len` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
