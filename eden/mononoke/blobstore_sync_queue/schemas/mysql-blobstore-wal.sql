/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-blobstore-wal.sql. Mononoke creates tables
-- only on SQLite: apply this file to the write-ahead log database. Applying
-- it again changes nothing.
--
-- Types follow src/write_ahead_log.rs: ids are u64, Timestamp is i64
-- nanoseconds, MultiplexId is i32, blob sizes are u64 and retry counts are
-- u32. `blobstore_key` is a String compared byte for byte; SQLite declares no
-- length, and 255 bytes matches the key columns of the SQL blobstore.

CREATE TABLE IF NOT EXISTS `blobstore_write_ahead_log` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `blobstore_key` VARBINARY(255) NOT NULL,
  `timestamp` BIGINT NOT NULL, /* time the blob was added to the queue */
  `multiplex_id` INT NOT NULL,
  `blob_size` BIGINT UNSIGNED NOT NULL,
  `retry_count` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
