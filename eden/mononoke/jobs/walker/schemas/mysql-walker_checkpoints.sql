/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-walker_checkpoints.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
--
-- Types follow src/detail/checkpoint.rs: bounds, run numbers and chunk
-- numbers are u64, timestamps are Timestamp (i64 nanoseconds), and checkpoint
-- names are strings compared byte for byte.

CREATE TABLE IF NOT EXISTS `walker_checkpoints` (
  `repo_id` INT NOT NULL,
  `checkpoint_name` VARBINARY(255) NOT NULL,
  `lower_bound` BIGINT UNSIGNED NOT NULL,
  `upper_bound` BIGINT UNSIGNED NOT NULL,
  `create_timestamp` BIGINT NOT NULL,
  `update_timestamp` BIGINT NOT NULL,
  `update_run_number` BIGINT UNSIGNED NOT NULL,
  `update_chunk_number` BIGINT UNSIGNED NOT NULL,
  `last_finish_timestamp` BIGINT NULL,
  `last_finish_run_number` BIGINT UNSIGNED NULL,
  `last_finish_chunk_number` BIGINT UNSIGNED NULL,
  UNIQUE KEY `repo_id_checkpoint_name` (`repo_id`, `checkpoint_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `walker_checkpoints_v2` (
  `repo_id` INT NOT NULL,
  `checkpoint_name` VARBINARY(255) NOT NULL,
  `lower_bound` BIGINT UNSIGNED NOT NULL,
  `upper_bound` BIGINT UNSIGNED NOT NULL,
  `create_timestamp` BIGINT NOT NULL,
  `update_timestamp` BIGINT NOT NULL,
  `update_run_number` BIGINT UNSIGNED NOT NULL,
  `update_chunk_number` BIGINT UNSIGNED NOT NULL,
  `last_finish_timestamp` BIGINT NULL,
  `last_finish_run_number` BIGINT UNSIGNED NULL,
  `last_finish_chunk_number` BIGINT UNSIGNED NULL,
  UNIQUE KEY `repo_id_checkpoint_name` (`repo_id`, `checkpoint_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
