/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-long_running_requests_queue.sql. Mononoke
-- creates tables only on SQLite: apply this file to the database that the
-- repo's `oss_remote` metadata config names. Applying it again changes
-- nothing.
--
-- Types follow src/types.rs and src/store.rs: RowId is u64, RepositoryId is
-- bound unsigned, every *_at column is a Timestamp (i64 nanoseconds) and
-- num_retries is u8. Request types, blobstore keys, statuses and names are
-- strings compared byte for byte. `status` is an enum in Meta's schema; here
-- it stays a byte string ('new', 'inprogress', 'ready', 'polled', 'failed').
--
-- Index names: the MySQL queries force `list_requests` and
-- `list_requests_any`, which have the same columns as SQLite's
-- `long_running_request_queue_list_requests` and
-- `long_running_request_queue_list_requests_any`. MySQL deprecates duplicate
-- indexes (warning 1831, "will be disallowed in a future release"), so each
-- pair is one index under the name the queries force. SQLite needs the table
-- prefix because its index names are global to the database; the other
-- indexes keep the SQLite names.

CREATE TABLE IF NOT EXISTS `long_running_request_queue` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `repo_id` INT UNSIGNED DEFAULT NULL,
  `bookmark` VARBINARY(512) DEFAULT NULL,
  `request_type` VARBINARY(255) NOT NULL,
  `args_blobstore_key` VARBINARY(255) NOT NULL,
  `result_blobstore_key` VARBINARY(255) DEFAULT NULL,
  `created_at` BIGINT NOT NULL,
  `started_processing_at` BIGINT DEFAULT NULL,
  `inprogress_last_updated_at` BIGINT DEFAULT NULL,
  `ready_at` BIGINT DEFAULT NULL,
  `polled_at` BIGINT DEFAULT NULL,
  `status` VARBINARY(32) NOT NULL,
  `claimed_by` VARBINARY(255) NULL,
  `num_retries` TINYINT UNSIGNED DEFAULT NULL,
  `failed_at` BIGINT DEFAULT NULL,
  `root_request_id` BIGINT UNSIGNED DEFAULT NULL,
  `created_by` VARBINARY(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `long_running_request_queue_request_status` (`status`, `request_type`),
  KEY `long_running_request_queue_request_creation` (`created_at`),
  KEY `long_running_request_queue_request_dequeue` (`status`, `repo_id`, `created_at`),
  KEY `long_running_request_queue_abandoned_request_index` (`repo_id`, `status`, `inprogress_last_updated_at`),
  KEY `long_running_request_queue_abandoned_request_index_any` (`status`, `inprogress_last_updated_at`),
  KEY `list_requests` (`status`, `repo_id`, `inprogress_last_updated_at`, `created_at`),
  KEY `list_requests_any` (`status`, `inprogress_last_updated_at`, `created_at`),
  KEY `long_running_request_queue_root_request` (`root_request_id`),
  UNIQUE KEY `long_running_request_queue_root_request_dedupe` (`root_request_id`, `request_type`, `repo_id`, `args_blobstore_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
