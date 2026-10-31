/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-enabled-derived-data-types.sql. Mononoke
-- creates tables only on SQLite: apply this file to the database that the
-- repo's `oss_remote` metadata config names. Applying it again changes
-- nothing. Types follow src/store.rs: the derived data type is its name
-- compared byte for byte, and root_request_id is an optional u64 request id.

CREATE TABLE IF NOT EXISTS `enabled_derived_data_types` (
  `repo_id` INT NOT NULL,
  `derived_data_type` VARBINARY(255) NOT NULL,
  `root_request_id` BIGINT UNSIGNED DEFAULT NULL,
  PRIMARY KEY (`repo_id`, `derived_data_type`),
  -- Lookups by `repo_id` ("types enabled for a repo") are served by the
  -- leftmost prefix of the primary key. This secondary index serves the reverse
  -- lookup ("repos with a given type enabled"); ordering `repo_id` after
  -- `derived_data_type` makes that an index-only scan.
  KEY `derived_data_type_repo_id_idx` (`derived_data_type`, `repo_id`),
  -- "Repos enabled by a given campaign" (WHERE root_request_id = ?); `repo_id`
  -- last makes it an index-only scan.
  KEY `root_request_id_repo_id_idx` (`root_request_id`, `repo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
