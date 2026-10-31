/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) schema of `streaming_changelog_chunks`, which streaming
-- clone reads when a client asks for a streaming clone. SQLite gets it from
-- the CREATION_QUERY in src/lib.rs (there is no sqlite-*.sql file); Mononoke
-- creates tables only on SQLite, so apply this file to the database that the
-- repo's `oss_remote` metadata config names. Applying it again changes
-- nothing. Columns follow that CREATION_QUERY: tags and blob names are &str
-- compared byte for byte, chunk numbers and sizes are u32/i32.

CREATE TABLE IF NOT EXISTS `streaming_changelog_chunks` (
  `repo_id` INT NOT NULL,
  `tag` VARBINARY(100) NOT NULL DEFAULT '',
  `chunk_num` INT NOT NULL,
  `idx_blob_name` VARBINARY(4096) NOT NULL,
  `idx_size` INT NOT NULL,
  `data_blob_name` VARBINARY(4096) NOT NULL,
  `data_size` INT NOT NULL,
  PRIMARY KEY (`repo_id`, `tag`, `chunk_num`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
