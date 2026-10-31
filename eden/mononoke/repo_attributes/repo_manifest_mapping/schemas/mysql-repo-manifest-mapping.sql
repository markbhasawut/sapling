/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

-- MySQL (InnoDB) version of sqlite-repo-manifest-mapping.sql. Mononoke creates
-- tables only on SQLite: apply this file to the database that the repo's
-- `oss_remote` metadata config names. Applying it again changes nothing.
--
-- Bidirectional `(repo_name, repo_branch) <-> (manifest_repo_id, manifest_branch)`
-- membership projection. "manifest" here means an AOSP/west-style repo-manifest
-- (a `default.xml` listing member repos and their branches), NOT a Mononoke
-- derived-data manifest.
--
-- Keys are git ref names stored as raw bytes so comparisons are byte-exact
-- (i.e. CASE-SENSITIVE). The primary key is 769 bytes, under InnoDB's 3072.
-- `reverse_idx` stays narrow: InnoDB appends the missing PK columns, so the fan-out read covers.
-- Do NOT index `(manifest_repo_id, manifest_branch)` separately -- it is the PK's leftmost prefix.
CREATE TABLE IF NOT EXISTS `repo_manifest_mapping` (
  `manifest_repo_id` INT NOT NULL,
  `manifest_branch` VARBINARY(255) NOT NULL,
  `repo_name` VARBINARY(255) NOT NULL,
  `repo_branch` VARBINARY(255) NOT NULL,
  PRIMARY KEY (`manifest_repo_id`, `manifest_branch`, `repo_name`, `repo_branch`),
  KEY `reverse_idx` (`repo_name`, `repo_branch`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
