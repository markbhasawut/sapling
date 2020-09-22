/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

CREATE TABLE `synced_commit_mapping` (
  `mapping_id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  `small_repo_id` int(11) NOT NULL,
  `small_bcs_id` binary(32) NOT NULL,
  `large_repo_id` int(11) NOT NULL,
  `large_bcs_id` binary(32) NOT NULL,
  `sync_map_version_name` varchar(255),
  UNIQUE (`small_repo_id`,`large_repo_id`,`large_bcs_id`)
);

CREATE TABLE `synced_working_copy_equivalence` (
  `mapping_id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  `small_repo_id` int(11) NOT NULL,
  `small_bcs_id` binary(32),
  `large_repo_id` int(11) NOT NULL,
  `large_bcs_id` binary(32) NOT NULL,
   UNIQUE (`large_repo_id`,`small_repo_id`,`large_bcs_id`)
);

 -- Small bcs id can map to multiple large bcs ids
 CREATE INDEX small_bcs_key ON synced_working_copy_equivalence
  (`large_repo_id`,`small_repo_id`,`small_bcs_id`);
