# Copyright (c) Facebook, Inc. and its affiliates.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License found in the LICENSE file in the root
# directory of this source tree.

  $ . "${TEST_FIXTURES}/library.sh"
  $ ENABLE_PRESERVE_BUNDLE2=1 BLOB_TYPE="blob_files" quiet default_setup

Set up script to output the raw bundle. This doesn't look at its arguments at all

  $ BUNDLE_PATH="$(realpath "${TESTTMP}/bundle")"
  $ BUNDLE_HELPER="$(realpath "${TESTTMP}/bundle_helper.sh")"
  $ cat > "$BUNDLE_HELPER" <<EOF
  > #!/bin/bash
  > cat "$BUNDLE_PATH"
  > EOF
  $ chmod +x "$BUNDLE_HELPER"

Pushrebase commit

  $ hg up -q 0
  $ echo "foo" > foo
  $ hg commit -Aqm "add foo"
  $ echo "bar" > bar
  $ hg commit -Aqm "add bar"
  $ hg log -r 0::. -T '{node}\n'
  426bada5c67598ca65036d57d9e4b64b0c1ce7a0
  4afe8a7fa62cf8320c8c11191d4dfdaaed9fb28b
  461b7a0d0ccf85d1168e2ae1be2a85af1ad62826
  $ quiet hgmn push -r . --to master_bookmark
  $ hg log -r ::master_bookmark -T '{node}\n'
  426bada5c67598ca65036d57d9e4b64b0c1ce7a0
  112478962961147124edd43549aedd1a335e44bf
  26805aba1e600a82e93661149f2313866a221a7b
  7d506888e440e3cd874b8973d641c29ac6a0c8ea
  c111c12cf96da82524957a6fbf4ab2d92ef48dad

Check bookmark history

  $ mononoke_admin bookmarks log -c hg master_bookmark
  * using repo "repo" repoid RepositoryId(0) (glob)
  (master_bookmark) c111c12cf96da82524957a6fbf4ab2d92ef48dad pushrebase * (glob)
  (master_bookmark) 26805aba1e600a82e93661149f2313866a221a7b blobimport * (glob)

Export the bundle so we can replay it as it if were coming from hg, through the $BUNDLE_HELPER

  $ quiet mononoke_admin hg-sync-bundle fetch-bundle --id 2 --output-file "$BUNDLE_PATH"

Blow everything away: we're going to re-do the push from scratch, in a new repo.

  $ kill -9 "$MONONOKE_PID"
  $ rm -rf "$TESTTMP/mononoke-config" "$TESTTMP/monsql" "$TESTTMP/blobstore"
  $ BLOB_TYPE="blob_files" quiet default_setup

Replay the push. This will fail because the entry does not exist (we need run this once to create the schema).

  $ unbundle_replay hg-recording "$BUNDLE_HELPER" 1
  * using repo "repo" repoid RepositoryId(0) (glob)
  * Execution error: Entry with id 1 does not exist (glob)
  Error: Execution failed
  [1]

Insert the entry. Note that in tests, the commit timestamp will always be zero.

  $ sqlite3 "$TESTTMP/monsql/sqlite_dbs" << EOS
  > INSERT INTO pushrebaserecording(repo_id, onto, ontorev, bundlehandle, timestamps, ordered_added_revs) VALUES (
  >   0,
  >   'master_bookmark',
  >   '26805aba1e600a82e93661149f2313866a221a7b',
  >   'handle123',
  >   '{"4afe8a7fa62cf8320c8c11191d4dfdaaed9fb28b": [0.0, 0], "461b7a0d0ccf85d1168e2ae1be2a85af1ad62826": [0.0, 0]}',
  >   '["7d506888e440e3cd874b8973d641c29ac6a0c8ea", "c111c12cf96da82524957a6fbf4ab2d92ef48dad"]'
  > );
  > EOS

Replay the push. It will succeed now

  $ quiet unbundle_replay hg-recording "$BUNDLE_HELPER" 1

Check history again. We're back to where we were:

  $ mononoke_admin bookmarks log -c hg master_bookmark
  * using repo "repo" repoid RepositoryId(0) (glob)
  (master_bookmark) c111c12cf96da82524957a6fbf4ab2d92ef48dad pushrebase * (glob)
  (master_bookmark) 26805aba1e600a82e93661149f2313866a221a7b blobimport * (glob)
