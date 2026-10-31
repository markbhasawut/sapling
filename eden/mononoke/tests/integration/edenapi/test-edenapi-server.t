# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License found in the LICENSE file in the root
# directory of this source tree.

  $ . "${TEST_FIXTURES}/library.sh"

Start up SaplingRemoteAPI server.
  $ setup_mononoke_config
  $ start_and_wait_for_mononoke_server
List repos.
  $ sslcurlas client0 -s "https://localhost:$MONONOKE_SOCKET/edenapi/repos"
  {"repos":["repo"]} (no-eol)
The list only has the repos the caller may read; the proxy identity is in no ACL.
  $ sslcurl -s "https://localhost:$MONONOKE_SOCKET/edenapi/repos"
  {"repos":[]} (no-eol)
Test request with a missing mandatory header
  $ sslcurl_noclientinfo_test -s "https://localhost:$MONONOKE_SOCKET/edenapi/repos"
  {"message:"Error: X-Client-Info header not provided or wrong format (expected json)."} (no-eol)
Test that health check request still passes
  $ sslcurl_noclientinfo_test -s "https://localhost:$MONONOKE_SOCKET/edenapi/health_check"
  I_AM_ALIVE (no-eol)
