# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License found in the LICENSE file in the root
# directory of this source tree.

  $ . "${TEST_FIXTURES}/library.sh"

  $ setup_common_config
  $ cd $TESTTMP
  $ testtool_drawdag -R repo << EOF
  > A
  > # bookmark: A master_bookmark
  > EOF
  A=aa53d24251ff3f54b1b2c29ae02826701b2abeb0079f1bb13b8434b54cd87675
  $ start_and_wait_for_mononoke_server --shutdown-grace-period 60
  $ hg clone -q mono:repo repo
  $ cd repo

The network doctor asks the server for its health once it answers, and a
healthy server is no problem:
  $ hg debugnetworkdoctor
  No network problems detected.

A server that is shutting down keeps serving for its grace period, and reports
itself degraded meanwhile. The doctor says the problem is on the server:
  $ kill -TERM "$MONONOKE_PID"
  $ for _ in $(seq 1 100); do [ "$(sslcurl -s "https://localhost:$MONONOKE_SOCKET/health_check")" = EXITING ] && break; sleep 0.1; done
  $ hg debugnetworkdoctor
  The Mononoke server reports degraded health. The problem is on the server, not with your network or configuration: retry later, and contact the server's administrators if it persists.
  
  server reports degraded health: server: the server is shutting down
  [1]

The step can be turned off:
  $ hg debugnetworkdoctor --config doctor.server-health-check=false
  No network problems detected.
  $ kill -KILL "$MONONOKE_PID"
