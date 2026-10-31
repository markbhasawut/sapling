# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License found in the LICENSE file in the root
# directory of this source tree.

  $ . "${TEST_FIXTURES}/library.sh"

client0 may read and write the repo and is in the admin group, client2 may only
read it, and client1 may do neither.
  $ cat >> "$ACL_FILE" << ACLS
  > {
  >   "repos": {
  >     "default": {
  >       "actions": {
  >         "read": ["$CLIENT0_ID_TYPE:$CLIENT0_ID_DATA", "$CLIENT2_ID_TYPE:$CLIENT2_ID_DATA"],
  >         "write": ["$CLIENT0_ID_TYPE:$CLIENT0_ID_DATA"]
  >       }
  >     }
  >   },
  >   "groups": {
  >     "admin": ["$CLIENT0_ID_TYPE:$CLIENT0_ID_DATA"]
  >   }
  > }
  > ACLS

  $ setup_common_config
  $ cd $TESTTMP
  $ testtool_drawdag -R repo << EOF
  > A
  > # bookmark: A master_bookmark
  > EOF
  A=aa53d24251ff3f54b1b2c29ae02826701b2abeb0079f1bb13b8434b54cd87675
  $ start_and_wait_for_mononoke_server
  $ hg clone -q mono:repo repo

  $ METRICS="https://localhost:$MONONOKE_SOCKET/metrics"

The admin group gets every stat of the server in the Prometheus text format. It
needs no X-Client-Info header:
  $ sslcurl_noclientinfo_test --cert "$TEST_CERTDIR/client0.crt" --key "$TEST_CERTDIR/client0.key" -s -o metrics -w "%{http_code} %{content_type}\n" "$METRICS"
  200 text/plain; version=0.0.4; charset=utf-8
  $ grep -e '^# TYPE mononoke_sql_ext_queries_statements ' -e '^# HELP mononoke_sql_ext_queries_statements ' metrics
  # HELP mononoke_sql_ext_queries_statements mononoke.sql_ext.queries.statements
  # TYPE mononoke_sql_ext_queries_statements summary
  $ awk '$1 == "mononoke_sql_ext_queries_statements_count" && $2 > 0 { print "statements counted" }' metrics
  statements counted
  $ grep -c '^mononoke_process_uptime_seconds [0-9]*$' metrics
  1

Every sample line is a name, an optional quantile, and a value:
  $ grep -v '^#' metrics | grep -Evc '^[A-Za-z_][A-Za-z0-9_]*(\{quantile="0\.(5|95|99)"\})? (NaN|-?[0-9]+)$'
  0
  [1]

Everyone else gets 403: a reader, a client without access, and the proxy, which
is judged by its own certificate:
  $ for client in client2 client1 proxy; do sslcurlas $client -s -o /dev/null -w "$client %{http_code}\n" "$METRICS"; done
  client2 403
  client1 403
  proxy 403

Only GET is served:
  $ sslcurlas client0 -s -o /dev/null -w "%{http_code}\n" -X POST "$METRICS"
  405
