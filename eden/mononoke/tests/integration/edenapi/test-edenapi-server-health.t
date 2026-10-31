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
  $ start_and_wait_for_mononoke_server --shutdown-grace-period 60
  $ hg clone -q mono:repo repo

  $ HEALTH="https://localhost:$MONONOKE_SOCKET/edenapi/repo/server_health"
  $ SUMMARY='{schema, repo, status, redacted, checks: [.checks[] | select(.name == "server" or .name == "metadata_db") | [.name, .status, .detail]], sql_status: (.checks[] | select(.name == "sql_queries") | .status), metadata_db, server: (.server | {exiting, metrics_available, uptime: (.uptime_secs >= 0)}), statements: (.sql.statements > 0), blobstore_operations: (.blobstore.operations > 0), bookmark_cache}'

An admin sees the whole report. The clone ran statements and read the sqlblob
blobstore, which counts its operations; the local SQLite metadata has no MySQL
cluster to report on, and the warm bookmark cache is off in tests:
  $ sslcurlas client0 -s "$HEALTH" | /usr/bin/jq -c "$SUMMARY"
  {"schema":1,"repo":"repo","status":"ok","redacted":false,"checks":[["server","ok","serving"],["metadata_db","ok","local SQLite database"]],"sql_status":"ok","metadata_db":{"kind":"local","path":"$TESTTMP/monsql","clusters":[]},"server":{"exiting":false,"metrics_available":true,"uptime":true},"statements":true,"blobstore_operations":true,"bookmark_cache":null}

A reader outside the admin group gets the same report, without the path. So
does SLAPI's other name:
  $ sslcurlas client2 -s "https://localhost:$MONONOKE_SOCKET/slapi/repo/server_health" | /usr/bin/jq -c "$SUMMARY"
  {"schema":1,"repo":"repo","status":"ok","redacted":true,"checks":[["server","ok","serving"],["metadata_db","ok","local SQLite database"]],"sql_status":"ok","metadata_db":{"kind":"local","path":"<redacted>","clusters":[]},"server":{"exiting":false,"metrics_available":true,"uptime":true},"statements":true,"blobstore_operations":true,"bookmark_cache":null}

Every key of schema 1 is there, in order:
  $ sslcurlas client0 -s "$HEALTH" | /usr/bin/jq -c 'keys_unsorted, (.server | keys_unsorted), (.sql | keys_unsorted), (.checks[0] | keys_unsorted)'
  ["schema","repo","status","redacted","server","checks","metadata_db","sql","blobstore","bookmark_cache"]
  ["version","build","uptime_secs","exiting","metrics_available"]
  ["window_secs","statements","transactions","failures","success_rate","retries","statement_latency_ms","checkout_latency_ms","errors_by_class"]
  ["name","status","detail"]

The route is authorized like every per-repo route: 403 without read access, 404
for a repo that does not exist. It only answers GET, and only for Mercurial:
  $ sslcurlas client1 -s -o /dev/null -w "%{http_code}\n" "$HEALTH"
  403
  $ sslcurlas client0 -s -o /dev/null -w "%{http_code}\n" "https://localhost:$MONONOKE_SOCKET/edenapi/nope/server_health"
  404
  $ sslcurlas client0 -s -o /dev/null -w "%{http_code}\n" -X POST "$HEALTH"
  405
  $ sslcurlas client0 -s -w "\n%{http_code}\n" "https://localhost:$MONONOKE_SOCKET/slapigit/repo/server_health"
  {"message":"Unsupported SaplingRemoteApi flavour","request_id":"*"} (glob)
  400

A server that is shutting down keeps serving for its grace period, and reports
itself degraded meanwhile:
  $ kill -TERM "$MONONOKE_PID"
  $ for _ in $(seq 1 100); do [ "$(sslcurl -s "https://localhost:$MONONOKE_SOCKET/health_check")" = EXITING ] && break; sleep 0.1; done
  $ sslcurlas client0 -s "$HEALTH" | /usr/bin/jq -c '{status, exiting: .server.exiting, server: .checks[0]}'
  {"status":"degraded","exiting":true,"server":{"name":"server","status":"degraded","detail":"the server is shutting down"}}
  $ kill -KILL "$MONONOKE_PID"
