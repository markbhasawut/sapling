
#require no-eden


  $ eagerepo
  $ touch $TESTTMP/stub

  $ setconfig experimental.network-doctor=True paths.default=mononoke://169.254.1.2/foo
Set up fake cert paths so we don't hit "missing certs" error.
  $ setconfig auth.test.cert=$TESTTMP/stub auth.test.key=$TESTTMP/stub auth.test.priority=1 auth.test.prefix=mononoke://*

  $ sl init repo && cd repo

  $ sl pull --config edenapi.url=https://test_fail/foo --config doctor.external-host-check-url=https://test_succeed
  pulling from mononoke://169.254.1.2/foo
  abort: command failed due to network error (see * for details) (glob)
  
  Please check your VPN or proxy (internet okay, but can't reach server).
  [1]


  $ sl pull --config edenapi.url=https://test_fail/foo --config doctor.external-host-check-url=https://test_succeed --verbose
  pulling from mononoke://169.254.1.2/foo
  abort: command failed due to network error (see * for details) (glob)
  
  Please check your VPN or proxy (internet okay, but can't reach server).
    no server connectivity: TCP error: test
  [1]


  $ sl pull --config edenapi.url=https://test_fail/foo --config doctor.external-host-check-url=https://test_succeed --debug
  pulling from mononoke://169.254.1.2/foo
  abort: command failed due to network error (see * for details) (glob)
  
  Please check your VPN or proxy (internet okay, but can't reach server).
    no server connectivity: TCP error: test
  
  Original error:
  \[6\] (Could not|Couldn't) resolve (hostname|host name) \(Could not resolve host: test_fail\) (re)
  [1]


Works for native rust commands as well.
  $ sl clone mononoke://169.254.1.2/banana --config commands.force-rust=clone --config edenapi.url=https://test_fail/foo --config doctor.external-host-check-url=https://test_succeed
  Cloning banana into $TESTTMP/repo/banana
  abort: command failed due to network error
  
  Please check your VPN or proxy (internet okay, but can't reach server).
  
  Details:
  
  NoServer(TCP(Custom { kind: Other, error: "test" }))
  
  Original error:
  
  Network Error: \[6\] (Could not|Couldn't) resolve (hostname|host name) \(Could not resolve host: test_fail\) (re)
  
  Caused by:
      \[6\] (Could not|Couldn't) resolve (hostname|host name) \(Could not resolve host: test_fail\) (re)
  
  [255]


The server is reachable and answers capabilities, so the doctor asks for its
health. A degraded or down server is reported as a problem on the server.

  $ cat > $TESTTMP/health.json << 'EOF'
  > {"schema": 1, "repo": "foo", "status": "degraded", "redacted": true, "checks": [
  >   {"name": "server", "status": "ok", "detail": "serving"},
  >   {"name": "metadata_db", "status": "degraded", "detail": "cluster 1 (identity): a new primary awaits confirmation"}
  > ]}
  > EOF
  $ sl debugpython -- "$TESTDIR/serverhealthhttp.py" --port-file $TESTTMP/health.port --pid $TESTTMP/health.pid --health-file $TESTTMP/health.json
  $ cat $TESTTMP/health.pid >> $DAEMON_PIDS
  $ HEALTH_URL=http://localhost:`cat $TESTTMP/health.port`/edenapi/

  $ sl debugnetworkdoctor --config edenapi.url=$HEALTH_URL --config remotefilelog.reponame=foo
  The Mononoke server reports degraded health. The problem is on the server, not with your network or configuration: retry later, and contact the server's administrators if it persists.
  
  server reports degraded health: metadata_db: cluster 1 (identity): a new primary awaits confirmation
  [1]

  $ sl debugnetworkdoctor --config edenapi.url=$HEALTH_URL --config remotefilelog.reponame=foo --config doctor.server-health-check=false
  No network problems detected.

The fake server fails other requests, so commands that hit a network error show
the same treatment.

  $ sl pull --config edenapi.url=$HEALTH_URL --config remotefilelog.reponame=foo --verbose
  pulling from mononoke://169.254.1.2/foo
  abort: command failed due to network error (see * for details) (glob)
  
  The Mononoke server reports degraded health. The problem is on the server, not with your network or configuration: retry later, and contact the server's administrators if it persists.
    server reports degraded health: metadata_db: cluster 1 (identity): a new primary awaits confirmation
  [1]

A slow answer is waited for up to doctor.server-health-timeout (2s by default),
then ignored.

  $ cat > $TESTTMP/health.json << 'EOF'
  > delay 0.5
  > {"status": "down", "checks": [{"name": "server", "status": "down", "detail": "slow"}]}
  > EOF
  $ sl debugnetworkdoctor --config edenapi.url=$HEALTH_URL --config remotefilelog.reponame=foo
  The Mononoke server reports down health. The problem is on the server, not with your network or configuration: retry later, and contact the server's administrators if it persists.
  
  server reports down health: server: slow
  [1]
  $ sl debugnetworkdoctor --config edenapi.url=$HEALTH_URL --config remotefilelog.reponame=foo --config doctor.server-health-timeout=100ms
  No network problems detected.

Servers without the route answer 404.

  $ echo 404 > $TESTTMP/health.json
  $ sl debugnetworkdoctor --config edenapi.url=$HEALTH_URL --config remotefilelog.reponame=foo
  No network problems detected.

  $ echo '{"status": "ok", "checks": []}' > $TESTTMP/health.json
  $ sl debugnetworkdoctor --config edenapi.url=$HEALTH_URL --config remotefilelog.reponame=foo
  No network problems detected.

  $ $PYTHON $TESTDIR/killdaemons.py $DAEMON_PIDS

A repo with no server, made with init or cloned from a local path, has no
network to check:

  $ sl debugnetworkdoctor --config paths.default= --config edenapi.url=
  No server configured for this repo: there is no network to check.
  $ sl debugnetworkdoctor --config paths.default=$TESTTMP/elsewhere --config edenapi.url=
  No server configured for this repo: there is no network to check.

A server without edenapi.url is still a configuration problem:

  $ sl debugnetworkdoctor --config edenapi.url= | head -1
  Invalid config: no config for edenapi.url
