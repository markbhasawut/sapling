# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License found in the LICENSE file in the root
# directory of this source tree.

The OSS build of SCS serving mutual TLS: callers are identified by their
certificates and checked against the repo's ACL, as SLAPI's are. Buck builds
serve TLS through srserver, without these flags.

#require no-bucktest

  $ . "${TEST_FIXTURES}/library.sh"
  $ setup_common_config
  $ testtool_drawdag -R repo --no-default-files <<'EOF'
  > A
  > # bookmark: A main
  > # modify: A file "content"
  > EOF
  A=* (glob)

The server's certificate names localhost, so clients dial that name.
  $ function scsc_tls_as {
  >   local name="$1"; shift
  >   "$SCS_CLIENT" --host "localhost:$SCS_PORT" --tls-ca "$TEST_CERTDIR/root-ca.crt" \
  >     --tls-certificate "$TEST_CERTDIR/$name.crt" --tls-private-key "$TEST_CERTDIR/$name.key" "$@"
  > }
  $ scs --tls-ca "$TEST_CERTDIR/root-ca.crt" --tls-certificate "$TEST_CERTDIR/localhost.crt" \
  >   --tls-private-key "$TEST_CERTDIR/localhost.key"
  $ wait_for_server "SCS server" SCS_PORT "$TESTTMP/scs_server.out" 120 \
  >   "$TESTTMP/scs_server_addr.txt" scsc_tls_as client0 repos

client0 is in the repo's ACL.
  $ scsc_tls_as client0 repos
  repo
  $ scsc_tls_as client0 cat -R repo -B main -p file
  content (no-eol)

proxy is in no ACL.
  $ scsc_tls_as proxy cat -R repo -B main -p file
  *: error: SourceControlService::repo_resolve_bookmark failed with request_error(RequestError): RequestError: permission denied: Repo metadata read access in repo 'repo' is not permitted with Identity for [X509_SUBJECT_NAME:CN=proxy,O=Mononoke,C=US,ST=CA] (glob)
  [1]

Plaintext isn't served.
  $ "$SCS_CLIENT" --host "$LOCALIP:$SCS_PORT" repos
  *: error: SourceControlService::list_repos failed with ThriftError: thrift call /SourceControlService/SourceControlService.list_repos to $LOCALIP:$LOCAL_PORT failed: $LOCALIP:$LOCAL_PORT did not accept the connection: * (glob)
  [1]

A certificate for localhost isn't one for its address.
  $ "$SCS_CLIENT" --host "$LOCALIP:$SCS_PORT" --tls-ca "$TEST_CERTDIR/root-ca.crt" \
  >   --tls-certificate "$TEST_CERTDIR/client0.crt" --tls-private-key "$TEST_CERTDIR/client0.key" repos
  *: error: SourceControlService::list_repos failed with ThriftError: thrift call /SourceControlService/SourceControlService.list_repos to $LOCALIP:$LOCAL_PORT failed: TLS handshake with $LOCALIP failed: *certificate verify failed* (glob)
  [1]
