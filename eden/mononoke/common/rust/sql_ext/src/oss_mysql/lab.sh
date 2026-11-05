#!/bin/bash
# The MySQL servers of the failover lab in lab.rs: three Percona servers on
# 127.0.0.1 with GTID replication and semi-sync (AFTER_SYNC):
#   a 13306 (server_id 1), b 13307 (server_id 2), c 13305 (server_id 3).
# a starts as the primary; b and c replicate from it with super_read_only=ON.
#
# Every mysqld/mysql/mysqladmin call passes --no-defaults first. Admin runs as
# root over the Unix socket. Passwords live only in mode-600 files under
# $L/secrets and reach mysql on stdin, never on a command line.
#
# Copy this script into an empty directory, the lab directory $L, and run it
# from there: $L holds the servers, their snapshot, the secrets and the
# results. Point MONONOKE_OSS_MYSQL_LAB at $L to run the lab tests. The
# sockets are $S/{a,b,c}.sock, in $L unless LAB_SOCKET_DIR names a shorter
# directory: macOS limits a socket path to 103 bytes. LAB_MYSQL_BIN and
# LAB_MYSQL_PLUGIN_DIR name Percona's bin and plugin directories, Homebrew's
# by default.
set -euo pipefail

L=$(cd "$(dirname "$0")" && pwd)
S=${LAB_SOCKET_DIR:-$L}
# "/a.sock" adds 7 bytes.
if [ $((${#S} + 7)) -gt 103 ]; then
  echo "sockets under $S would exceed 103 bytes: set LAB_SOCKET_DIR to a shorter directory" >&2
  exit 2
fi
B=${LAB_MYSQL_BIN:-/opt/homebrew/opt/percona-server/bin}
PLUGIN_DIR=${LAB_MYSQL_PLUGIN_DIR:-/opt/homebrew/opt/percona-server/lib/percona-server/plugin}
SERVERS="a b c"

usage() {
  cat >&2 <<'EOF'
usage: lab.sh COMMAND
  init                  initialise a, b, c (a primary) and snapshot them
  reset                 restore the snapshot and start a, b, c
  start X [ro [norepl]] start X, read-only, without starting replication
  stop X | kill X | pause X | cont X
                        shut down, kill -9, kill -STOP, kill -CONT
  demote X              SET GLOBAL super_read_only=ON
  promote X             STOP REPLICA; RESET REPLICA ALL; writable
  writable X            make X writable without touching replication
  repoint X Y           make X a replica of Y
  catchup X Y           wait until X executed everything Y executed
  wait-applied X        stop X's receiver and wait until it applied it all
  sql X STATEMENT       run STATEMENT on X as root
  kill-idle X           kill the idle connections of the mononoke user on X
  rotate-password X     change the mononoke password on X; the new one waits
                        in secrets/mononoke.pw.pending
  commit-password       move the pending password into secrets/mononoke.pw
  status | teardown
EOF
  exit 2
}

port() { case $1 in a) echo 13306 ;; b) echo 13307 ;; c) echo 13305 ;; *) echo "bad server $1" >&2; exit 2 ;; esac; }
sid() { case $1 in a) echo 1 ;; b) echo 2 ;; c) echo 3 ;; esac; }
sock() { echo "$S/$1.sock"; }

# Copies a data directory, as an APFS clone on macOS.
copy_tree() {
  if [ "$(uname)" = Darwin ]; then cp -cR "$1" "$2"; else cp -R --reflink=auto "$1" "$2"; fi
}
pidfile() { echo "$L/$1/mysqld.pid"; }

mysql_root() { "$B/mysql" --no-defaults -S "$(sock "$1")" -uroot --batch --skip-column-names --connect-timeout=5 "${@:2}"; }

# Runs the statements $2 on $1, given on stdin. Retries while the server
# refuses the connection with 1040: scenario I9 caps max_connections, and
# root then has a single spare connection.
admin() {
  local out
  for _ in $(seq 1 100); do
    if out=$(printf '%s\n' "$2" | mysql_root "$1" 2>&1); then
      if [ -n "$out" ]; then printf '%s\n' "$out"; fi
      return 0
    fi
    case $out in
      *"ERROR 1040"*) sleep 0.1 ;;
      *) echo "$out" >&2; return 1 ;;
    esac
  done
  echo "$out" >&2
  return 1
}

running() {
  local p
  p=$(cat "$(pidfile "$1")" 2>/dev/null) || return 1
  kill -0 "$p" 2>/dev/null
}

wait_up() {
  for _ in $(seq 1 300); do
    if "$B/mysqladmin" --no-defaults -S "$(sock "$1")" -uroot ping >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "server $1 did not come up; see $L/$1/err.log" >&2
  exit 1
}

start() {
  local x=$1 ro=${2:-}
  local extra=()
  if [ "$ro" = ro ]; then extra+=(--super-read-only=ON); fi
  if [ "${3:-}" = norepl ]; then extra+=(--skip-replica-start); fi
  if running "$x"; then echo "$x already running"; return 0; fi
  mkdir -p "$L/$x/tmp"
  "$B/mysqld" --no-defaults --datadir="$L/$x/data" --port="$(port "$x")" --bind-address=127.0.0.1 \
    --socket="$(sock "$x")" --mysqlx=OFF --server-id="$(sid "$x")" \
    --log-bin=binlog --relay-log=relay-bin --gtid-mode=ON --enforce-gtid-consistency=ON \
    --max-connections=500 --max-connect-errors=100000 --skip-name-resolve \
    --innodb-redo-log-capacity=33554432 --innodb-buffer-pool-size=134217728 \
    --plugin-dir="$PLUGIN_DIR" --plugin-load-add=semisync_source.so --plugin-load-add=semisync_replica.so \
    --loose-rpl-semi-sync-source-enabled=ON --loose-rpl-semi-sync-replica-enabled=ON \
    --loose-rpl-semi-sync-source-timeout=2000 --loose-rpl-semi-sync-source-wait-point=AFTER_SYNC \
    --general-log=ON --general-log-file="$L/$x/general.log" \
    --tmpdir="$L/$x/tmp" \
    --pid-file="$(pidfile "$x")" --log-error="$L/$x/err.log" --daemonize ${extra[@]+"${extra[@]}"} >/dev/null
  wait_up "$x"
}

stop() {
  local x=$1 p
  running "$x" || { rm -f "$(sock "$x")" "$(sock "$x").lock"; return 0; }
  p=$(cat "$(pidfile "$x")")
  kill -CONT "$p" 2>/dev/null || true
  "$B/mysqladmin" --no-defaults -S "$(sock "$x")" -uroot --connect-timeout=5 shutdown >/dev/null 2>&1 || kill -TERM "$p" 2>/dev/null || true
  for _ in $(seq 1 300); do kill -0 "$p" 2>/dev/null || break; sleep 0.1; done
  if kill -0 "$p" 2>/dev/null; then kill -9 "$p"; fi
  rm -f "$(sock "$x")" "$(sock "$x").lock"
}

signal() {
  local p
  p=$(cat "$(pidfile "$2")")
  kill "$1" "$p"
}

pw() { cat "$L/secrets/$1.pw"; }

repoint() {
  local x=$1 y=$2
  admin "$x" "STOP REPLICA;
$(printf "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=%s, SOURCE_USER='repl', SOURCE_PASSWORD='%s', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1, SOURCE_CONNECT_RETRY=1, SOURCE_RETRY_COUNT=86400;" "$(port "$y")" "$(pw repl)")
START REPLICA;"
}

catchup() {
  local gtids
  gtids=$(admin "$2" "SELECT @@GLOBAL.gtid_executed" | tr -d '\n')
  admin "$1" "SELECT WAIT_FOR_EXECUTED_GTID_SET('$gtids', 60)"
}

wait_applied() {
  local gtids
  admin "$1" "STOP REPLICA IO_THREAD" >/dev/null
  gtids=$(admin "$1" "SELECT received_transaction_set FROM performance_schema.replication_connection_status" | tr -d '\n')
  admin "$1" "SELECT WAIT_FOR_EXECUTED_GTID_SET('$gtids', 60)"
}

new_password() { python3 -I -c 'import secrets; print(secrets.token_hex(16))'; }

init() {
  if [ -e "$L/pristine" ]; then echo "already initialised; run teardown first" >&2; exit 1; fi
  if command -v git >/dev/null && git -C "$L" rev-parse --git-dir >/dev/null 2>&1; then
    echo "$L is in a git checkout: copy lab.sh into an empty directory and run it there" >&2
    exit 1
  fi
  mkdir -p "$L/secrets"
  chmod 700 "$L/secrets"
  for u in mononoke repl; do
    (umask 077; new_password > "$L/secrets/$u.pw")
  done
  for x in $SERVERS; do
    mkdir -p "$L/$x/tmp"
    "$B/mysqld" --no-defaults --initialize-insecure --datadir="$L/$x/data" --log-error="$L/$x/err.log"
  done
  start a
  start b ro
  start c ro
  admin a "CREATE DATABASE mononoke;
$(printf "CREATE USER 'mononoke'@'127.0.0.1' IDENTIFIED BY '%s' WITH MAX_USER_CONNECTIONS 450;" "$(pw mononoke)")
GRANT SELECT, INSERT, UPDATE, DELETE ON mononoke.* TO 'mononoke'@'127.0.0.1';
$(printf "CREATE USER 'repl'@'127.0.0.1' IDENTIFIED BY '%s';" "$(pw repl)")
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'127.0.0.1';
CREATE TABLE mononoke.lab_writes (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  uuid VARCHAR(64) NOT NULL,
  seq BIGINT NOT NULL,
  op INT NOT NULL,
  server_id INT UNSIGNED NOT NULL,
  origin CHAR(1) NOT NULL,
  created TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY lab_writes_uuid (uuid),
  KEY lab_writes_seq (seq)
) ENGINE=InnoDB;
CREATE TABLE mononoke.lab_txn (
  id INT NOT NULL PRIMARY KEY,
  n BIGINT NOT NULL,
  last_uuid VARCHAR(64) NOT NULL
) ENGINE=InnoDB;
INSERT INTO mononoke.lab_txn VALUES (1, 0, '');"
  repoint b a
  repoint c a
  catchup b a >/dev/null
  catchup c a >/dev/null
  for x in $SERVERS; do stop "$x"; done
  mkdir -p "$L/pristine"
  for x in $SERVERS; do copy_tree "$L/$x/data" "$L/pristine/$x"; done
  cp -p "$L/secrets/mononoke.pw" "$L/pristine/mononoke.pw"
  echo "initialised"
}

reset() {
  for x in $SERVERS; do stop "$x"; done
  for x in $SERVERS; do
    rm -rf "$L/$x/data" "$L/$x/general.log" "$L/$x/err.log"
    mkdir -p "$L/$x/tmp"
    copy_tree "$L/pristine/$x" "$L/$x/data"
  done
  # I10 rotates the password: go back to the one in the snapshot.
  cp -p "$L/pristine/mononoke.pw" "$L/secrets/mononoke.pw"
  rm -f "$L/secrets/mononoke.pw.pending"
  start a
  start b ro
  start c ro
  for x in b c; do
    for _ in $(seq 1 100); do
      st=$(admin "$x" "SELECT SERVICE_STATE FROM performance_schema.replication_connection_status" | tr -d '\n')
      [ "$st" = ON ] && break
      sleep 0.1
    done
  done
  echo "reset"
}

status() {
  for x in $SERVERS; do
    if ! running "$x"; then echo "$x: down"; continue; fi
    mysql_root "$x" --connect-timeout=2 -e "SELECT '$x', @@port, @@server_id, @@read_only, @@super_read_only, (SELECT COUNT(*) FROM mononoke.lab_writes), (SELECT GROUP_CONCAT(CONCAT(PORT, ':', SERVICE_STATE)) FROM performance_schema.replication_connection_status JOIN performance_schema.replication_connection_configuration USING (CHANNEL_NAME))" 2>&1 || echo "$x: no answer"
  done
}

kill_idle() {
  local ids
  ids=$(admin "$1" "SELECT ID FROM information_schema.PROCESSLIST WHERE USER = 'mononoke' AND COMMAND = 'Sleep'")
  for id in $ids; do admin "$1" "KILL $id" 2>/dev/null || true; done
  echo "killed $(echo "$ids" | grep -c . || true) idle connections"
}

rotate_password() {
  local new
  new=$(new_password)
  admin "$1" "$(printf "ALTER USER 'mononoke'@'127.0.0.1' IDENTIFIED BY '%s';" "$new")"
  (umask 077; printf '%s\n' "$new" > "$L/secrets/mononoke.pw.pending")
}

teardown() {
  for x in $SERVERS; do stop "$x"; done
  rm -rf "$L/a" "$L/b" "$L/c" "$L/pristine" "$L/secrets"
  rm -f "$S/a.sock" "$S/b.sock" "$S/c.sock" "$S/a.sock.lock" "$S/b.sock.lock" "$S/c.sock.lock"
  echo "torn down"
}

cmd=${1:-}
shift || true
case $cmd in
  init) init ;;
  reset) reset ;;
  start) start "$@" ;;
  stop) stop "$1" ;;
  kill) signal -9 "$1" ;;
  pause) signal -STOP "$1" ;;
  cont) signal -CONT "$1" ;;
  demote) admin "$1" "SET GLOBAL super_read_only=ON" ;;
  promote) admin "$1" "STOP REPLICA; RESET REPLICA ALL; SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF" ;;
  writable) admin "$1" "SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF" ;;
  repoint) repoint "$1" "$2" ;;
  catchup) catchup "$1" "$2" ;;
  wait-applied) wait_applied "$1" ;;
  sql) admin "$1" "$2" ;;
  kill-idle) kill_idle "$1" ;;
  rotate-password) rotate_password "$1" ;;
  commit-password) mv "$L/secrets/mononoke.pw.pending" "$L/secrets/mononoke.pw" ;;
  status) status ;;
  teardown) teardown ;;
  *) usage ;;
esac
