#!/usr/bin/env bash
# Builds and runs the overselling demonstration (load/oversell_demo) and appends
# its table to load/RESULTS.md. Needs `docker compose up -d` (PostgreSQL and Redis); it
# creates and drops its own schema (oversell_demo) and uses Redis database 13, never the
# application's tables or keys. The app containers may be up or down.
#
# Usage: load/oversell_demo.sh     (BUYERS, UNITS, PG_POOL, THINK_US, LOCK_TIMEOUT_MS, ... see
#                                   the header of load/oversell_demo/src/main.rs)
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose up -d postgres redis >/dev/null
echo "Building"
cargo build -q --release --manifest-path load/oversell_demo/Cargo.toml
BIN=load/oversell_demo/target/release/oversell-demo
WORK=$(mktemp -d)

echo "Running all five strategies"
"$BIN" | tee "$WORK/main.txt"

# The same row-lock strategy again with a short lock_timeout: the tail of the queue gives up
LT=${LOCK_TIMEOUT_MS_DEMO:-20}
echo
echo "FOR UPDATE again with lock_timeout ${LT} ms"
ONLY=for_update LOCK_TIMEOUT_MS=$LT "$BIN" | tee "$WORK/lock.txt"

{
  echo
  echo "## $(date -u +'%F %H:%M') UTC: oversell demo, ${BUYERS:-5000} buyers, ${UNITS:-500} units"
  echo
  echo "Every buyer is a task that starts at the same moment; PostgreSQL strategies share a pool of ${PG_POOL:-50} connections (latency includes waiting for one). One machine, PostgreSQL 16 and Redis 7 in Docker, default isolation (READ COMMITTED). Own schema \`oversell_demo\` and Redis database 13."
  echo
  cat "$WORK/main.txt"
  echo
  echo "With a short lock timeout:"
  echo
  sed -n '3,$p' "$WORK/lock.txt" | sed '/^$/q'
} >> load/RESULTS.md
echo "Appended to load/RESULTS.md"
