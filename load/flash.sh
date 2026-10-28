#!/usr/bin/env bash
# Steps 37-38: N users (default 10,000; the design target is 100,000) arrive within
# ARRIVAL_SECS (10) of the sale opening and try to buy one of UNITS (500) units through
# NGINX and three Order Service replicas. PAY_RATIO of the winners pay, a few pay too
# late, the rest abandon. Afterwards the script waits for the system to settle and asserts
# the invariants (PASS/FAIL per check, non-zero exit on any FAIL) and appends a report
# to load/RESULTS.md.
#
# Needs the repository's docker compose (postgres, redis, kafka start if they are not
# up). It rebuilds and starts the app profile with test settings:
#   PAYMENT_WINDOW_SECS=30 (so abandoned orders expire within the run) and
#   GATEWAY_TRUST_XFF=1 (each simulated user gets its own X-Forwarded-For address).
# At the end it brings the app profile back up with the default settings, the gateway
# no longer trusting X-Forwarded-For. Do not run `cargo test` while the app is up.
#
# Usage: load/flash.sh [N]     (UNITS, PAY_RATIO, LATE_RATIO, ARRIVAL_SECS, SESSION_SAMPLE,
#                              RACE_RATIO, PAYMENT_WINDOW_SECS, START_IN_SECS can be overridden)
set -euo pipefail
cd "$(dirname "$0")/.."
source load/lib.sh

N=${1:-10000}
UNITS=${UNITS:-500}
export PAYMENT_WINDOW_SECS=${PAYMENT_WINDOW_SECS:-30}
export GATEWAY_TRUST_XFF=1
export PAY_RATIO=${PAY_RATIO:-0.6} LATE_RATIO=${LATE_RATIO:-0.05} RACE_RATIO=${RACE_RATIO:-0.05} ARRIVAL_SECS=${ARRIVAL_SECS:-10}
export POLL_MS=${POLL_MS:-2000} SESSION_SAMPLE=${SESSION_SAMPLE:-50} DEADLINE_SECS=${DEADLINE_SECS:-150}
START_IN=${START_IN_SECS:-25}
trap restore_defaults EXIT

echo "Building the load generator"
cargo build -q --release --manifest-path load/flash/Cargo.toml
start_stack

echo "Inserting $N users and creating a sale of $UNITS units opening in ${START_IN}s"
insert_users "$N" "$WORK/users"
SALE=$(create_sale "$UNITS" "$START_IN")
echo "Sale $SALE"

wait_ports_free
echo "Running the generator"
SALE_ID=$SALE USERS_FILE=$WORK/users JWT_SECRET=$JWT_SECRET BASE_URL=$BASE_URL REPORT_JSON=$WORK/report.json \
  ./load/flash/target/release/flash-load | tee "$WORK/report.txt"

echo "Waiting for the system to settle"
CONVERGED=0
wait_converged "$SALE" $((PAYMENT_WINDOW_SECS + 120)) && CONVERGED=1

# ---- assertions ----------------------------------------------------------
echo
j() { jq -r "$1" "$WORK/report.json"; }
CONFIRMED=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $SALE AND status = 'CONFIRMED'")
REFUNDED=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $SALE AND status = 'REFUNDED'")
BY_STATUS=$(psql_ "SELECT coalesce(string_agg(status || ' ' || n, ', '), 'none') FROM (SELECT status::text, count(*) AS n FROM orders WHERE product_id = $SALE GROUP BY status ORDER BY status) s")
P_SOLD=$(psql_ "SELECT sold FROM products WHERE id = $SALE")
R_STOCK=$(rcli GET "stock:{$SALE}")
FAILED=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $SALE AND status = 'FAILED'")
DOUBLE=$(psql_ "SELECT count(*) FROM (SELECT user_id FROM orders WHERE product_id = $SALE AND status IN ('AWAITING_PAYMENT','CONFIRMED') GROUP BY user_id HAVING count(*) > 1) s")
LIVE=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $SALE AND status = 'AWAITING_PAYMENT'")
IN_TIME=$(j .payments.in_time); BOUNDARY=$(j .payments.boundary); LATE=$(j .payments.late)

check_cond "the system settled (no AWAITING_PAYMENT orders, inbox and outbox drained)" "converged=$CONVERGED, orders: $BY_STATUS" $((1 - CONVERGED))
check_cond "CONFIRMED orders equal the payments that arrived in time, and are at most $UNITS" \
  "CONFIRMED $CONFIRMED; payments surely in time $IN_TIME, at the boundary $BOUNDARY" \
  $([ "$CONFIRMED" -ge "$IN_TIME" ] && [ "$CONFIRMED" -le $((IN_TIME + BOUNDARY)) ] && [ "$CONFIRMED" -le "$UNITS" ] && echo 0 || echo 1)
check "every payment surely in time ended CONFIRMED (generator's own polling)" "$(j .payments.in_time_not_confirmed)" 0
check "every payment accepted after the link expired ended REFUNDED (generator's own polling; $LATE such)" "$(j .payments.late_not_refunded)" 0
check "every payment at the boundary of the expiry ended CONFIRMED or REFUNDED (never lost; $BOUNDARY such)" "$(j .payments.boundary_neither_confirmed_nor_refunded)" 0
check "CONFIRMED orders equal users the generator saw CONFIRMED" "$CONFIRMED" "$(j '.outcomes.confirmed // 0')"
check "products.sold equals CONFIRMED orders" "$P_SOLD" "$CONFIRMED"
check "users with two live orders" "$DOUBLE" 0
check "orders still AWAITING_PAYMENT" "$LIVE" 0
check "stock:{sale} equals $UNITS minus products.sold (every unit of an expired order returned once, none of a confirmed one)" "$R_STOCK" "$((UNITS - P_SOLD))"
check "FAILED orders (no publish failed)" "$FAILED" 0
check "server errors (5xx) in the generator's samples" "$(j .server_errors_5xx)" 0
check "network failures in the generator's samples" "$(j .network_failures)" 0
RATE_LIMITED=$(j .rate_limited_429)
echo "INFO  turned away by rate limits (429): $RATE_LIMITED"

{
  echo
  echo "## $(date -u +'%F %H:%M') UTC: flash sale, $N users, $UNITS units"
  echo
  echo "Users arrive uniformly within ${ARRIVAL_SECS} s of the sale opening; pay ratio $PAY_RATIO, of which ${LATE_RATIO} pay only after the link expired and ${RACE_RATIO} pay 150 ms before it, racing the expiration worker; poll every ${POLL_MS} ms; payment window ${PAYMENT_WINDOW_SECS} s; ${SESSION_SAMPLE} users got their token from POST /v1/session, the rest were signed locally. Everything on one machine ($(nproc) cores): the generator, NGINX, three Order Service replicas, one order processor, one expiration worker, PostgreSQL, Redis and Kafka in Docker. Each user has its own X-Forwarded-For (gateway started with GATEWAY_TRUST_XFF=1). Real load for 100,000 users is the same script with N=100000 (see load/README.md)."
  echo
  sed -n '/^Finished/,/^JSON summary/p' "$WORK/report.txt" | grep -v '^JSON summary'
  echo
  echo "Orders after settling: ${BY_STATUS}. Requests turned away by rate limits (429): ${RATE_LIMITED} (the generator waited \`Retry-After\` and retried)."
  echo
  echo "$CHECKS"
} >> load/RESULTS.md
echo "Appended to load/RESULTS.md"
[ "$FAIL" = 0 ] && echo "ALL CHECKS PASSED" || echo "SOME CHECKS FAILED"
exit $FAIL
