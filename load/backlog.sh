#!/usr/bin/env bash
# Step 39: the backlog test. The order processors are throttled (PROCESSOR_MAX_PER_SEC) so
# the checkout queue in Kafka builds a real backlog: UNITS (default 10,000) units, N users
# (default 10,000) all checking out within ARRIVAL_SECS (10) of the sale opening, the
# processor starting RATE (default 200) messages per second. The last link then appears
# about N / RATE seconds after the first (50 s), longer than the payment window (30 s), so a
# user who waited in the queue longer than the window shows that the payment timer does not
# run while an order is queued: the deadline is set by the processor when it creates the order. The roadmap's design point is the same script with
# N=UNITS=100000 and RATE=1000.
#
# Asserts (PASS/FAIL per check, non-zero exit on a FAIL, report appended to load/RESULTS.md):
#   - nothing waiting in Kafka was failed or expired: no FAILED orders, no EXPIRED order without a
#     payment link, every user got an order (a queued ticket has no order and no deadline at all);
#   - every user who got a link got the full payment window (the time left to pay when the
#     generator first saw the link is the window minus at most one polling interval), and
#     the users who pay late in that window (after waiting longer than the window in the
#     queue) are all CONFIRMED;
#   - the backlog was real (some link came later than the window after checkout);
#   - the usual consistency checks (products.sold = CONFIRMED orders, one order per user, stock, no 5xx).
#
# Starts the app profile with PAYMENT_WINDOW_SECS=30, PROCESSOR_MAX_PER_SEC=$RATE and
# GATEWAY_TRUST_XFF=1, and restores the defaults at the end. Do not run `cargo test` while
# the app is up.
#
# Usage: load/backlog.sh [N]    (UNITS, RATE, PAYMENT_WINDOW_SECS, PAY_RATIO overridable)
set -euo pipefail
cd "$(dirname "$0")/.."
source load/lib.sh

N=${1:-10000}
UNITS=${UNITS:-$N}
RATE=${RATE:-200}
export PAYMENT_WINDOW_SECS=${PAYMENT_WINDOW_SECS:-30}
export PROCESSOR_MAX_PER_SEC=$RATE GATEWAY_TRUST_XFF=1
export PAY_RATIO=${PAY_RATIO:-0.5} LATE_RATIO=0 RACE_RATIO=0 ARRIVAL_SECS=${ARRIVAL_SECS:-10}
export POLL_MS=${POLL_MS:-2000} SESSION_SAMPLE=${SESSION_SAMPLE:-0} VIEW_SALE=0
# Payers decide late in the window: 55-70 % of it, after the link appeared
export PAY_THINK_MIN_MS=$((PAYMENT_WINDOW_SECS * 550)) PAY_THINK_MAX_MS=$((PAYMENT_WINDOW_SECS * 700))
EXPECTED_DRAIN=$((N / RATE))
export DEADLINE_SECS=${DEADLINE_SECS:-$((EXPECTED_DRAIN * 3 + PAYMENT_WINDOW_SECS + 60))}
trap restore_defaults EXIT

echo "Building the load generator"
cargo build -q --release --manifest-path load/flash/Cargo.toml
start_stack

echo "Inserting $N users and creating a sale of $UNITS units; the queue drains at $RATE/s (about ${EXPECTED_DRAIN}s)"
insert_users "$N" "$WORK/users"
SALE=$(create_sale "$UNITS" 25)
echo "Sale $SALE"

wait_ports_free
target_urls
echo "Running the generator"
run_generator

echo "Waiting for the system to settle"
CONVERGED=0
wait_converged "$SALE" $((PAYMENT_WINDOW_SECS + 180)) && CONVERGED=1

echo
j() { jq -r "$1" "$WORK/report.json"; }
CONFIRMED=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $SALE AND status = 'CONFIRMED'")
ORDERS=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $SALE")
FAILED_ORDERS=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $SALE AND status = 'FAILED'")
UNLINKED=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $SALE AND status IN ('EXPIRED','FAILED') AND checkout_url IS NULL")
LIVE=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $SALE AND status = 'AWAITING_PAYMENT'")
BY_STATUS=$(psql_ "SELECT coalesce(string_agg(status || ' ' || n, ', '), 'none') FROM (SELECT status::text, count(*) AS n FROM orders WHERE product_id = $SALE GROUP BY status ORDER BY status) s")
P_SOLD=$(psql_ "SELECT sold FROM products WHERE id = $SALE")
R_STOCK=$(rcli GET "stock:{$SALE}")
DOUBLE=$(psql_ "SELECT count(*) FROM (SELECT user_id FROM orders WHERE product_id = $SALE AND status IN ('AWAITING_PAYMENT','CONFIRMED') GROUP BY user_id HAVING count(*) > 1) s")
MAX_LINK=$(j .checkout_to_link_ms.max); MIN_LEFT=$(j .min_window_left_ms)
WINDOW_MS=$((PAYMENT_WINDOW_SECS * 1000))
SLACK=$((POLL_MS * 12 / 10 + 1000))   # one polling interval with jitter, plus a second

check_cond "the system settled" "converged=$CONVERGED, orders: $BY_STATUS" $((1 - CONVERGED))
check "every user got an order (none dropped, none left in the queue)" "$ORDERS" "$N"
check "FAILED orders (none: no publish failed, nothing queued was given up)" "$FAILED_ORDERS" 0
check "EXPIRED or FAILED orders that never had a payment link" "$UNLINKED" 0
check "generator outcomes failed, timeout or error" "$(j '[.outcomes.failed // 0, .outcomes.timeout // 0, .outcomes.error // 0, .outcomes.sold_out // 0, .outcomes.conflict // 0] | add')" 0
check_cond "the backlog was real: the last link came after more than the payment window" \
  "slowest checkout to link ${MAX_LINK} ms, window ${WINDOW_MS} ms" $([ "$MAX_LINK" -gt "$WINDOW_MS" ] && echo 0 || echo 1)
check_cond "every user got the full payment window (time left to pay when the link was first seen at least window - ${SLACK} ms)" \
  "least time left ${MIN_LEFT} ms of ${WINDOW_MS} ms" $([ "$MIN_LEFT" != null ] && [ "$MIN_LEFT" -ge $((WINDOW_MS - SLACK)) ] && echo 0 || echo 1)
check "payers who paid in the last half of the window all ended CONFIRMED (in time but not CONFIRMED)" "$(j .payments.in_time_not_confirmed)" 0
check "CONFIRMED orders equal payments accepted in time (the generator pays only in time here)" "$CONFIRMED" "$(( $(j .payments.in_time) + $(j .payments.boundary) ))"
check "products.sold equals CONFIRMED orders" "$P_SOLD" "$CONFIRMED"
check "users with two live orders" "$DOUBLE" 0
check "orders still AWAITING_PAYMENT" "$LIVE" 0
check "stock:{sale} equals $UNITS minus products.sold (every unit of an expired order returned once)" "$R_STOCK" "$((UNITS - P_SOLD))"
check "server errors (5xx) in the generator's samples" "$(j .server_errors_5xx)" 0
check "network failures in the generator's samples" "$(j .network_failures)" 0
echo "INFO  turned away by rate limits (429): $(j .rate_limited_429)"

{
  echo
  echo "## $(date -u +'%F %H:%M') UTC: backlog, $N users, $UNITS units, processor throttled to $RATE/s"
  echo
  echo "All users check out within ${ARRIVAL_SECS} s of the sale opening; the processor starts $RATE messages per second, so the queue needs about ${EXPECTED_DRAIN} s to drain. Payment window ${PAYMENT_WINDOW_SECS} s; ${PAY_RATIO} of the users pay, 55-70 % of the window after their link appeared; poll every ${POLL_MS} ms. One machine, one order processor, three replicas behind NGINX (gateway started with GATEWAY_TRUST_XFF=1). The design point is N=UNITS=100000 at 1,000/s (same script)."
  echo
  setup_note
  echo
  sed -n '/^Finished/,/^JSON summary/p' "$WORK/report.txt" | grep -v '^JSON summary'
  echo
  echo "Orders after settling: ${BY_STATUS}."
  echo
  echo "$CHECKS"
} >> load/RESULTS.md
echo "Appended to load/RESULTS.md"
[ "$FAIL" = 0 ] && echo "ALL CHECKS PASSED" || echo "SOME CHECKS FAILED"
exit $FAIL
