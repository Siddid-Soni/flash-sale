#!/usr/bin/env bash
# Step 8.3: N users (default 10,000) try to buy one of 100 units; admitted
# users reserve a unit, PAY_RATIO of them pay and the rest walk away. Then
# checks the outcome in Postgres and Redis and appends a report to
# load/RESULTS.md.
#
# Needs `docker compose up -d` (Postgres, Redis, Kafka). Runs release builds
# of the API and workers itself, with short holds so abandoned units come
# back quickly; stops the compose `app` containers first to free port 8080.
# REDIS_DB (default 0) picks the Redis database.
# Usage: load/final.sh [N]

set -euo pipefail
cd "$(dirname "$0")/.."

N=${1:-10000}
UNITS=100
export PAY_RATIO=${PAY_RATIO:-0.7}
export HOLD_TTL_SECS=${HOLD_TTL_SECS:-20} PAYMENT_EXTENSION_SECS=${PAYMENT_EXTENSION_SECS:-20}
export TOKEN_TTL_SECS=${TOKEN_TTL_SECS:-60} FAKE_PAYMENT_DELAY_MS=${FAKE_PAYMENT_DELAY_MS:-1000}
# Redis database to use (the binaries started here get it through REDIS_URL)
REDIS_DB=${REDIS_DB:-0}
export REDIS_URL=redis://127.0.0.1:6379/$REDIS_DB
API=http://localhost:8080
H='Content-Type: application/json'
R=$RANDOM$RANDOM
WORK=$(mktemp -d)
PIDS=()
trap 'kill "${PIDS[@]}" 2>/dev/null || true' EXIT

psql_() { docker compose exec -T postgres psql -U actix -d flash_sale -tAc "$1" </dev/null; }
rcli() { docker compose exec -T redis redis-cli -n "$REDIS_DB" "$@" </dev/null; }

echo "Building"
cargo build -q --release --bins
cargo build -q --release --manifest-path load/final/Cargo.toml
docker compose --profile app stop api admission-worker payment-worker cleanup-worker >/dev/null 2>&1 || true

# Request logging off: it would dominate the API's work at this rate
RUST_LOG=warn ./target/release/flash-sale > "$WORK/api.log" 2>&1 & PIDS+=($!)
until curl -s -o /dev/null $API/auth/me; do sleep 0.5; done

echo "Setting up $N users and a product with $UNITS units"
# Guest rows inserted directly (no password to hash); the load generator signs their tokens itself
psql_ "WITH new_users AS (INSERT INTO users (is_guest) SELECT true FROM generate_series(1, $N) RETURNING id)
       SELECT id FROM new_users ORDER BY id" > "$WORK/users"
P=$(curl -sf -XPOST $API/products -H "$H" -d "{\"sku\":\"load$R\",\"name\":\"load$R\",\"price_cents\":5000,\"stock\":$UNITS,\"sale_opens_at\":\"$(date -u -d '+15 seconds' +%FT%TZ)\",\"sale_ends_at\":\"$(date -u -d '+1 day' +%FT%TZ)\"}" | jq .id)

./target/release/admission_worker "$P" > "$WORK/admission.log" 2>&1 & PIDS+=($!)
./target/release/payment_worker > "$WORK/payment.log" 2>&1 & PIDS+=($!)
./target/release/cleanup_worker > "$WORK/cleanup.log" 2>&1 & PIDS+=($!)

echo "Running (the sale opens 15 s after the product was created)"
BASE_URL=$API PRODUCT_ID=$P USER_IDS="$WORK/users" \
  JWT_SECRET=$(grep '^JWT_SECRET=' .env | cut -d= -f2-) \
  ./load/final/target/release/final-load | tee "$WORK/report" || true

# Let the last abandoned or unpaid holds expire and the payment worker finish
sleep $((HOLD_TTL_SECS + PAYMENT_EXTENSION_SECS + 5))

TOTAL=$(psql_ "SELECT stock_total FROM inventory WHERE product_id = $P")
IN_STOCK=$(psql_ "SELECT stock FROM inventory WHERE product_id = $P")
SOLD=$(psql_ "SELECT COALESCE(sum(quantity), 0) FROM orders WHERE product_id = $P AND status = 'CONFIRMED'")
OVER=$(psql_ "SELECT count(*) FROM (SELECT user_id FROM orders WHERE product_id = $P AND status = 'CONFIRMED'
              GROUP BY user_id HAVING sum(quantity) > (SELECT max_per_user FROM products WHERE id = $P)) s")
STUCK=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $P AND status = 'PAYMENT_PENDING'
               AND expires_at < now() - interval '5 seconds'")
CONFIRMED=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $P AND status = 'CONFIRMED'")
BY_STATUS=$(psql_ "SELECT string_agg(status || ' ' || n, ', ') FROM (SELECT status::text, count(*) AS n FROM orders WHERE product_id = $P GROUP BY status ORDER BY status) s")
PAID=$(grep -oP '^  Paid: \K[0-9]+' "$WORK/report" || echo 0)
R_SOLD=$(rcli GET sold:$P); R_STOCK=$(rcli GET stock:$P); R_HOLDS=$(rcli ZCARD holds:expiry)
# Units still held (reservations not yet expired or paid) in Redis
R_HELD=$(rcli EVAL "local n = 0 for _, k in ipairs(redis.call('KEYS', 'resv:*')) do local raw = redis.call('GET', k) if raw then local r = cjson.decode(raw) if tostring(r.product) == ARGV[1] then n = n + r.quantity end end end return n" 0 "$P")

check() { if [ "$2" = "$3" ]; then echo "- [x] $1 ($2)"; else echo "- [ ] **FAILED** $1: $2, expected $3"; FAIL=1; fi; }
FAIL=0
CHECKS=$(
  check "units sold, at most $UNITS" "$SOLD" "$([ "$SOLD" -le $UNITS ] && echo "$SOLD" || echo "<= $UNITS")"
  check "units sold (stock_total - inventory.stock) equal to units in CONFIRMED orders" "$((TOTAL - IN_STOCK))" "$SOLD"
  check "users who bought more than max_per_user" "$OVER" 0
  check "PAYMENT_PENDING orders past expires_at + 5 s" "$STUCK" 0
  check "CONFIRMED orders equal to users who paid" "$CONFIRMED" "$PAID"
  check "Redis sold:{product} equal to units sold" "$R_SOLD" "$SOLD"
  check "Redis stock:{product} equal to unsold units" "$R_STOCK" "$((UNITS - SOLD))"
  check "Redis stock + held units + sold equal to stock_total" "$((R_STOCK + R_HELD + R_SOLD))" "$TOTAL"
)
echo "$CHECKS"; grep -q FAILED <<<"$CHECKS" && FAIL=1

{
  echo
  echo "## $(date -u +%F\ %H:%M) UTC: $N users, $UNITS units"
  echo
  echo "Pay ratio $PAY_RATIO, hold ${HOLD_TTL_SECS} s + ${PAYMENT_EXTENSION_SECS} s for payment, token ${TOKEN_TTL_SECS} s, fake payment ${FAKE_PAYMENT_DELAY_MS} ms. Release builds on one machine, one API process and one of each worker; Postgres, Redis and Kafka in Docker."
  echo
  sed -n '/^Finished/,$p' "$WORK/report"
  echo
  echo "Orders: ${BY_STATUS}. Holds still queued in Redis (all products): $R_HOLDS."
  echo
  echo "$CHECKS"
} >> load/RESULTS.md
echo "Appended to load/RESULTS.md"
exit $FAIL
