#!/usr/bin/env bash
# The buying path under load, without status polling: N users (default
# 10,000) join a product with as many units right after its sale opens, are
# all admitted (the script watches Redis instead of users polling status),
# then at once reserve a unit and pay (PAY_RATIO) or walk away. Times each
# phase, including admission and payment confirmation, checks the outcome in
# Postgres and Redis, and appends a report to load/RESULTS.md.
#
# With CAP set, the waiting room admits at most CAP shoppers at a time (its
# MAX_ACTIVE_TOKENS) instead of everyone: users learn they are admitted by
# checking their status every POLL_MS (until it is ACTIVE), and join,
# admission and shopping run as one phase.
#
# Needs `docker compose up -d`. REDIS_DB (default 0) picks the Redis database.
# Usage: [CAP=2000] load/shop.sh [N] [PAYMENT_WORKERS]

set -euo pipefail
cd "$(dirname "$0")/.."

N=${1:-10000}
PAYMENT_WORKERS=${2:-1}
UNITS=$N
SPREAD_MS=${SPREAD_MS:-2000}
export PAY_RATIO=${PAY_RATIO:-0.7}
CAP=${CAP:-}
export MAX_ACTIVE_TOKENS=${CAP:-$((N * 2))} TOKEN_TTL_SECS=600
export POLL_MS=${POLL_MS:-2000}
export HOLD_TTL_SECS=${HOLD_TTL_SECS:-20} PAYMENT_EXTENSION_SECS=${PAYMENT_EXTENSION_SECS:-120}
export FAKE_PAYMENT_DELAY_MS=${FAKE_PAYMENT_DELAY_MS:-1000}
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
now_ms() { date +%s%3N; }
secs() { awk "BEGIN { printf \"%.1f\", ($2 - $1) / 1000 }"; }

cargo build -q --release --bins
cargo build -q --release --manifest-path load/final/Cargo.toml
docker compose --profile app stop api admission-worker payment-worker cleanup-worker >/dev/null 2>&1 || true

RUST_LOG=warn ./target/release/flash-sale > "$WORK/api.log" 2>&1 & PIDS+=($!)
until curl -s -o /dev/null $API/auth/me; do sleep 0.5; done

echo "Setting up $N users and a product with $UNITS units"
# Guest rows inserted directly (no password to hash); the load generator signs their tokens itself
psql_ "WITH new_users AS (INSERT INTO users (is_guest) SELECT true FROM generate_series(1, $N) RETURNING id)
       SELECT id FROM new_users ORDER BY id" > "$WORK/users"
P=$(curl -sf -XPOST $API/products -H "$H" -d "{\"sku\":\"shop$R\",\"name\":\"shop$R\",\"price_cents\":5000,\"stock\":$UNITS,\"sale_opens_at\":\"$(date -u -d '+2 seconds' +%FT%TZ)\",\"sale_ends_at\":\"$(date -u -d '+1 day' +%FT%TZ)\"}" | jq .id)

./target/release/admission_worker "$P" > "$WORK/admission.log" 2>&1 & PIDS+=($!)
./target/release/cleanup_worker > "$WORK/cleanup.log" 2>&1 & PIDS+=($!)
for i in $(seq 1 "$PAYMENT_WORKERS"); do ./target/release/payment_worker > "$WORK/payment$i.log" 2>&1 & PIDS+=($!); done
until grep -q "Lobby flushed" "$WORK/admission.log"; do sleep 0.5; done

gen() {
  MODE=$1 JOIN_SPREAD_MS=$SPREAD_MS BASE_URL=$API PRODUCT_ID=$P USER_IDS="$WORK/users" \
    JWT_SECRET=$(grep '^JWT_SECRET=' .env | cut -d= -f2-) ./load/final/target/release/final-load
}

if [ -n "$CAP" ]; then
  echo "== Sale ($N users within ${SPREAD_MS} ms of opening, at most $CAP shopping at once)"
  T_JOIN=$(now_ms)
  gen queue > "$WORK/shop.report" || true
  T_SHOPPED=$(now_ms)
  cp "$WORK/shop.report" "$WORK/join.report"
else
  echo "== Join ($N users within ${SPREAD_MS} ms of opening)"
  T_JOIN=$(now_ms)
  gen join > "$WORK/join.report" || true
  T_JOINED=$(now_ms)
  until [ "$(rcli ZCARD active_tokens:$P)" -ge "$N" ]; do sleep 0.2; done
  T_ADMITTED=$(now_ms)

  echo "== Shop (all $N at once, within ${SPREAD_MS} ms)"
  gen shop > "$WORK/shop.report" || true
  T_SHOPPED=$(now_ms)
fi
PAID=$(grep -oP '^  Paid: \K[0-9]+' "$WORK/shop.report" || echo 0)
until [ "$(psql_ "SELECT count(*) FROM orders WHERE product_id = $P AND status IN ('CONFIRMED', 'REFUNDED')")" -ge "$PAID" ]; do sleep 0.5; done
T_CONFIRMED=$(now_ms)

# Abandoned holds expire, then the checks
sleep $((HOLD_TTL_SECS + 5))

TOTAL=$(psql_ "SELECT stock_total FROM inventory WHERE product_id = $P")
IN_STOCK=$(psql_ "SELECT stock FROM inventory WHERE product_id = $P")
SOLD=$(psql_ "SELECT COALESCE(sum(quantity), 0) FROM orders WHERE product_id = $P AND status = 'CONFIRMED'")
OVER=$(psql_ "SELECT count(*) FROM (SELECT user_id FROM orders WHERE product_id = $P AND status = 'CONFIRMED'
              GROUP BY user_id HAVING sum(quantity) > (SELECT max_per_user FROM products WHERE id = $P)) s")
STUCK=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $P AND status = 'PAYMENT_PENDING'
               AND expires_at < now() - interval '5 seconds'")
CONFIRMED=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $P AND status = 'CONFIRMED'")
REFUNDED=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $P AND status = 'REFUNDED'")
R_SOLD=$(rcli GET sold:$P); R_STOCK=$(rcli GET stock:$P)
# Units still held (reservations not yet expired or paid) in Redis
R_HELD=$(rcli EVAL "local n = 0 for _, k in ipairs(redis.call('KEYS', 'resv:*')) do local raw = redis.call('GET', k) if raw then local r = cjson.decode(raw) if tostring(r.product) == ARGV[1] then n = n + r.quantity end end end return n" 0 "$P")
MESSAGES=$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic waiting.$P 2>/dev/null </dev/null | cut -d: -f3)

check() { if [ "$2" = "$3" ]; then echo "- [x] $1 ($2)"; else echo "- [ ] **FAILED** $1: $2, expected $3"; fi; }
CHECKS=$(
  check "units sold (stock_total - inventory.stock) equal to units in CONFIRMED orders" "$((TOTAL - IN_STOCK))" "$SOLD"
  check "units sold, at most stock_total" "$SOLD" "$([ "$SOLD" -le "$TOTAL" ] && echo "$SOLD" || echo "<= $TOTAL")"
  check "CONFIRMED orders equal to users who paid" "$CONFIRMED" "$PAID"
  check "payments refunded" "$REFUNDED" 0
  check "users who bought more than max_per_user" "$OVER" 0
  check "PAYMENT_PENDING orders past expires_at + 5 s" "$STUCK" 0
  check "Redis sold:{product} equal to units sold" "$R_SOLD" "$SOLD"
  check "Redis stock:{product} equal to unsold units" "$R_STOCK" "$((TOTAL - SOLD))"
  check "Redis stock + held units + sold equal to stock_total" "$((R_STOCK + R_HELD + R_SOLD))" "$TOTAL"
  check "Kafka messages equal to users" "$MESSAGES" "$N"
)
if [ -n "$CAP" ]; then
  PHASES="| phase | seconds |
|---|---|
| whole sale: joins, admission at most $CAP at a time, shopping | $(secs $T_JOIN $T_SHOPPED) |
| purchases per second (paid / whole sale) | $(awk "BEGIN { printf \"%.0f\", $PAID / (($T_SHOPPED - $T_JOIN) / 1000) }") |"
else
  PHASES="| phase | seconds |
|---|---|
| $N joins | $(secs $T_JOIN $T_JOINED) |
| admission of all $N (after the last join) | $(secs $T_JOINED $T_ADMITTED) |
| shopping: reserve, pay | $(secs $T_ADMITTED $T_SHOPPED) |"
fi
PHASES="$PHASES
| last payment confirmed (after shopping ended; fake provider ${FAKE_PAYMENT_DELAY_MS} ms, $PAYMENT_WORKERS payment worker(s)) | $(secs $T_SHOPPED $T_CONFIRMED) |"

echo; echo "$PHASES"; echo; [ -z "$CAP" ] && { echo "Join:"; sed -n '/^|/p' "$WORK/join.report"; echo; }; sed -n '/^Outcomes/,$p' "$WORK/shop.report"; echo; echo "$CHECKS"

if [ -n "$CAP" ]; then SHOPPING="status polled every ${POLL_MS} ms, at most $CAP shopping"; else SHOPPING="no status polling"; fi
{
  echo
  echo "## Buying path $(date -u +%F\ %H:%M) UTC: $N users, $UNITS units, $SHOPPING"
  echo
  if [ -n "$CAP" ]; then
    echo "Everyone joins within ${SPREAD_MS} ms of opening; the waiting room admits at most $CAP shoppers at a time. Users learn they are admitted by checking their status every ${POLL_MS} ms (until it is ACTIVE), then reserve a unit (retrying when the stock ran out) and pay ($PAY_RATIO) or walk away. One API process, one admission and one cleanup worker, $PAYMENT_WORKERS payment worker(s); all on one machine."
  else
    echo "Everyone joins within ${SPREAD_MS} ms of opening and is admitted at once (the script watches Redis; nobody polls status), then everyone shops within ${SPREAD_MS} ms: reserve a unit (retrying when the stock ran out), then pay ($PAY_RATIO) or walk away. One API process, one admission and one cleanup worker, $PAYMENT_WORKERS payment worker(s); all on one machine."
  fi
  echo
  echo "$PHASES"
  echo
  [ -z "$CAP" ] && sed -n '/^|/p' "$WORK/join.report"
  echo
  sed -n '/^Outcomes/,$p' "$WORK/shop.report"
  echo
  echo "$CHECKS"
} >> load/RESULTS.md
echo "Appended to load/RESULTS.md"
