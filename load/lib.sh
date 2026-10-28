# Shared helpers for load/flash.sh and load/backlog.sh (sourced, not run).
# Expects the caller to have cd'd to the repository root and set `set -euo pipefail`.

ADMIN_TOKEN=$(sed -n 's/^ADMIN_TOKEN=//p' .env | head -1)
JWT_SECRET=$(sed -n 's/^JWT_SECRET=//p' .env | head -1)
BASE_URL=${BASE_URL:-http://localhost:8000}
WORK=$(mktemp -d)

psql_() { docker compose exec -T postgres psql -U actix -d flash_sale -tAc "$1" </dev/null; }
rcli() { docker compose exec -T redis redis-cli "$@" </dev/null; }

# Start the stack. Extra settings come from the environment of the caller
# (PAYMENT_WINDOW_SECS, PROCESSOR_MAX_PER_SEC, GATEWAY_TRUST_XFF, ...).
start_stack() {
  docker compose up -d postgres redis kafka >/dev/null
  docker compose --profile app up -d --build >/dev/null
  echo "Waiting for the gateway"
  local i
  for i in $(seq 120); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE_URL/gateway-health")" = 200 ] && break
    sleep 1
  done
  [ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE_URL/gateway-health")" = 200 ] || { echo "gateway is not healthy"; exit 1; }
  # Every app container must be up (the processor and the worker have no health check; the
  # provider has one, and the gateway waits for it)
  for i in $(seq 60); do
    local running
    running=$(docker compose --profile app ps --status running --format '{{.Service}}' | sort | tr '\n' ' ')
    [[ "$running" == *"expiration-worker"* && "$running" == *"order-processor"* && "$running" == *"fake-payment-provider"* ]] && break
    sleep 1
  done
  sleep 5   # let the processor join its consumer group
}

# Local limit: every connection of a previous run lingers in TIME_WAIT for a minute, and with
# ~28,000 ephemeral ports a 100,000-user run started right after another one fails to connect.
# Waits (up to 3 minutes) until the host has few of them.
wait_ports_free() {
  local i tw
  for i in $(seq 60); do
    tw=$(ss -s | sed -n 's/.*timewait \([0-9]*\).*/\1/p' | head -1)
    [ "${tw:-0}" -lt 3000 ] && return 0
    [ "$i" = 1 ] && echo "Waiting for ${tw} sockets in TIME_WAIT from an earlier run to drain"
    sleep 3
  done
}

# Back to the defaults: compose recreates what differs (the gateway trusts no X-Forwarded-For again)
restore_defaults() {
  echo "Restoring default settings (gateway without X-Forwarded-For trust)"
  env -u PAYMENT_WINDOW_SECS -u PROCESSOR_MAX_PER_SEC -u GATEWAY_TRUST_XFF \
    docker compose --profile app up -d >/dev/null 2>&1 || true
}

# insert_users N FILE: N guest users straight into Postgres (no session call needed;
# the generator signs their tokens), ids written to FILE
insert_users() {
  psql_ "WITH new_users AS (INSERT INTO users (created_at) SELECT now() FROM generate_series(1, $1) RETURNING id)
         SELECT id FROM new_users ORDER BY id" > "$2"
}

# create_sale UNITS START_IN_SECS: prints the sale id. Admin routes answer 403 from the host, so the
# call is made from inside the gateway container.
create_sale() {
  local start name resp
  start=$(date -u -d "+$2 seconds" +%FT%TZ)
  name="load-$(date +%s)"
  resp=$(docker compose exec -T gateway wget -qO- \
    --header "X-Admin-Token: $ADMIN_TOKEN" --header 'Content-Type: application/json' \
    --post-data "{\"name\":\"$name\",\"price_cents\":49900,\"total_inventory\":$1,\"sale_start_time\":\"$start\"}" \
    http://127.0.0.1/v1/admin/sales </dev/null)
  jq -er '.id' <<<"$resp" || { echo "could not create the sale: $resp" >&2; return 1; }
}

# wait_converged SALE TIMEOUT_SECS: no AWAITING_PAYMENT orders, no holds queued, the
# sale's payment inbox events and the provider's webhook outbox drained. Waiting for
# holds:expiry to be empty includes the paid holds: the expiration worker closes them at their
# deadline and releases the unpaid ones, so stock:{sale} is final after this (sales are counted
# only in PostgreSQL, products.sold).
wait_converged() {
  local sale=$1 limit=$2 t=0 live pend exp inbox outbox
  while [ "$t" -lt "$limit" ]; do
    live=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $sale AND status = 'AWAITING_PAYMENT'")
    pend=$(rcli ZCARD "holds:pending:{$sale}"); exp=$(rcli ZCARD "holds:expiry:{$sale}")
    inbox=$(psql_ "SELECT count(*) FROM payment_inbox WHERE processed_at IS NULL AND payload->'data'->'object'->>'ticket' LIKE '$sale.%'")
    outbox=$(rcli ZCARD fakepay:outbox)
    if [ "$live" = 0 ] && [ "$pend" = 0 ] && [ "$exp" = 0 ] && [ "$inbox" = 0 ] && [ "$outbox" = 0 ]; then
      echo "Converged after ${t}s"; return 0
    fi
    sleep 2; t=$((t + 2))
  done
  echo "NOT converged after ${limit}s: live orders $live, holds pending $pend, holds expiry $exp, inbox $inbox, outbox $outbox"
  return 1
}

FAIL=0
CHECKS=""
# check "description" ACTUAL EXPECTED
check() {
  if [ "$2" = "$3" ]; then CHECKS+="- [x] $1 ($2)"$'\n'; echo "PASS  $1 ($2)"
  else CHECKS+="- [ ] **FAILED** $1: $2, expected $3"$'\n'; echo "FAIL  $1: $2, expected $3"; FAIL=1; fi
}
# check_true "description" "details" CONDITION_EXIT_CODE
check_cond() {
  if [ "$3" = 0 ]; then CHECKS+="- [x] $1 ($2)"$'\n'; echo "PASS  $1 ($2)"
  else CHECKS+="- [ ] **FAILED** $1: $2"$'\n'; echo "FAIL  $1: $2"; FAIL=1; fi
}
