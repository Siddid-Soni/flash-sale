# Shared helpers for load/flash.sh and load/backlog.sh (sourced, not run).
# Expects the caller to have cd'd to the repository root and set `set -euo pipefail`.

ADMIN_TOKEN=$(sed -n 's/^ADMIN_TOKEN=//p' .env | head -1)
JWT_SECRET=$(sed -n 's/^JWT_SECRET=//p' .env | head -1)
# Where the generator sends its requests. Unset, it is the gateway container's own address
# (see target_urls): through localhost:8000 every connection would also pass Docker's
# userland port proxy, which took ~9 cores at 30k req/s and capped the runs.
GENERATOR_BASE_URL=${BASE_URL:-}
BASE_URL=${BASE_URL:-http://localhost:8000}
WORK=$(mktemp -d)

# Separate cores for the generator and the stack (PIN_CPUS=0 turns it off), so the generator
# cannot take CPU from the system it measures: by default the first ~30% of the cores run the
# generator, the rest the whole stack (compose override load/compose.pin.yml)
# Linux only (taskset, and Docker's own cpuset); off elsewhere.
if command -v taskset >/dev/null && [ "$(uname -s)" = Linux ]; then PIN_CPUS=${PIN_CPUS:-1}; else PIN_CPUS=0; fi
# No gateway access log during a run (GATEWAY_ACCESS_LOG=1 keeps it): one line per request, through
# Docker's log collection, is work outside what the test measures
export GATEWAY_ACCESS_LOG=${GATEWAY_ACCESS_LOG:-0}
# Likewise the services' own per-request log line (actix's Logger); everything else stays at info
export RUST_LOG=${RUST_LOG:-info,actix_web::middleware::logger=warn}
if [ "$PIN_CPUS" = 1 ]; then
  cores=$(getconf _NPROCESSORS_ONLN)
  gen=$(( cores * 3 / 10 )); [ "$gen" -lt 1 ] && gen=1
  GEN_CPUS=${GEN_CPUS:-0-$((gen - 1))}
  export STACK_CPUS=${STACK_CPUS:-$gen-$((cores - 1))}
  export COMPOSE_FILE=docker-compose.yml:load/compose.pin.yml
fi

psql_() { docker compose exec -T postgres psql -U actix -d flash_sale -tAc "$1" </dev/null; }
rcli() { docker compose exec -T redis redis-cli "$@" </dev/null; }

# Start the stack. Extra settings come from the environment of the caller
# (PAYMENT_WINDOW_SECS, PROCESSOR_MAX_PER_SEC, GATEWAY_TRUST_XFF, ...).
start_stack() {
  docker compose up -d postgres redis redis-cache kafka >/dev/null
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
  command -v ss >/dev/null || return 0   # ss is Linux's; elsewhere, no wait
  for i in $(seq 60); do
    tw=$(ss -s | sed -n 's/.*timewait \([0-9]*\).*/\1/p' | head -1)
    [ "${tw:-0}" -lt 3000 ] && return 0
    [ "$i" = 1 ] && echo "Waiting for ${tw} sockets in TIME_WAIT from an earlier run to drain"
    sleep 3
  done
}

# Back to the defaults: compose recreates what differs (the gateway trusts no X-Forwarded-For
# again, no container is pinned to cores)
restore_defaults() {
  echo "Restoring default settings (gateway without X-Forwarded-For trust, access logs on, no CPU pinning)"
  env -u PAYMENT_WINDOW_SECS -u PROCESSOR_MAX_PER_SEC -u GATEWAY_TRUST_XFF -u GATEWAY_ACCESS_LOG -u RUST_LOG -u COMPOSE_FILE -u STACK_CPUS \
    docker compose --profile app up -d >/dev/null 2>&1 || true
}

# After start_stack: the addresses the generator uses. The gateway and the provider are reached
# at their container addresses (the host routes to the compose network directly), unless
# BASE_URL was given. Only on Linux: with Docker Desktop (macOS, Windows) the containers run in a
# VM whose addresses the host cannot reach, so the generator goes through localhost:8000.
target_urls() {
  if [ -z "$GENERATOR_BASE_URL" ] && [ "$(uname -s)" != Linux ]; then
    GENERATOR_BASE_URL=$BASE_URL
  fi
  if [ -z "$GENERATOR_BASE_URL" ]; then
    GENERATOR_BASE_URL="http://$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
      "$(docker compose ps -q gateway)")"
    PROVIDER_URL=${PROVIDER_URL:-http://172.28.0.50:8090}
  fi
  export PROVIDER_URL=${PROVIDER_URL:-http://localhost:8090}
  echo "Generator targets $GENERATOR_BASE_URL (provider $PROVIDER_URL)"
}

# run_generator: the load generator with the common settings, on its own cores when pinned
run_generator() {
  local pin=()
  if [ "$PIN_CPUS" = 1 ]; then
    pin=(taskset -c "$GEN_CPUS")
    echo "Generator on cores $GEN_CPUS, stack on cores $STACK_CPUS"
  fi
  SALE_ID=$SALE USERS_FILE=$WORK/users JWT_SECRET=$JWT_SECRET BASE_URL=$GENERATOR_BASE_URL \
    REPORT_JSON=$WORK/report.json "${pin[@]}" ./load/flash/target/release/flash-load | tee "$WORK/report.txt"
}

# insert_users N FILE: N guest users straight into Postgres (no session call needed;
# the generator signs their tokens), ids written to FILE
insert_users() {
  psql_ "WITH new_users AS (INSERT INTO users (created_at) SELECT now() FROM generate_series(1, $1) RETURNING id)
         SELECT id FROM new_users ORDER BY id" > "$2"
}

# create_sale UNITS START_IN_SECS: prints the sale id. Admin routes answer 403 from the host, so the
# gateway container's create-sale command makes the call (and retries while the services start).
create_sale() {
  local resp
  resp=$(docker compose exec -T -e ADMIN_TOKEN="$ADMIN_TOKEN" gateway \
    create-sale "load-$(date +%s)" "$1" 49900 "$2" </dev/null 2>/dev/null) || true
  jq -er '.id' <<<"$resp" || { echo "could not create the sale: $resp" >&2; return 1; }
}

# wait_converged SALE TIMEOUT_SECS: PostgreSQL has no AWAITING_PAYMENT order left for the sale,
# its payment inbox events (including the ones postponed until the expiration worker expired
# their order) are processed, and the provider's webhook outbox is drained. The expiration
# worker returns each unit right after expiring its order, so stock:{sale} is final a moment
# after this (sales are counted only in PostgreSQL, products.sold).
wait_converged() {
  local sale=$1 limit=$2 t=0 live inbox outbox
  while [ "$t" -lt "$limit" ]; do
    live=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $sale AND status = 'AWAITING_PAYMENT'")
    inbox=$(psql_ "SELECT count(*) FROM payment_inbox WHERE processed_at IS NULL AND payload->'data'->'object'->>'ticket' LIKE '$sale.%'")
    outbox=$(rcli ZCARD fakepay:outbox)
    if [ "$live" = 0 ] && [ "$inbox" = 0 ] && [ "$outbox" = 0 ]; then
      sleep 2   # the last units returned to Redis
      echo "Converged after ${t}s"; return 0
    fi
    sleep 2; t=$((t + 2))
  done
  echo "NOT converged after ${limit}s: AWAITING_PAYMENT orders $live, inbox $inbox, outbox $outbox"
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

# One line for the report: how the generator reached the stack and which cores each had
setup_note() {
  local pin="no CPU pinning"
  [ "$PIN_CPUS" = 1 ] && pin="generator pinned to cores $GEN_CPUS, the whole stack to cores $STACK_CPUS"
  echo "Generator target: $GENERATOR_BASE_URL (provider $PROVIDER_URL); $pin."
}
