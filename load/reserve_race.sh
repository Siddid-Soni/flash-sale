#!/usr/bin/env bash
# Step 5.4: N admitted users all try to reserve all N units of a product at
# the same moment (the waiting room admits no more shoppers than there are
# units, so N admitted users need N units; each may hold up to N). Expect
# exactly one 201 and N-1 409s, every unit held by that one user, and nothing
# written to Postgres (holds live in Redis until payment).
#
# Needs the API on localhost:8080 and `docker compose up -d`. Starts its own
# admission worker for the product. REDIS_DB (default 0) picks the Redis
# database; the API must use the same one (REDIS_URL=redis://127.0.0.1:6379/$REDIS_DB).
# Usage: load/reserve_race.sh [N]

set -euo pipefail
cd "$(dirname "$0")/.."

N=${1:-1000}
# Redis database to use (the worker started here gets it through REDIS_URL)
REDIS_DB=${REDIS_DB:-0}
export REDIS_URL=redis://127.0.0.1:6379/$REDIS_DB
API=localhost:8080
H='Content-Type: application/json'
R=$RANDOM$RANDOM
WORK=$(mktemp -d)
WORKER=
trap 'kill $WORKER 2>/dev/null || true; rm -rf "$WORK"' EXIT

psql_() { docker compose exec -T postgres psql -U actix -d flash_sale -tAc "$1" </dev/null; }
rcli() { docker compose exec -T redis redis-cli -n "$REDIS_DB" "$@" </dev/null; }

# curl --parallel with one request per `next` block: every request carries its own token
# (the last block's `next` is dropped: an empty block is an error)
parallel_curl() { sed -i '$d' "$1"; curl -s --parallel --parallel-immediate --parallel-max "$N" -K "$1"; }

echo "Setting up a product with $N units and $N users"
P=$(curl -sf -XPOST $API/products -H "$H" -d "{\"sku\":\"race$R\",\"name\":\"race$R\",\"price_cents\":1000,\"stock\":$N,\"max_per_user\":$N,\"sale_opens_at\":\"$(date -u -d '+1 hour' +%FT%TZ)\",\"sale_ends_at\":\"$(date -u -d '+2 days' +%FT%TZ)\"}" | jq .id)

# One guest session per user
seq 1 "$N" | xargs -P 20 -I{} curl -sf -XPOST $API/session -o "$WORK/jwt.{}"
for i in $(seq 1 "$N"); do jq -r .token "$WORK/jwt.$i"; done > "$WORK/jwts"

# Everyone joins the lobby, then the sale opens now and the worker admits them all
: > "$WORK/join.cfg"
while read -r T; do
  printf 'url = "http://%s/waiting-room/%s/join"\nrequest = "POST"\nheader = "Authorization: Bearer %s"\noutput = "/dev/null"\nnext\n' $API "$P" "$T" >> "$WORK/join.cfg"
done < "$WORK/jwts"
parallel_curl "$WORK/join.cfg"
psql_ "UPDATE products SET sale_opens_at = now() WHERE id = $P" >/dev/null
MAX_ACTIVE_TOKENS=$((N * 2)) ./target/debug/admission_worker "$P" > "$WORK/worker.log" 2>&1 &
WORKER=$!
until [ "$(rcli ZCARD active_tokens:$P)" -ge "$N" ]; do sleep 0.5; done
echo "All $N users are ACTIVE"

# The race: everyone reserves all the units at once
: > "$WORK/reserve.cfg"
while read -r T; do
  printf 'url = "http://%s/reservations"\nrequest = "POST"\nheader = "Authorization: Bearer %s"\nheader = "%s"\ndata = "{\\"product_id\\":%s,\\"quantity\\":%s}"\noutput = "/dev/null"\nwrite-out = "%%{http_code}\\n"\nnext\n' $API "$T" "$H" "$P" "$N" >> "$WORK/reserve.cfg"
done < "$WORK/jwts"
START=$(date +%s%N)
parallel_curl "$WORK/reserve.cfg" > "$WORK/codes"
END=$(date +%s%N)

echo "Responses ($(awk "BEGIN { printf \"%.2f\", ($END - $START) / 1e9 }")s):"
sort "$WORK/codes" | uniq -c
ORDERS=$(psql_ "SELECT count(*) FROM orders WHERE product_id = $P")
STOCK=$(rcli GET stock:$P)
echo "Orders written: $ORDERS   units held: $((N - STOCK))   stock: $STOCK of $N"

CREATED=$(grep -c '^201$' "$WORK/codes" || true)
CONFLICT=$(grep -c '^409$' "$WORK/codes" || true)
if [ "$CREATED" -eq 1 ] && [ "$CONFLICT" -eq $((N - 1)) ] && [ "$ORDERS" -eq 0 ] && [ "$STOCK" -eq 0 ]; then
  echo "PASS"
else
  echo "FAIL"; exit 1
fi
