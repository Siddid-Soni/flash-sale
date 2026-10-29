#!/usr/bin/env bash
# Create a sale through the admin route. The gateway refuses /v1/admin/ from the host,
# so the request is made from inside the gateway container.
#
#   scripts/create_sale.sh [name] [units] [price_cents] [opens_in_seconds]
#   scripts/create_sale.sh "Phone" 500 49900 60     # the defaults
#
# Prints the created sale as JSON; its page is http://localhost:8000/?sale=<id>.
# ADMIN_TOKEN defaults to the development value in .env.
set -euo pipefail

name=${1:-Phone}
units=${2:-500}
price=${3:-49900}
opens_in=${4:-60}
token=${ADMIN_TOKEN:-dev-admin-token}

start=$(date -u -d "+${opens_in} seconds" +%FT%TZ)
body=$(printf '{"name":"%s","price_cents":%d,"total_inventory":%d,"sale_start_time":"%s"}' \
  "$name" "$price" "$units" "$start")

cd "$(dirname "$0")/.."
# Retried for up to a minute: right after `docker compose up` the site service can take a
# while to start listening, and until then the gateway answers 502
for attempt in $(seq 30); do
  sale=$(docker compose exec -T gateway wget -qO- \
    --header "X-Admin-Token: $token" --header 'Content-Type: application/json' \
    --post-data "$body" http://127.0.0.1/v1/admin/sales 2>/dev/null) && break
  [ "$attempt" = 30 ] && { echo "could not create the sale (is the stack up?)" >&2; exit 1; }
  sleep 2
done

echo "$sale"
id=$(grep -oE '"id":[0-9]+' <<<"$sale" | cut -d: -f2)
echo "Opens at $start: http://localhost:8000/?sale=$id"
