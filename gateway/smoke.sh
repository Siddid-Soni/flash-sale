#!/usr/bin/env bash
# Smoke test for the gateway, including the site service's routes. Needs the
# whole stack, including the site service, the fake payment provider (published on
# localhost:8090, outside the gateway) and the order processor:
#     docker compose --profile app up -d --build
# Run from anywhere:  gateway/smoke.sh
# Prints PASS or FAIL per check and exits non-zero if any check failed.
# It creates one sale (5 units) and leaves it behind, and briefly stops
# order-service-2 (it is started again before the script ends).

set -u
cd "$(dirname "$0")/.."

BASE="${BASE:-http://localhost:8000}"
PROVIDER="${PROVIDER:-http://localhost:8090}"
ADMIN_TOKEN="${ADMIN_TOKEN:-$(sed -n 's/^ADMIN_TOKEN=//p' .env 2>/dev/null | head -1)}"
ADMIN_TOKEN="${ADMIN_TOKEN:-dev-admin-token}"
BROWSER_UA='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36'
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"; docker compose --profile app start order-service-2 >/dev/null 2>&1' EXIT

FAILED=0
pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; [ $# -gt 1 ] && printf '        %s\n' "$2"; FAILED=1; }
check() { # check "name" <command...>: PASS when the command succeeds
    local name="$1"; shift
    if "$@"; then pass "$name"; else fail "$name"; fi
}

# curl as a browser; $1.. are curl arguments. Headers go to $TMP/h, body to $TMP/b, status on stdout.
call() { curl -s -o "$TMP/b" -D "$TMP/h" -w '%{http_code}' -A "$BROWSER_UA" "$@"; }
header() { grep -i "^$1:" "$TMP/h" | head -1 | cut -d: -f2- | tr -d '\r' | sed 's/^ *//'; }

# --- wait for the gateway ---------------------------------------------------
for _ in $(seq 60); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/gateway-health")" = 200 ] && break
    sleep 1
done
[ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/gateway-health")" = 200 ] || { echo "FAIL  gateway not reachable at $BASE"; exit 1; }

# --- 1. frontend ------------------------------------------------------------
code=$(call "$BASE/")
if [ "$code" = 200 ] && grep -q 'id="app"' "$TMP/b" && [[ "$(header cache-control)" == *no-cache* ]]; then
    pass "frontend: / serves index.html with Cache-Control: no-cache"
else
    fail "frontend: / serves index.html with Cache-Control: no-cache" "status $code, cache-control: $(header cache-control)"
fi
asset=$(grep -o '/assets/[^"]*\.js' "$TMP/b" | head -1)
code=$(call -H 'Accept-Encoding: gzip' "$BASE$asset")
if [ "$code" = 200 ] && [[ "$(header cache-control)" == *"max-age=31536000"*immutable* ]] && [ "$(header content-encoding)" = gzip ]; then
    pass "frontend: $asset is immutable and gzip-encoded"
else
    fail "frontend: $asset is immutable and gzip-encoded" "status $code, cache-control: $(header cache-control), content-encoding: $(header content-encoding)"
fi

# --- 2. admin: refused from the host, allowed from inside --------------------
code=$(call -X POST -H "X-Admin-Token: $ADMIN_TOKEN" -H 'Content-Type: application/json' -d '{}' "$BASE/v1/admin/sales")
if [ "$code" = 403 ] && grep -q '"message"' "$TMP/b"; then
    pass "admin: /v1/admin/sales from the host is 403 (JSON)"
else
    fail "admin: /v1/admin/sales from the host is 403 (JSON)" "status $code: $(cat "$TMP/b")"
fi
# A forged X-Forwarded-For must not make a host request look internal
code=$(call -X POST -H 'X-Forwarded-For: 172.28.0.7' -H "X-Admin-Token: $ADMIN_TOKEN" -H 'Content-Type: application/json' -d '{}' "$BASE/v1/admin/sales")
[ "$code" = 403 ] && pass "admin: a forged X-Forwarded-For does not open it" || fail "admin: a forged X-Forwarded-For does not open it" "status $code"

sale_json=$(docker compose exec -T -e ADMIN_TOKEN="$ADMIN_TOKEN" gateway create-sale "Smoke phone" 5 49900 25 2>/dev/null)
SALE=$(echo "$sale_json" | jq -r '.id // empty' 2>/dev/null)
if [ -n "$SALE" ]; then
    pass "admin: from inside the gateway container the sale is created (id $SALE)"
else
    fail "admin: from inside the gateway container the sale is created" "got: $sale_json"
    exit 1
fi

# --- 3. session -------------------------------------------------------------
# Retries when the per-IP session limit answers 429 (earlier runs may have used the bucket)
new_session() {
    local code i
    for i in $(seq 12); do
        code=$(call -X POST "$BASE/v1/session")
        [ "$code" = 429 ] || break
        sleep 1
    done
    [ "$code" = 201 ] || return 1
    jq -r .token "$TMP/b"
}
TOKEN=$(new_session)
[ -n "$TOKEN" ] && pass "session: POST /v1/session returns a token" || { fail "session: POST /v1/session returns a token"; exit 1; }

# --- 4. edge cache ----------------------------------------------------------
code1=$(call "$BASE/v1/sales/$SALE"); s1=$(header x-cache-status); cc=$(header cache-control)
code2=$(call "$BASE/v1/sales/$SALE"); s2=$(header x-cache-status)
if [ "$code1" = 200 ] && [ "$code2" = 200 ] && [ "$s1" = MISS ] && [ "$s2" = HIT ] && [[ "$cc" == *max-age=1* ]]; then
    pass "cache: GET /v1/sales/$SALE is MISS then HIT"
else
    fail "cache: GET /v1/sales/$SALE is MISS then HIT" "got $code1/$s1 then $code2/$s2 ($cc)"
fi
# NGINX counts entry age in whole seconds, so an entry lives between 1 and 2 s.
# Polling for 3 s must show it refreshed once or twice, not on every request.
refreshes=0
for _ in $(seq 8); do
    call "$BASE/v1/sales/$SALE" >/dev/null
    case "$(header x-cache-status)" in MISS|EXPIRED|UPDATING|STALE) refreshes=$((refreshes + 1)) ;; esac
    sleep 0.4
done
{ [ "$refreshes" -ge 1 ] && [ "$refreshes" -le 3 ]; } \
    && pass "cache: 8 requests over 3 s refreshed the entry $refreshes time(s), not 8" \
    || fail "cache: the entry lives about a second" "refreshed $refreshes of 8"
# A stampede on an expired entry reaches the app once: the others wait for it
# (proxy_cache_lock) or get the old copy (updating)
sleep 2.2
for i in $(seq 40); do
    ( curl -s -o /dev/null -D - -A "$BROWSER_UA" "$BASE/v1/sales/$SALE" | tr -d '\r' | sed -n 's/^[Xx]-[Cc]ache-[Ss]tatus: //p' > "$TMP/c$i" ) &
done
wait
misses=$(cat "$TMP"/c* | grep -cE '^(MISS|EXPIRED)$'); stats=$(cat "$TMP"/c* | sort | uniq -c | awk '{printf "%s x%s ", $2, $1}'); rm -f "$TMP"/c*
[ "$misses" = 1 ] && pass "cache: 40 concurrent requests on an expired entry reached the app once ($stats)" || fail "cache: 40 concurrent requests on an expired entry reach the app once" "$stats"

# --- 4b. the sale list and search (site service, edge cache) ----------------------
code1=$(call "$BASE/v1/sales"); s1=$(header x-cache-status); cc=$(header cache-control)
code2=$(call "$BASE/v1/sales?x=$RANDOM"); s2=$(header x-cache-status)
if [ "$code1" = 200 ] && [ "$code2" = 200 ] && [ -n "$s1" ] && [ "$s2" = HIT ] \
    && [[ "$cc" == *max-age=5* && "$cc" == *stale-while-revalidate=30* ]]; then
    pass "cache: GET /v1/sales is cached for 5 s and ignores other query parameters (${s1} then ${s2})"
else
    fail "cache: GET /v1/sales is cached for 5 s and ignores other query parameters" "got $code1/$s1 then $code2/$s2 ($cc)"
fi
Q="smoke%20phone"
code1=$(call "$BASE/v1/sales/search?q=$Q"); s1=$(header x-cache-status); cc=$(header cache-control)
found=$(jq -r --argjson id "$SALE" '[.[] | select(.id == $id)] | length' "$TMP/b" 2>/dev/null)
code2=$(call "$BASE/v1/sales/search?q=$Q&x=$RANDOM"); s2=$(header x-cache-status)
code3=$(call "$BASE/v1/sales/search?q=smoke%20tablet-$RANDOM"); s3=$(header x-cache-status)
if [ "$code1" = 200 ] && [ "$found" = 1 ] && [[ "$cc" == *max-age=5* ]] && [ "$code2" = 200 ] && [ "$s2" = HIT ] && [ "$code3" = 200 ] && [ "$s3" = MISS ]; then
    pass "search: finds the sale, caches per q (same q HIT with another parameter, new q MISS)"
else
    fail "search: finds the sale, caches per q" "status $code1/$code2/$code3, found $found, cache $s1/$s2/$s3, cache-control: $cc"
fi

# --- 5. load balancing ------------------------------------------------------
# /v1/nothing is a 404 from the app: cheap, unlimited, and it carries X-Replica
replicas_of() { # replicas_of N: prints the X-Replica of N parallel requests
    local n="$1" i
    for i in $(seq "$n"); do
        ( curl -s -o /dev/null -D - -A "$BROWSER_UA" "$BASE/v1/nothing" | tr -d '\r' | sed -n 's/^[Xx]-[Rr]eplica: //p' > "$TMP/r$i" ) &
    done
    wait
    cat "$TMP"/r* 2>/dev/null | sort | uniq -c | awk '{printf "%s:%s ", $2, $1}'
    rm -f "$TMP"/r*
}
dist=$(replicas_of 30)
if [ "$(echo "$dist" | wc -w)" -ge 3 ]; then
    pass "balancing: 30 requests reached all three replicas ($dist)"
else
    fail "balancing: 30 requests reached all three replicas" "distribution: $dist"
fi

# --- 6. bots ----------------------------------------------------------------
code=$(curl -s -o "$TMP/b" -w '%{http_code}' -A 'python-requests/2.31' "$BASE/v1/sales/$SALE")
{ [ "$code" = 403 ] && grep -q '"message"' "$TMP/b"; } && pass "bots: python-requests User-Agent gets 403" || fail "bots: python-requests User-Agent gets 403" "status $code"
code=$(curl -s -o "$TMP/b" -w '%{http_code}' -H 'User-Agent:' "$BASE/v1/sales/$SALE")
[ "$code" = 403 ] && pass "bots: an empty User-Agent gets 403" || fail "bots: an empty User-Agent gets 403" "status $code"
code=$(call "$BASE/v1/sales/$SALE")
[ "$code" = 200 ] && pass "bots: a browser User-Agent works" || fail "bots: a browser User-Agent works" "status $code"
# The webhook is exempt from the bot filter: a bot User-Agent gets the webhook's own answer (the
# address check), not the bot filter's
code=$(curl -s -o "$TMP/b" -w '%{http_code}' -A 'python-requests/2.31' -X POST -d '{}' "$BASE/v1/payments/webhook")
{ [ "$code" = 403 ] && ! grep -q 'automated clients' "$TMP/b"; } && pass "bots: the webhook is exempt from the bot filter (it answers on its own terms: $code)" || fail "bots: the webhook is exempt from the bot filter" "status $code: $(cat "$TMP/b")"

# --- 6b. the payment webhook: only the provider's address -----------------------
code=$(call -X POST -H 'Content-Type: application/json' -d '{}' "$BASE/v1/payments/webhook")
if [ "$code" = 403 ] && jq -e '.message | contains("payment provider")' "$TMP/b" >/dev/null; then
    pass "webhook: POST /v1/payments/webhook from the host is 403 (JSON)"
else
    fail "webhook: POST /v1/payments/webhook from the host is 403 (JSON)" "status $code: $(cat "$TMP/b")"
fi
# The provider's address in a forged X-Forwarded-For must not open it
code=$(call -X POST -H 'X-Forwarded-For: 172.28.0.50' -H 'Content-Type: application/json' -d '{}' "$BASE/v1/payments/webhook")
[ "$code" = 403 ] && pass "webhook: a forged X-Forwarded-For does not open it" || fail "webhook: a forged X-Forwarded-For does not open it" "status $code"
# From inside the network but not from the provider's address (the gateway itself) it is refused too
out=$(docker compose exec -T gateway wget -qO- --post-data '{}' --header 'Content-Type: application/json' \
    http://127.0.0.1/v1/payments/webhook 2>&1 </dev/null)
[[ "$out" == *"403"* ]] && pass "webhook: from the gateway container (not the provider's address) it is 403" || fail "webhook: from the gateway container it is 403" "$out"
# The payment page is the provider's own, outside the gateway: published on its own port, and
# not proxied by the gateway any more (there the path is just a frontend deep link)
code=$(call "$PROVIDER/fake-pay/fake_cs_nothing")
[ "$code" = 404 ] && grep -q 'does not exist' "$TMP/b" && pass "provider: $PROVIDER/fake-pay/ answers (unknown session: 404 page)" || fail "provider: $PROVIDER/fake-pay/ answers" "status $code"
code=$(call "$BASE/fake-pay/fake_cs_nothing")
! grep -q 'does not exist' "$TMP/b" && pass "provider: the gateway does not serve /fake-pay/ (the path falls through to the frontend: $code)" || fail "provider: the gateway does not serve /fake-pay/" "status $code"

# --- 7. failover ------------------------------------------------------------
docker compose --profile app stop order-service-2 >/dev/null 2>&1
ok=0; bad=0; seen=""
for _ in $(seq 25); do
    code=$(call "$BASE/v1/nothing")
    if [ "$code" = 404 ]; then ok=$((ok + 1)); else bad=$((bad + 1)); fi
    seen="$seen $(header x-replica)"
done
if [ "$bad" = 0 ] && ! echo "$seen" | grep -qw 2; then
    pass "failover: with order-service-2 stopped 25/25 requests succeeded (replicas:$(echo "$seen" | tr -s ' ' '\n' | grep . | sort | uniq -c | awk '{printf " %s x%s", $2, $1}'))"
else
    fail "failover: with order-service-2 stopped all requests succeed" "ok=$ok bad=$bad replicas:$seen"
fi
# a POST still works too (the connect to the dead replica fails before anything is sent)
code=$(call -X POST -H "Authorization: Bearer $TOKEN" -H 'Idempotency-Key: failover-probe' "$BASE/v1/sales/$SALE/checkout")
[[ "$code" == 403 || "$code" == 202 ]] && pass "failover: a POST checkout still gets the app's answer ($code: sale not started)" || fail "failover: a POST checkout still gets the app's answer" "status $code: $(cat "$TMP/b")"
docker compose --profile app start order-service-2 >/dev/null 2>&1
back=no
for _ in $(seq 60); do
    if replicas_of 12 | grep -q '2:'; then back=yes; break; fi
    sleep 1
done
[ "$back" = yes ] && pass "failover: order-service-2 receives traffic again after it is started" || fail "failover: order-service-2 receives traffic again after it is started"

# --- 8. a full purchase through the gateway -----------------------------------
auth=(-H "Authorization: Bearer $TOKEN")
KEY="smoke-$(date +%s)-$RANDOM"
ticket=""; code=0
for _ in $(seq 90); do
    code=$(call -X POST "${auth[@]}" -H "Idempotency-Key: $KEY" "$BASE/v1/sales/$SALE/checkout")
    [ "$code" = 403 ] || break      # 403 = the sale has not started
    sleep 1
done
if [ "$code" = 202 ]; then
    ticket=$(jq -r .queue_ticket_id "$TMP/b")
    pass "purchase: checkout after the sale start is 202 (ticket $ticket)"
else
    fail "purchase: checkout after the sale start is 202" "status $code: $(cat "$TMP/b")"
fi
# a retry with the same key gets the same ticket (200)
code=$(call -X POST "${auth[@]}" -H "Idempotency-Key: $KEY" "$BASE/v1/sales/$SALE/checkout")
{ [ "$code" = 200 ] && [ "$(jq -r .queue_ticket_id "$TMP/b")" = "$ticket" ]; } && pass "purchase: a retry with the same Idempotency-Key is 200 with the same ticket" || fail "purchase: a retry with the same Idempotency-Key is 200 with the same ticket" "status $code"

poll_until() { # poll_until STATUS SECONDS
    local want="$1" i st
    for i in $(seq "$2"); do
        code=$(call "${auth[@]}" "$BASE/v1/orders/status/$ticket")
        st=$(jq -r .status "$TMP/b" 2>/dev/null)
        [ "$st" = "$want" ] && return 0
        sleep 1
    done
    return 1
}
if [ -n "$ticket" ] && poll_until AWAITING_PAYMENT 60; then
    pay_url=$(jq -r .payment_url "$TMP/b")
    pass "purchase: status reaches AWAITING_PAYMENT ($pay_url)"
    if [[ "$pay_url" == "$PROVIDER"/fake-pay/* ]]; then pass "purchase: payment_url points at the provider ($PROVIDER), not the gateway"; else fail "purchase: payment_url points at the provider" "$pay_url"; fi
    code=$(call -X POST "$pay_url/pay")
    if [[ "$code" == 303 && "$(header location)" == "$BASE/?sale=$SALE" ]]; then
        pass "purchase: the fake-pay form post is accepted (303 back to the sale on the frontend)"
    else
        fail "purchase: the fake-pay form post is accepted (303 back to the sale on the frontend)" "status $code, location: $(header location)"
    fi
    if poll_until CONFIRMED 60; then
        pass "purchase: status reaches CONFIRMED (the webhook came from the provider, through the gateway, to an order processor)"
    else
        fail "purchase: status reaches CONFIRMED" "last status: $(jq -r .status "$TMP/b")"
    fi
else
    fail "purchase: status reaches AWAITING_PAYMENT" "last: $(cat "$TMP/b")"
fi

# --- 9. rate limits -----------------------------------------------------------
# Checkout, same user: the bucket is 2 r/s plus a burst of 5, so 12 quick requests must hit it
n429=0; ra=""
for i in $(seq 12); do
    code=$(call -X POST "${auth[@]}" -H "Idempotency-Key: $KEY" "$BASE/v1/sales/$SALE/checkout")
    if [ "$code" = 429 ]; then n429=$((n429 + 1)); ra=$(header retry-after); body=$(cat "$TMP/b"); fi
done
if [ "$n429" -ge 1 ] && [ "$ra" = 1 ] && echo "$body" | jq -e .message >/dev/null; then
    pass "limits: 12 quick checkouts by one user: $n429 got 429 with Retry-After: 1 and a JSON message"
else
    fail "limits: a burst of checkouts by one user gets 429 with Retry-After" "429s: $n429, retry-after: $ra"
fi
# another user has their own bucket
sleep 1
TOKEN2=$(new_session)
code=$(call -X POST -H "Authorization: Bearer $TOKEN2" -H "Idempotency-Key: other-$KEY" "$BASE/v1/sales/$SALE/checkout")
[[ "$code" == 202 || "$code" == 200 ]] && pass "limits: a second user is not affected by the first user's bucket ($code)" || fail "limits: a second user is not affected by the first user's bucket" "status $code"

# Session, same IP: 1 r/s plus a burst of 5
n201=0; n429=0; ra=""
for i in $(seq 12); do
    code=$(call -X POST "$BASE/v1/session")
    if [ "$code" = 201 ]; then n201=$((n201 + 1)); fi
    if [ "$code" = 429 ]; then n429=$((n429 + 1)); ra=$(header retry-after); body=$(cat "$TMP/b"); fi
done
if [ "$n429" -ge 1 ] && [ "$ra" = 1 ] && echo "$body" | jq -e .message >/dev/null; then
    pass "limits: 12 quick sessions from one IP: $n201 created, $n429 got 429 with Retry-After: 1 and a JSON message"
else
    fail "limits: a burst of sessions from one IP gets 429 with Retry-After" "201s: $n201, 429s: $n429, retry-after: $ra"
fi
# Search, same IP: 5 r/s plus a burst of 20, so 40 quick searches (every q is new) must hit it
n429=0; ra=""
for i in $(seq 40); do
    code=$(call "$BASE/v1/sales/search?q=limit-$RANDOM-$i")
    if [ "$code" = 429 ]; then n429=$((n429 + 1)); ra=$(header retry-after); body=$(cat "$TMP/b"); fi
done
if [ "$n429" -ge 1 ] && [ "$ra" = 1 ] && echo "$body" | jq -e .message >/dev/null; then
    pass "limits: 40 quick searches from one IP: $n429 got 429 with Retry-After: 1 and a JSON message"
else
    fail "limits: a burst of searches from one IP gets 429 with Retry-After" "429s: $n429, retry-after: $ra"
fi
# static files and the status route are unaffected by the session bucket
code=$(call "$BASE/"); [ "$code" = 200 ] && pass "limits: static files are not rate limited" || fail "limits: static files are not rate limited" "status $code"

echo
if [ "$FAILED" = 0 ]; then echo "All checks passed."; else echo "Some checks FAILED."; fi
exit "$FAILED"
