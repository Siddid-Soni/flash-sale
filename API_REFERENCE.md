# API Reference

All examples go through the gateway at `http://localhost:8000` (see [README.md](README.md) to start the stack). Every route is under `/v1` except the fake provider's payment page. Four services answer: the **Order Service** replicas serve checkout and order status only (the case study's two endpoints); the **site service** serves sessions, sale details, the sale list, search and admin; the order processors serve the webhook; the fake provider's container serves the payment page, on its own address `http://localhost:8090`, outside the gateway. The gateway sends each `/v1` route to the right place. Bodies are JSON. Money is integer cents. Times are ISO 8601 in UTC.

## Conventions

**Errors.** Every error has the same body, whatever produces it (the app or the gateway):

```json
{"message": "sold out"}
```

Internal errors answer `500 {"message": "internal error"}`; the detail goes to the log only. A malformed JSON body is `400` with the parser's message, a wrong content type `415`.

**Authentication.** There are no accounts. `POST /v1/session` creates a guest and returns an HS256 JWT whose `sub` is the user id. Routes marked *user* need `Authorization: Bearer <token>`; the user always comes from the token, never from a body. A missing, malformed or wrongly signed token is `401 {"message": "missing bearer token"}` or `"invalid token"`; an expired one `"token expired"`. Tokens live `JWT_TTL_SECS` (3600).

**Request headers**

| Header | On | Meaning |
|---|---|---|
| `Authorization: Bearer <jwt>` | checkout, status | The caller |
| `Idempotency-Key` | checkout | Required, 1-255 characters. One key per purchase attempt, reused by every retry of it |
| `X-Admin-Token` | `/v1/admin/*` | Must equal the server's `ADMIN_TOKEN` (constant-time comparison) |
| `Fake-Signature` | webhook | `t=<unix seconds>,v1=<hex>`, set by the provider |
| `X-Challenge` | session | Passed to the session challenge hook; ignored while `SESSION_CHALLENGE=off` |
| `User-Agent` | everything under `/v1/` | The gateway refuses an empty one and known automation (see Gateway behaviour) |

**Response headers**

| Header | Where | Meaning |
|---|---|---|
| `Location` | checkout `200`/`202`, fake payment page `303` | Status URL / where the browser goes back to |
| `Retry-After` | `503` from checkout, every `429`, gateway `503` | Seconds to wait (always `1`) |
| `Cache-Control` | sale details `public, max-age=1, stale-while-revalidate=5`; sale list and search `public, max-age=5, stale-while-revalidate=30`; status `no-store`; gateway errors `no-store` | |
| `X-Replica` | every Order Service response when `REPLICA_ID` is set (compose sets `1`..`3`); not on the site service, the webhook or the payment page | Which replica answered |
| `X-Cache-Status` | `GET /v1/sales`, `/v1/sales/{id}`, `/v1/sales/search` | NGINX cache result: `MISS`, `HIT`, `EXPIRED`, `UPDATING`, `STALE` |

## Routes

| Method and path | Served by | Auth | Purpose |
|---|---|---|---|
| `POST /v1/session` | site | none | Create a guest session |
| `GET /v1/sales` | site | none | The latest sales (home page) |
| `GET /v1/sales/search?q=` | site | none | Search sales by name |
| `GET /v1/sales/{sale_id}` | site | none | Sale details |
| `POST /v1/admin/sales` | site | `X-Admin-Token` | Create a sale |
| `POST /v1/sales/{sale_id}/checkout` | Order Service | user | Reserve one unit |
| `GET /v1/orders/status/{queue_ticket_id}` | Order Service | user | Poll an order |
| `POST /v1/payments/webhook` | order processors | provider's address and signature | Payment provider events |
| `GET /fake-pay/{session_id}` | fake provider, `:8090` | none | The fake provider's payment page |
| `POST /fake-pay/{session_id}/pay` | fake provider, `:8090` | none | Pay |
| `POST /fake-pay/{session_id}/cancel` | fake provider, `:8090` | none | Cancel |

### `POST /v1/session`

Creates a guest user and a token. Clients call it when the page loads, before the sale opens.

```bash
curl -s -X POST http://localhost:8000/v1/session
```

| Status | Body |
|---|---|
| `201` | `{"token": "<jwt>", "expires_at": "2026-10-28T21:00:00Z", "user_id": 7}` |
| `403` | Only if a session challenge is configured and the `X-Challenge` header fails it |
| `429` | Gateway: more than 1 request per second per IP (burst 5) |

### `GET /v1/sales`

Public, served by the site service. The latest 50 sales, latest start time first, each with the same fields as `GET /v1/sales/{sale_id}`. Cached by the gateway for 5 s (a new sale shows within about 5 s, nothing is purged; the query string is ignored); read from PostgreSQL (one query) and Redis (one `MGET` for `sold_out`) on a miss.

```bash
curl -s http://localhost:8000/v1/sales
```

| Status | Body and headers |
|---|---|
| `200` | `[{"id": 1, "name": "Phone", "price_cents": 49900, "sale_start_time": "2026-10-28T20:00:00Z", "total_inventory": 500, "sold_out": false}, ...]` (`[]` when there are none); `Cache-Control: public, max-age=5, stale-while-revalidate=30`, `X-Cache-Status` |

### `GET /v1/sales/search?q=`

Public, served by the site service. The sales whose name contains `q`, any case, in the shape and order of the list, at most 50. The server trims `q`, lowercases it and uses its first 64 characters; `%`, `_` and `\` in it match themselves, not as wildcards. An empty or missing `q` answers like `GET /v1/sales`. Cached by the gateway for 5 s per `q` (other query parameters are ignored); a per-IP limit of 5 r/s (burst 20) applies, over which the gateway answers `429`.

```bash
curl -s 'http://localhost:8000/v1/sales/search?q=phone'
```

| Status | Body and headers |
|---|---|
| `200` | The same array as the list (`[]` when nothing matches); `Cache-Control: public, max-age=5, stale-while-revalidate=30`, `X-Cache-Status` |

### `GET /v1/sales/{sale_id}`

Public, served by the site service. Cached by the gateway for 1 to 2 seconds (NGINX counts whole seconds) and by the site process for 1 second; the query string is ignored.

```bash
curl -si http://localhost:8000/v1/sales/1
```

| Status | Body and headers |
|---|---|
| `200` | `{"id": 1, "name": "Phone", "price_cents": 49900, "sale_start_time": "2026-10-28T20:00:00Z", "total_inventory": 500, "sold_out": false}`; `Cache-Control: public, max-age=1, stale-while-revalidate=5`, `X-Cache-Status` |
| `404` | `{"message": "sale 9 not found"}` (a non-numeric id is also `404`) |

`sold_out` is true when `stock:{sale}` is 0 or missing. It can turn false again when the expiration worker expires an unpaid order and its unit returns to stock.

### `POST /v1/sales/{sale_id}/checkout`

Reserves one unit for the caller. This is the hot path: it touches PostgreSQL only when the Kafka publish fails.

```bash
TOKEN=$(curl -s -X POST http://localhost:8000/v1/session | jq -r .token)
curl -si -X POST http://localhost:8000/v1/sales/1/checkout \
  -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: $(uuidgen)"
```

Checks happen in this order: the replica's sold-out flag, then in Redis: sale known, sale started (Redis `TIME`), idempotency key, one live ticket per user (`user_live`), stock.

| Status | Meaning | Body and headers |
|---|---|---|
| `202` | A unit is reserved; the order is queued | `{"queue_ticket_id": "1.6f1c2a0e-...", "status_url": "/v1/orders/status/1.6f1c2a0e-..."}`, `Location: <status_url>` |
| `200` | A retry of an `Idempotency-Key` already seen | The same body and `Location` as the original `202` |
| `400` | `Idempotency-Key` missing or not 1-255 characters | `{"message": "an Idempotency-Key header of 1-255 characters is required"}` |
| `401` | No valid token | `{"message": "missing bearer token"}` |
| `403` | The sale has not started | `{"message": "the sale has not started"}` |
| `404` | Unknown sale | `{"message": "sale 9 not found"}` |
| `409` | The caller already holds or bought one | `{"message": "you already hold or bought one"}` |
| `410` | Sold out | `{"message": "sold out"}` |
| `429` | Gateway rate limit | see Gateway behaviour |
| `503` | The Kafka publish failed. The ticket was recorded as a `FAILED` order, the unit returned and the key forgotten, so a retry with the same key starts over | `{"message": "the checkout queue is unavailable, retry with the same key"}`, `Retry-After: 1` |

Notes:

- The ticket is `<sale>.<uuid v4>`. The sale id in it tells the status endpoint which sale the ticket belongs to.
- If the publish fails but the message had reached Kafka after all (an order for the ticket already exists), the answer is the normal `202` with the ticket, not `503`.
- Retrying with the same key is always safe. A retry of a successful checkout gets `200` and the same ticket, not `409`.
- Once a replica's sold-out flag is set (refreshed every 100 ms from Redis), it answers `410` without asking Redis, also to a winner retrying. The winner's ticket still works on the status endpoint.
- A replica that has not yet seen a sale asks Redis; unknown sales are never short-circuited.

### `GET /v1/orders/status/{queue_ticket_id}`

Only winners poll, every 2 seconds. The answer comes from the read cache; if that entry is missing it falls back to PostgreSQL. A ticket with neither is `404`: Redis keeps no record of a ticket (a ticket whose cache write was lost reads `404` until its order exists).

```bash
curl -s http://localhost:8000/v1/orders/status/1.6f1c2a0e-... -H "Authorization: Bearer $TOKEN"
```

`200` bodies (`Cache-Control: no-store`):

| `status` | Extra fields | Meaning |
|---|---|---|
| `PENDING` | `position` | In the queue; no order exists yet (`PENDING` is never stored in PostgreSQL: it comes from the read cache only). `position` 1 is next, `0` is being processed. It counts messages on the same Kafka partition, other sales' included, so it is an upper bound. Missing if the entry does not know its partition |
| `AWAITING_PAYMENT` | `payment_url`, `expires_at` | Pay at `payment_url` before `expires_at` (RFC 3339, milliseconds) (answered from the order row when the cache entry is gone) |
| `CONFIRMED` | | Paid in time (PostgreSQL decided and counted it in `products.sold`). Final. The unit stays taken: the expiration worker never returns a confirmed order's stock |
| `EXPIRED` | | The payment window ran out; the expiration worker expired the order and returned the unit. Final |
| `FAILED` | | The checkout never became an order: the publish failed and the unit was returned at once, or the order could not be created. Final |
| `REFUNDED` | | A payment arrived but could not be honoured (the order was already `EXPIRED` or `FAILED`, or the PostgreSQL guard refused an in-time payment) and was refunded. Final. Replaces `EXPIRED` and `FAILED`, never `CONFIRMED` |

```json
{"status": "PENDING", "position": 42}
{"status": "AWAITING_PAYMENT", "payment_url": "http://localhost:8090/fake-pay/fake_cs_3f2a...", "expires_at": "2026-10-28T20:05:01.234Z"}
{"status": "CONFIRMED"}
```

| Status | Body |
|---|---|
| `401` | No valid token |
| `403` | `{"message": "this ticket belongs to someone else"}` |
| `404` | `{"message": "no such ticket"}` (also for a malformed ticket) |
| `429` | Gateway: more than 2 requests per second per user (burst 10) |

### `POST /v1/payments/webhook`

Called by the payment provider, not by clients, and served by the order processors (`PROCESSOR_HTTP_ADDR`, port 8081), not by the Order Service. The body is the event exactly as signed.

**Who may call it.** Two layers. The gateway admits only the provider's addresses (`gateway/payment_provider_ips.conf`: locally the provider container's fixed address `172.28.0.50`; in production the provider's published webhook IPs, e.g. Stripe's) and answers everyone else `403 {"message": "the payment webhook is only reachable from the payment provider"}`; it checks the real TCP peer, so a forged `X-Forwarded-For` does not help. The processor then verifies the signature (below), so an address on the list that is not the provider still cannot forge an event.

| Status | Meaning |
|---|---|
| `200` `{"received": true}` | The event is stored in `payment_inbox` (also when it was stored before). A success event is handled at once: PostgreSQL decides between the payment and the deadline, and the order is `CONFIRMED`, or `REFUNDED` and refunded through the provider. If handling fails after the event was stored, the response is still `200`: the order processors' inbox loop retries it, so the provider need not |
| `400` `{"message": "invalid webhook"}` | Signature missing, malformed, wrong, older or newer than 5 minutes, or the event is not readable. Nothing is stored |
| `403` | Gateway: not from the provider's address |
| `500` | The event could not be stored (database down). The provider retries |

**Signature.** Header `Fake-Signature: t=<unix seconds>,v1=<hex>`, where `v1` is `HMAC-SHA256(PAYMENT_WEBHOOK_SECRET, "<t>.<raw body>")` in lowercase hex. Several `v1=` entries are accepted (key rotation). The timestamp must be within 300 seconds of the receiver's clock, so a captured request cannot be replayed later; the provider signs again for every delivery attempt, so a retry hours later still passes.

```bash
BODY='{"id":"evt_x","type":"checkout.session.completed","created":1793000000,"data":{"object":{"session_id":"fake_cs_x","ticket":"1.abc","amount_cents":49900,"payment_id":"fake_pi_x"}}}'
T=$(date +%s)
SIG=$(printf '%s.%s' "$T" "$BODY" | openssl dgst -sha256 -hmac "$PAYMENT_WEBHOOK_SECRET" -hex | sed 's/^.* //')
curl -s -X POST http://localhost:8000/v1/payments/webhook -H "Fake-Signature: t=$T,v1=$SIG" \
  -H 'Content-Type: application/json' -d "$BODY"
```

That event is made up, but it is stored and a processor keeps retrying it because no such order exists; use it only on a throwaway database (`DELETE FROM payment_inbox WHERE provider_event_id = 'evt_x'` removes it).

**Event shape**

```json
{
  "id": "evt_<hex>",
  "type": "checkout.session.completed",
  "created": 1793000000,
  "data": {"object": {"session_id": "fake_cs_<hex>", "ticket": "1.<uuid>", "amount_cents": 49900, "payment_id": "fake_pi_<hex>"}}
}
```

| `type` | Sent when | Effect |
|---|---|---|
| `checkout.session.completed` | The user paid (`payment_id` is set) | The order is confirmed if its deadline is still ahead on PostgreSQL's clock. If the order is already `EXPIRED` or `FAILED` it is refunded. If its deadline has passed but the expiration worker has not expired it yet, nothing is decided: the event stays in the inbox and is retried, and is refunded once the worker has expired the order |
| `checkout.session.payment_failed` | The user cancelled (`payment_id` is `null`) | Nothing; the order runs out by itself |

`id` and `payment_id` are derived from the session, so a redelivered event has the same id and the inbox's unique `provider_event_id` drops the duplicate. The fake provider delivers at least once: it retries with a delay of 1 s doubling to 1 h and gives up after 24 h. The webhook route has no rate limit and no bot check at the gateway (only the address check).

### `POST /v1/admin/sales`

Served by the site service. Creates a product (the sale) and loads `stock:{id}` and `sale_start:{id}` into Redis (Redis does not count sales; `products.sold` in PostgreSQL does). From outside the compose network the gateway answers `403`; run it inside the gateway container.

```bash
docker compose exec -T gateway wget -qO- \
  --header 'X-Admin-Token: dev-admin-token' --header 'Content-Type: application/json' \
  --post-data '{"name":"Phone","price_cents":49900,"total_inventory":500,"sale_start_time":"2026-10-28T20:00:00Z"}' \
  http://127.0.0.1/v1/admin/sales
```

Body: `name` (1-255 characters), `price_cents` (positive), `total_inventory` (1 to 10,000,000), `sale_start_time` (ISO 8601).

| Status | Body |
|---|---|
| `201` | The product: `{"id", "name", "price_cents", "total_inventory", "sold": 0, "sale_start_time", "created_at"}` |
| `400` | Validation message, e.g. `{"message": "price_cents must be positive"}` |
| `403` | `{"message": "a valid X-Admin-Token header is required"}` from the app, or `{"message": "admin routes are only reachable from the internal network"}` from the gateway |

### Fake payment page

`GET /fake-pay/{session_id}` is the page the user is sent to (`payment_url`): amount, ticket, deadline, a Pay and a Cancel button. It is served by the `fake_payment_provider` container on its own address (`http://localhost:8090`, `PAYMENT_PAGE_BASE_URL`), not through the gateway, as a real provider's hosted page would be; it is HTML with `Cache-Control: no-store`. `POST /fake-pay/{session_id}/pay` and `/cancel` settle the session and queue the webhook event.

| Status | Meaning |
|---|---|
| `200` | The page, for an `OPEN` session |
| `303` | Pay or cancel done (or already done the same way): `Location: {FRONTEND_URL}/?sale=<id>` |
| `404` | Unknown session |
| `409` | The session is already `PAID`, `CANCELED` or `REFUNDED` (page says so) |
| `410` | The session is `EXPIRED`; payment is no longer possible |

The provider does not know our deadline. It accepts a payment on any `OPEN` session, even after the deadline, and the backend then refunds. It refuses only `EXPIRED` sessions, which the expiration worker sets after expiring an order. Sessions live 7 days so a late payment can still be refunded.

### Fake provider API (internal)

Not routed by the gateway: only the order processor and the expiration worker call it, on the compose network (`PAYMENT_PROVIDER_URL`, `http://fake-payment-provider:8090`; the stand-in publishes one port, 8090, for the payment page, so this API is on the host too, which a real provider's would not be). It is shaped like Stripe's and every call is idempotent. Errors have the usual `{"message": ...}` body.

| Call | Meaning |
|---|---|
| `POST /v1/checkout/sessions` with header `Idempotency-Key` (required) and body `{"ticket", "amount_cents", "expires_at"}` (RFC 3339) | Creates a session; `200 {"id", "url", "state"}`. The same ticket always gets the same session, whatever the key or the session's state. `400` for a missing key, a negative amount or a malformed body |
| `POST /v1/checkout/sessions/{id}/expire` | Stops an open session from accepting payment. `200 {"id", "state"}` also for an unknown, paid or already expired session |
| `POST /v1/refunds` with body `{"session_id"}` | Refunds a paid session: `200`, and `200` again when repeated. `409` for a session that was not paid or is unknown |

## Order states

A ticket has one of six statuses (`status:<ticket>`, API). `PENDING` is the sixth: it exists only in the read cache, while the checkout event waits in Kafka. The order row is created later, directly as `AWAITING_PAYMENT` (or as `FAILED` by the Order Service when a publish fails), so `orders.status` has the other five. PostgreSQL decides every outcome, between a payment and the payment deadline too, on its own clock. Redis gates admission only: it holds `stock:{sale}`, the user's `user_live` marker and the idempotency key, and no per-ticket state.

```
checkout --> PENDING --(order created)--> AWAITING_PAYMENT --(paid before expires_at)--> CONFIRMED
 (Redis: unit,  |                                |
  user_live)    | publish failed                 | unpaid: 5 min after the link
                v                                v
             FAILED                           EXPIRED          (a payment after that: REFUNDED)
```

| Status | Waiting for | Leaves by |
|---|---|---|
| `PENDING` (never in PostgreSQL) | An order processor (no clock runs; no order row yet) | The processor inserts the order as `AWAITING_PAYMENT`; or the publish failed: the Order Service inserts a `FAILED` order and returns the unit at once |
| `AWAITING_PAYMENT` | The user to pay, until `expires_at` | Payment: the order becomes `CONFIRMED`; or, at `expires_at`, the expiration worker expires the order and returns the unit (`INCR stock`, clear `user_live`) |
| `CONFIRMED` | | Final. The unit stays taken, `user_live` stays until it expires a day after the sale, the sale is counted in `products.sold` |
| `EXPIRED` | | Final (a late payment still turns it into `REFUNDED`, moving no stock) |
| `FAILED` | | Final (same) |
| `REFUNDED` | | Final. Moves no stock: the worker already returned the unit of an expired order, and an in-time payment refused by the guard never had a unit to sell |

The status shown to clients only moves forward: `PENDING` < `AWAITING_PAYMENT` < `CONFIRMED`, `EXPIRED`, `FAILED` < `REFUNDED`; a delayed write never overwrites a later state, and the first final state wins (except that `REFUNDED` replaces `EXPIRED` and `FAILED`). When the read cache has no entry the status endpoint reads the order row from PostgreSQL.

The rare failures this design accepts, each of which can only sell fewer units than there are, are in [docs/FAILURE_CASES.md](docs/FAILURE_CASES.md).

## Gateway behaviour

What a client sees from NGINX before the app answers (details in [gateway/README.md](gateway/README.md)):

| Case | Answer |
|---|---|
| Over a rate limit | `429 {"message": "too many requests, retry after a moment"}` with `Retry-After: 1`. Limits: `POST /v1/session` 1 r/s per IP (burst 5); `GET /v1/sales/search` 5 r/s per IP (burst 20); checkout 2 r/s per user (burst 5) and 20 r/s per IP (burst 40); status 2 r/s per user (burst 10); 100 concurrent connections per IP on `/v1/*`. The webhook and static files are not limited |
| Empty `User-Agent`, or one containing `bot`, `crawler`, `spider`, or matching `python-requests`, `python-urllib`, `go-http-client`, `scrapy`, `headlesschrome`, `phantomjs` | `403 {"message": "automated clients are not allowed"}` on `/v1/` (not the webhook). `curl` and `wget` pass |
| `/v1/admin/*` from outside the compose network (the host included, also with a forged `X-Forwarded-For`) | `403 {"message": "admin routes are only reachable from the internal network"}` |
| `/v1/payments/webhook` from any address but the payment provider's (the host included, also with a forged `X-Forwarded-For`) | `403 {"message": "the payment webhook is only reachable from the payment provider"}` |
| A replica is down or does not answer | A failed or timed-out connect is retried on the next replica; a request that was already sent is never repeated. NGINX's own 502, 503 and 504 become `503 {"message": "service temporarily unavailable, retry shortly"}` with `Retry-After: 1`. An app `503` (checkout queue) is passed on as it is |

A client that gets `429` or `503` should wait `Retry-After` seconds and retry with the same `Idempotency-Key`; the frontend does. `202`, `409` and `410` are the app's real answers and are never replaced by the gateway.

Static files: `/assets/*` is `public, max-age=31536000, immutable`, `index.html` is `no-cache`, everything is compressed.
