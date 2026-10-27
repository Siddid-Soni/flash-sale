# API Reference

All bodies are JSON unless noted. Errors are `{"message": "..."}` with the status codes listed per endpoint (a malformed JSON body or query string is `400` with a `message` too); `500` hides internal details (they are logged). Endpoints marked **auth** need `Authorization: Bearer <jwt>` from `POST /session` or `POST /auth/login`, and return `401` without a valid one.

A purchase goes: create a guest session, join the product's waiting room, poll status until `ACTIVE`, reserve units, pay, then poll the order until it is `CONFIRMED`. See [ROADMAP.md](ROADMAP.md) for how each step works.

## Sessions and Authentication

### `POST /session`
Creates a guest user (no username or password) and a token for it. Clients call it when the product page loads, before the sale opens, so the opening spike does not hit this insert.
- `201 {"token", "expires_at", "user_id"}`: the JWT, valid for `JWT_TTL_SECS`

### `POST /auth/register`
Body `{"username", "password"}`: username 3-50 characters of letters, digits, `_`, `-`, `.`; password 8-128 characters.
- `201 {"id", "username"}`
- `400` invalid username or password, `409` username taken

### `POST /auth/login`
Body `{"username", "password"}`. Guests cannot log in.
- `200 {"token", "expires_at"}`: the JWT, valid for `JWT_TTL_SECS`
- `401` wrong username or password

### `GET /auth/me` (auth)
- `200 {"id"}`: the caller's user id

## Products

These are unauthenticated (there is no admin role yet).

### `POST /products`
Body `{"sku", "name", "description"?, "image_url"?, "price_cents", "stock", "max_per_user"?, "sale_opens_at", "sale_ends_at"}`. `sku` is 1-64 characters of letters, digits, `_`, `-`; `name` is not empty; `price_cents` is not negative; `stock` is 1 to 1,000,000 units; `max_per_user` (units one user may hold or buy) is at least 1 and defaults to `DEFAULT_MAX_PER_USER`. Both times in the future, `sale_opens_at` before `sale_ends_at`. Creates the product and its inventory row, its stock counters in Redis and its Kafka topic.
- `201` the product (`id`, `sku`, `name`, `description`, `image_url`, `price_cents`, `max_per_user`, `sale_opens_at`, `sale_ends_at`, `status`, `created_at`) plus `stock`
- `400` invalid field or times, `409` sku taken

The status starts `SCHEDULED`, becomes `ON_SALE` at `sale_opens_at` and `CLOSED` at `sale_ends_at` (both done by the admission worker).

### `GET /products`
The catalog, soonest `sale_opens_at` first. Product details do not change during a sale, so browsers and CDNs may cache the response for 30 seconds. Query `limit` (default 50, clamped to 1-100) and `offset` (default 0).
- `200 {"products": [...]}`: each as in `POST /products`, without `stock`; canceled products are left out

### `GET /products/{id}`
Cacheable for 30 seconds.
- `200` the product, as in `POST /products` without `stock`
- `404` product not found

### `GET /products/{id}/stock`
The units left, for the product page. Reads Redis only and is cacheable for 1 second, so the crowd refreshing it is absorbed by CDNs and proxies. Unauthenticated.
- `200 {"product_id", "available", "sold", "total", "closed"}`: `available` counts units neither held nor sold, `sold` units paid for, `total` the units the sale started with
- `404` product not found (unknown to Redis)

### `POST /products/{id}/close`
Ends the sale now instead of at `sale_ends_at`. Final and idempotent: joins then get `410`, waiting users see `CLOSED`, the admission worker stops.
- `200 {"product_id", "status": "CLOSED"}`
- `404` product not found

## Waiting Room

### `POST /waiting-room/{product}/join` (auth)
Idempotent: returns the caller's current token while it is in play. Before `sale_opens_at` the token waits in a lobby that is shuffled into a random order when the sale opens, so joining first before the opening gives no advantage; after that, first come, first served. Once a turn is over (the reservation was released, expired or paid, or the token expired unused), joining again queues a new token at the back of the line.
- `202 {"token_id", "status", "seq"?}`: `status` is `LOBBY` or `QUEUED` (or the current token's status); `seq` is the place in line, absent in the lobby
- `404` product not found, `410` sale closed

### `GET /waiting-room/{product}/status` (auth)
The caller's token, found by user; one Redis round trip, meant to be polled.
- `200 {"token_id", "status", ...}` with, by status:
  - `LOBBY`: `sale_opens_at`
  - `QUEUED`: `position` (1 is next), `estimated_wait_secs` (absent until measured, or while admission is stalled), and `reason` when stalled: `all_units_held` (every unit in stock is held; wait for holds to end) or `sold_out` (every unit is paid for; only a cancellation would reopen it)
  - `ACTIVE`: `expires_in_ms`, the time left to reserve
  - `USED`: the turn was spent on a reservation; `can_rejoin` once that reservation is over
  - `EXPIRED`: the token ran out; `can_rejoin`
  - `CLOSED`: the sale closed while waiting
- `404` never joined

## Reservations

### `POST /reservations` (auth)
Body `{"product_id", "quantity"?}`; `quantity` defaults to 1. Holds the units for `HOLD_TTL_SECS`, all or none. Needs an `ACTIVE` token; a successful hold uses it up (the next person in line is admitted). A refusal (out of stock, limit, invalid quantity) leaves the token `ACTIVE` to try again, for example with a smaller quantity, or after a hold is released. Redis only.
- `201 {"reservation_id", "product_id", "quantity", "expires_at"}`
- `200` the same, for a retry after the hold was made; `quantity` is that of the hold, whatever the retry asked for
- `400` quantity below 1, not a whole number or above the product's `max_per_user` (`quantity must be between 1 and {max}`)
- `403` not admitted, or the turn was already used
- `409` `only {n} left` or `sold out` (also when Redis does not know the product, for example while it is being rebuilt), or the user would exceed `max_per_user` for the product
- `410` sale closed

### `DELETE /reservations/{id}` (auth)
Releases the caller's hold; the units are back in stock at once. The turn stays used.
- `204`
- `400` invalid reservation id, `403` not the caller's, `404` no live hold, `409` payment has started

## Payment

### `POST /reservations/{id}/pay` (auth)
Header `Idempotency-Key` (1-255 characters). Extends the hold once by `PAYMENT_EXTENSION_SECS` (after this it cannot be released), records the order (`PAYMENT_PENDING`, `unit_price_cents` taken from the product, `total_cents` = price times quantity) and creates the payment intent. The order's id is the reservation id. Every retry returns the same order and intent. The outcome arrives later from the provider, so the answer is always `202`: the payment worker then confirms the order (`CONFIRMED`, units sold) or, if the hold ran out first, refunds it (`REFUNDED`). Poll `tracking_url` for the result.
- `202 {"order_id", "reservation_id", "status", "payment_intent_id", "amount_cents", "expires_at", "tracking_url"}` with header `Location: /orders/{order_id}` (the same as `tracking_url`). A retry gets `202` too, with the order's current `status`.
- `400` missing key or invalid reservation id, `403` not the caller's, `409` already paid, `410` hold expired or released

## Orders

### `GET /orders/{id}` (auth)
The caller's order. Never cached. While the status is `PAYMENT_PENDING` the response carries `Retry-After: 1`, the interval to poll at. A reservation that has not been paid yet has no order.
- `200 {"id", "product_id", "quantity", "unit_price_cents", "total_cents", "status", "created_at", "updated_at", "expires_at"}`: `status` is `PAYMENT_PENDING`, `CONFIRMED`, `EXPIRED`, `REFUNDED` (or `CANCELED`, which nothing sets yet)
- `400` invalid order id, `403` not the caller's, `404` order not found

### `GET /orders` (auth)
The caller's orders, newest first. Never cached. Query `limit` (default 20, clamped to 1-100).
- `200 {"orders": [...]}`: each as in `GET /orders/{id}`
