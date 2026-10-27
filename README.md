# Flash Sale

A backend and storefront for a flash sale: 500 smartphones go on sale at a fixed time, more than 100,000 requests reach checkout in the first 10 seconds, and exactly 500 units are sold, never more. Winners get an answer in milliseconds (`202 Accepted` and a queue ticket), everyone else gets `410 Gone` just as fast, and the slow parts (Kafka, order processing, payment, PostgreSQL) only ever see the roughly 500 winners. Stock is reserved by one atomic Lua script in Redis; PostgreSQL has a conditional `UPDATE` as a second line of defence.

The design is in [ROADMAP.md](ROADMAP.md). How it answers the placement case study is in [CASE_STUDY.md](CASE_STUDY.md). The HTTP API is in [API_REFERENCE.md](API_REFERENCE.md).

## Architecture

| Component | What it does | Code |
|---|---|---|
| Client | Preact app: countdown, buy button (locks on the first click), queue and payment states, status polling every 2 s | `web/` |
| Gateway | NGINX on `:8000`: serves the frontend, balances `/v1/` over three replicas, rate limits, bot filter, hides `/v1/admin/`, caches sale details for 1 s, sends `/fake-pay/` to the provider and the webhook (from the provider's address only) to the order processors | `gateway/` |
| Order Service x3 | Stateless HTTP API: guest sessions, sale details, checkout, order status, admin | `src/bin/order_service.rs`, `src/api/` |
| Redis (inventory) | Stock counter, holds, pending and expiry queues, processor progress; append-only file on | `scripts/*.lua`, `src/cache.rs` |
| Redis (read cache) | `status:<ticket>`, what clients poll. The same instance locally (`READ_CACHE_URL`) | `src/cache.rs` |
| Kafka | Topic `orders.checkout` (6 partitions): checkout events from the Order Service to the processors | `src/queue.rs` |
| Order Processor | Turns each checkout event into an order and a payment link; serves the provider's webhook (`POST /v1/payments/webhook` on `:8081`) and confirms or refunds a paid order at once; retries unprocessed payment events from the inbox | `src/bin/order_processor.rs`, `src/processor.rs`, `src/inbox.rs` |
| Expiration Worker | At each payment deadline, asks PostgreSQL what became of the hold: releases the unpaid ones, counts the paid ones in `sold:{sale}`. Also releases leaked holds. The only place stock is returned (besides immediate compensation) | `src/expiry.rs` |
| Fake payment provider | A service of its own (`fake_payment_provider`, fixed address `172.28.0.50`), Stripe-like: the payment page `/fake-pay/{id}`, an HTTP API to create a session (idempotent), expire and refund, and signed webhooks with retries. Owns the `fakepay:*` Redis keys | `src/bin/fake_payment_provider.rs`, `src/payment/` |
| PostgreSQL 16 | Durable record: `users`, `products`, `orders`, `payment_inbox` | `migrations/` |

```
Browser --> NGINX :8000 --> Order Service 1..3 --> Redis (checkout.lua)  -- 202 / 410 --> Browser
                |                  |
                |                  +--> Kafka orders.checkout --> Order Processor --> PostgreSQL
                |                                                      |  ^   \--> HTTP API --> fake provider
                |                                                      |  |                         |
                +--> /fake-pay/ ------------------------------------------------------------------>|
                +--> /v1/payments/webhook (provider's address only) --> Order Processor <-- webhook-+
Expiration Worker --> PostgreSQL (decides), Redis (release.lua, settle_hold.lua), Kafka offsets (read only)
```

### Request flow

1. The client loads the app and `GET /v1/sales/{id}` (cached 1 s at the gateway), creates a guest session with `POST /v1/session`, and waits for the start time.
2. `POST /v1/sales/{id}/checkout` with an `Idempotency-Key`. `checkout.lua` runs once in Redis: it checks the start time, the key, one-per-user and stock, takes one unit and creates a `PENDING` hold. No hold means `409` or `410`.
3. The Order Service publishes the checkout event to Kafka (`acks=all`, idempotent producer), stores `status:<ticket>` and answers `202 {queue_ticket_id, status_url}`. If the publish fails, the hold is released at once and the answer is `503`.
4. An order processor inserts the order, creates the payment session and moves the hold to `AWAITING_PAYMENT` (the 5-minute payment window starts here, not at checkout). The client, polling the status endpoint, gets `payment_url` and goes to pay.
5. The provider sends a signed webhook. It reaches an order processor through the gateway (only from the provider's address, and only with a valid signature). The processor stores the event in `payment_inbox` and decides at once, in one PostgreSQL transaction: `UPDATE orders SET status = 'CONFIRMED' WHERE ... AND status = 'AWAITING_PAYMENT' AND payment_session_id = $2 AND expires_at > now()` together with `UPDATE products SET sold = sold + 1 WHERE id = $1 AND sold < total_inventory`. The payment path writes no inventory key in Redis, only the read-cache status. If the order cannot be confirmed (the deadline passed on PostgreSQL's clock, the order is already `EXPIRED` or `FAILED`, or the guard refused), the same transaction marks it `REFUNDED` and the processor refunds through the provider; the inbox row is marked processed only when that worked, and the inbox loop retries anything unfinished.
6. The Expiration Worker looks at each hold at its payment deadline and asks PostgreSQL (`UPDATE orders SET status = 'EXPIRED' ... AND status = 'AWAITING_PAYMENT' AND expires_at <= now()`, the mirror image of the confirmation; the row lock and the one clock give exactly one winner). An expired order's unit goes back to stock; a `CONFIRMED` order's hold is closed and counted in `sold:{sale}` (the only place it is counted, so `sold:{sale}` follows `products.sold` up to one payment window later); a `REFUNDED` one is released, or dropped without stock if PostgreSQL had no unit for it. It also returns the stock of holds whose Kafka message can no longer arrive.

## Quick start

Needs Docker with compose v2. The first build compiles the Rust binaries and the frontend, so it takes a while.

```bash
docker compose up -d                          # PostgreSQL 16, Redis 7, Kafka
docker compose --profile app up -d --build    # + 3 Order Services, order processor, expiration worker, fake payment provider, NGINX
```

Open <http://localhost:8000>. There is no sale yet; create one from inside the gateway container (the host is refused with `403` on purpose). This opens in one minute:

```bash
docker compose exec -T gateway wget -qO- \
  --header 'X-Admin-Token: dev-admin-token' --header 'Content-Type: application/json' \
  --post-data "{\"name\":\"Phone\",\"price_cents\":49900,\"total_inventory\":500,\"sale_start_time\":\"$(date -u -d '+60 seconds' +%FT%TZ)\"}" \
  http://127.0.0.1/v1/admin/sales
```

The answer contains the sale `id`. The frontend shows sale `1` by default; open `http://localhost:8000/?sale=<id>` for another. `dev-admin-token` is `ADMIN_TOKEN` in `.env`. Pay on the fake payment page (served by the provider container through the gateway); the webhook arrives within a second or so and the order becomes `CONFIRMED`.

Stop only the application with `docker compose --profile app stop gateway order-service-1 order-service-2 order-service-3 order-processor expiration-worker fake-payment-provider` (`--profile app stop` alone also stops PostgreSQL, Redis and Kafka). `docker compose down -v` also removes the data.

## Running locally with cargo

```bash
docker compose up -d                  # infrastructure only
cargo run --bin order_service         # :8080; runs the migrations and creates the topic, so start it first
cargo run --bin fake_payment_provider # :8090; the payment page and the provider API, delivers webhooks
cargo run --bin order_processor       # :8081 serves the webhook (the provider posts to it directly)
cargo run --bin expiration_worker
```

`.env` holds the development values and is loaded by every binary (`PUBLIC_BASE_URL=http://localhost:8080`, the payment page at `PAYMENT_PAGE_BASE_URL=http://localhost:8090`, the provider's API at `PAYMENT_PROVIDER_URL=http://127.0.0.1:8090`, webhooks to `PAYMENT_WEBHOOK_URL=http://127.0.0.1:8081/v1/payments/webhook`). Without the gateway, call the API on `:8080` directly, including the admin route:

```bash
curl -s -X POST http://localhost:8080/v1/admin/sales -H 'X-Admin-Token: dev-admin-token' \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"Phone\",\"price_cents\":49900,\"total_inventory\":500,\"sale_start_time\":\"$(date -u -d '+60 seconds' +%FT%TZ)\"}"
```

Frontend with hot reload: `cd web && npm install && npm run dev` (<http://localhost:5173>, proxies `/v1` to `:8080`; `/fake-pay` is proxied to `:8080` too, but locally the provider serves it on `:8090`, so open payment links as given, `http://localhost:8090/fake-pay/...`). Set `FRONTEND_URL=http://localhost:5173` in `.env` so the payment page sends the browser back to the Vite server. More in [web/README.md](web/README.md).

## Configuration

Every binary reads its environment (and `.env`) through `src/config.rs`. A variable without a default is required: the process stops at startup if it is missing.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | required | PostgreSQL connection string |
| `JWT_SECRET` | required | HS256 key for guest tokens |
| `ADMIN_TOKEN` | required | Value of `X-Admin-Token` for `/v1/admin/*` (compared in constant time; empty never matches) |
| `PAYMENT_WEBHOOK_SECRET` | required | HMAC-SHA256 key that signs webhooks (the provider) and verifies them (the order processor) |
| `BIND_ADDR` | `127.0.0.1:8080` | Where the Order Service listens (`0.0.0.0:8080` in containers) |
| `REDIS_URL` | `redis://127.0.0.1:6379` | Inventory Redis: stock, holds, queues, processor progress. The fake provider keeps its `fakepay:*` keys in it too (and nothing else touches them) |
| `READ_CACHE_URL` | `REDIS_URL` | Read cache Redis: `status:<ticket>` and queue progress. Separate cluster in production |
| `REDIS_CONNECTIONS` | `8` | Redis connections per Order Service (each multiplexes many requests) |
| `KAFKA_BROKERS` | `localhost:9092` | Bootstrap servers |
| `CHECKOUT_TOPIC` | `orders.checkout` | Topic for checkout events |
| `CHECKOUT_PARTITIONS` | `6` | Partitions the topic is created with (an existing topic is left alone); the most processors that can share the work |
| `KAFKA_DELIVERY_TIMEOUT_MS` | `30000` | How long the producer retries a publish before it reports failure. Also the safety margin before a leaked hold is released |
| `DB_POOL_SIZE` | `10` | PostgreSQL connections per process (compose gives each Order Service 5). The order processor needs at least 2 |
| `JWT_TTL_SECS` | `3600` | Lifetime of a guest token |
| `PAYMENT_WINDOW_SECS` | `300` | Time a user has to pay once the link is ready |
| `PAYMENT_PROVIDER` | `fake` | Only `fake` is implemented; any other value stops the process at startup |
| `PAYMENT_PROVIDER_URL` | `http://127.0.0.1:8090` | Where the order processor and the expiration worker call the provider's HTTP API (`http://fake-payment-provider:8090` in compose) |
| `PROVIDER_BIND_ADDR` | `127.0.0.1:8090` | Where `fake_payment_provider` listens (`0.0.0.0:8090` in compose) |
| `FAKE_PAYMENT_DELAY_MS` | `2000` | Read but not used by any code; the fake provider's webhooks go out within about 250 ms |
| `PUBLIC_BASE_URL` | `http://localhost:8080` | Where clients reach the API; the default of `PAYMENT_PAGE_BASE_URL` and `FRONTEND_URL` |
| `PAYMENT_PAGE_BASE_URL` | `PUBLIC_BASE_URL` | Where users reach the provider's payment page; the page URL is `{PAYMENT_PAGE_BASE_URL}/fake-pay/{session}` (`.env` points it at the provider's own port, compose at the gateway) |
| `PAYMENT_WEBHOOK_URL` | `http://127.0.0.1:8081/v1/payments/webhook` | Where the fake provider delivers webhooks, plain `http://` only (compose: `http://gateway/v1/payments/webhook`, through the gateway to the order processors) |
| `PROCESSOR_HTTP_ADDR` | `0.0.0.0:8081` | Where the order processor serves the webhook, `POST /v1/payments/webhook` (nothing else) |
| `FRONTEND_URL` | `PUBLIC_BASE_URL` | Where the payment page returns the browser to (`/?sale=<id>`) |
| `PROCESSOR_CONCURRENCY` | `32` | Checkout messages one processor works on at once, per partition |
| `PROCESSOR_MAX_PER_SEC` | `0` | Cap on messages one processor starts per second (0 = unlimited); used by the backlog test |
| `EXPIRY_BATCH` | `200` | Due holds settled per sale and pass; a full batch that made progress is followed at once by another |
| `EXPIRY_INTERVAL_MS` | `250` | Sleep between idle passes of the unpaid-hold loop |
| `EXPIRY_LEAKED_EVERY_MS` | `1000` | How often leaked holds are checked (reads consumer offsets from Kafka) |
| `EXPIRY_SWEEP_SECS` | `60` | How often orders left behind by a crashed worker are swept |
| `REPLICA_ID` | unset | If set, every response carries `X-Replica: <id>` |
| `SESSION_CHALLENGE` | `off` | Hook for a proof of work or CAPTCHA on `POST /v1/session`; only `off` exists |
| `HTTP_MAX_CONNECTIONS` | `25000` | Connections accepted per worker thread |
| `HTTP_BACKLOG` | `4096` | Listen queue length, so a burst waits instead of being refused |
| `HTTP_KEEP_ALIVE_SECS` | `75` | Idle keep-alive time; keep it above the gateway's upstream `keepalive_timeout` (60 s) |
| `RUST_LOG` | `info` | Log level (`env_logger`) |
| `TEST_REDIS_URL` | `redis://127.0.0.1:6379/15` | Redis database used by `cargo test` only |

Two more switches belong to compose, not to the binaries: `GATEWAY_TRUST_XFF=1` makes the gateway take the client address from `X-Forwarded-For` (load tests only), and `PAYMENT_WINDOW_SECS` / `PROCESSOR_MAX_PER_SEC` can be set in the shell when running `docker compose` because `docker-compose.yml` passes them on.

## Tests

```bash
docker compose up -d     # PostgreSQL, Redis, Kafka must be running
cargo test               # integration tests in tests/, unit tests in src/
cargo clippy --all-targets
```

The integration tests use Redis database 15 and create a unique Kafka topic per run (it stays on the local broker). Do not run `cargo test` while the application containers are up: they share PostgreSQL's connections, the payment inbox and the Kafka consumer group (an order processor would handle the tests' payment events, a worker would settle their holds), and the expiry lag test fails. The frontend has its own checks: `cd web && npm test && npm run budgets`. The tests start the fake provider's API and payment page on a free port of their own, so nothing but PostgreSQL, Redis and Kafka needs to run. `gateway/smoke.sh` checks the gateway end to end against the running stack (including that the webhook is refused from the host and a full purchase through the provider and the order processor); it creates a 5-unit sale and briefly stops `order-service-2` to test failover.

## Load tests

[load/README.md](load/README.md) describes the scripts; every run appends its settings, latency table and checks to [load/RESULTS.md](load/RESULTS.md).

| Script | What it shows | Result (one machine, 20 cores) |
|---|---|---|
| `load/flash.sh 100000` | 100,000 users arrive within 10 s for 500 units, through NGINX and three replicas | Checkout: 500 `202`, 99,500 `410`; peak 10,153 req/s, p50 1.1 ms, p95 13.4 ms, p99 78.6 ms; 0 rate-limit answers, 0 5xx, 0 network failures; every invariant check passed (`products.sold` = `sold:{sale}` = `CONFIRMED` orders, never more than 500, no user with two orders, all abandoned stock returned) |
| `load/backlog.sh` | A throttled processor builds a real queue; no `PENDING` hold is released and everyone gets the full payment window | 10,000 users and units at 200/s: slowest checkout-to-link 43 s against a 30 s window, 0 `FAILED` orders, least time left to pay when a link appeared 27.8 s of 30 s |
| `load/oversell_demo.sh` | Five ways to decrement stock, 5,000 buyers for 500 units | Naive `SELECT` then `UPDATE` told all 5,000 yes; the other four sold exactly 500; Lua took 0.02 s, the conditional `UPDATE` 0.57 s, `FOR UPDATE` 4.61 s, optimistic 10.26 s |

These runs have limits (one machine, the 100,000 run measured once successfully, the backlog test at a tenth of its design point); [CASE_STUDY.md](CASE_STUDY.md) lists them.

## Repository layout

| Path | Contents |
|---|---|
| `src/bin/` | The four binaries: `order_service`, `order_processor`, `expiration_worker`, `fake_payment_provider` |
| `src/api/` | HTTP routes (`session`, `sales`, `checkout`, `orders`, `admin` for the Order Service; `payments`, the webhook, for the order processor) |
| `src/cache.rs` | Redis keys and the Rust wrappers around the Lua scripts |
| `src/processor.rs`, `src/inbox.rs` | Checkout event handling with progress tracking; payment events: the decision between payment and deadline, confirm or refund, on receipt and on retry |
| `src/expiry.rs` | Expiration worker logic: unpaid loop (PostgreSQL decides, Redis follows), leaked loop, sweep |
| `src/queue.rs` | Kafka producer and topic creation |
| `src/payment/` | Provider interface and its HTTP client, the fake provider (state, API, payment page), webhook signing, webhook dispatcher |
| `src/actions/`, `src/models.rs`, `src/schema.rs` | PostgreSQL queries (Diesel), models |
| `scripts/` | Lua: `checkout`, `release`, `link_ready`, `settle_hold`, `set_status`, `set_max`, and the fake provider's `fakepay_*` |
| `migrations/` | The one schema migration (embedded in the Order Service, run at startup) |
| `tests/` | Integration tests against real PostgreSQL, Redis and Kafka |
| `gateway/` | NGINX configuration, bot list, njs JWT reader, `smoke.sh` ([README](gateway/README.md)) |
| `web/` | Preact + Vite storefront ([README](web/README.md)) |
| `load/` | Load generator, backlog test, oversell demo, `RESULTS.md` ([README](load/README.md)) |
| `docker-compose.yml`, `Dockerfile` | Infrastructure and the application image (all four binaries) |

## More documents

- [API_REFERENCE.md](API_REFERENCE.md): every route, status code, header and example.
- [CASE_STUDY.md](CASE_STUDY.md): the case study mapped to the code and the measured results, with the trade-offs.
- [ROADMAP.md](ROADMAP.md): the design, hold states, Redis keys and the build stages.
