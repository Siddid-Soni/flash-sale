# Flash Sale

A backend and storefront for a flash sale: 500 smartphones go on sale at a fixed time, more than 100,000 requests reach checkout in the first 10 seconds, and exactly 500 units are sold, never more. Winners get an answer in milliseconds (`202 Accepted` and a queue ticket), everyone else gets `410 Gone` just as fast, and the slow parts (Kafka, order processing, payment, PostgreSQL) only ever see the roughly 500 winners. Stock is reserved by one atomic Lua script in Redis; PostgreSQL has a conditional `UPDATE` as a second line of defence.

The design is in [ROADMAP.md](ROADMAP.md). How it answers the placement case study is in [CASE_STUDY.md](CASE_STUDY.md). The HTTP API is in [API_REFERENCE.md](API_REFERENCE.md).

## Architecture

| Component | What it does | Code |
|---|---|---|
| Client | Preact app: countdown, buy button (locks on the first click), queue and payment states, status polling every 2 s | `web/` |
| Gateway | NGINX on `:8000`: serves the frontend, balances `/v1/` over three replicas, rate limits, bot filter, hides `/v1/admin/`, caches sale details for 1 s | `gateway/` |
| Order Service x3 | Stateless HTTP API: guest sessions, sale details, checkout, order status, payment webhook, the fake payment page | `src/bin/order_service.rs`, `src/api/` |
| Redis (inventory) | Stock counter, holds, pending and expiry queues, processor progress; append-only file on | `scripts/*.lua`, `src/cache.rs` |
| Redis (read cache) | `status:<ticket>`, what clients poll. The same instance locally (`READ_CACHE_URL`) | `src/cache.rs` |
| Kafka | Topic `orders.checkout` (6 partitions): checkout events from the Order Service to the processors | `src/queue.rs` |
| Order Processor | Turns each checkout event into an order and a payment link; confirms or refunds paid orders from the payment inbox | `src/processor.rs`, `src/inbox.rs` |
| Expiration Worker | Releases unpaid holds and leaked holds; the only place stock is returned (besides immediate compensation) | `src/expiry.rs` |
| Fake payment provider | Same interface as Stripe: checkout session with a URL, expire, refund, signed webhooks with retries | `src/payment/` |
| PostgreSQL 16 | Durable record: `users`, `products`, `orders`, `payment_inbox` | `migrations/` |

```
Browser --> NGINX :8000 --> Order Service 1..3 --> Redis (checkout.lua)  -- 202 / 410 --> Browser
                                   |
                                   +--> Kafka orders.checkout --> Order Processor --> PostgreSQL
                                                                        |  \--> fake provider --> webhook --> Order Service
Expiration Worker --> Redis (release.lua), PostgreSQL, Kafka offsets (read only)
```

### Request flow

1. The client loads the app and `GET /v1/sales/{id}` (cached 1 s at the gateway), creates a guest session with `POST /v1/session`, and waits for the start time.
2. `POST /v1/sales/{id}/checkout` with an `Idempotency-Key`. `checkout.lua` runs once in Redis: it checks the start time, the key, one-per-user and stock, takes one unit and creates a `PENDING` hold. No hold means `409` or `410`.
3. The Order Service publishes the checkout event to Kafka (`acks=all`, idempotent producer), stores `status:<ticket>` and answers `202 {queue_ticket_id, status_url}`. If the publish fails, the hold is released at once and the answer is `503`.
4. An order processor inserts the order, creates the payment session and moves the hold to `AWAITING_PAYMENT` (the 5-minute payment window starts here, not at checkout). The client, polling the status endpoint, gets `payment_url` and goes to pay.
5. The provider sends a signed webhook. The Order Service stores it in `payment_inbox` and claims the hold in Redis at once; a processor then commits the order as `CONFIRMED` together with `UPDATE products SET sold = sold + 1 WHERE id = $1 AND sold < total_inventory`, or refunds.
6. The Expiration Worker returns the stock of holds that were never paid and of holds whose Kafka message can no longer arrive.

## Quick start

Needs Docker with compose v2. The first build compiles the Rust binaries and the frontend, so it takes a while.

```bash
docker compose up -d                          # PostgreSQL 16, Redis 7, Kafka
docker compose --profile app up -d --build    # + 3 Order Services, order processor, expiration worker, NGINX
```

Open <http://localhost:8000>. There is no sale yet; create one from inside the gateway container (the host is refused with `403` on purpose). This opens in one minute:

```bash
docker compose exec -T gateway wget -qO- \
  --header 'X-Admin-Token: dev-admin-token' --header 'Content-Type: application/json' \
  --post-data "{\"name\":\"Phone\",\"price_cents\":49900,\"total_inventory\":500,\"sale_start_time\":\"$(date -u -d '+60 seconds' +%FT%TZ)\"}" \
  http://127.0.0.1/v1/admin/sales
```

The answer contains the sale `id`. The frontend shows sale `1` by default; open `http://localhost:8000/?sale=<id>` for another. `dev-admin-token` is `ADMIN_TOKEN` in `.env`. Pay on the fake payment page; the webhook arrives within a second or so and the order becomes `CONFIRMED`.

Stop only the application with `docker compose --profile app stop gateway order-service-1 order-service-2 order-service-3 order-processor expiration-worker` (`--profile app stop` alone also stops PostgreSQL, Redis and Kafka). `docker compose down -v` also removes the data.

## Running locally with cargo

```bash
docker compose up -d                  # infrastructure only
cargo run --bin order_service         # :8080; runs the migrations and creates the topic, so start it first
cargo run --bin order_processor
cargo run --bin expiration_worker
```

`.env` holds the development values and is loaded by every binary (`PUBLIC_BASE_URL=http://localhost:8080`). Without the gateway, call the API on `:8080` directly, including the admin route:

```bash
curl -s -X POST http://localhost:8080/v1/admin/sales -H 'X-Admin-Token: dev-admin-token' \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"Phone\",\"price_cents\":49900,\"total_inventory\":500,\"sale_start_time\":\"$(date -u -d '+60 seconds' +%FT%TZ)\"}"
```

Frontend with hot reload: `cd web && npm install && npm run dev` (<http://localhost:5173>, proxies `/v1` and `/fake-pay` to `:8080`). Set `FRONTEND_URL=http://localhost:5173` in `.env` so the payment page sends the browser back to the Vite server. More in [web/README.md](web/README.md).

## Configuration

Every binary reads its environment (and `.env`) through `src/config.rs`. A variable without a default is required: the process stops at startup if it is missing.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | required | PostgreSQL connection string |
| `JWT_SECRET` | required | HS256 key for guest tokens |
| `ADMIN_TOKEN` | required | Value of `X-Admin-Token` for `/v1/admin/*` (compared in constant time; empty never matches) |
| `PAYMENT_WEBHOOK_SECRET` | required | HMAC-SHA256 key that signs and verifies webhooks |
| `BIND_ADDR` | `127.0.0.1:8080` | Where the Order Service listens (`0.0.0.0:8080` in containers) |
| `REDIS_URL` | `redis://127.0.0.1:6379` | Inventory Redis: stock, holds, queues, processor progress, fake provider sessions |
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
| `FAKE_PAYMENT_DELAY_MS` | `2000` | Read but not used by any code; the fake provider's webhooks go out within about 250 ms |
| `PUBLIC_BASE_URL` | `http://localhost:8080` | Where clients reach the API; the payment page URL is `{PUBLIC_BASE_URL}/fake-pay/{session}` |
| `PAYMENT_WEBHOOK_URL` | `{PUBLIC_BASE_URL}/v1/payments/webhook` | Where the fake provider delivers webhooks (plain `http://` only) |
| `FRONTEND_URL` | `PUBLIC_BASE_URL` | Where the payment page returns the browser to (`/?sale=<id>`) |
| `PROCESSOR_CONCURRENCY` | `32` | Checkout messages one processor works on at once, per partition |
| `PROCESSOR_MAX_PER_SEC` | `0` | Cap on messages one processor starts per second (0 = unlimited); used by the backlog test |
| `EXPIRY_BATCH` | `200` | Holds released per sale and pass; a full batch is followed at once by another |
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

The integration tests use Redis database 15 and create a unique Kafka topic per run (it stays on the local broker). Do not run `cargo test` while the application containers are up: they share PostgreSQL's connections and the Kafka consumer group, and the expiry lag test fails. The frontend has its own checks: `cd web && npm test && npm run budgets`. `gateway/smoke.sh` checks the gateway end to end against the running stack; it creates a 5-unit sale and briefly stops `order-service-2` to test failover.

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
| `src/bin/` | The three binaries: `order_service`, `order_processor`, `expiration_worker` |
| `src/api/` | HTTP routes (`session`, `sales`, `checkout`, `orders`, `payments`, `admin`, `fake_pay`) |
| `src/cache.rs` | Redis keys and the Rust wrappers around the Lua scripts |
| `src/processor.rs`, `src/inbox.rs` | Checkout event handling with progress tracking; payment inbox loop (confirm or refund) |
| `src/expiry.rs` | Expiration worker logic: unpaid loop, leaked loop, sweep |
| `src/queue.rs` | Kafka producer and topic creation |
| `src/payment/` | Provider interface, fake provider, webhook signing, webhook dispatcher |
| `src/actions/`, `src/models.rs`, `src/schema.rs` | PostgreSQL queries (Diesel), models |
| `scripts/` | Lua: `checkout`, `release`, `link_ready`, `confirm`, `set_status`, `set_max`, and the fake provider's `fakepay_*` |
| `migrations/` | The one schema migration (embedded in the Order Service, run at startup) |
| `tests/` | Integration tests against real PostgreSQL, Redis and Kafka |
| `gateway/` | NGINX configuration, bot list, njs JWT reader, `smoke.sh` ([README](gateway/README.md)) |
| `web/` | Preact + Vite storefront ([README](web/README.md)) |
| `load/` | Load generator, backlog test, oversell demo, `RESULTS.md` ([README](load/README.md)) |
| `docker-compose.yml`, `Dockerfile` | Infrastructure and the application image (all three binaries) |

## More documents

- [API_REFERENCE.md](API_REFERENCE.md): every route, status code, header and example.
- [CASE_STUDY.md](CASE_STUDY.md): the case study mapped to the code and the measured results, with the trade-offs.
- [ROADMAP.md](ROADMAP.md): the design, hold states, Redis keys and the build stages.
