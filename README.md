# Flash Sale

An e-commerce service for flash sales of limited stock, built with Rust (Actix-Web, Diesel), PostgreSQL, Redis and Kafka. Users wait in a queue and are let in so that a fixed number shop at once; they hold units of a product for a limited time and confirm them by paying. The stock is a counter, so a sale of 500 identical units sells exactly 500.

[ROADMAP.md](ROADMAP.md) describes the architecture, [API_REFERENCE.md](API_REFERENCE.md) the endpoints, and [STEPS.md](STEPS.md) how it was built and tested. Not built yet: Stripe (a fake payment provider stands in), the gateway and the frontend.

## How a Sale Works

1. **Session.** A client gets a guest session (`POST /session`) when the product page loads, before the sale opens.
2. **Join.** Users join a product's waiting room. Before the sale opens they wait in a lobby, shuffled into a random order at the opening; afterwards the queue is first come, first served. The queue is a Kafka topic per product.
3. **Admission.** The admission worker lets the next user in whenever fewer than `min(MAX_ACTIVE_TOKENS, units in stock)` are shopping. Units in stock are those neither held nor sold.
4. **Reserve.** A reservation holds a quantity of units (up to the product's `max_per_user`, 1 unless set) by decrementing a Redis counter for `HOLD_TTL_SECS`, all or none, and uses up the user's turn. It never touches PostgreSQL: this is the busiest step.
5. **Pay.** Paying extends the hold once, records the order and answers `202 Accepted` with a `Location` header: the order to poll. The payment provider reports the outcome to an inbox table; a success claims the hold at once, so a payment that arrives in time is kept however busy the payment worker is. The worker then confirms the order, taking the units from `inventory.stock` only where `stock >= quantity` (`CONFIRMED`), or refunds a payment that arrived after the hold ran out.
6. **Expire.** The cleanup worker releases holds whose time is up; the units return to stock.

## Components

| Component        | Role                                                                    |
|------------------|-------------------------------------------------------------------------|
| API              | HTTP endpoints (`src/main.rs`)                                          |
| Admission worker | Opens and closes sales, admits users (`src/bin/admission_worker/`)      |
| Payment worker   | Processes payment events from the inbox (`src/bin/payment_worker.rs`)   |
| Cleanup worker   | Releases expired holds; rebuilds lost stock counters (`src/bin/cleanup_worker.rs`) |
| PostgreSQL       | Durable record: users, products, inventory, orders, payment inbox       |
| Redis            | Tokens, stock counters, holds, hold expiry queue (AOF enabled)          |
| Kafka            | Waiting room order, one single-partition topic per product              |

Every worker can run several copies: Kafka gives each product to one admission worker (the others stand by), and the payment and cleanup workers share the work.

## Running

Requirements: Rust (stable) and Docker.

Everything in containers:

```bash
docker compose --profile app up -d --build
```

Or only PostgreSQL, Redis and Kafka in containers, and the application with cargo:

```bash
docker compose up -d
cargo run                              # API (binary `flash-sale`); applies database migrations at startup
cargo run --bin admission_worker       # every scheduled and on-sale product (or pass a product id)
cargo run --bin payment_worker
cargo run --bin cleanup_worker
```

The API listens on `BIND_ADDR` (`127.0.0.1:8080`) and uses the PostgreSQL database `flash_sale`, which compose creates. Configuration comes from the environment, with `.env` as a fallback; `DATABASE_URL` and `JWT_SECRET` are required.

After the rename from the earlier ticketing project, an existing local setup needs fresh Redis and Kafka state: run `docker compose down -v` first. Product ids restart at 1 in the new database, and old Redis keys and Kafka topics with the same ids (`waiting.1`, `closed:1`, user tokens and so on) would otherwise be reused by new products.

| Variable                 | Default                  | Meaning                                         |
|--------------------------|--------------------------|-------------------------------------------------|
| `DATABASE_URL`           | required                 |                                                 |
| `JWT_SECRET`             | required                 |                                                 |
| `BIND_ADDR`              | `127.0.0.1:8080`         | API address                                     |
| `REDIS_URL`              | `redis://127.0.0.1:6379` |                                                 |
| `KAFKA_BROKERS`          | `localhost:9092`         | `kafka:29092` from inside compose               |
| `DB_POOL_SIZE`           | `10`                     | Per process                                     |
| `REDIS_CONNECTIONS`      | `8`                      | Per API process; one becomes a bottleneck       |
| `MAX_ACTIVE_TOKENS`      | `2000`                   | Users shopping at once, per product             |
| `TOKEN_TTL_SECS`         | `600`                    | Time an admitted user has to reserve            |
| `HOLD_TTL_SECS`          | `300`                    | How long a reservation holds its units          |
| `PAYMENT_EXTENSION_SECS` | `120`                    | Added once to a hold when payment starts        |
| `DEFAULT_MAX_PER_USER`   | `1`                      | Units one user may hold or buy, for a product created without `max_per_user` |
| `JWT_TTL_SECS`           | `3600`                   |                                                 |
| `PAYMENT_PROVIDER`       | `fake`                   | Succeeds every payment                          |
| `FAKE_PAYMENT_DELAY_MS`  | `2000`                   | How long the fake provider takes to report      |

## Tests

```bash
docker compose up -d
cargo test                     # unit tests, plus Redis script and provider tests against the containers
```

`cargo test` includes a race of 200 admitted users for 10 units, which must end with exactly 10 holds and `stock` at 0.

The scripts in `load/` (`reserve_race.sh`, `final.sh`, `capacity.sh`, `shop.sh`) run against the products API. `REDIS_DB` (default 0) picks the Redis database they and the binaries they start use. `reserve_race.sh` needs the API already running on the same database. They append their reports to `load/RESULTS.md` (created on the first run). In the ticketing project the ceiling on one machine was about 30,000-37,000 status polls per second, set by Redis.

## Project Layout

```
src/main.rs            API server startup
src/lib.rs             Shared library used by the server and the workers
src/bin/               Admission, payment and cleanup workers
src/config.rs          Environment configuration
src/db.rs              PostgreSQL pool and embedded migrations
src/cache.rs           Redis connection, keys, and a wrapper per Lua script
src/waiting.rs         WaitingQueue trait and its Kafka implementation
src/payment/           PaymentProvider trait and the fake provider
src/auth.rs            Password hashing (Argon2id) and JWTs
src/error.rs           ApiError: maps errors to JSON responses with status codes
src/models.rs          Diesel models and enums
src/schema.rs          Diesel schema (generated)
src/actions/<area>.rs  Database operations, one file per area; synchronous
src/api/<group>.rs     One file per route group, each exposing `configure`
scripts/*.lua          Redis scripts: every multi-key change is one atomic script
migrations/            Database migrations
tests/                 Tests against Redis and PostgreSQL
load/                  Load tests and their results
```
