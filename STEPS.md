# Implementation Steps

A follow-along checklist for [ROADMAP.md](ROADMAP.md). Complete each step, run its check, commit, then move on. Every step leaves the project building.

Stages 0-8 describe the ticketing model (venues, seats, events, bookings, the seat bitmap) as it was built; Stage 9 replaced it with the products API, and the names, endpoints and keys in those stages are the old ones. Stage 9 below lists what changed.

---

## Stage 0: Foundations

### 0.1 Configuration module
- [x] Create `src/config.rs` with a `Config` struct: `database_url`, `redis_url`, `kafka_brokers`, `db_pool_size`, `max_active_tokens` (replaced `admission_rate_per_sec`), `token_ttl_secs`, `hold_ttl_secs`, `payment_extension_secs`, `max_seats_per_user`, `jwt_secret`.
- [x] Implement `Config::from_env()` with defaults for everything except `DATABASE_URL` and `JWT_SECRET`.
- [x] Add the new variables to `.env`.
- [x] Load it once in `main` and share it via `web::Data<Config>`.
- [x] Replace the hardcoded `localhost`/`guest` in `queue.rs` and the pool size in `initialize_db_pool`.

**Check:** `cargo run` starts; changing `DB_POOL_SIZE` in `.env` changes the pool size (log it at startup).

### 0.2 Redis service
- [x] Add to `docker-compose.yml`:
  ```yaml
  redis:
    image: redis:7-alpine
    command: ["redis-server", "--appendonly", "yes"]
    ports: ["6379:6379"]
    volumes: ["redis_data:/data"]
  ```
- [x] Add `redis_data:` under `volumes:`.

**Check:** `docker compose up -d redis && docker compose exec redis redis-cli ping` prints `PONG`.

### 0.3 Kafka service
- [x] Add to `docker-compose.yml` (single-node KRaft; must be Kafka 4.0+ because `kafkit-client` only speaks the KIP-848 consumer group protocol):
  ```yaml
  kafka:
    image: apache/kafka:4.2.0
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_DIRS: /var/lib/kafka/data
    volumes: ["kafka_data:/var/lib/kafka/data"]
  ```
- [x] Add `kafka_data:` under `volumes:`.
- [x] Add `KAFKA_BROKERS=localhost:9092` to `.env`.

**Check:** `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` runs without error.

### 0.4 Redis client in the app
- [x] Add `redis = { version = "1.7.1", features = ["tokio-comp", "connection-manager", "script"] }` (needs tokio 1.51+: `cargo update -p tokio`).
- [x] Build one `redis::aio::ConnectionManager` at startup (`initialize_redis` in `main.rs`) and `PING` it. It multiplexes all requests over one connection and reconnects on its own, so there is no pool.
- [x] Share it via `web::Data`; handlers clone it (cheap) before each command.
- [ ] ~~Add `GET /health` that runs `PING` against Redis and `SELECT 1` against Postgres.~~ Skipped for now.

**Check:** the app logs `Connected to Redis` at startup and exits with a clear error when Redis is down.

### 0.5 Baseline load test (dropped: the legacy `/book` endpoint it measured has been removed; `git show 432fa31` has the old code if a baseline is ever wanted)
- [ ] Install k6.
- [ ] Create `load/baseline.js`: setup creates 1 conference with 100 slots and 2,000 users; the scenario has all users call `POST /book` at once.
- [ ] After the run, query Postgres for confirmed count and duplicate users.
- [ ] Record throughput, p95/p99 latency, error rate, and confirmed count in `load/RESULTS.md`.

**Check:** `k6 run load/baseline.js` completes and results are recorded.

---

## Stage 1: Schema

### 1.1 Enums and core tables
- [x] `diesel migration generate ticketing_schema`.
- [x] In `up.sql` create enums `seat_status` (`AVAILABLE`, `SOLD`, `BLOCKED`) and `booking_state` (`PENDING`, `PAYMENT_PENDING`, `CONFIRMED`, `EXPIRED`, `CANCELED`, `REFUNDED`).
- [x] Create `users (id serial pk, username varchar unique not null, password_hash not null, created_at)`.
- [x] Create `venues (id, name, location, seat_map_config jsonb)`.
- [x] Create `seats (id, venue_id fk, section, row_number, seat_number, type)` with `UNIQUE (venue_id, section, row_number, seat_number)`.
- [x] Create `events (id, venue_id fk, name unique, start_time, status)`.
- [x] Write `down.sql` that drops them in reverse order.

**Check:** `diesel migration run`, then `diesel migration redo` both succeed.

### 1.2 Booking tables
Migration `booking_tables`:
- [x] `bookings (id uuid pk, user_id fk, event_id fk, status booking_state default 'PENDING', total_cents int, payment_idempotency_key unique, payment_intent_id unique, created_at, expires_at, updated_at)`. The id is generated by the app, because it also names the Redis hold `resv:{id}`. `updated_at` is maintained by `diesel_manage_updated_at`.
- [x] `event_seats (id, event_id fk, seat_id fk, seat_index int, status seat_status default 'AVAILABLE', booking_id uuid fk null, price_cents int, version int default 0)` with `UNIQUE (event_id, seat_id)`, `UNIQUE (event_id, seat_index)`, and `CHECK ((status = 'SOLD') = (booking_id IS NOT NULL))`.
- [x] `booking_seats (booking_id, event_seat_id)`: which seats each booking held. Kept after expiry as history; used by the startup rebuild.
- [x] `payment_inbox (id bigserial, provider, provider_event_id unique, event_type, payload jsonb, received_at, processed_at null, attempts, last_error)`.
- [x] Indexes: `bookings (status, expires_at)`, `bookings (user_id)`, `bookings (event_id)`, `event_seats (event_id, status)`, `booking_seats (event_seat_id)`, and a partial index on unprocessed `payment_inbox` rows.

Money is stored as integer cents, so no decimal type is needed in Rust.

**Check:** `diesel migration redo --all` succeeds; `\dt` shows all tables.

### 1.3 Diesel models
- [x] Regenerate `src/schema.rs`.
- [x] Add Rust enums `SeatStatus` and `BookingState` with `ToSql`/`FromSql` (`#[derive(diesel_derive_enum::DbEnum)]` or a manual `ToSql`/`FromSql` impl).
- [x] Add `Queryable`/`Insertable` structs for every new table in `src/models.rs`.

**Check:** `cargo build` passes.

### 1.4 Create venue
- [x] Define the `seat_map_config` shape (`SeatMapConfig` in `models.rs`, validated: at least one section, unique non-empty names, rows and seats > 0, at most 100,000 seats; `type` defaults to `standard`), e.g. `{"sections":[{"name":"A","rows":10,"seats_per_row":20,"type":"standard"}]}`.
- [x] `POST /venue` inserts the venue and generates one `seats` row per seat, in one transaction.

**Check:** creating a venue with 10 rows of 20 returns `201`, and `SELECT count(*) FROM seats WHERE venue_id = $id` is 200.

### 1.5 Create event
- [x] `POST /events` takes `venue_id`, `name`, `start_time`, and `prices` (section name to price in cents). Every venue section must be priced, unknown sections are rejected, prices must be non-negative, `start_time` must be in the future.
- [x] In one transaction, insert the event and one `event_seats` row per venue seat, with `seat_index` assigned 0..n-1 in `seats.id` order (the order the venue's seats were generated: section, row, seat), built by one `INSERT ... SELECT`.

**Check:** `event_seats` count equals the venue seat count; `seat_index` values are 0..n-1 with no gaps.

### 1.6 Seating cache load
- [x] After the event transaction commits, `cache::init_event` runs one atomic pipeline: `DEL seats:{event}`, `SETBIT seats:{event} {n-1} 0` (allocates, all free), `SET available:{event} n`. Non-`AVAILABLE` seats are set by the startup rebuild (7.4).
- [x] Split the seat map in two so the static part can be cached:
  - `GET /events/{id}/layout`: seat details from Postgres in `seat_index` order (served by the `UNIQUE (event_id, seat_index)` index). Never changes, so `Cache-Control: public, max-age=31536000, immutable`.
  - `GET /events/{id}/availability` (admitted users only since step 3.4): the raw Redis bitmap as `application/octet-stream` (bit i = seat_index i, most significant bit first), `X-Available-Count` header, `ETag` (FNV-1a of the bytes) with `304` on `If-None-Match`, and `Cache-Control: private, no-cache` (no CDN; clients revalidate every time). Never touches Postgres. A missing bitmap returns `404`.

**Check:** `redis-cli STRLEN seats:{event}` is `ceil(n/8)`; `/availability` returns all-zero bytes; after `SETBIT seats:{event} 3 1` it returns `0x10` and a new `ETag`; resending the old `ETag` before the change returns `304`.


### 1.7 Sale window
- [x] Migration `sale_window`: add `sale_opens_at TIMESTAMPTZ NOT NULL` to `events` with `CHECK (sale_opens_at < start_time)`.
- [x] `POST /events` takes `sale_opens_at` (in the future, before `start_time`).
- [x] `cache::init_event` also sets `seat_count:{event}` = n, `sold:{event}` = 0 and `sale_opens_at:{event}`, used for the sold-out status (step 4.8).

**Check:** an event with `sale_opens_at` after `start_time` returns `400`; `redis-cli GET seat_count:{event}` equals the seat count.
---

## Stage 2: Authentication

### 2.1 Registration and login
- [x] Add crates `jsonwebtoken` (11, `rust_crypto` backend), `argon2` (0.6), and `JWT_TTL_SECS` (default 3600) to `Config`.
- [x] `password_hash` column on `users` (already in the `ticketing_schema` migration).
- [x] `POST /auth/register` (username 3-50 of `[A-Za-z0-9_.-]`, password 8-128; `409` if taken) and `POST /auth/login`; login returns a JWT (HS256, `sub` = user id, `iat`, `exp`) and `expires_at`. Hashing runs in `web::block`. Wrong password and unknown user both return `401 invalid username or password` and take equally long (a dummy hash is verified for unknown users).

**Check:** register, log in, decode the token at jwt.io and see the user id.

### 2.2 Auth extractor
- [x] Implement an Actix `FromRequest` extractor `AuthUser { id }` (in `src/auth.rs`) that validates `Authorization: Bearer <jwt>`: signature check only, no database lookup, no clock-skew leeway.
- [x] Return `401` with `missing bearer token`, `token expired`, or `invalid token`.
- [x] `GET /auth/me` returns the caller's id.

**Check:** `GET /auth/me` returns `401` without a token, the user id with one, `401 invalid token` for a tampered or wrongly signed token or `alg: none`, and `401 token expired` after the TTL.

---

## Stage 3: Waiting Service

### 3.1 Queue abstraction
- [x] Add `kafkit-client = "=0.1.9"` (pure Rust, tokio; pinned exactly because the crate is unmaintained). It needs tokio 1.52+, which `cargo` will pull in.
- [x] Create `src/waiting.rs` with a trait (errors are `QueueError`, which converts to a `500`):
  ```rust
  #[async_trait]
  pub trait WaitingQueue: Send + Sync {
      async fn ensure_event(&self, event_id: i32) -> QueueResult<()>;
      async fn publish(&self, event_id: i32, token_id: &str) -> QueueResult<()>;
  }
  ```
- [x] Implement `KafkaQueue`: `ensure_event` creates topic `waiting.{event_id}` with `KafkaAdmin::create_topics([NewTopic::new(name, 1, 1).with_config("retention.ms", ...)])` (1 partition so order is total; the crate already treats "topic exists" as success); `publish` calls `KafkaProducer::send_to(topic, KafkaMessage::new(token_id))` with `ProducerConfig::with_acks(-1).with_enable_idempotence(true)` and awaits the returned `ProduceAck`.
- [x] Connect once at startup (`KafkaQueue::connect`) and share it as `web::Data<SharedQueue>` (`Arc<dyn WaitingQueue>`).
- [x] Call `ensure_event` from event creation, after the commit (3-day `retention.ms`).

**Check:** creating an event makes `waiting.{id}` appear in `kafka-topics.sh --list`.

### 3.2 Join endpoint with a pre-sale lobby
Everyone who joins before the sale opens waits in a lobby and is shuffled into a random order when it opens (step 4.6), so being first to click before opening gives no advantage. Joins after opening are first come, first served.

A token's place in line is its **Kafka offset**: `seq = offset + 1`. The queue is the topic, so its order is the admission order. (Numbering in Redis and publishing afterwards was tried first: concurrent joins reached Kafka in a different order than their numbers.)
- [x] `POST /waiting-room/{event}/join` (requires `AuthUser`); `410` if `closed:{event}` is set, `404` if `seat_count:{event}` is missing (the event check stays in Redis, off Postgres).
- [x] `scripts/join.lua`, atomic:
  1. If `user_token:{event}:{user}` points to a token still in play, return it (idempotent join). `LOBBY`, `QUEUED` and `ACTIVE` are in play; `USED` only while its hold lasts (`resv:{id}` exists). Once the hold is released, expired or paid, joining again creates a new token at the back of the line. The rule lives here so it covers every way a hold ends; the per-user seat limit (`user_seats`) is not reset by rejoining.
  2. Create `token:{id}` with `user`, `event`.
  3. If `lobby_flushed:{event}` is not set (the sale has not opened yet, or the lobby is still being shuffled): `status LOBBY`, `SADD lobby:{event} {id}`.
  4. Otherwise `status QUEUED`, with no `seq` yet.
- [x] For a `QUEUED` token without `published`: `ensure_event` (remembered per process, so only the first join of an event in each API process sends a CreateTopics request; one per join doubled join latency), `publish` (returns the offset), then in one atomic pipeline `HSETNX seq {offset + 1}` and `HSET published 1`. This also retries a publish that failed on an earlier join. A duplicate message is harmless: admission skips tokens that are no longer `QUEUED`, and `HSETNX` keeps the first place.
- [x] Return `202 {token_id, status, seq}` (`seq` absent in the lobby).

**Check:** joining twice returns the same token (also for 20 concurrent joins by one user); before the sale opens the token is `LOBBY`; after it opens, 30 concurrent users get unique seqs and the token at Kafka offset i has seq i+1; with Kafka down the join returns `500` and the next join publishes the same token.

### 3.3 Waiting room status endpoint
- [x] `GET /waiting-room/{event}/status` (requires `AuthUser`). The caller's token is found through `user_token:{event}:{user}`, so no token id is needed, ownership is automatic, and an expired token can be told apart from one that never existed. (Planned as `GET /waiting-room/token/{id}`, which cannot do either once the token hash is gone.)
- [x] One round trip, Redis only: the read-only `scripts/status.lua` returns the token (status, seq, published, `PTTL`) plus `admitted_upto`, `available`, `sold`, `seat_count`, `closed` and `sale_opens_at`. Event creation now stores `sale_opens_at:{event}` in Redis (`cache::init_event`) for this.
- [x] Responses: `404` if the user never joined; `LOBBY` with `sale_opens_at`; `QUEUED` with `position = seq - admitted_upto` (at least 1), `estimated_wait_secs` (step 4.7) and `reason` `sold_out` (`sold >= seat_count`, not final) or `all_seats_held` (`available` is 0); `ACTIVE` with `expires_in_ms`; `USED`; `CLOSED` for `LOBBY`/`QUEUED` tokens when `closed:{event}` is set; `EXPIRED` when the token is gone. `USED` and `EXPIRED` also report `can_rejoin` (the hold, if any, is over and the sale is not closed), so clients can offer to join again.
- [x] A `QUEUED` token that was never published (Kafka was down at join) is published here too, via the same helper as join.

**Check:** five queued users report positions 1-5; after `admitted_upto` reaches 2 the third reports 1; `available` 0 gives `all_seats_held`, `sold` equal to `seat_count` gives `sold_out`; a token given `ACTIVE` and an expiry reports `expires_in_ms`; a deleted token reports `EXPIRED`; `closed` turns lobby and queued tokens `CLOSED` but leaves `ACTIVE` ones.

### 3.4 Seat map behind the queue
- [x] `GET /events/{id}/availability` requires `AuthUser`, and the caller's token for the event (found through `user_token:{event}:{user}`, like the status endpoint, so no token header is needed) must be `ACTIVE`; otherwise `403`. The read-only `scripts/availability.lua` checks this and returns the bitmap and free count in one round trip. Only admitted users can poll availability, so the queued crowd never reaches it.
- [x] `GET /events/{id}/layout` stays public and cacheable: it never changes and says nothing about availability.

**Check:** `/availability` returns `401` without a login, `403` for a user who never joined, is queued, or whose token expired, the exact bitmap bytes (`ff 00 01` round-trips) with an `ACTIVE` token, `304` with no body for a matching `ETag`, and `404` when the seat map is missing.
---

## Stage 4: Admission Worker

### 4.1 Basic consumer
- [x] Create `src/bin/admission_worker.rs` taking an event id: `cargo run --bin admission_worker -- <event_id>` (`default-run` in `Cargo.toml` keeps `cargo run` starting the API). Ctrl-C calls `shutdown()` so the worker leaves the group at once.
- [x] Connect a `KafkaConsumer` with `ConsumerConfig::new(brokers, "admission-{event}").with_auto_offset_reset(AutoOffsetReset::Earliest).with_enable_auto_commit(false)` and `subscribe(vec!["waiting.{event}"])`.
- [x] `poll()` returns a batch (`ConsumerRecords`); process its records one at a time.
- [x] For each record, run `scripts/admit.lua` (step 4.2) and act on its result. Commit the offset (`commit_offsets(vec![CommitOffset { topic, partition, offset: record.offset + 1 }])`, the *next* offset to read) after `admitted` or `skipped`, never after `full`. Committed once per polled batch (the last record done), not per record: a commit is a broker round trip, and records read again after a crash are only skipped. A failed commit is logged and ignored for the same reason.

**Check:** run the worker; joined tokens become `ACTIVE` and their status endpoint shows remaining time.

### 4.2 Admit script: a fixed-size group of active tokens
- [x] Create `scripts/admit.lua`. Inputs: token id, event id, the record's `offset + 1`, `MAX_ACTIVE_TOKENS`, token TTL ms. All in one atomic call:
  1. `now` from `redis.call('TIME')`; `ZREMRANGEBYSCORE active_tokens:{event} -inf {now}` drops tokens that expired unused, freeing their slots.
  2. If `token:{id}` is missing or its status is not `QUEUED`, advance `admitted_upto` past it (so positions behind it stay accurate) and return `skipped`. Covers expired tokens and replays: delivery is at-least-once, so a message can come back after a crash and must not re-activate a token that was already `USED`.
  3. `limit = min(max_active, GET available:{event})`; if `ZCARD active_tokens:{event} >= limit`, return `full`.
  4. `HSET token:{id} status ACTIVE`, `PEXPIRE token:{id} {ttl}`, `ZADD active_tokens:{event} {now + ttl} {id}`, `admitted_upto:{event}` = `max(current, offset + 1)` (from the record, so a replayed older message cannot move it back); return `admitted`.
- [x] On `full`, keep the record in hand and retry every 200 ms without calling `poll()`. Group heartbeats run on the client's own background task, so the worker stays in the group while it waits.

**Check:** with `MAX_ACTIVE_TOKENS=3`, join 10 users: exactly 3 become `ACTIVE`. When their TTL runs out the next 3 become `ACTIVE` within a second; the rest stay `QUEUED`. (A manual `DEL token:{id}` does not free a slot: the slot lives in `active_tokens:{event}` until its score passes. Use a short `TOKEN_TTL_SECS` to test.) Restarting the worker resumes from the committed offset without replays.

### 4.3 Limit by free seats
- [x] `available:{event}` is created with the seat map (step 1.6), decremented by the reserve script and incremented by release and expiry. The admit limit already includes it, so no separate pause is needed: with 0 free seats the limit is 0.

**Check:** (verified by setting `available:{event}` by hand until reservations exist) with a 5-seat event, 5 reserved seats and a large `MAX_ACTIVE_TOKENS`, admission stops and status shows `reason: all_seats_held`; releasing one seat admits one more.

### 4.4 Skip expired tokens
- [x] Covered by `admit.lua` returning `skipped`; commit the offset and continue.

**Check:** delete a queued token manually; the worker skips it and admits the next one.

### 4.5 Single worker per event
- [x] Nothing to build: the topic has one partition, and Kafka assigns a partition to only one consumer in a group. Extra workers in `admission-{event}` sit idle as hot standbys.
- [x] Log partition assignment and revocation with `ConsumerConfig::with_rebalance_listener(ConsumerRebalanceListener::new(|e| ...))`; it reports `Assigned`, `Revoked` and `Lost`.
- [x] The listener also keeps an `owns_partition` flag. A worker that loses the partition while holding a token (event full) drops it without committing and seeks back to it, so the new owner, which resumes from the committed offset, admits it instead. The flag also decides which worker opens the sale (step 4.6).

**Check:** start two workers for the same event; one logs the assignment, the other gets none. Kill the first; the second takes over within the session timeout (about 45 s after `kill -9`, about 1 s after Ctrl-C, which leaves the group) and continues from the last committed offset.


### 4.6 Open the sale: shuffle the lobby
- [x] The worker (`src/bin/admission_worker/lobby.rs`, a task beside admission) waits until `sale_opens_at`, then sets the event `ON_SALE` in Postgres. Only the partition owner does this and the flush below, so standbys never publish the same tokens; a new owner resumes from `lobby_published`. It runs apart from admission so a full event cannot stall it (joins would pile up in the lobby meanwhile).
- [x] Loop until done, alternating two things:
  - `scripts/flush_lobby.lua` with a batch size of 1,000: `SPOP lobby:{event} 1000`, then a Fisher-Yates shuffle of the batch seeded from Rust (`SPOP` picks members at random but returns them in the set's internal order, which for small sets is insertion order: join order); for each token `HSET token:{id} status QUEUED` and `RPUSH lobby_order:{event} {id}`. If `SPOP` returned nothing **and** everything in `lobby_order` has been published (`LLEN lobby_order == GET lobby_published`), `SET lobby_flushed:{event} 1` and return `done`.
  - Publish the not-yet-published part of `lobby_order:{event}` to Kafka in order, recording each token's `seq = offset + 1` and `published` as the join does, and advancing `lobby_published:{event}` after each batch is acknowledged, so a restarted worker continues where it stopped. A batch is sent concurrently; each seq comes from its own ack, so seq always matches the Kafka order. Replayed tokens are skipped by the admit script.
- [x] Until `lobby_flushed` is set, `join.lua` keeps sending joins to the lobby, so they are shuffled in with the next batch rather than published among the lobby users. Once it is set, every lobby user is already in Kafka and new joins get later offsets.

**Check:** join 100 users before opening; after opening all 100 are `QUEUED` with seqs 1-100 in an order different from their join order, and a user joining afterwards gets seq 101.

### 4.7 Estimated wait
- [x] Every 5 seconds the partition owner records a sample: `LPUSH admit_samples:{event} "{now_ms}:{admitted_upto}"`, `LTRIM admit_samples:{event} 0 11` (the last minute).
- [x] The status endpoint (`status.lua` also returns the newest and oldest sample, still one round trip) computes `rate = (newest seq - oldest seq) / (newest time - oldest time)` and `estimated_wait_secs = position / rate`; `null` when the rate is 0 (admission waiting for seats) or there are fewer than two samples. The window is a minute, so right after the sale opens it still holds pre-opening samples and the estimate runs high until they age out.

**Check:** (verified with a short `TOKEN_TTL_SECS` as the steady flow: estimate 35 s, actual 29 s) with a steady flow of reservations, `estimated_wait_secs` for a queued token falls roughly in step with the real wait.

### 4.8 Sold out and closing the sale
- [x] Sold out is a status, not a flag: the payment worker increments `sold:{event}` when it confirms a booking (step 6.3) and decrements it on a cancellation; the status endpoint compares it with `seat_count:{event}` (step 3.3).
- [x] Closing is final. The worker closes the sale at `start_time` (or when `POST /events/{id}/close` is called, which the worker notices within a second): `SET closed:{event} 1` first, so joins and admission stop at once, then set the event `CLOSED` in Postgres, and stop consuming. If the Postgres update fails the sale is still closed; a retry records it, and so does the worker whenever it sees the flag (it closes Postgres before exiting). The endpoint checks the event in Redis, like joins: `scripts/close.lua` sets the flag only if `seat_count:{event}` exists (`404` otherwise), so it never reads Postgres; the Postgres update leaves a canceled event canceled. `admit.lua` also returns `closed` when the flag is set, so nobody is admitted between the close and the worker noticing it. A worker started for a closed event exits at once. `ACTIVE` tokens are left to run out. Joins then return `410` and every `LOBBY` or `QUEUED` token reports `CLOSED`.

**Check:** after closing, joining returns `410` and a queued token reports `CLOSED`; with every seat paid for, a queued token reports `reason: sold_out`.
---

## Stage 5: Reservation

### 5.1 Reserve script
- [x] Create `scripts/reserve.lua`. Inputs: user id, event id, reservation id, seat indexes, hold ttl ms, grace ms, max seats. The token is found through `user_token:{event}:{user}` (like status and availability), so no token id is passed and ownership is automatic.
- [x] Logic, all or nothing (also refuses with `closed` once `closed:{event}` is set, and `invalid_seat` for an index outside `seat_count`):
  1. Token exists, status `ACTIVE`, belongs to user, matches event. If `USED`, return the existing `resv_id` stored on the token.
  2. `user_seats:{event}:{user}` + seat count ≤ max seats.
  3. Every requested `GETBIT seats:{event} i` is 0.
  4. `SETBIT` each to 1; `DECRBY available:{event} n`; `INCRBY user_seats:{event}:{user} n`.
  5. Compute `now` from `redis.call('TIME')`; `SET resv:{id} {json} PX {ttl + grace}` (JSON `{user, event, token, seats}`; grace 10 minutes, so the cleanup worker still finds the key after the hold ends); `ZADD holds:expiry {now + ttl} {id}`.
  6. `HSET token:{id} status USED resv {id}`; `ZREM active_tokens:{event} {token}` so the admission worker can let the next person in.
  - A refusal (seat taken by a faster buyer, per-user limit, invalid seat) leaves the token `ACTIVE`, so the user can pick again until one hold of up to `MAX_SEATS_PER_USER` succeeds. After that the turn is spent for good: release, expiry or failed payment never make the token `ACTIVE` again, and a reserve with a `USED` token whose hold has ended returns `turn_used` (`403`).
- [x] Load it with `redis::Script` in Rust (`cache::reserve_seats`).

**Check:** (`tests/reserve_scripts.rs`, needs `docker compose up -d redis`) unit test against a real Redis: two calls for the same seat, second returns "seat taken"; a retry with the same token returns the same reservation.

### 5.2 Reserve endpoint
- [x] `POST /reserve` (requires `AuthUser`) with `{event_id, seat_indexes}`: `400` for an empty, repeated or too long list; `403` if not admitted; `409` for a taken seat or the per-user limit; `410` once closed. A retry returns the first hold with `200`.
- [x] Run the script and nothing else: **reserve never touches Postgres.** It is the busiest moment of the sale, which is why holds live in Redis; the booking (with its locked price and seats) is created when payment starts (step 6.2).
- [x] Return `201 {reservation_id, expires_at}`. No amount: clients price seats from the cacheable `/layout`, and the price is locked in the booking at payment.

**Check:** full flow works: join, wait for active, reserve, see the seat taken in `GET /events/{id}/availability`.

### 5.3 Release
- [x] Create `scripts/release.lua`: if `resv:{id}` exists, `DEL` it, `ZREM holds:expiry {id}`, clear seat bits, `INCRBY available:{event} n`, `DECRBY user_seats:{event}:{user} n`, and nothing else: the token stays `USED`, since the turn was spent on the reservation. Returns `released`, `not_found`, or `forbidden` when an owner is given and does not match (checked in the same call).
- [x] `DELETE /reserve/{id}` (owner only, `403` otherwise; `404` with no live hold): run the script and return `204`. Redis only, like reserve. **Stage 6 must stop this releasing a hold whose payment is in progress** (e.g. `release.lua` refuses once the hold is extended for payment).

**Check:** reserve then release; the seat is available again, admission resumes if it was paused, and the releasing user cannot reserve again (`403`).

### 5.4 Concurrency test
- [x] `load/reserve_race.sh [N]` (curl `--parallel`; k6 is not installed): 1,000 active tokens all reserve the same seat. Result: one `201`, 999 `409`, `available` 999, zero rows written to Postgres, in 0.13 s.

**Check:** exactly one `201`, all others "seat taken".

---

## Stage 6: Payment

### 6.1 Provider abstraction
- [x] Create `src/payment/mod.rs` with a trait `PaymentProvider { create_intent(reservation_id, amount, idempotency_key) -> intent_id; refund(intent_id) }`. Blocking methods: the payment worker is a synchronous loop, and the API calls them inside `web::block`. `PAYMENT_PROVIDER` picks it (`fake` by default).
- [x] Implement `FakeProvider` that succeeds and, after `FAKE_PAYMENT_DELAY_MS`, inserts a synthetic event into `payment_inbox`. The intent id is derived from the idempotency key (same key, same intent, one event), and the event has Stripe's shape (`data.object.id`, `data.object.metadata.reservation_id`) so one parser serves both.

**Check:** (`tests/fake_provider.rs`) unit test creates an intent and sees an inbox row appear.

### 6.2 Pay endpoint
- [x] `POST /pay/{resv}` with header `Idempotency-Key` (`400` without it; `403` for another user's hold; `410` if the hold is gone; `409` if already paid).
- [x] If the reservation already has a booking, return its intent (created with the booking's first key, whatever key a retry sends).
- [x] Extend the hold once with a Lua script (`scripts/start_payment.lua`; the extension is added to the hold's own expiry, since `now + extension` could shorten it; a past-due hold counts as gone): if `resv:{id}` exists, belongs to the caller and its `state` is `held` (absent means `held`), `ZADD holds:expiry XX {now + extension} {id}`, `PEXPIRE resv:{id}`, set `state` to `paying`; if it is already `paying`, return it unchanged (a retried pay). Return the hold (event, seats). The hold's `state` moves `held` → `paying` → `paid`; `release.lua` releases only `held` holds, so a user cannot cancel during payment.
- [x] Create the booking here, the first Postgres write for this purchase: in one transaction a `PAYMENT_PENDING` `bookings` row (id = the reservation id, `total_cents` summed from `event_seats.price_cents`, the new `expires_at`, the key and intent id) plus one `booking_seats` row per seat.

**Check:** calling twice with the same key returns the same intent; the hold expiry moved forward once only.

### 6.3 Payment worker
- [x] Create `src/bin/payment_worker.rs` that loops: `SELECT ... FROM payment_inbox WHERE processed_at IS NULL ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`, one event per transaction (a failure rolls back only that event, which records the attempt and is retried; payments are rare next to joins, so this costs little).
- [x] For a success event, confirm in two phases so a retry after a worker crash is safe. (Deleting `resv:{id}` as the claim would not be: if the worker crashed after the delete and before the Postgres commit, the retried inbox row would find the key gone, read that as "expired", and refund a customer who paid.)
  1. **Claim** with `confirm.lua` (reservation id, payment intent id): if `resv:{id}` is gone or past due (so payment and expiry never both win, even before the cleanup worker runs), return `gone`. If `paid_by` is set, return `won` when it is this intent (a retry), else `gone` (a second payment for the same hold). Otherwise set `paid_by` and `state` `paid`, keep the key for 24 hours (`CLAIMED_HOLD_TTL_MS`), `ZREM holds:expiry {id}` so expiry can no longer take it, and return `won` with the hold.
  - **The claim first runs when the event is received** (`payment::record_event`, used by the fake provider and later the Stripe webhook): store the inbox row, then claim. The worker's claim then finds its own. Without this, a success event that arrived in time but waited in the inbox past the hold's expiry was refunded: the capped 50,000-user buying-path test refunded 11,634 of 34,947 buyers because one payment worker fell 225 s behind. The row is stored before the claim so a failed claim only falls back to the worker; the other order could claim a hold with no event left to confirm it. Claims share one Redis connection (`ClaimConnection`, reconnecting and retrying once): opening one per event made Redis reset connections at about 666 payments a second, and those 6,794 failed claims were refunded by the backlogged worker. **Check:** the same test with one payment worker 906 s behind refunds 0 of 35,266 payments (`tests/reserve_scripts.rs` `payment_claimed_on_receipt_survives_a_worker_backlog` covers the scripts).
  2. **Commit** on `won`: one transaction, see below.
  3. **Finalize** after the commit with `finalize.lua`: `DEL resv:{id}` and, only if that deleted it, `INCRBY sold:{event} n` (step 4.8), so the two happen together. A crash before this leaves the key to expire on its own TTL, and `sold` is corrected by the startup rebuild (step 7.4).
  - Commit: run `UPDATE event_seats SET status = 'SOLD', booking_id = $1 WHERE id = ANY($2) AND status = 'AVAILABLE'`, check the affected count, set the booking `CONFIRMED`. The status check is the guard (the `version` column is not needed); it costs nothing, since the update has to run anyway, and only matters if Redis and Postgres disagree (Redis data loss, or a rebuild during a confirmation). If the count is short, roll back and refund. Leave `user_seats:{event}:{user}` as it is: paid seats keep counting toward the per-user limit, which a user who rejoins must not escape.
  - On `gone`: call `refund`, set the booking `REFUNDED`. (Every payment has a booking: it is created in step 6.2 before the intent.)
- [x] Set `processed_at` in the same transaction. An event for a booking already `CONFIRMED` or `REFUNDED` is a duplicate and only marked processed.

**Check:** fake payment confirms a booking; `event_seats` rows are `SOLD` with the booking id. Kill the worker between the claim and the commit, restart it: the booking is `CONFIRMED`, not `REFUNDED` (tested by running `confirm.lua` by hand with the event's intent, the state a crash leaves, then starting the worker).

### 6.4 Late payment
- [x] Set the hold TTL to a few seconds and the fake provider delay longer than the TTL plus extension.

**Check:** the booking ends `REFUNDED` and the seat is available again (`AVAILABLE` in Postgres at once; free in Redis once the cleanup worker of Stage 7 releases the hold).

### 6.5 Stripe (test mode)
- [ ] Implement `StripeProvider` using PaymentIntents with `Idempotency-Key`.
- [ ] `POST /webhooks/stripe` verifies the signature, inserts into `payment_inbox` (ignore duplicate `provider_event_id`), returns `200`.

**Check:** `stripe listen --forward-to localhost:8080/webhooks/stripe` plus a test card confirms a booking; replaying the same event does nothing.

---

## Stage 7: Expiry

### 7.1 Claim script
- [x] Create `scripts/claim_expired.lua`: `ZRANGEBYSCORE holds:expiry -inf {now} LIMIT 0 {batch}`; for each id, `ZREM`, then if `DEL resv:{id}` returns 1, clear seat bits and adjust counters; return released ids with their event and seats. Like `release.lua`, leave the token `USED`: the turn was spent (with the hold gone, the user may join again). Never releases a hold with `paid_by` (it has left the queue anyway). Returns how many were due plus each released id with its `state`, so only `paying` ones touch Postgres.

**Check:** unit test with two holds, one expired, returns only the expired one.

### 7.2 Cleanup worker
- [x] Create `src/bin/cleanup_worker.rs` (synchronous, like the payment worker): loop running the claim script (batch 500); bulk `UPDATE bookings SET status = 'EXPIRED' WHERE id = ANY($1) AND status = 'PAYMENT_PENDING'` (most expired holds never started payment and have no booking, so they touch nothing); if the batch was full, run again immediately, otherwise sleep 500 ms. Ids Postgres refuses are kept and retried. Once a minute a sweep also expires `PAYMENT_PENDING` bookings whose hold ended over 10 minutes ago, in case a worker crashed between releasing a hold and marking its booking (a late claim by a payment still confirms: the payment worker only skips `CONFIRMED` and `REFUNDED`).

**Check:** reserve without paying; after the hold TTL the seat is available and no booking exists. Start payment but never confirm it; after the extended TTL the booking is `EXPIRED`.

### 7.3 Multiple cleanup workers
**Check:** run three workers with 5,000 holds expiring together; every hold is released exactly once (count `EXPIRED` rows) within a second or two. Result: 5,000 released (1,500 / 1,500 / 2,000 per worker), all within a second of coming due; 5,000 `EXPIRED`, `available` 5,000, no bits set, `user_seats` 0 (a double release would make it negative).

### 7.4 Startup rebuild
- [x] Rebuild `seats:{event}` and `available:{event}` from `event_seats` (`SOLD`, `BLOCKED`) plus the seats in every live `resv:{id}` (holds are in Redis only, so they survive only as long as those keys do), `sold:{event}` from the `SOLD` count, `user_seats` (paid seats plus live holds), `sale_opens_at`, `closed`, and `lobby_flushed` once the sale is open. Only for events whose seat map is **missing**: a rebuild over a live map would lose holds made meanwhile. The cleanup worker checks every 10 seconds (so a Redis restart is handled without restarting the app), takes `rebuild_lock:{event}`, and runs `scripts/rebuild.lua`, which re-reads each hold and writes `seat_count` last in the same atomic call; joins and reserves refuse an event without `seat_count`, so no hold can be made during the rebuild. Tokens, queue positions and the lobby are not rebuilt: after losing them, users join again.

**Check:** `FLUSHALL` in Redis except reservations, restart; the seat map matches Postgres. (Tested by deleting an event's keys except its holds: within 10 seconds sold, held and blocked seats were taken again, counters matched, joins worked.)

---

## Stage 8: Consolidation

### 8.1 Remove legacy code (done early, during Stage 1)
- [x] Delete the old conference handlers, `actions.rs`, `queue.rs`, `models.rs`, the Python tests, and `API_REFERENCE.md`.
- [x] Remove the `rabbitmq` service and volume from `docker-compose.yml`, the `amqprs` and `regex` crates, and `rabbitmq_url` from `Config`.
- [x] Delete the old migrations; the schema is now only `ticketing_schema` and `booking_tables`.

**Check:** `cargo build` with no warnings; `diesel migration run` succeeds.

### 8.2 Split binaries
- [x] Move shared code into a library crate (`src/lib.rs`). Done during Stage 1.
- [x] Binaries `conference` (the API), `admission_worker`, `cleanup_worker`, `payment_worker` each have their own `main`. The admission worker without an event id runs every scheduled and on-sale event (one task each, found every 10 seconds, restarted if one fails), so one service covers events created later; with an id it runs just that one.
- [x] Add each to `docker-compose.yml`, under the `app` profile and one image (`Dockerfile`, multi-stage release build): `docker compose --profile app up -d --build`. Without the profile only Postgres, Redis and Kafka start, for running the app with cargo. Kafka gained an `INTERNAL` listener (`kafka:29092`) for containers; `localhost:9092` stays for the host. The API runs the embedded migrations at startup (`diesel_migrations`) and listens on `BIND_ADDR`.

**Check:** `docker compose --profile app up` starts all services; the full flow works end to end. (Verified: join before opening, admitted at opening by the containerized worker, reserve, pay, `CONFIRMED` by the payment worker; a fresh database is migrated by the API.)

### 8.3 Final load test
- [x] `load/final.sh [N]` (k6 is not installed; the simulated buyers are a small Rust crate, `load/final/`, that signs its users' JWTs with `JWT_SECRET` instead of 10,000 slow Argon2 logins): 10,000 users join an event with 100 seats; admitted users reserve a random free seat; 70% pay, 30% abandon. Runs release builds of the API and workers with short holds (20 s + 20 s for payment) so abandoned seats return quickly.
- [x] End-of-run assertions in SQL:
  - `SELECT count(*) FROM event_seats WHERE event_id = $1 AND status = 'SOLD'` ≤ 100
  - no `booking_id` appears on more seats than it reserved
  - no `PAYMENT_PENDING` booking past `expires_at` + a few seconds
  - also: every sold seat's booking is `CONFIRMED`, `CONFIRMED` bookings equal the users who paid, and Redis `sold` and `available` match Postgres
- [x] Record results in `load/RESULTS.md`.

**Check:** all assertions pass; results recorded. Result (after the fix below): 100 sold to exactly the 100 buyers who paid, 50 abandoned holds resold, 9,850 turned away as sold out, no server errors; status served 3,800 req/s at p50 0.3 ms, p99 0.8 ms. The first 10,000-user run found a bug: status polls during the lobby shuffle republished tokens the worker had not published yet (15,340 messages for 10,000 users, one `500`, status p99 59 ms); lobby tokens are now marked `published = lobby` by `flush_lobby.lua` so the API leaves them to the worker.

### 8.5 Capacity
- [x] `load/capacity.sh USERS POLL_MS DURATION [APIS] [JOIN_SPREAD_MS]`: joins after the sale opened, then steady status polling; samples Redis, API and generator CPU; checks one Kafka message and one distinct place in line per join. The load generator gained a `poll` mode and round-robin over several API addresses.
- [x] Found and fixed: kafkit-client's idempotent producer rejected its own batches under a join burst (idempotence off, 2 ms linger: join p50 98 ms to 3.6 ms), and a single Redis connection per API process became the bottleneck (`REDIS_CONNECTIONS`, 8).

**Check:** find the ceiling. Result: about 30,000-37,000 status polls per second on one machine, set by Redis (one core near 500,000 commands per second); p99 ≤ 5 ms up to 20,000 users polling every second. Details and next steps in `load/RESULTS.md`.

### 8.4 Documentation
- [x] Update `README.md` and `API_REFERENCE.md` for the new endpoints and services. (`API_REFERENCE.md` was rewritten from the handlers; the old one described the removed conference API.)

---

## Stage 9: Flash-Sale Model

The concurrency design (waiting room, Redis-only holds, the payment inbox with claim, commit and finalize, the cleanup worker, the rebuild) is unchanged; each step below renames or reshapes the data it works on.

### 9.1 Renames
- [x] Cargo package `conference` became `flash-sale` (library `flash_sale`, API binary `flash-sale`, `default-run = "flash-sale"`); every `use conference::` was updated.
- [x] Database `conferences` became `flash_sale` (`docker-compose.yml`, `.env`); Docker image `ampq-waitlist-app` became `flash-sale`, and the Dockerfile and the API's compose command run `flash-sale`.
- [x] `MAX_SEATS_PER_USER` became `DEFAULT_MAX_PER_USER`, default 1: the per-user limit of a product created without `max_per_user`.

**Check:** `cargo build --all-targets` succeeds; `grep -rniE "seat|venue|booking|conference"` over `src`, `scripts`, `migrations`, `tests`, `Cargo.toml`, `Dockerfile`, `docker-compose.yml` and `.env` finds nothing.

### 9.2 Schema
- [x] The three ticketing migrations were replaced (the database is renamed, so it starts fresh); `00000000000000_diesel_initial_setup` stays. `..._products` creates `users` (nullable `username` and `password_hash`, `is_guest`, a check that non-guests have both), `products` (unique `sku`, `price_cents`, `max_per_user` default 1, `sale_opens_at`, `sale_ends_at`, `status` check) and `inventory` (`stock_total`, `stock` with `0 <= stock <= stock_total`, `version`). `..._orders` creates the `order_status` enum, `orders` (UUID id = the reservation id, `quantity`, `unit_price_cents`, `total_cents`, the two payment keys) with its indexes, and `payment_inbox` as before.
- [x] Dropped: venues, seats, events, event seats, bookings, booking seats and their enums. Removed `src/actions/venues.rs`, `src/api/venues.rs`, the layout and availability endpoints, `scripts/availability.lua`, `cache::is_taken`, `SeatMapConfig` and the ETag code. `src/schema.rs` and the models follow the new tables; `OrderStatus` replaces `BookingState`.

**Check:** the API applies the migrations to an empty `flash_sale` database at startup.

### 9.3 Stock as counters
- [x] Redis keys per product replace the seat keys: `stock:{p}` (units neither held nor sold, replacing the bitmap and `available`), `stock_total:{p}` (replacing `seat_count`; its existence means Redis knows the product), `sold:{p}`, `max_per_user:{p}` (read inside `reserve.lua`) and `user_units:{p}:{u}`. The waiting room keys keep their shape with the product id in place of the event id.
- [x] `cache::init_product(product_id, stock_total, max_per_user, sale_opens_at)` sets them, and clears the product's leftover waiting room keys (`closed`, lobby keys, `admitted_upto`, `active_tokens`, `admit_samples`), since a recreated database reuses product ids and a stale `closed` flag would end the new sale. `cache::stock_snapshot` reads `available`, `sold`, `total` and `closed` in one pipeline (`None` when `stock_total` is missing).
- [x] `reserve.lua` takes a quantity: closed, token, a retry of a `USED` token (returns the hold with its real quantity), not `ACTIVE`, product unknown to Redis (`out_of_stock`), quantity not a whole number from 1 to `max_per_user` (`invalid_quantity`), user total over the limit (`too_many`), `stock < quantity` (`out_of_stock`); then `DECRBY stock`, `INCRBY user_units`, the hold, the expiry entry and the token `USED`. A refusal leaves the token `ACTIVE`. The BITPOS idea from the Roadmap was not needed: fungible units are never picked.
- [x] `release.lua` and `claim_expired.lua` give the quantity back to `stock` and take it off `user_units`; `finalize.lua` adds it to `sold` (or takes it off `user_units` after a refund); `admit.lua` admits while active tokens are below `min(max_active, stock)`; `join.lua`, `close.lua` and `status.lua` use `stock_total`, `stock` and `sold` (the status reasons are `all_units_held` and `sold_out`).
- [x] `rebuild.lua` takes the total, the sold units (`stock_total - inventory.stock`), the live holds (each re-read, its quantity subtracted) and the paid units per user from `CONFIRMED` orders, and sets `stock = total - sold - held`, writing `stock_total` last. The cleanup worker scans the live holds first, then reads Postgres in one repeatable-read snapshot and skips holds whose order is already `CONFIRMED`: their units are in the sold count, and counting them as holds too would take them twice with nothing to give them back.

**Check:** `cargo test` (below) covers the scripts; the rebuild is checked end to end in 9.8.

### 9.4 Products API
- [x] `POST /products` (`400` for a bad sku, name, price, stock outside 1 to 1,000,000, `max_per_user` below 1, times not in the future or not in order; `409` for a duplicate sku) inserts the product and its inventory row in one transaction, then, after the commit, `init_product` and the Kafka topic.
- [x] `GET /products` (not canceled, soonest sale first, `limit` default 50 and at most 100, `offset`; cacheable for 30 s), `GET /products/{id}` (30 s), `GET /products/{id}/stock` (Redis only, cacheable for 1 s, `404` when Redis does not know the product) and `POST /products/{id}/close`. Query-string errors answer `400 {"message"}` like every other error.
- [x] The admission worker works on products (`sale_opens_at` opens the sale, `sale_ends_at` closes it).

**Check:** see 9.8 (a product with 3 units is created, listed and read).

### 9.5 Reservations
- [x] `POST /reservations` with `{"product_id", "quantity"}` (quantity defaults to 1): `201` with `{reservation_id, product_id, quantity, expires_at}`, `200` for a retry (with the hold's quantity), `400` for an invalid quantity, `403` not admitted or turn used, `409` out of stock (`only {n} left`, `sold out`) or over the limit, `410` closed. `DELETE /reservations/{id}` releases a hold.

**Check:** `tests/reserve_scripts.rs`, 9 tests against Redis (`docker compose up -d`): a hold takes units from `stock` and is refused as out of stock when too few remain; a retry returns the same reservation; unadmitted users, invalid quantities, an unknown product and a closed sale are refused; release and expiry return the units while the turn stays used; a user cannot rejoin until the hold ends and keeps the unit limit when rejoining; a payment claimed on receipt survives a worker backlog; and 200 admitted users reserving at once for 10 units (`max_per_user` 1) end with exactly 10 held, 190 out of stock and `stock:{p}` equal to 0.

### 9.6 Payment and orders
- [x] `POST /reservations/{id}/pay` (same logic as the old `/pay/{id}`, with `Idempotency-Key`) creates the order with `unit_price_cents` from the product and `total_cents = unit_price_cents * quantity`, and always answers `202 Accepted` with `Location: /orders/{id}` and `{order_id, reservation_id, status, payment_intent_id, amount_cents, expires_at, tracking_url}`, retries included.
- [x] `GET /orders/{id}` (`403` for another user's order, `404`, `Cache-Control: no-store`, `Retry-After: 1` while `PAYMENT_PENDING`) and `GET /orders` (newest first, `limit` default 20 and at most 100).
- [x] The payment worker confirms with `actions::orders::confirm_order` in a savepoint: `UPDATE inventory SET stock = stock - $qty, version = version + 1 WHERE product_id = $1 AND stock >= $qty` must update exactly one row; otherwise it rolls back and refunds. Then the order becomes `CONFIRMED` and `finalize.lua` adds the quantity to `sold`. The cleanup worker expires `PAYMENT_PENDING` orders in place of bookings.

**Check:** see 9.8 (three payments return `202` with a `Location` header and the orders reach `CONFIRMED`).

### 9.7 Guest sessions
- [x] `POST /session` inserts a guest user (`is_guest`, no username or password) inside `web::block` and returns `201 {token, expires_at, user_id}`. Register, login and `GET /auth/me` stay; login rejects guest rows with the same "invalid username or password".

**Check:** the four guest sessions created with curl for the run in 9.8 were accepted by the waiting room, reservation and order endpoints.

### 9.8 End-to-end run
- [x] `cargo build --all-targets` and `cargo clippy --all-targets` have no errors; `cargo test` passes with `docker compose up -d` running.
- [x] Against a fresh database, with the API and the three workers started with cargo, using curl: created a product with 3 units and a sale opening about 20 s ahead; created 4 guest sessions; all joined (`LOBBY`); after the opening they were admitted (`ACTIVE`) and reserved 1 unit each; paid for the held units (`202` with `Location`) and polled the orders.
- [x] Release and expiry: with a short `HOLD_TTL_SECS`, an unpaid hold expired and its unit returned to `stock`; a released hold did the same. Closing the product made joins answer `410` and waiting users read `CLOSED`. Deleting a product's counters from Redis (except its holds) made the cleanup worker rebuild them within 10 seconds.

**Check:** 3 orders `CONFIRMED` for the 3 units, no fourth order; `GET /products/{id}/stock` shows `sold` 3 and `available` 0; `SELECT stock FROM inventory WHERE product_id = ...` is 0.
