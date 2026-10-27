# Case Study: Flash Sale for 500 Smartphones

This document answers the placement case study (`Fullstack_Placement_Case_Study.docx`) section by section. For each section it points to the code that does the work and to the measured result, and it says plainly what was not measured. It is written to be read aloud in an interview.

All measurements come from [load/RESULTS.md](load/RESULTS.md) unless a line says otherwise. They were taken on one development machine (20 cores) that also ran the load generator, so they show that the design holds together under that load, not what a production cluster would do. Section 8 lists the limits.

Contents: 1 Scenario and problem, 2 Core requirements, 3 Evaluation framework (frontend, backend, database), 4 Reference solution compared, 5 Holds, 6 Races, 7 Trade-offs, 8 Limits of the measurements, 9 Scorecard, 10 Reproducing the results.

## 1. Scenario and problem statement

500 smartphones go on sale at a fixed time; more than 100,000 requests reach checkout in the first 10 seconds. The earlier trial had high latency, row lock timeouts, request timeouts, and sold 540 units against 500 in stock.

Each symptom has one cause in the old design (a synchronous request that reads stock from the database, then writes it) and one answer here:

| Symptom | Cause | What this system does |
|---|---|---|
| Overselling (540 sold) | Check and decrement were separate steps, so two requests could both see the last unit | The check and the decrement are one Redis Lua script (`scripts/checkout.lua`); PostgreSQL has a conditional `UPDATE` as a second guard (`src/actions/orders.rs`, `confirm_order`) |
| Row lock timeouts | Every request waited on the same stock row in PostgreSQL | The hot path never touches PostgreSQL. Only about 500 winners reach it |
| High latency, timeouts | The request waited for slow work (order insert, payment) | `202 Accepted` with a ticket after one Redis call and one Kafka publish; everything else happens behind a queue |
| Spike lands on the database | 100,000 requests, one resource | 99,500 of them get `410` from Redis, or from memory once the replica knows the sale is sold out |

## 2. Technical core requirements

The case study's table has four layers. For each: what was required, where it is, what was measured.

| Layer | Requirement | Where | Measured |
|---|---|---|---|
| Frontend | Stop repeated clicks from sending repeated requests | `web/src/app.tsx` (`beginAttempt` disables the button in the click handler, before any `await`); one `Idempotency-Key` per attempt, reused by every retry; the server also dedups by key (`checkout.lua`) | Unit tests in `web/src/**/*.test.ts`. No browser-level duplicate-click test was run |
| Frontend | Graceful loading and queue states | `web/src/logic/state.ts` (screens: loading, ready, reserving, queue with position, payment, confirmed, sold out, expired, failed, refunded, retry); `429` honours `Retry-After`, `503` and network errors back off with jitter | Unit tests for state mapping, backoff and `Retry-After` (`web/src/logic/*.test.ts`); not tested in a browser here |
| Frontend | Bundle < 200 KB, TTI < 1.5 s | `web/scripts/check-budgets.mjs` (`npm run budgets`) | `npm run budgets`: 38.6 kB raw, 14.7 kB gzipped (JavaScript and CSS together), limit 200 kB raw and 50 kB gzipped. **TTI was not measured**: the script runs Lighthouse when it is installed, and it was not installed here |
| Gateway | Load balancing across replicas, 0% dropped requests | `gateway/nginx.conf`: `least_conn` over three replicas, upstream keepalive, failover on connect errors, large listen backlog | 100,000-user run: 0 network failures, 0 5xx, 0 requests turned away. 10,000-user run: the same |
| Gateway | Rate-limit bad actors, block bots | `gateway/nginx.conf` (`limit_req`, `limit_conn`), `gateway/bots.conf`, `gateway/jwt.js` | `gateway/smoke.sh` is a functional check of the rules against the running stack (not a load result). In the load runs every simulated user had its own address and stayed under the limits, so the runs show **no** `429`s by design |
| Application | Stateless cluster, event-loop safety | All state is in Redis, Kafka or PostgreSQL. Actix-web handlers are async; Diesel calls run in `web::block`; Redis and Kafka calls are async (`src/api/`, `src/bin/order_service.rs`) | Every run went through the gateway and three replicas; the split per replica was not recorded |
| Application | Heavy writes out of the request cycle; 202 and a tracking URL | `src/api/checkout.rs` (`202` and `Location: /v1/orders/status/{ticket}`); order insert, payment session and confirmation in `src/processor.rs` and `src/inbox.rs` | Checkout p50 1.1 ms, p95 13.4 ms, p99 78.6 ms at a peak of 10,153 req/s (100,000 users) |
| Application | Message queue and workers | Kafka topic `orders.checkout` (`src/queue.rs`), order processors, expiration worker (`src/expiry.rs`) | Backlog run: 10,000 queued orders at 200/s, all served |
| Database | Exactly 500 sold, atomic decrement | `scripts/checkout.lua`; `tests/checkout_scripts.rs` (`ten_thousand_concurrent_checkouts_sell_exactly_500`) | 100,000-user run: exactly 500 `202`, 99,500 `410`; `CONFIRMED` never above the payments that arrived in time (265 of 500; see section 8) |
| Database | ACID, conditional SQL update as the backstop | `confirm_order` in `src/actions/orders.rs`: order to `CONFIRMED` and `UPDATE products SET sold = sold + 1 WHERE id = $1 AND sold < total_inventory` in one transaction; `CHECK (sold BETWEEN 0 AND total_inventory)` and a partial unique index (one live order per user and product) in `migrations/` | `products.sold` = `sold:{sale}` = number of `CONFIRMED` orders in every run |
| Database | No row lock timeouts, no application-level counters | Checkout and status never open a PostgreSQL connection. The only counter in PostgreSQL moves in the confirming transaction, one row update per paid order | 0 5xx in the 100,000-user run; the hot path opens no PostgreSQL connection, so there is no row lock to wait on |

## 3. Evaluation framework

### 3.1 Frontend strategy

- **Debouncing and throttling.** Debouncing delays a click, which is wrong here: the first click should go out at once. The button locks synchronously on the first click (`web/src/app.tsx`, before any state update or `await`), so two clicks in one tick cannot make two requests. The request carries an `Idempotency-Key` created once per attempt and reused by every retry, including retries after `429`, `503` and network errors. If a retry races the original, `checkout.lua` returns the original ticket with `200` instead of making a second hold. The gateway limits checkout to 2 requests per second per user (burst 5) as a backstop for clients that are not our frontend.
- **Asset prefetching.** The product image is preloaded in `index.html`; the checkout and status code is a separate chunk (`web/src/flow.ts`) fetched with `import('./flow')` before the sale opens, so the first click does not wait for a download. Static files are served as `public, max-age=31536000, immutable` with hashed names. The user's guest session is created while the page waits for the start time, so session creation (an insert) is not part of the spike.
- **Polling versus WebSockets.** The client polls `GET /v1/orders/status/{ticket}` every 2 s, and only winners poll. Non-winners get `410` and stop. The trade-off is in section 7.
- **Optimistic UI.** On click the page moves to a "reserving" screen immediately, before the server answers, and shows queue position while the order is `PENDING`. It does not show "you got one" before the `202`: the answer is only known after Redis decides, and a wrong optimistic success would be worse than a short wait. The redirect to payment is also not optimistic; it waits for the link.

### 3.2 Backend and middleware architecture

- **Load balancing.** NGINX with `least_conn` over three identical replicas (`docker-compose.yml`, `gateway/nginx.conf`). A failed or timed-out connect goes to the next replica; a request that was already sent is never repeated, so the gateway never runs a checkout twice (the idempotency key covers the client's own retry). Replicas are re-resolved through Docker DNS so a restarted one rejoins without a reload. Every response has `X-Replica`. The gateway also absorbs the burst: the listen backlog is 4096 and upstream connections are kept alive. The app's own `HTTP_BACKLOG` and `HTTP_MAX_CONNECTIONS` do the same, so a burst waits in a queue instead of being refused. *Measured:* no failed connection in the 100,000-user run. *Not measured:* a replica dying under load (the failover rule is configured and checked by `gateway/smoke.sh`, not load-tested).
- **Distributed caching.** Two Redis roles. The inventory Redis (`REDIS_URL`) holds `stock:{sale}`, the holds and the queues; the read cache (`READ_CACHE_URL`) holds `status:<ticket>`, what clients poll, so polling cannot slow down checkout. They are one instance locally and meant to be separate clusters in production. Sale details are cached for 1 s at the gateway and 1 s in each replica (`src/api/sales.rs`), with a lock so a stampede reaches the database once. In the 100,000-user run `GET /v1/sales/{id}` served 100,000 requests at a peak of 10,147 req/s with p50 0.8 ms and p99 58.7 ms. Each replica also keeps an in-memory sold-out flag, refreshed from Redis every 100 ms, so once the last unit is gone, later requests are answered `410` from memory without a Redis call (`SoldOutFlags` in `src/api/checkout.rs`).
- **Message queue.** Kafka carries `{ticket, sale, user, checkout_ms}` from checkout to the order processors. It absorbs a burst of winners so processors, payment and PostgreSQL run at their own speed. The producer uses `acks=all` and idempotence; if the publish fails, the hold is released at once and the client gets `503` with `Retry-After` (`src/api/checkout.rs`; `tests/checkout.rs`, `a_failed_publish_returns_the_unit_and_the_retry_succeeds`). Kafka against RabbitMQ is discussed in section 7.
- **Workers.** The order processor (`src/processor.rs`) handles several messages per partition at once and commits only the contiguous finished prefix, so a crash redelivers rather than loses work; every step is idempotent. Its second loop (`src/inbox.rs`) confirms or refunds paid orders from the `payment_inbox` table. The expiration worker (`src/expiry.rs`) returns unpaid and leaked holds. Several of each can run at once: Kafka splits partitions among processors, inbox rows are claimed with `FOR UPDATE SKIP LOCKED`, and `release.lua` hands each hold to exactly one releaser.

### 3.3 Database consistency and concurrency control

**Pessimistic and optimistic locking.** Pessimistic locking (`SELECT ... FOR UPDATE`) makes the second buyer wait until the first commits. It is correct and simple, but under a spike all buyers queue on one row, each holding the lock across a network round trip and a commit. Optimistic locking (a `version` column) lets everyone read and detects the conflict at write time; the loser retries. It is also correct, and it is cheap when conflicts are rare. For a hot row they are the common case, so most attempts are wasted.

**Why a Redis Lua script and not `SELECT` then `UPDATE`.** `SELECT stock` followed by `UPDATE ... SET stock = <value read> - 1` is a read-modify-write across two statements. Between them, another request can read the same value. Both write the same result and one decrement is lost; both buyers are told yes. That is the 540-unit bug. A Redis script avoids it because Redis runs a script as one command on a single thread: no other command runs in the middle, so the check and the decrement cannot be separated. There is no lock to wait on, no version to retry, and no round trip between read and write.

`checkout.lua` does more than decrement, and that is the second reason to use a script. In one atomic step it checks the start time (using Redis's clock, not the replica's), the idempotency key, whether the user already holds one, and the stock, then decrements, creates the hold, adds it to `holds:pending:{sale}` and records the user and key. A plain `DECR` would leave gaps: the counter could go below zero and need undoing, and a crash between `DECR` and writing the hold would lose a unit.

**The demo** (`load/oversell_demo.sh`, 2026-10-27 20:57): 5,000 buyers start at the same moment for 500 units; the SQL strategies share a pool of 50 connections and the latency includes waiting for one; PostgreSQL 16 at the default isolation (READ COMMITTED), Redis 7, same machine. The Lua script here is a bare decrement-if-positive, not the full `checkout.lua`.

| Strategy | Told yes | Units taken in the store | Oversold | Seconds | Buyers/s | p50 ms | p99 ms | Notes |
|---|---|---|---|---|---|---|---|---|
| Naive `SELECT` then `UPDATE` | 5,000 | 116 | 4,500 | 4.24 | 1,179 | 2,115 | 4,201 | The store lost 4,884 of 5,000 decrements |
| `SELECT ... FOR UPDATE` | 500 | 500 | no | 4.61 | 1,086 | 2,376 | 4,566 | Row lock wait: mean 44.9 ms, max 528 ms |
| Optimistic `version` column | 500 | 500 | no | 10.26 | 487 | 10,202 | 10,257 | 12,800 retries (2.6 per buyer, up to 200); 11 buyers gave up |
| `UPDATE ... WHERE stock > 0` | 500 | 500 | no | 0.57 | 8,787 | 502 | 565 | |
| Redis Lua script | 500 | 500 | no | 0.02 | 208,225 | 16.0 | 19.8 | No PostgreSQL |
| `FOR UPDATE` with `lock_timeout` 20 ms | 500 | 500 | no | 2.53 | 1,976 | 1,470 | 2,505 | 2,313 of 5,000 buyers got a lock timeout error and were not told sold out |

How each strategy fails:

- **Naive: lost updates.** All 5,000 buyers read a positive stock before any write landed, so all 5,000 were told yes, and the table ended with 116 decrements recorded. The case study's 540 is the same bug with less concurrency (a few buyers share a read).
- **`FOR UPDATE`: lock waits and timeouts.** Correct, but buyers queue behind one row. Latency is the position in the queue: p50 2.4 s, p99 4.6 s, with 5,000 buyers and 50 connections. Add a lock timeout, which production systems have, and the tail of the queue gets errors instead of a clean sold-out: 2,313 buyers failed in the last row. Those are the "row lock timeouts" of the case study.
- **Optimistic: retry storm.** Every successful write invalidates every other buyer's read. With 5,000 contenders, most attempts lose and retry, and the retries compete again. The run took 10.3 s, about 2.6 retries per buyer on average, and 11 buyers hit the 200-retry cap. Throughput is worse than the locking version because the lost work is repeated.
- **Conditional `UPDATE`: correct, but a single-row hot spot.** `UPDATE ... SET stock = stock - 1 WHERE id = $1 AND stock > 0` is atomic in the database, so there is no lost update, and it is the better SQL answer by a wide margin (0.57 s). But all 5,000 buyers still send a transaction to PostgreSQL, and every update to that one row waits for the previous one's lock and commit. The ceiling is the commit rate of one row (8,787 buyers/s here with 50 connections). The 4,500 losers also send a transaction and wait for the same row.
- **Redis Lua: atomic, single-threaded.** No row, no lock, no disk commit on the decision path, and the 4,500 losers cost one script call each. 0.02 s for 5,000 buyers. The number is for a bare decrement and is not a promise for the full script through the gateway (the full path peaked at about 10,000 requests per second in the 100,000-user run, which is the offered load; which component limited it was not measured).

The conditional `UPDATE` is not dropped: it is the backstop in `confirm_order`, where it runs at most once per paid order and never under a spike.

## 4. The reference solution compared

| Case study | Here | Why it differs |
|---|---|---|
| A: lock the button, CDN for static assets, state manager with polling or SSE | Same. Button lock, hashed immutable assets from NGINX (stand-in for a CDN), polling every 2 s | See section 7 for what a real CDN changes |
| B: NGINX/ALB in front of an auto-scaling stateless cluster | NGINX in front of three fixed replicas | Autoscaling is not implemented; section 7 |
| B: Redis holds the inventory token; atomic because single-threaded | `checkout.lua` | Same idea, with the checks and the hold in the same script |
| B: RabbitMQ; 202 and a tracking URL | Kafka; `202` and `status_url` | Section 7 |
| C: consumers read the queue sequentially and run `UPDATE inventory SET stock = stock - 1 WHERE product_id = :id AND stock > 0` | Processors run concurrently (32 per partition). The conditional update is `sold < total_inventory` in the confirming transaction, per paid order | In the reference the SQL guard is the primary control. Here Redis is the primary control and SQL the second guard. Checking inside the consumer alone would leave users told "yes" at the door and refused after a long wait; here the user is told at the door |

The reference solution's claim that the conditional update "completely removes the possibility of a race condition" is true for that statement. What it does not remove is the cost of sending every buyer to PostgreSQL, and in the reference design the buyers are told yes before the consumer decides.

## 5. The two hold states

A winner holds a unit from checkout until payment, expiry or release. The hold has two states with two different release rules (code: `scripts/`, `src/expiry.rs`):

```
checkout --> PENDING --(link ready)--> AWAITING_PAYMENT --(payment claimed)--> PAID --> CONFIRMED
               |                              |
               | leaked: behind progress      | unpaid: 5 min after the link
               v                              v
            FAILED                         EXPIRED
```

| State | Waiting for | Released by | Rule |
|---|---|---|---|
| `PENDING` | An order processor | Expiration worker (leaked loop) | `checkout_ms < min(processed_upto) - delivery.timeout.ms` and still `PENDING` |
| `PENDING`, publish failed | | The Order Service, at once | `release.lua` |
| `AWAITING_PAYMENT` | The user to pay | Expiration worker (unpaid loop) | Score in `holds:expiry:{sale}` (link ready + `PAYMENT_WINDOW_SECS`) has passed |
| `PAID` | A processor to commit the order | Never | |

**A queue backlog never fails anyone.** A `PENDING` hold has no timer. While its message sits in Kafka the processor will get to it, and the five-minute payment window starts when the link is ready (`link_ready.lua` scores the hold in `holds:expiry` with `now + window`), not at checkout. If the window started at checkout, a slow queue would take the users' payment time or cancel their orders, and the last users in a long queue would be punished for being last. *Measured:* `load/backlog.sh` throttled the processor to 200 messages per second with 10,000 users and 10,000 units, so the queue took about 50 s to drain against a 30 s payment window. The slowest checkout-to-link time was 43,149 ms (p50 21,197 ms, p95 39,418 ms), longer than the whole payment window, and still: 0 `FAILED` orders, 0 leaked-hold releases logged, the least time left to pay when a link first appeared was 27,805 ms of 30,000 ms (the difference is about one polling interval), and every user who paid in the last half of their window ended `CONFIRMED` (4,942 payments, 4,942 `CONFIRMED`). *Not measured:* the roadmap's design point, 100,000 units at 1,000 messages per second. The 10,000-unit run is a tenth of that.

**A leaked hold always comes back.** If an Order Service dies after `checkout.lua` and before the publish, a hold exists and no message will ever arrive. With no timer it would hold the unit forever. The rule that releases it must not fire for a message that is merely slow. The expiration worker compares the hold's `checkout_ms` with how far the processors have progressed (`processed_upto:{sale}:{partition}`, written after each contiguous finished prefix), minus the longest a publish may take, `KAFKA_DELIVERY_TIMEOUT_MS` (30 s). Reasoning: the producer either delivers a message within that timeout or reports failure, and on failure checkout releases the hold itself. So a message published for a hold stamped `T` is in Kafka by `T + 30 s`. Once the processors have worked past `T + 30 s`, that message would already have been handled, so a still-`PENDING` hold has no message. Details that make this safe (`release_cutoff` in `src/expiry.rs`):

- Progress is the lowest value over all partitions. A partition with no lag counts as caught up to *now*, so an idle partition does not block releases. A lagging partition with no progress recorded blocks them.
- Lag is read from Kafka: the group's committed offset against the partition's high watermark. A message published between the two reads only makes a partition look lagging, which is the safe direction.
- A stalled processor releases nothing, because progress stops moving.
- Out-of-order publishes from different replicas are covered by the same 30 s margin.
- **The margin assumes a publish ends within `delivery.timeout.ms` of the checkout script.** `delivery.timeout.ms` counts from the moment the message is queued in the producer. If `publish` could wait for room in a full producer queue, that wait would add to the margin and the guarantee would break. So `KafkaQueue::publish` never waits for room (`Timeout::After(0)` in `src/queue.rs`): a full queue (500,000 messages) fails at once, the hold is compensated, and the client gets `503`.

*Tested:* `tests/expiry.rs` (`a_hold_without_a_message_is_released_once_progress_passes_it`, `a_slow_processor_releases_nothing`, `the_lag_of_a_partition_is_its_committed_offset_against_its_watermark`) against real Kafka. *Not load-tested:* an Order Service killed mid-checkout under load.

One consequence to know: returned stock is available to the next buyer. In the 100,000-user run 235 units came back after the payment window and went unsold, because every other user had already been told `410` and did not retry. In a real sale a page that polls `sold_out` would show them again.

## 6. Exactly one winner

Every state change on a hold is a Lua script that first checks the state and then moves the hold out of a sorted set with `ZREM`. `ZREM` removes a member once, so when two callers race, one sees `1` and the other `0`. The loser gets a defined answer.

| Race | How one wins | The loser |
|---|---|---|
| **Payment against expiry** (`confirm.lua` claim against `release.lua`) | Both work on the hold's entry in `holds:expiry:{sale}`. The claim needs the score to be in the future by Redis `TIME`; the release needs it to be due, by the same clock. The conditions cannot both be true, and `ZREM` settles the rest | A payment that loses the claim is refunded; the order and the client status become `REFUNDED`. A release that loses finds the hold `PAID` and does nothing |
| **Link against release** (`link_ready.lua` against `release.lua`, `PENDING`) | Both `ZREM` the hold from `holds:pending:{sale}` | A processor that loses sees `gone`, marks the order `FAILED` and expires its payment session. A release that loses leaves the hold alone |
| **Several expiration workers** | `release.lua` returns the unit (`INCR stock`) only in the call whose `ZREM` removed the hold | The others get `not_released`; stock is returned once |
| **Several processors on one payment event** | `FOR UPDATE SKIP LOCKED` on the inbox row; the order update is `WHERE status = 'AWAITING_PAYMENT'`, so only one confirms | The other skips |

*Tested* against real Redis and PostgreSQL: `tests/confirm_script.rs` and `tests/expiry.rs` (`payment_and_expiry_racing_leave_exactly_one_winner_per_hold`, `several_workers_release_each_hold_once`), `tests/link_ready.rs` (`link_and_release_race_has_one_winner`), `tests/inbox.rs` (`several_workers_share_the_inbox`). *Measured at load:* in the 100,000-user run 5% of payers paid 150 ms before their link expired, racing the expiration worker. 15 payments landed within 1.5 s of the deadline; 4 ended `CONFIRMED` and 11 `REFUNDED`, none lost. 0 payments surely in time failed to confirm and 0 payments after the deadline were left unrefunded (the provider refused the 4 that came after the worker had expired the session).

Two ordering rules around a release keep the loser safe: the worker marks the order `EXPIRED` only `WHERE status = 'AWAITING_PAYMENT'` so it cannot overwrite a `REFUNDED` the inbox just wrote, and it expires the payment session only after the release, never before. The status entry in the read cache only moves forward (`set_status.lua`), so a delayed write cannot undo a later one.

## 7. Trade-offs

### Redis durability

Compose starts Redis with `--appendonly yes`, so the append-only file is on with Redis's default `appendfsync everysec`. A crash can lose roughly the last second of writes. (The one-second figure is Redis's documented behaviour; a Redis crash was not tested here.) What the loss would mean:

- A lost decrement or hold: `stock:{sale}` too high, so more than 500 holds could be granted. The guard in the confirming transaction (`sold < total_inventory`) makes the 501st payment fail; it is refunded, so the total sold stays at 500. Users who got a `202` and then a refund had a bad experience, but there is no oversell.
- A lost hold whose payment arrives: the claim finds no hold and the payment is refunded.
- A lost release: a unit missing from stock, so a sale that sells out one unit early.
- `sold:{sale}` is a derived counter; `products.sold` is the record. They agree in every run, and PostgreSQL wins if they do not.
- The read cache (`status:<ticket>`) can be lost with no harm: the status endpoint falls back to the hold, then to PostgreSQL.

Rebuilding `stock:{sale}` from PostgreSQL (`total_inventory - sold -` live orders) and a reconciliation job are in the design but **not implemented**; after a Redis loss an operator would have to reset the counters by hand. For a stricter setup: `appendfsync always` (slower), Redis replicas (replication is asynchronous, so failover can lose the same last moments), and separate clusters for inventory and read cache. Kafka has the same shape. Compose runs one broker with replication factor 1, so `acks=all` means one copy. Production needs three brokers with `min.insync.replicas=2`. A Kafka message lost anyway becomes a leaked hold, which the expiration worker returns after the delivery-timeout margin; that is the safety net of section 5.

### Polling versus SSE

Only winners poll: about 500 users at one request per 2 s is roughly 250 requests per second, served from the read cache. The gateway allows 2 requests per second per user. In the backlog run 10,000 users all polled (237,456 requests, peak 5,072 req/s, p99 2.7 ms), about twenty times the rate expected for 500 winners, and the read-cache path kept its latency. SSE would update the user at once rather than within 2 s, and it would cut requests. It would also need long-lived connections through the gateway (buffering off, timeouts raised, per-IP connection limits reconsidered), a way to fan out state changes to the replica that holds each connection (Redis pub/sub or similar), and reconnect handling. Two seconds is invisible next to a redirect to a payment page, and a poll is stateless, so any replica can answer it. SSE becomes worth the work if the audience that polls grows by orders of magnitude or if latency of status matters. It is in the deferred list.

### Kafka versus RabbitMQ

The case study names RabbitMQ; this system uses Kafka. At about 500 messages per sale, throughput does not decide it, and either broker would do the job of absorbing the burst.

What Kafka gives this design: offsets. A message's position in its partition minus the processors' progress is the **queue position** shown to the user. The processors' committed offsets against the high watermark are the **lag** the leaked-hold rule uses to know whether a message can still arrive. Both come from the log's order; they would need separate machinery on a queue where messages are acknowledged and removed. A log can also be replayed after a bug. What it costs: heavier operations, parallelism capped at the partition count (6 here), and per-partition ordering where RabbitMQ's competing consumers are simpler. A slow message holds back its partition's committed offset, which is why the processor handles 32 messages per partition at once and commits only the contiguous finished prefix. RabbitMQ is the better fit for a plain work queue with per-message retry and dead-lettering, and would be the simpler choice if queue position and the offset-based release rule were not needed.

### What a real CDN and autoscaling change

- **CDN.** NGINX here serves static files and caches `GET /v1/sales/{id}` for 1 s on the same machine as the application. A real CDN would serve both from edge locations, so the origin would see roughly one request per second per edge location for sale details instead of a share of 100,000 (not measured). It cannot cache checkout or status, which are per-user. It also changes things the gateway relies on: the client address arrives in a header, so the per-IP limits would need a trusted-proxy list (the gateway has one switch for that, used by the load tests); bot filtering moves to the CDN's WAF and a real CAPTCHA, where this system has only a User-Agent list and a stub hook (`SESSION_CHALLENGE`). Answering "sold out" at the edge is possible and is deferred. TTI would be measured against the CDN, not this origin, and was not measured at all here.
- **Autoscaling.** The Order Service is stateless, so replicas can be added; the order processors scale up to the partition count (6) and the expiration worker can run several copies. A 10-second spike is shorter than the time to start instances, so capacity has to be in place before 8:00 PM (scheduled scaling, not CPU-triggered scaling). The processors could scale on Kafka lag. None of this exists here: replicas are fixed at three, and there is no metrics endpoint to scale on (deferred).

### Stock buckets across shards (deferred)

One Redis primary holds `stock:{sale}`, and one script call per checkout is enough for the load in the case study. For more load or many simultaneous products, the design splits a sale's stock into N buckets (`stock:{sale:b}`), sends each user to a home bucket by hashing the user id, tries the other buckets when the home bucket is empty so no stock is stranded, and reports sold out only when every bucket is empty. The user's live-order marker stays in the home bucket's slot. Key names already carry the sale as a hash tag (`stock:{42}`), so a sale's keys share a slot if Redis is clustered. Not implemented, and the single primary is a single point of failure.

## 8. Limits of the measurements

- **One machine.** The load generator, NGINX, three replicas, one processor, one expiration worker, PostgreSQL, Redis and Kafka shared 20 cores, in Docker with the userland proxy between the host and the gateway. Client-side latencies include the generator's own CPU use. It is a harsher test of the machine than of the design, and it says nothing about network latency between machines or about failover, because there is no second copy of anything.
- **Arrival pattern.** The generator spreads 100,000 users uniformly over 10 s, so the offered load is about 10,000 checkouts per second plus the same number of sale-detail requests; it is not 100,000 requests in one instant. The checkout peak of 10,153 req/s is the offered rate, not a measured ceiling.
- **The 100,000 run has one recorded success and one earlier failure.** A first 100,000-user attempt, started right after another run, had about 3,300 network failures. The cause was ephemeral-port exhaustion: a client host has about 28,000 local ports and the previous run's connections were still in `TIME_WAIT`. The generator retried with the same `Idempotency-Key`, so the counts stayed correct, but the check for zero network failures failed. `load/lib.sh` now waits for `TIME_WAIT` sockets to drain before the generator starts, and the recorded run (2026-10-27 20:52, in `load/RESULTS.md`) passed every check. That first attempt is not in `RESULTS.md`; `load/README.md` (Known local limits) describes it. Only one passing 100,000-user run is recorded; it was not repeated.
- **Confirmed is not 500.** In the 100,000-user run 500 users got a `202`, but only 60% of winners paid (5% of those too late), so the run ended with 265 `CONFIRMED`, 224 `EXPIRED` and 11 `REFUNDED`. The invariant is that `CONFIRMED` equals the payments that arrived in time and never exceeds 500; a run in which all 500 winners pay was not made at load. `tests/checkout_scripts.rs` covers the Redis side (10,000 concurrent checkouts, exactly 500 holds, stock never below zero, one hold per user).
- **Generator details that favour the system.** The generator signs most JWTs itself (50 of 100,000 users used `POST /v1/session`), every user has its own `X-Forwarded-For` (the gateway trusts it only in the load-test mode), and it sends a browser-like User-Agent. So no rate limit or bot rule was exercised at load (0 `429`). Their behaviour is checked by `gateway/smoke.sh`.
- **Backlog run at a tenth of the design point.** 10,000 users and units at 200 messages per second, not 100,000 at 1,000.
- **Oversell demo.** Latencies include waiting for one of 50 pooled connections. The Lua script is a bare decrement. Results from one run each.
- **Not measured:** TTI (Lighthouse was not installed); a replica, the processor or the worker killed under load; Redis or Kafka loss; the full `checkout.lua` throughput in isolation; the benefit of a real CDN; the cost of rate limiting at the gateway.
- **Test coverage that stands in for some of it:** the integration tests in `tests/` run the scripts, races, crash-recovery paths (a crash after the commit, a hold without a message, a failing event retried) against real PostgreSQL, Redis and Kafka.

## 9. Scorecard: the traits of the top tier

The case study's top tier asks for these. Where each is shown:

| Trait | Where |
|---|---|
| Identifies the database row lock bottleneck and the oversell cause | Section 3.3 and the demo: lost updates, lock waits, retry storm, single-row hot spot |
| Asynchronous queue with atomic Redis tokens | `checkout.lua`, Kafka, `202` and a ticket (sections 2, 3.2) |
| Deep awareness of bundle optimisation and edge CDNs | 14.7 kB gzipped bundle with a lazy chunk, immutable hashed assets, 1 s cache on sale details; TTI not measured; what a real CDN changes in section 7 |
| Handles the failures after the happy path | Sections 5 and 6: leaked holds, backlog, payment against expiry, refunds, crash recovery |

## 10. Reproducing the results

```bash
docker compose up -d
docker compose --profile app up -d --build
load/flash.sh 100000      # exactly-500 invariants, latency table appended to load/RESULTS.md
load/backlog.sh           # 10,000 users, processor throttled to 200/s
load/oversell_demo.sh     # the five strategies side by side
cd web && npm run budgets # bundle size (and Lighthouse TTI if installed)
gateway/smoke.sh          # gateway rules: limits, bots, admin, cache
cargo test                # with the app containers stopped
```

Each load script prints PASS or FAIL per check and exits non-zero on a FAIL. Setup and settings are in [load/README.md](load/README.md).
