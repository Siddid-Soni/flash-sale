# Case Study: Flash Sale for 500 Smartphones

This document answers the placement case study (`Fullstack_Placement_Case_Study.docx`) section by section. For each section it points to the code that does the work and to the measured result, and it says plainly what was not measured. It is written to be read aloud in an interview.

All measurements come from [load/RESULTS.md](load/RESULTS.md) unless a line says otherwise. They were taken on one development machine (20 cores) that also ran the load generator, so they show that the design holds together under that load, not what a production cluster would do. Section 8 lists the limits.

Contents: 1 Scenario and problem, 2 Core requirements, 3 Evaluation framework (frontend, backend, database), 4 Reference solution compared, 5 What Redis keeps and what PostgreSQL decides, 6 Races, 7 Trade-offs, 8 Limits of the measurements, 9 Scorecard, 10 Reproducing the results.

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
| Frontend | Graceful loading and queue states | `web/src/logic/state.ts` (screens: loading, ready, reserving, queue (unit held, payment link being prepared), payment, confirmed, sold out, expired, failed, refunded, retry); `429` honours `Retry-After`, `503` and network errors back off with jitter | Unit tests for state mapping, backoff and `Retry-After` (`web/src/logic/*.test.ts`); not tested in a browser here |
| Frontend | Bundle < 200 KB, TTI < 1.5 s | `web/scripts/check-budgets.mjs` (`npm run budgets`) | `npm run budgets`: 47.5 kB raw, 16.7 kB gzipped (JavaScript and CSS together), limit 200 kB raw and 50 kB gzipped. Lighthouse time to interactive: 0.37 s desktop on the built files; through the gateway 0.4 s desktop and 1.4 s with Lighthouse's mobile throttling on a sale page (limit 1.5 s; the mobile margin is small) |
| Gateway | Load balancing across replicas, 0% dropped requests | `gateway/nginx.conf`: `least_conn` over three replicas, upstream keepalive, failover on connect errors, large listen backlog | 100,000 users in 10 s: 0 network failures, 0 5xx, 0 requests turned away. 100,000 users in 3 s: the same, at 34,314 checkouts/s. 10,000-user run: the same |
| Gateway | Rate-limit bad actors, block bots | `gateway/nginx.conf` (`limit_req`, `limit_conn`), `gateway/bots.conf`, `gateway/jwt.js` | `gateway/smoke.sh` is a functional check of the rules against the running stack (not a load result). In the load runs every simulated user had its own address and stayed under the limits, so the runs show **no** `429`s by design |
| Application | Stateless cluster, event-loop safety | All state is in Redis, Kafka or PostgreSQL. Actix-web handlers are async; Diesel calls run in `web::block`; Redis and Kafka calls are async (`src/api/`, `src/bin/order_service.rs`). The Order Service serves only checkout and order status; sessions, sale details, the sale list, search and admin are a separate `site` service (`src/bin/site.rs`), so they cannot take capacity, connections or database time from checkout | Every run went through the gateway and three replicas; the split per replica was not recorded |
| Application | Heavy writes out of the request cycle; 202 and a tracking URL | `src/api/checkout.rs` (`202` and `Location: /v1/orders/status/{ticket}`); order insert, payment session and confirmation in `src/processor.rs` and `src/inbox.rs` | Checkout p50 0.5 ms, p95 0.9 ms, p99 1.4 ms at a peak of 10,105 req/s (100,000 users in 10 s); p50 6.3 ms, p99 119.6 ms at a peak of 34,314 req/s (100,000 users in 3 s) |
| Application | Message queue and workers | Kafka topic `orders.checkout` (`src/queue.rs`), order processors, expiration worker (`src/expiry.rs`) | Backlog run: 10,000 queued orders at 200/s, all served |
| Database | Exactly 500 sold, atomic decrement | `scripts/checkout.lua`; `tests/checkout_scripts.rs` (`ten_thousand_concurrent_checkouts_sell_exactly_500`) | 100,000-user run: exactly 500 `202`, 99,500 `410`; `CONFIRMED` never above the payments that arrived in time (265 of 500; see section 8) |
| Database | ACID, conditional SQL update as the backstop | `confirm_order` in `src/actions/orders.rs`: order to `CONFIRMED` and `UPDATE products SET sold = sold + 1 WHERE id = $1 AND sold < total_inventory` in one transaction; `CHECK (sold BETWEEN 0 AND total_inventory)` and a partial unique index (one live order per user and product) in `migrations/` | `products.sold` = number of `CONFIRMED` orders in every run, never above 500 |
| Database | No row lock timeouts, no application-level counters | Checkout and status never open a PostgreSQL connection. The only counter in PostgreSQL moves in the confirming transaction, one row update per paid order | 0 5xx in the 100,000-user run; the hot path opens no PostgreSQL connection, so there is no row lock to wait on |

## 3. Evaluation framework

### 3.1 Frontend strategy

- **Debouncing and throttling.** Debouncing delays a click, which is wrong here: the first click should go out at once. The button locks synchronously on the first click (`web/src/app.tsx`, before any state update or `await`), so two clicks in one tick cannot make two requests. The request carries an `Idempotency-Key` created once per attempt and reused by every retry, including retries after `429`, `503` and network errors. If a retry races the original, `checkout.lua` returns the original ticket with `200` instead of making a second hold. The gateway limits checkout to 2 requests per second per user (burst 5) as a backstop for clients that are not our frontend.
- **Asset prefetching.** The product image is preloaded in `index.html`; the checkout and status code is a separate chunk (`web/src/flow.ts`) fetched with `import('./flow')` before the sale opens, so the first click does not wait for a download. Static files are served as `public, max-age=31536000, immutable` with hashed names. The user's guest session is created while the page waits for the start time, so session creation (an insert) is not part of the spike.
- **Polling versus WebSockets.** The client polls `GET /v1/orders/status/{ticket}` every 2 s, and only winners poll. Non-winners get `410` and stop. The trade-off is in section 7.
- **Optimistic UI.** On click the page moves to a "reserving" screen immediately, before the server answers, and, while the order is `PENDING`, says the unit is held and the payment link is being prepared (no place in line: a reserved unit has nothing left to wait for but its link, a few seconds). It does not show "you got one" before the `202`: the answer is only known after Redis decides, and a wrong optimistic success would be worse than a short wait. The redirect to payment is also not optimistic; it waits for the link.

### 3.2 Backend and middleware architecture

- **Load balancing.** NGINX with `least_conn` over three identical replicas (`docker-compose.yml`, `gateway/nginx.conf`). A failed or timed-out connect goes to the next replica; a request that was already sent is never repeated, so the gateway never runs a checkout twice (the idempotency key covers the client's own retry). Replicas are re-resolved through Docker DNS so a restarted one rejoins without a reload. Every response has `X-Replica`. Checkout and status go to the replicas; the site service is a single upstream. The gateway also absorbs the burst: the listen backlog is 4096 and upstream connections are kept alive. The app's own `HTTP_BACKLOG` and `HTTP_MAX_CONNECTIONS` do the same, so a burst waits in a queue instead of being refused. *Measured:* no failed connection in the 100,000-user run. *Not measured:* a replica dying under load (the failover rule is configured and checked by `gateway/smoke.sh`, not load-tested).
- **Distributed caching.** Two Redis roles. The inventory Redis (`REDIS_URL`) gates admission only (`stock:{sale}`, `user_live`, the idempotency key, `sale_start:{sale}`); it keeps no per-ticket state; the read cache (`READ_CACHE_URL`) holds `status:<ticket>`, what clients poll, so polling cannot slow down checkout. They are one instance locally and meant to be separate clusters in production. Sale details are cached for 1 s at the gateway and 1 s in each replica (`src/api/sales.rs`), with a lock so a stampede reaches the database once. In the 100,000-user run `GET /v1/sales/{id}` served 100,000 requests at a peak of 10,147 req/s with p50 0.8 ms and p99 58.7 ms. Each replica also keeps an in-memory sold-out flag, refreshed from Redis every 100 ms, so once the last unit is gone, later requests are answered `410` from memory without a Redis call (`SoldOutFlags` in `src/api/checkout.rs`).
- **Message queue.** Kafka carries `{ticket, sale, user, checkout_ms, idempotency_key}` from checkout to the order processors, everything they need to create the order: Redis keeps no record of the ticket. It absorbs a burst of winners so processors, payment and PostgreSQL run at their own speed. The producer uses `acks=all` and idempotence; if the publish fails, PostgreSQL decides what happens to the unit: the ticket is inserted as a `FAILED` order, and only the call that created that row returns the unit and answers `503` with `Retry-After` (`src/api/checkout.rs`; `tests/checkout.rs`, `a_failed_publish_records_a_failed_order_returns_the_unit_and_the_retry_succeeds`). Kafka against RabbitMQ is discussed in section 7.
- **Workers.** The order processor (`src/processor.rs`) handles several messages per partition at once and commits only the contiguous finished prefix, so a crash redelivers rather than loses work; every step is idempotent. Its second loop (`src/inbox.rs`) confirms or refunds paid orders from the `payment_inbox` table. The expiration worker (`src/expiry.rs`) expires unpaid orders from PostgreSQL and returns their units. Several of each can run at once: Kafka splits partitions among processors, inbox rows and due orders are claimed with `FOR UPDATE SKIP LOCKED`, and the `UPDATE ... SET status = 'EXPIRED'` hands each order to exactly one worker.

### 3.3 Database consistency and concurrency control

**Pessimistic and optimistic locking.** Pessimistic locking (`SELECT ... FOR UPDATE`) makes the second buyer wait until the first commits. It is correct and simple, but under a spike all buyers queue on one row, each holding the lock across a network round trip and a commit. Optimistic locking (a `version` column) lets everyone read and detects the conflict at write time; the loser retries. It is also correct, and it is cheap when conflicts are rare. For a hot row they are the common case, so most attempts are wasted.

**Why a Redis Lua script and not `SELECT` then `UPDATE`.** `SELECT stock` followed by `UPDATE ... SET stock = <value read> - 1` is a read-modify-write across two statements. Between them, another request can read the same value. Both write the same result and one decrement is lost; both buyers are told yes. That is the 540-unit bug. A Redis script avoids it because Redis runs a script as one command on a single thread: no other command runs in the middle, so the check and the decrement cannot be separated. There is no lock to wait on, no version to retry, and no round trip between read and write.

`checkout.lua` does more than decrement, and that is the second reason to use a script. In one atomic step it checks the start time (using Redis's clock, not the replica's), the idempotency key, whether the user already holds one, and the stock, then decrements and records the user (`user_live`, with a TTL that outlives the sale) and the key. A plain `DECR` would leave gaps: the counter could go below zero and need undoing, and a crash between `DECR` and writing the markers would let one user take two units.

**The demo** (`load/oversell_demo.sh`, 2026-10-29 01:53): 5,000 buyers start at the same moment for 500 units; the SQL strategies share a pool of 50 connections and the latency includes waiting for one; PostgreSQL 16 at the default isolation (READ COMMITTED), Redis 7, same machine. The Lua script here is a bare decrement-if-positive, not the full `checkout.lua`.

| Strategy | Told yes | Units taken in the store | Oversold | Seconds | Buyers/s | p50 ms | p99 ms | Notes |
|---|---|---|---|---|---|---|---|---|
| Naive `SELECT` then `UPDATE` | 5,000 | 87 | 4,500 | 3.87 | 1,293 | 1,921 | 3,828 | The store lost 4,913 of 5,000 decrements |
| `SELECT ... FOR UPDATE` | 500 | 500 | no | 4.25 | 1,176 | 2,146 | 4,210 | Row lock wait: mean 41.4 ms, max 382 ms |
| Optimistic `version` column | 500 | 500 | no | 10.64 | 470 | 10,580 | 10,635 | 12,894 retries (2.6 per buyer, up to 200); 46 buyers gave up |
| `UPDATE ... WHERE stock > 0` | 500 | 500 | no | 0.55 | 9,045 | 490 | 549 | |
| Redis Lua script | 500 | 500 | no | 0.02 | 243,034 | 11.4 | 15.4 | No PostgreSQL |
| `FOR UPDATE` with `lock_timeout` 20 ms | 500 | 500 | no | 2.47 | 2,025 | 1,422 | 2,443 | 2,293 of 5,000 buyers got a lock timeout error and were not told sold out |

How each strategy fails:

- **Naive: lost updates.** All 5,000 buyers read a positive stock before any write landed, so all 5,000 were told yes, and the table ended with 87 decrements recorded. The case study's 540 is the same bug with less concurrency (a few buyers share a read).
- **`FOR UPDATE`: lock waits and timeouts.** Correct, but buyers queue behind one row. Latency is the position in the queue: p50 2.1 s, p99 4.2 s, with 5,000 buyers and 50 connections. Add a lock timeout, which production systems have, and the tail of the queue gets errors instead of a clean sold-out: 2,293 buyers failed in the last row. Those are the "row lock timeouts" of the case study.
- **Optimistic: retry storm.** Every successful write invalidates every other buyer's read. With 5,000 contenders, most attempts lose and retry, and the retries compete again. The run took 10.6 s, about 2.6 retries per buyer on average, and 46 buyers hit the 200-retry cap. Throughput is worse than the locking version because the lost work is repeated.
- **Conditional `UPDATE`: correct, but a single-row hot spot.** `UPDATE ... SET stock = stock - 1 WHERE id = $1 AND stock > 0` is atomic in the database, so there is no lost update, and it is the better SQL answer by a wide margin (0.55 s). But all 5,000 buyers still send a transaction to PostgreSQL, and every update to that one row waits for the previous one's lock and commit. The ceiling is the commit rate of one row (9,045 buyers/s here with 50 connections). The 4,500 losers also send a transaction and wait for the same row.
- **Redis Lua: atomic, single-threaded.** No row, no lock, no disk commit on the decision path, and the 4,500 losers cost one script call each. 0.02 s for 5,000 buyers. The number is for a bare decrement and is not a promise for the full script through the gateway (the full path, through the gateway, took 34,314 checkouts per second in the 3 s burst of 100,000 users without reaching its ceiling).

The conditional `UPDATE` is not dropped: it is the backstop in `confirm_order`, where it runs at most once per paid order and never under a spike.

## 4. The reference solution compared

| Case study | Here | Why it differs |
|---|---|---|
| A: lock the button, CDN for static assets, state manager with polling or SSE | Same. Button lock, hashed immutable assets from NGINX (stand-in for a CDN), polling every 2 s | See section 7 for what a real CDN changes |
| B: NGINX/ALB in front of an auto-scaling stateless cluster | NGINX in front of three fixed replicas | Autoscaling is not implemented; section 7 |
| B: Redis holds the inventory token; atomic because single-threaded | `checkout.lua` | Same idea, with the checks and the markers in the same script |
| B: RabbitMQ; 202 and a tracking URL | Kafka; `202` and `status_url` | Section 7 |
| C: consumers read the queue sequentially and run `UPDATE inventory SET stock = stock - 1 WHERE product_id = :id AND stock > 0` | Processors run concurrently (32 per partition). The conditional update is `sold < total_inventory` in the confirming transaction, per paid order | In the reference the SQL guard is the primary control. Here Redis is the primary control and SQL the second guard. Checking inside the consumer alone would leave users told "yes" at the door and refused after a long wait; here the user is told at the door |

The reference solution's claim that the conditional update "completely removes the possibility of a race condition" is true for that statement. What it does not remove is the cost of sending every buyer to PostgreSQL, and in the reference design the buyers are told yes before the consumer decides.

## 5. What Redis keeps and what PostgreSQL decides

Redis gates admission only. Kafka is the queue. PostgreSQL decides every outcome (code: `scripts/`, `src/processor.rs`, `src/inbox.rs`, `src/expiry.rs`). A winner holds a unit from checkout until payment or expiry, but no hold object records it: the unit is the decrement of `stock:{sale}`, the user is marked by `user_live:{sale}:{user}`, and the order is whatever PostgreSQL has for the ticket.

```
checkout --> PENDING --(order created)--> AWAITING_PAYMENT --(paid before expires_at)--> CONFIRMED
 (Redis only    |                                |
  admits)       | publish failed                 | unpaid: 5 min after the link
                v                                v
             FAILED                           EXPIRED          (a payment after that: REFUNDED)
```

| Stage | Waiting for | Ended by | Rule |
|---|---|---|---|
| `PENDING` (read cache only, no row) | An order processor | The processor inserts the order `AWAITING_PAYMENT` | No clock runs: the deadline is set when the order is created |
| `PENDING`, publish failed | | The Order Service, at once | Inserts a `FAILED` order (`ON CONFLICT DO NOTHING`); only if it created the row: `INCR stock`, clear `user_live` and the key (`return_unit.lua`) |
| `AWAITING_PAYMENT` | The user to pay | Expiration worker | `status = 'AWAITING_PAYMENT' AND expires_at <= now()` in PostgreSQL; one `UPDATE` for a batch, then `return_unit.lua` once per order |
| `CONFIRMED` | | Never | The unit stays taken; `user_live` is cleaned up by its TTL (a day past the sale) |

**A queue backlog never fails anyone.** A queued ticket has no order and so no timer. While its message sits in Kafka the processor will get to it, and the five-minute payment window starts when the order is created (`expires_at` is set by the processor, not at checkout). If the window started at checkout, a slow queue would take the users' payment time or cancel their orders, and the last users in a long queue would be punished for being last. Nothing in Redis or the worker looks at queued tickets at all, so a backlog cannot release one. *Measured*: `load/backlog.sh` throttled the processor to 200 messages per second with 10,000 users and 10,000 units, so the queue took about 50 s to drain against a 30 s payment window. The slowest checkout-to-link time was 50,429 ms (p50 29,171 ms, p95 47,430 ms), longer than the whole payment window, and still: 0 `FAILED` orders, the least time left to pay when a link first appeared was 27,804 ms of 30,000 ms (the difference is about one polling interval), and every user who paid in the last half of their window ended `CONFIRMED` (5,025 payments, 5,025 `CONFIRMED`). *Not measured:* the design point, 100,000 units at 1,000 messages per second.

**A failed publish is PostgreSQL's decision too.** The producer either delivers a message within `delivery.timeout.ms` or reports failure. On failure the Order Service inserts the ticket as a `FAILED` order. The insert is `ON CONFLICT DO NOTHING` on the ticket, and only a call that created the row returns the unit. If the row already existed, the message reached Kafka after all and a processor made the order: the checkout succeeded and the answer is `202`. If the message arrives *after* the `FAILED` row, the processor finds it, expires the payment session it just made, and writes nothing else. The `FAILED` row is stored under the ticket as its idempotency key, not the client's key, so the client's retry with the same key (the compensation forgets it in Redis) can still make its order. *Tested:* `tests/checkout.rs` (`a_failed_publish_whose_message_made_an_order_is_a_successful_checkout`, `a_failed_publish_records_a_failed_order_...`), `tests/processor.rs` (`a_failed_order_stops_the_processor_and_expires_its_session`).

**Expiry is driven by PostgreSQL, and the inbox never decides a deadline.** The worker's `UPDATE orders SET status = 'EXPIRED' ... WHERE id IN (SELECT id ... WHERE status = 'AWAITING_PAYMENT' AND expires_at <= now() ORDER BY expires_at LIMIT n FOR UPDATE SKIP LOCKED) RETURNING ...` expires a batch (`orders_awaiting_payment_expires_at_idx` is a partial index over just those rows). For each order it returns, once and without retry, `return_unit.lua` runs. Then the cache status and the payment session follow, and those may be retried (the sweep repairs the rest). A payment for an order still `AWAITING_PAYMENT` after its `expires_at` changes nothing: the event stays unprocessed in the inbox, and once the worker has expired the order the retry finds it `EXPIRED` and refunds it, moving no stock. So only one actor, the worker, returns stock for an expired order, and the processors never touch an inventory key.

**What this accepts.** The rare failures are listed in [docs/FAILURE_CASES.md](docs/FAILURE_CASES.md), and each can only sell *fewer* units than there are: an Order Service that dies between the script and the publish leaves a unit and a `user_live` marker with no order; a worker that dies, or Redis failing, between the order's `EXPIRED` update and the `INCR` loses that unit. The worker cannot tell after a failure whether its `INCR` ran, and a second one could give a unit to two buyers, so it does not retry. Neither case can oversell, because PostgreSQL confirms a payment only with `UPDATE products SET sold = sold + 1 WHERE sold < total_inventory`. The price of the older design was a per-ticket record in Redis that duplicated Kafka's queue and PostgreSQL's decisions, plus a rule to find leaked records by comparing timestamps with consumer progress.

One consequence to know: returned stock is available to the next buyer. In the 100,000-user run 235 units came back after the payment window and went unsold, because every other user had already been told `410` and did not retry. In a real sale a page that polls `sold_out` would show them again.

## 6. Exactly one winner

Every decision is made by PostgreSQL's row lock on the order, or by a statement that does nothing a second time. When two callers race, one acts and the other finds the result. The loser gets a defined answer.

| Race | How one wins | The loser |
|---|---|---|
| **Payment against expiry** (`settle_payment` against `expire_batch`) | Both need the order's row lock. The confirming `UPDATE` requires `status = 'AWAITING_PAYMENT' AND expires_at > now()`, the worker's requires `expires_at <= now()`, on PostgreSQL's one clock, so the conditions cannot both be true | A payment that arrives after the deadline but before the worker is left in the inbox and retried; once the order is `EXPIRED` it is refunded (`REFUNDED`, no stock moved). A worker that loses finds the order `CONFIRMED` and does nothing |
| **Publish failure against the processor** (`insert_failed` against `insert_awaiting_payment`) | Both insert the ticket `ON CONFLICT DO NOTHING`; the first row wins | The Order Service that loses answers `202` and returns no unit; a processor that loses sees `FAILED`, expires its session and stops |
| **Several expiration workers** | `FOR UPDATE SKIP LOCKED` and the status change: an order is returned by exactly one `UPDATE` | The others never see it; each unit is returned once |
| **Several processors on one payment event** | `FOR UPDATE SKIP LOCKED` on the inbox row; the order update is `WHERE status = 'AWAITING_PAYMENT'`, so only one confirms | The other skips |

*Tested* against real Redis and PostgreSQL: `tests/expiry.rs` (`payment_and_expiry_racing_leave_exactly_one_winner_per_order`, `several_workers_return_each_expired_unit_exactly_once`, `confirmed_refunded_and_failed_orders_never_return_stock`), `tests/inbox.rs` (`a_payment_after_the_deadline_but_before_the_worker_is_left_for_the_retry`, `several_workers_share_the_inbox`). The racing test ends with `stock + products.sold == total`: every unit is either back in stock or sold, none twice. *Measured at load*: in the 10,000-user run 5% of payers paid 150 ms before their link expired, racing the expiration worker. 17 payments landed within 1.5 s of the deadline; 6 ended `CONFIRMED` and 11 `REFUNDED`, none lost. 0 payments surely in time failed to confirm and 0 payments after the deadline were left unrefunded (the provider refused the 9 that came after the worker had expired the session).

Two ordering rules around the expiry keep the loser safe: the worker marks the order `EXPIRED` only `WHERE status = 'AWAITING_PAYMENT'` so it cannot overwrite a `REFUNDED` or `CONFIRMED` the inbox wrote, and it expires the payment session only after the unit is back, never before (a session that was paid meanwhile is left as it is, for the inbox to refund). The status entry in the read cache only moves forward (`set_status.lua`), so a delayed write cannot undo a later one.

## 7. Trade-offs

### Redis durability

Compose starts Redis with `--appendonly yes`, so the append-only file is on with Redis's default `appendfsync everysec`. A crash can lose roughly the last second of writes. (The one-second figure is Redis's documented behaviour; a Redis crash was not tested here.) What the loss would mean:

- A lost decrement: `stock:{sale}` too high, so more than 500 units could be reserved. The guard in the confirming transaction (`sold < total_inventory`) makes the 501st payment fail; it is refunded, so the total sold stays at 500. Users who got a `202` and then a refund had a bad experience, but there is no oversell.
- A lost `user_live` marker: a user could reserve a second unit; the unique index on live orders per user and product refuses the second order.
- A lost stock return: a unit missing from stock, so a sale that sells out one unit early (the same shape as the accepted failure cases).
- Redis keeps no sale counter: `products.sold` is the only count.
- The read cache (`status:<ticket>`) is the only source of the status endpoint, which never reads PostgreSQL, so a flood of made-up tickets cannot reach the database. Every writer retries until its write lands and entries live 24 h, far beyond the payment window; losing the read cache's data (see Redis loss) would show `404` until the next transition writes the entry again.

Rebuilding `stock:{sale}` from PostgreSQL (`total_inventory - sold -` live orders) and a reconciliation job are in the design but **not implemented**; after a Redis loss an operator would have to reset the counters by hand. For a stricter setup: `appendfsync always` (slower), Redis replicas (replication is asynchronous, so failover can lose the same last moments), and separate clusters for inventory and read cache. Kafka has the same shape. Compose runs one broker with replication factor 1, so `acks=all` means one copy. Production needs three brokers with `min.insync.replicas=2`. A Kafka message lost anyway leaves a reserved unit with no order, which is lost for the rest of the sale (accepted failure case 1, section 5): fewer units sold, never more.

### Polling versus SSE

Only winners poll: about 500 users at one request per 2 s is roughly 250 requests per second, served from the read cache. The gateway allows 2 requests per second per user. In the backlog run 10,000 users all polled (275,901 requests, peak 5,096 req/s, p99 0.7 ms), about twenty times the rate expected for 500 winners, and the read-cache path kept its latency. SSE would update the user at once rather than within 2 s, and it would cut requests. It would also need long-lived connections through the gateway (buffering off, timeouts raised, per-IP connection limits reconsidered), a way to fan out state changes to the replica that holds each connection (Redis pub/sub or similar), and reconnect handling. Two seconds is invisible next to a redirect to a payment page, and a poll is stateless, so any replica can answer it. SSE becomes worth the work if the audience that polls grows by orders of magnitude or if latency of status matters. It is in the deferred list.

### Kafka versus RabbitMQ

The case study names RabbitMQ; this system uses Kafka. At about 500 messages per sale, throughput does not decide it, and either broker would do the job of absorbing the burst.

What Kafka gives this design: a durable, replayable log. Committed offsets say exactly what has been processed, and a log can be replayed after a bug. What it costs: heavier operations, parallelism capped at the partition count (6 here), and per-partition ordering where RabbitMQ's competing consumers are simpler. A slow message holds back its partition's committed offset, which is why the processor handles 32 messages per partition at once and commits only the contiguous finished prefix. RabbitMQ is the better fit for a plain work queue with per-message retry and dead-lettering, and would be an equally good choice here: nothing in this design depends on offsets beyond committing them.

### What a real CDN and autoscaling change

- **CDN.** NGINX here serves static files and caches the site service's sale routes (`GET /v1/sales/{id}` for 1 s, the list and search for 5 s) on the same machine as the application. A real CDN would serve both from edge locations, so the origin would see roughly one request per second per edge location for sale details instead of a share of 100,000 (not measured). It cannot cache checkout or status, which are per-user. It also changes things the gateway relies on: the client address arrives in a header, so the per-IP limits would need a trusted-proxy list (the gateway has one switch for that, used by the load tests); bot filtering moves to the CDN's WAF and a real CAPTCHA, where this system has only a User-Agent list and a stub hook (`SESSION_CHALLENGE`). Answering "sold out" at the edge is possible and is deferred. TTI would be measured against the CDN, not this origin, and was not measured at all here.
- **Autoscaling.** The Order Service is stateless, so replicas can be added; the order processors scale up to the partition count (6) and the expiration worker can run several copies. A 10-second spike is shorter than the time to start instances, so capacity has to be in place before 8:00 PM (scheduled scaling, not CPU-triggered scaling). The processors could scale on Kafka lag. None of this exists here: replicas are fixed at three, and there is no metrics endpoint to scale on (deferred).

### Stock buckets across shards (deferred)

One Redis primary holds `stock:{sale}`, and one script call per checkout is enough for the load in the case study. For more load or many simultaneous products, the design splits a sale's stock into N buckets (`stock:{sale:b}`), sends each user to a home bucket by hashing the user id, tries the other buckets when the home bucket is empty so no stock is stranded, and reports sold out only when every bucket is empty. The user's live-order marker stays in the home bucket's slot. Key names already carry the sale as a hash tag (`stock:{42}`), so a sale's keys share a slot if Redis is clustered. Not implemented, and the single primary is a single point of failure.

## 8. Limits of the measurements

- **One machine.** The load generator, NGINX, three replicas, one processor, one expiration worker, PostgreSQL, Redis and Kafka shared 20 cores in Docker: the generator pinned to 10 of them, the whole stack to the other 10, and the generator sending to the gateway's container address (no Docker port proxy in between). Client-side latencies still include the generator's own work. It says nothing about network latency between machines or about failover, because there is no second copy of anything.
- **Arrival pattern.** The 10 s runs spread 100,000 users uniformly over 10 s, so the offered load is about 10,000 checkouts per second plus the same number of sale-detail requests; their checkout peak of 10,105 req/s is the offered rate, not a measured ceiling. The 3 s burst offers about 33,000 checkouts per second and was absorbed at 34,314 req/s peak (p99 119.6 ms), with the stack using about 8 of its 10 cores; the ceiling was not reached.
- **The harness had to be fixed before it measured the app.** Through `localhost:8000`, Docker's userland port proxy took about 9 cores at 30,000 requests per second, and with the generator unpinned it competed with the stack for CPU; tools polling Docker (`lazydocker`) took 2 more cores. Earlier 3 s runs therefore failed with timeouts and ephemeral-port exhaustion while NGINX and the Order Services were mostly idle. `load/README.md` (Known local limits) lists the settings the recorded runs used, including the host's port range.
- **Confirmed is not 500.** In the 100,000-user runs 500 users got a `202`, but only 60% of winners paid (5% of those too late), so the 10 s run ended with 300 `CONFIRMED` and 200 `EXPIRED`. The invariant is that `CONFIRMED` equals the payments that arrived in time and never exceeds 500; a run in which all 500 winners pay was not made at load. `tests/checkout_scripts.rs` covers the Redis side (10,000 concurrent checkouts, exactly 500 reserved, stock never below zero, one `user_live` marker per winner).
- **Generator details that favour the system.** The generator signs most JWTs itself (50 of 100,000 users used `POST /v1/session`), every user has its own `X-Forwarded-For` (the gateway trusts it only in the load-test mode), and it sends a browser-like User-Agent. So no rate limit or bot rule was exercised at load (0 `429`). Their behaviour is checked by `gateway/smoke.sh`.
- **Each run was recorded once.** Checkout-to-link time varied between otherwise identical 10 s runs (p50 1.6 s in one, 9.7 s in the recorded one); that was not investigated.
- **Backlog run at a tenth of the design point.** 10,000 users and units at 200 messages per second, not 100,000 at 1,000.
- **Oversell demo.** Latencies include waiting for one of 50 pooled connections. The Lua script is a bare decrement. Results from one run each.
- **Not measured:** a replica, the processor or the worker killed under load; Redis or Kafka loss; the full `checkout.lua` throughput in isolation; the benefit of a real CDN; the cost of rate limiting at the gateway.
- **Test coverage that stands in for some of it:** the integration tests in `tests/` run the scripts, races, crash-recovery paths (a crash after the commit, a failing event retried, a payment postponed until the worker expires the order) against real PostgreSQL, Redis and Kafka.

## 9. Scorecard: the traits of the top tier

The case study's top tier asks for these. Where each is shown:

| Trait | Where |
|---|---|
| Identifies the database row lock bottleneck and the oversell cause | Section 3.3 and the demo: lost updates, lock waits, retry storm, single-row hot spot |
| Asynchronous queue with atomic Redis tokens | `checkout.lua`, Kafka, `202` and a ticket (sections 2, 3.2) |
| Deep awareness of bundle optimisation and edge CDNs | 16.7 kB gzipped bundle with a lazy chunk, immutable hashed assets, 1 s cache on sale details; TTI 0.4 s desktop, 1.4 s mobile-throttled; what a real CDN changes in section 7 |
| Handles the failures after the happy path | Sections 5 and 6: the accepted failure cases, backlog, publish failure, payment against expiry, refunds, crash recovery |

## 10. Reproducing the results

```bash
sudo sysctl -w net.ipv4.ip_local_port_range="10000 65535" net.ipv4.tcp_tw_reuse=1   # once per boot
export GEN_CPUS=0-9 STACK_CPUS=10-19   # on 20 cores; close anything polling Docker
docker compose up -d
docker compose --profile app up -d --build
load/flash.sh 100000      # exactly-500 invariants, latency table appended to load/RESULTS.md
ARRIVAL_SECS=3 load/flash.sh 100000   # the 3 s burst
load/backlog.sh           # 10,000 users, processor throttled to 200/s
load/oversell_demo.sh     # the five strategies side by side
cd web && npm run budgets # bundle size (and Lighthouse TTI if installed)
gateway/smoke.sh          # gateway rules: limits, bots, admin, cache
cargo test                # with the app containers stopped
```

Each load script prints PASS or FAIL per check and exits non-zero on a FAIL. Setup and settings are in [load/README.md](load/README.md).
