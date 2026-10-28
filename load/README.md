# Load tests (Stage 9)

Scripts run from anywhere (`cd` to the repository root themselves), need Docker compose, `jq`,
`psql` access through the postgres container, and a Rust toolchain. Each prints PASS or FAIL per check,
exits non-zero on a FAIL, and appends a dated report (settings, latency table, checks) to `RESULTS.md`.

| Script | What it proves |
|---|---|
| `flash.sh [N]` | N users (default 10,000; the design target is 100,000) arrive within 10 s of the sale opening and try to buy one of 500 units through NGINX and three Order Service replicas. Winners poll, 60 % pay (5 % of the payers too late, 5 % racing the expiry), the rest abandon. Asserts: `CONFIRMED` orders equal the payments that arrived in time and are at most 500; a payment after the link expired ends `REFUNDED`; `products.sold` = `CONFIRMED` orders (sales are counted only in PostgreSQL); the run waits until PostgreSQL has no `AWAITING_PAYMENT` order for the sale and its inbox is drained, then `stock` = 500 - `products.sold` (every unit of an expired order returned once, none of a confirmed one); no `FAILED` orders; no user with two live orders; zero 5xx and zero network failures. Requests turned away by the rate limits (`429`) are reported separately. |
| `backlog.sh [N]` | The order processor is throttled (`RATE`, default 200 messages per second) so a real queue builds: N users and N units (default 10,000), so the last link comes ~40 s after the user's checkout, longer than the 30 s payment window. Asserts that nothing waiting in Kafka was given up (no `FAILED` order, no `EXPIRED` order without a link, every user got an order), that every user got the full payment window (time left when the link was first seen is the window minus at most one polling interval), that users paying late in that window are all `CONFIRMED`, plus the consistency checks of `flash.sh`. The roadmap's design point is `N=UNITS=100000 RATE=1000 load/backlog.sh 100000`. |
| `oversell_demo.sh` | 5,000 concurrent buyers (`BUYERS`) for 500 units (`UNITS`) against five strategies, each on its own table or key: naive `SELECT` then `UPDATE` (oversells), `SELECT ... FOR UPDATE` (correct, queues on the row lock; run again with a short `lock_timeout`), optimistic `version` (correct, retry storm), `UPDATE ... WHERE stock > 0` (correct), Redis Lua (correct, fastest). Uses its own schema `oversell_demo` (dropped afterwards) and Redis database 13. |

`flash/` is the generator (a separate crate, like `oversell_demo/`; `flash.sh` and `backlog.sh` build it). It signs
the users' JWTs itself with `JWT_SECRET`, so 100,000 sessions do not queue behind `POST /v1/session`'s per-IP limit
before the sale; `SESSION_SAMPLE` (default 50) users call `POST /v1/session` for real. `lib.sh` holds the shared shell helpers.

## How load reaches the system

- Port 8000 only (the replicas publish no port). The scripts start the gateway with `GATEWAY_TRUST_XFF=1` and the
  generator sends a distinct `X-Forwarded-For` (`10.x.y.z` from the user's index) and a browser-like `User-Agent`;
  otherwise every request would come from the bridge address and meet the per-IP limits by design.
- The sale is created from inside the gateway container (`docker compose exec -T gateway wget ...`), because
  `/v1/admin/` is refused from the host.
- The generator honours `429` + `Retry-After`, and retries `503`, other 5xx and network errors with jittered backoff
  and the same `Idempotency-Key`.
- Test settings are environment variables that `docker-compose.yml` passes on (`PAYMENT_WINDOW_SECS` default 300,
  `PROCESSOR_MAX_PER_SEC` default 0 = unlimited). The scripts set `PAYMENT_WINDOW_SECS=30` and restore the defaults
  (and the gateway without X-Forwarded-For trust) when they end.
- Do not run `cargo test` while the app containers are up. Stop only the app with
  `docker compose --profile app stop gateway order-service-1 order-service-2 order-service-3 order-processor expiration-worker`
  (`--profile app stop` alone also stops PostgreSQL, Redis and Kafka).

## Running at 100,000

`load/flash.sh 100000` (inserting the users takes a few seconds). Everything runs on one machine: the generator, NGINX,
three replicas, a processor, a worker, PostgreSQL, Redis and Kafka compete for the same cores, so this is a harsher test
of the machine than of the design. Knobs: `ARRIVAL_SECS`, `PAY_RATIO`, `LATE_RATIO`, `RACE_RATIO`, `POLL_MS`, `UNITS`,
`PAYMENT_WINDOW_SECS`, `SESSION_SAMPLE`.

## Known local limits

- **Ephemeral ports.** Clients on one host have about 28,000 local ports; connections of a finished run stay in
  `TIME_WAIT` for a minute. A 100,000-user run started right after another one saw ~3,300 connection failures (retried
  with the same key, so correctness held, but the check "zero network failures" failed). The scripts wait for the
  `TIME_WAIT` count to fall under 3,000 before starting the generator.
- **Docker's userland proxy** sits between the host and the gateway's published port, and the generator's own CPU use
  adds to the load on the machine; client-side latencies include both. Against a real deployment the generator would
  run on several machines.
- **One processor, one worker, one Redis, one broker**: no failover is exercised. The checkout-to-link time in the flash
  test (about 1-3 s) is the processor's single pass, not a capacity limit of the design.
- **Pay timing.** The classification "surely in time / boundary / after expiry" uses a margin (`MARGIN_MS`, 1.5 s)
  around the link's expiry; a payment inside the margin may end `CONFIRMED` or `REFUNDED` and is only required to end
  in one of them.
