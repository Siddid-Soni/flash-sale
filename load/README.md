# Load tests

Scripts run from anywhere (`cd` to the repository root themselves), need Docker compose, `jq`,
`psql` access through the postgres container, and a Rust toolchain. Each prints PASS or FAIL per check,
exits non-zero on a FAIL, and appends a dated report (settings, latency table, checks) to `RESULTS.md`.

| Script | What it proves |
|---|---|
| `flash.sh [N]` | N users (default 10,000; the design target is 100,000) arrive within 10 s of the sale opening and try to buy one of 500 units through NGINX and three Order Service replicas. Winners poll, 60 % pay (5 % of the payers too late, 5 % racing the expiry), the rest abandon. Asserts: `CONFIRMED` orders equal the payments that arrived in time and are at most 500; a payment after the link expired ends `REFUNDED`; `products.sold` = `CONFIRMED` orders (sales are counted only in PostgreSQL); the run waits until PostgreSQL has no `AWAITING_PAYMENT` order for the sale and its inbox is drained, then `stock` = 500 - `products.sold` (every unit of an expired order returned once, none of a confirmed one); no `FAILED` orders; no user with two live orders; zero 5xx and zero network failures. Requests turned away by the rate limits (`429`) are reported separately. |
| `backlog.sh [N]` | The order processor is throttled (`RATE`, default 200 messages per second) so a real queue builds: N users and N units (default 10,000), so the last link comes ~40 s after the user's checkout, longer than the 30 s payment window. Asserts that nothing waiting in Kafka was given up (no `FAILED` order, no `EXPIRED` order without a link, every user got an order), that every user got the full payment window (time left when the link was first seen is the window minus at most one polling interval), that users paying late in that window are all `CONFIRMED`, plus the consistency checks of `flash.sh`. The design point is `N=UNITS=100000 RATE=1000 load/backlog.sh 100000`. |
| `oversell_demo.sh` | 5,000 concurrent buyers (`BUYERS`) for 500 units (`UNITS`) against five strategies, each on its own table or key: naive `SELECT` then `UPDATE` (oversells), `SELECT ... FOR UPDATE` (correct, queues on the row lock; run again with a short `lock_timeout`), optimistic `version` (correct, retry storm), `UPDATE ... WHERE stock > 0` (correct), Redis Lua (correct, fastest). Uses its own schema `oversell_demo` (dropped afterwards) and Redis database 13. |

`flash/` is the generator (a separate crate, like `oversell_demo/`; `flash.sh` and `backlog.sh` build it). It signs
the users' JWTs itself with `JWT_SECRET`, so 100,000 sessions do not queue behind `POST /v1/session`'s per-IP limit
before the sale; `SESSION_SAMPLE` (default 50) users call `POST /v1/session` for real. `lib.sh` holds the shared shell helpers.

## How load reaches the system

- The generator sends to the gateway container's own address on the compose network (the host routes there
  directly), not to `localhost:8000`: through the published port every connection also passes Docker's userland
  proxy, which used ~9 cores at 30k req/s and capped earlier runs. Payments go to the provider's container address
  (`172.28.0.50:8090`) the same way. `BASE_URL` (and `PROVIDER_URL`) override both.
- The generator runs on its own cores (`taskset`) and the whole stack on the rest (`load/compose.pin.yml`, a cpuset
  per service), so the generator cannot take CPU from what it measures: by default the first ~30% of the cores
  (`GEN_CPUS`, `0-5` on 20 cores) and the rest (`STACK_CPUS`). `PIN_CPUS=0` turns it off. The report says which.
- The replicas publish no port. The scripts start the gateway with `GATEWAY_TRUST_XFF=1` and the
  generator sends a distinct `X-Forwarded-For` (`10.x.y.z` from the user's index) and a browser-like `User-Agent`;
  otherwise every request would come from the bridge address and meet the per-IP limits by design.
- The sale is created from inside the gateway container (`docker compose exec -T gateway wget ...`), because
  `/v1/admin/` is refused from the host; the gateway routes it to the `site` service, as it does sessions and sale details.
- Payers follow the `payment_url` they were given: the fake provider's own address, `http://localhost:8090`, outside the
  gateway (`PROVIDER_URL` overrides the host the generator posts to).
- The generator honours `429` + `Retry-After`, and retries `503`, other 5xx and network errors with jittered backoff
  and the same `Idempotency-Key`.
- Test settings are environment variables that `docker-compose.yml` passes on (`PAYMENT_WINDOW_SECS` default 300,
  `PROCESSOR_MAX_PER_SEC` default 0 = unlimited). The scripts set `PAYMENT_WINDOW_SECS=30` and restore the defaults
  (and the gateway without X-Forwarded-For trust) when they end.
- Do not run `cargo test` while the app containers are up. Stop only the app with
  `docker compose --profile app stop gateway site order-service-1 order-service-2 order-service-3 order-processor expiration-worker fake-payment-provider`
  (`--profile app stop` alone also stops PostgreSQL, Redis and Kafka).

## Running at 100,000

`load/flash.sh 100000` (inserting the users takes a few seconds). Everything runs on one machine: the generator, NGINX,
three replicas, a processor, a worker, PostgreSQL, Redis and Kafka compete for the same cores, so this is a harsher test
of the machine than of the design. Knobs: `ARRIVAL_SECS`, `PAY_RATIO`, `LATE_RATIO`, `RACE_RATIO`, `POLL_MS`, `UNITS`,
`PAYMENT_WINDOW_SECS`, `SESSION_SAMPLE`.

## Known local limits

- **Ephemeral ports.** Clients on one host have about 28,000 local ports by default (`32768-60999`), and closed
  connections stay in `TIME_WAIT` for a minute. A burst of 100,000 users in 3 s needs more connections than that:
  without the settings below the generator fails with `Cannot assign requested address (os error 99)`. The default
  `tcp_tw_reuse = 2` reuses `TIME_WAIT` ports for loopback only, and the generator's connections cross the Docker
  bridge. Before a large run (they reset on reboot):

  ```bash
  sudo sysctl -w net.ipv4.ip_local_port_range="10000 65535" net.ipv4.tcp_tw_reuse=1
  ```

  The range starts at 10000 so no outgoing connection takes a port a local service listens on (5432, 6379, 8000...).
  The scripts also wait for the `TIME_WAIT` count to fall under 3,000 before starting the generator.
- **Anything polling Docker.** Tools that stream container stats (`lazydocker`, `docker stats`, a dashboard) kept
  `dockerd` and `containerd` at about 2 cores even with the stack idle. Those daemons are not pinned, so they ran on
  the generator's cores and starved it. Close them during a run.
- **Generator cores.** With 6 cores (the default split on 20) and a 3 s burst of 100,000 users the generator could
  not keep up; with 10 (`GEN_CPUS=0-9 STACK_CPUS=10-19`) the run was clean: 34k checkouts/s, 0 network failures. The
  stack used about 8 of its 10 cores.
- **One machine.** Even pinned, the generator shares the host's memory bandwidth, kernel and network stack with the
  stack. Against a real deployment it would run on several machines.
- **One processor, one worker, one Redis, one broker**: no failover is exercised. The checkout-to-link time in the flash
  test (about 1-3 s) is the processor's single pass, not a capacity limit of the design.
- **Pay timing.** The classification "surely in time / boundary / after expiry" uses a margin (`MARGIN_MS`, 1.5 s)
  around the link's expiry; a payment inside the margin may end `CONFIRMED` or `REFUNDED` and is only required to end
  in one of them.
