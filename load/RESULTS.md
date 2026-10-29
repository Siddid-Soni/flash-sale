# Load test results

Appended by `flash.sh`, `backlog.sh` and `oversell_demo.sh`; see `README.md`. Only runs on the current design and the current load setup are kept: the generator on its own cores and the stack on the rest (`GEN_CPUS`/`STACK_CPUS`), and the generator sending to the gateway's container address rather than through Docker's port proxy (see `README.md`, Known local limits).

## 2026-10-29 01:25 UTC: flash sale, 100000 users, 500 units

Users arrive uniformly within 3 s of the sale opening; pay ratio 0.6, of which 0.05 pay only after the link expired and 0.05 pay 150 ms before it, racing the expiration worker; poll every 2000 ms; payment window 30 s; 50 users got their token from POST /v1/session, the rest were signed locally. Everything on one machine (20 cores): the generator, NGINX, three Order Service replicas, one order processor, one expiration worker, PostgreSQL, Redis and Kafka in Docker. Each user has its own X-Forwarded-For (gateway started with GATEWAY_TRUST_XFF=1). Real load for 100,000 users is the same script with N=100000 (see load/README.md).

Generator target: http://172.28.0.12 (provider http://172.28.0.50:8090); generator pinned to cores 0-9, the whole stack to cores 10-19.

Finished in 63.6s

Outcomes:
  confirmed: 312
  expired: 188
  sold_out: 99500

| endpoint | requests | peak req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| POST /v1/session | 50 | 20 | 2.9 | 7.1 | 7.5 | 7.5 | 201: 50 |
| GET /v1/sales/{id} | 100000 | 33795 | 3.1 | 43.2 | 59.7 | 104.0 | 200: 100000 |
| POST /v1/sales/{id}/checkout | 100000 | 34314 | 6.3 | 86.0 | 119.6 | 182.5 | 202: 500, 410: 99500 |
| GET /v1/orders/status/{ticket} | 5937 | 263 | 0.8 | 8.1 | 71.7 | 178.7 | 200: 5937 |
| POST /fake-pay/{session}/pay | 322 | 58 | 0.7 | 1.2 | 3.4 | 6.0 | 303: 312, 410: 10 |

Turned away by rate limits (429): 0; server errors (5xx): 0; network failures: 0
Retries: 0 after 5xx, 0 after network errors; 0 waits on 429
Checkout to payment link: 500 users, p50 7820 ms, p95 8975 ms, max 9497 ms; least time left to pay when seen: 27820 ms
Payments accepted by the provider: 312 (surely in time 293, at the boundary 19, after expiry 0); refused by the provider: 10
In-time payments not CONFIRMED: 0; late payments not REFUNDED: 0

Orders after settling: CONFIRMED 312, EXPIRED 188. Requests turned away by rate limits (429): 0 (the generator waited `Retry-After` and retried).

- [x] the system settled (no AWAITING_PAYMENT orders, inbox and outbox drained) (converged=1, orders: CONFIRMED 312, EXPIRED 188)
- [x] CONFIRMED orders equal the payments that arrived in time, and are at most 500 (CONFIRMED 312; payments surely in time 293, at the boundary 19)
- [x] every payment surely in time ended CONFIRMED (generator's own polling) (0)
- [x] every payment accepted after the link expired ended REFUNDED (generator's own polling; 0 such) (0)
- [x] every payment at the boundary of the expiry ended CONFIRMED or REFUNDED (never lost; 19 such) (0)
- [x] CONFIRMED orders equal users the generator saw CONFIRMED (312)
- [x] products.sold equals CONFIRMED orders (312)
- [x] users with two live orders (0)
- [x] orders still AWAITING_PAYMENT (0)
- [x] stock:{sale} equals 500 minus products.sold (every unit of an expired order returned once, none of a confirmed one) (188)
- [x] FAILED orders (no publish failed) (0)
- [x] server errors (5xx) in the generator's samples (0)
- [x] network failures in the generator's samples (0)


## 2026-10-29 01:49 UTC: flash sale, 10000 users, 500 units

Users arrive uniformly within 10 s of the sale opening; pay ratio 0.6, of which 0.05 pay only after the link expired and 0.05 pay 150 ms before it, racing the expiration worker; poll every 2000 ms; payment window 30 s; 50 users got their token from POST /v1/session, the rest were signed locally. Everything on one machine (20 cores): the generator, NGINX, three Order Service replicas, one order processor, one expiration worker, PostgreSQL, Redis and Kafka in Docker. Each user has its own X-Forwarded-For (gateway started with GATEWAY_TRUST_XFF=1). Real load for 100,000 users is the same script with N=100000 (see load/README.md).

Generator target: http://172.28.0.12 (provider http://172.28.0.50:8090); generator pinned to cores 0-9, the whole stack to cores 10-19.

Finished in 58.0s

Outcomes:
  confirmed: 273
  expired: 216
  refunded: 11
  sold_out: 9500

| endpoint | requests | peak req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| POST /v1/session | 50 | 21 | 2.8 | 7.8 | 7.8 | 7.8 | 201: 50 |
| GET /v1/sales/{id} | 10000 | 1049 | 0.1 | 0.3 | 0.3 | 3.5 | 200: 10000 |
| POST /v1/sales/{id}/checkout | 10000 | 1049 | 0.2 | 0.9 | 7.0 | 25.4 | 202: 500, 410: 9500 |
| GET /v1/orders/status/{ticket} | 4610 | 258 | 0.6 | 1.1 | 1.3 | 1.7 | 200: 4610 |
| POST /fake-pay/{session}/pay | 293 | 60 | 0.5 | 0.8 | 1.0 | 2.2 | 303: 284, 410: 9 |

Turned away by rate limits (429): 0; server errors (5xx): 0; network failures: 0
Retries: 0 after 5xx, 0 after network errors; 0 waits on 429
Checkout to payment link: 500 users, p50 1003 ms, p95 1916 ms, max 2221 ms; least time left to pay when seen: 27792 ms
Payments accepted by the provider: 284 (surely in time 267, at the boundary 17, after expiry 0); refused by the provider: 9
In-time payments not CONFIRMED: 0; late payments not REFUNDED: 0

Orders after settling: CONFIRMED 273, EXPIRED 216, REFUNDED 11. Requests turned away by rate limits (429): 0 (the generator waited `Retry-After` and retried).

- [x] the system settled (no AWAITING_PAYMENT orders, inbox and outbox drained) (converged=1, orders: CONFIRMED 273, EXPIRED 216, REFUNDED 11)
- [x] CONFIRMED orders equal the payments that arrived in time, and are at most 500 (CONFIRMED 273; payments surely in time 267, at the boundary 17)
- [x] every payment surely in time ended CONFIRMED (generator's own polling) (0)
- [x] every payment accepted after the link expired ended REFUNDED (generator's own polling; 0 such) (0)
- [x] every payment at the boundary of the expiry ended CONFIRMED or REFUNDED (never lost; 17 such) (0)
- [x] CONFIRMED orders equal users the generator saw CONFIRMED (273)
- [x] products.sold equals CONFIRMED orders (273)
- [x] users with two live orders (0)
- [x] orders still AWAITING_PAYMENT (0)
- [x] stock:{sale} equals 500 minus products.sold (every unit of an expired order returned once, none of a confirmed one) (227)
- [x] FAILED orders (no publish failed) (0)
- [x] server errors (5xx) in the generator's samples (0)
- [x] network failures in the generator's samples (0)


## 2026-10-29 01:52 UTC: backlog, 10000 users, 10000 units, processor throttled to 200/s

All users check out within 10 s of the sale opening; the processor starts 200 messages per second, so the queue needs about 50 s to drain. Payment window 30 s; 0.5 of the users pay, 55-70 % of the window after their link appeared; poll every 2000 ms. One machine, one order processor, three replicas behind NGINX (gateway started with GATEWAY_TRUST_XFF=1). The design point is N=UNITS=100000 at 1,000/s (same script).

Generator target: http://172.28.0.12 (provider http://172.28.0.50:8090); generator pinned to cores 0-9, the whole stack to cores 10-19.

Finished in 114.3s

Outcomes:
  confirmed: 5025
  expired: 4975

| endpoint | requests | peak req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| POST /v1/sales/{id}/checkout | 10000 | 1075 | 3.1 | 3.8 | 4.3 | 21.7 | 202: 10000 |
| GET /v1/orders/status/{ticket} | 275901 | 5096 | 0.3 | 0.5 | 0.7 | 4.2 | 200: 275901 |
| POST /fake-pay/{session}/pay | 5025 | 122 | 0.4 | 0.6 | 0.8 | 10.6 | 303: 5025 |

Turned away by rate limits (429): 0; server errors (5xx): 0; network failures: 0
Retries: 0 after 5xx, 0 after network errors; 0 waits on 429
Checkout to payment link: 10000 users, p50 29171 ms, p95 47430 ms, max 50429 ms; least time left to pay when seen: 27804 ms
Payments accepted by the provider: 5025 (surely in time 5025, at the boundary 0, after expiry 0); refused by the provider: 0
In-time payments not CONFIRMED: 0; late payments not REFUNDED: 0

Orders after settling: CONFIRMED 5025, EXPIRED 4975.

- [x] the system settled (converged=1, orders: CONFIRMED 5025, EXPIRED 4975)
- [x] every user got an order (none dropped, none left in the queue) (10000)
- [x] FAILED orders (none: no publish failed, nothing queued was given up) (0)
- [x] EXPIRED or FAILED orders that never had a payment link (0)
- [x] generator outcomes failed, timeout or error (0)
- [x] the backlog was real: the last link came after more than the payment window (slowest checkout to link 50429 ms, window 30000 ms)
- [x] every user got the full payment window (time left to pay when the link was first seen at least window - 3400 ms) (least time left 27804 ms of 30000 ms)
- [x] payers who paid in the last half of the window all ended CONFIRMED (in time but not CONFIRMED) (0)
- [x] CONFIRMED orders equal payments accepted in time (the generator pays only in time here) (5025)
- [x] products.sold equals CONFIRMED orders (5025)
- [x] users with two live orders (0)
- [x] orders still AWAITING_PAYMENT (0)
- [x] stock:{sale} equals 10000 minus products.sold (every unit of an expired order returned once) (4975)
- [x] server errors (5xx) in the generator's samples (0)
- [x] network failures in the generator's samples (0)


## 2026-10-29 01:53 UTC: oversell demo, 5000 buyers, 500 units

Every buyer is a task that starts at the same moment; PostgreSQL strategies share a pool of 50 connections (latency includes waiting for one). One machine, PostgreSQL 16 and Redis 7 in Docker, default isolation (READ COMMITTED). Own schema `oversell_demo` and Redis database 13.

5000 buyers race for 500 units (PostgreSQL pool of 50 connections; think time 0 us)

| strategy | told yes | sold out | gave up | units taken in the store | oversold | seconds | buyers/s | p50 ms | p95 ms | p99 ms | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| naive SELECT then UPDATE | 5000 | 0 | 0 | 87 | **4500 OVERSOLD** | 3.87 | 1293 | 1920.6 | 3681.1 | 3828.3 |  |
| SELECT ... FOR UPDATE | 500 | 4500 | 0 | 500 | no | 4.25 | 1176 | 2145.8 | 4019.6 | 4210.0 | no lock_timeout; row lock wait mean 41.4 ms, max 382 ms |
| optimistic version column | 500 | 4454 | 46 | 500 | no | 10.64 | 470 | 10580.0 | 10630.6 | 10634.5 | 12894 retries (2.6 per buyer), up to 200 per buyer |
| UPDATE ... WHERE stock > 0 | 500 | 4500 | 0 | 500 | no | 0.55 | 9045 | 490.0 | 544.5 | 549.2 |  |
| Redis Lua script | 500 | 4500 | 0 | 500 | no | 0.02 | 243034 | 11.4 | 14.8 | 15.4 | no PostgreSQL |

naive SELECT then UPDATE: 5000 buyers were told yes but the store lost 4913 of their decrements

With a short lock timeout:

| strategy | told yes | sold out | gave up | units taken in the store | oversold | seconds | buyers/s | p50 ms | p95 ms | p99 ms | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| SELECT ... FOR UPDATE | 500 | 2207 | 2293 | 500 | no | 2.47 | 2025 | 1422.4 | 2356.8 | 2442.5 | lock_timeout 20 ms: 2293 lock timeouts; row lock wait mean 23.8 ms, max 458 ms |


## 2026-10-29 01:55 UTC: flash sale, 100000 users, 500 units

Users arrive uniformly within 10 s of the sale opening; pay ratio 0.6, of which 0.05 pay only after the link expired and 0.05 pay 150 ms before it, racing the expiration worker; poll every 2000 ms; payment window 30 s; 50 users got their token from POST /v1/session, the rest were signed locally. Everything on one machine (20 cores): the generator, NGINX, three Order Service replicas, one order processor, one expiration worker, PostgreSQL, Redis and Kafka in Docker. Each user has its own X-Forwarded-For (gateway started with GATEWAY_TRUST_XFF=1). Real load for 100,000 users is the same script with N=100000 (see load/README.md).

Generator target: http://172.28.0.12 (provider http://172.28.0.50:8090); generator pinned to cores 0-9, the whole stack to cores 10-19.

Finished in 65.4s

Outcomes:
  confirmed: 300
  expired: 200
  sold_out: 99500

| endpoint | requests | peak req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| POST /v1/session | 50 | 18 | 2.8 | 7.8 | 8.1 | 8.1 | 201: 50 |
| GET /v1/sales/{id} | 100000 | 10101 | 0.2 | 0.4 | 0.5 | 10.8 | 200: 100000 |
| POST /v1/sales/{id}/checkout | 100000 | 10105 | 0.5 | 0.9 | 1.4 | 42.4 | 202: 500, 410: 99500 |
| GET /v1/orders/status/{ticket} | 6468 | 261 | 0.7 | 1.1 | 1.2 | 3.1 | 200: 6468 |
| POST /fake-pay/{session}/pay | 314 | 58 | 0.7 | 1.1 | 1.3 | 1.6 | 303: 300, 410: 14 |

Turned away by rate limits (429): 0; server errors (5xx): 0; network failures: 0
Retries: 0 after 5xx, 0 after network errors; 0 waits on 429
Checkout to payment link: 500 users, p50 9675 ms, p95 10788 ms, max 11738 ms; least time left to pay when seen: 27782 ms
Payments accepted by the provider: 300 (surely in time 287, at the boundary 13, after expiry 0); refused by the provider: 14
In-time payments not CONFIRMED: 0; late payments not REFUNDED: 0

Orders after settling: CONFIRMED 300, EXPIRED 200. Requests turned away by rate limits (429): 0 (the generator waited `Retry-After` and retried).

- [x] the system settled (no AWAITING_PAYMENT orders, inbox and outbox drained) (converged=1, orders: CONFIRMED 300, EXPIRED 200)
- [x] CONFIRMED orders equal the payments that arrived in time, and are at most 500 (CONFIRMED 300; payments surely in time 287, at the boundary 13)
- [x] every payment surely in time ended CONFIRMED (generator's own polling) (0)
- [x] every payment accepted after the link expired ended REFUNDED (generator's own polling; 0 such) (0)
- [x] every payment at the boundary of the expiry ended CONFIRMED or REFUNDED (never lost; 13 such) (0)
- [x] CONFIRMED orders equal users the generator saw CONFIRMED (300)
- [x] products.sold equals CONFIRMED orders (300)
- [x] users with two live orders (0)
- [x] orders still AWAITING_PAYMENT (0)
- [x] stock:{sale} equals 500 minus products.sold (every unit of an expired order returned once, none of a confirmed one) (200)
- [x] FAILED orders (no publish failed) (0)
- [x] server errors (5xx) in the generator's samples (0)
- [x] network failures in the generator's samples (0)

