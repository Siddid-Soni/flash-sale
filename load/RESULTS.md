# Load test results

Appended by `flash.sh`, `backlog.sh` and `oversell_demo.sh`; see `README.md`.

## 2026-10-27 20:52 UTC: flash sale, 100000 users, 500 units

Users arrive uniformly within 10 s of the sale opening; pay ratio 0.6, of which 0.05 pay only after the link expired and 0.05 pay 150 ms before it, racing the expiration worker; poll every 2000 ms; payment window 30 s; 50 users got their token from POST /v1/session, the rest were signed locally. Everything on one machine (20 cores): the generator, NGINX, three Order Service replicas, one order processor, one expiration worker, PostgreSQL, Redis and Kafka in Docker. Each user has its own X-Forwarded-For (gateway started with GATEWAY_TRUST_XFF=1). Real load for 100,000 users is the same script with N=100000 (see load/README.md).

Finished in 57.8s

Outcomes:
  confirmed: 265
  expired: 224
  refunded: 11
  sold_out: 99500

| endpoint | requests | peak req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| POST /v1/session | 50 | 21 | 3.7 | 5.9 | 6.6 | 6.6 | 201: 50 |
| GET /v1/sales/{id} | 100000 | 10147 | 0.8 | 9.6 | 58.7 | 120.2 | 200: 100000 |
| POST /v1/sales/{id}/checkout | 100000 | 10153 | 1.1 | 13.4 | 78.6 | 140.8 | 202: 500, 410: 99500 |
| GET /v1/orders/status/{ticket} | 4776 | 266 | 0.9 | 6.4 | 24.2 | 109.2 | 200: 4776 |
| POST /fake-pay/{session}/pay | 280 | 47 | 1.3 | 5.4 | 12.2 | 17.5 | 303: 276, 410: 4 |

Turned away by rate limits (429): 0; server errors (5xx): 0; network failures: 0
Retries: 0 after 5xx, 0 after network errors; 0 waits on 429
Checkout to payment link: 500 users, p50 1256 ms, p95 2398 ms, max 2820 ms; least time left to pay when seen: 27721 ms
Payments accepted by the provider: 276 (surely in time 261, at the boundary 15, after expiry 0); refused by the provider: 4
In-time payments not CONFIRMED: 0; late payments not REFUNDED: 0

Orders after settling: CONFIRMED 265, EXPIRED 224, REFUNDED 11. Requests turned away by rate limits (429): 0 (the generator waited `Retry-After` and retried).

- [x] the system settled (no PENDING/AWAITING_PAYMENT orders, no queued holds, inbox and outbox drained) (converged=1, orders: CONFIRMED 265, EXPIRED 224, REFUNDED 11)
- [x] CONFIRMED orders equal the payments that arrived in time, and are at most 500 (CONFIRMED 265; payments surely in time 261, at the boundary 15)
- [x] every payment surely in time ended CONFIRMED (generator's own polling) (0)
- [x] every payment accepted after the link expired ended REFUNDED (generator's own polling; 0 such) (0)
- [x] every payment at the boundary of the expiry ended CONFIRMED or REFUNDED (never lost; 15 such) (0)
- [x] CONFIRMED orders equal users the generator saw CONFIRMED (265)
- [x] products.sold equals sold:{sale} (265)
- [x] products.sold equals CONFIRMED orders (265)
- [x] users with two live orders (0)
- [x] orders still PENDING or AWAITING_PAYMENT (0)
- [x] stock:{sale} equals 500 minus sold (every abandoned hold released) (235)
- [x] holds left in holds:pending and holds:expiry (0 + 0)
- [x] server errors (5xx) in the generator's samples (0)
- [x] network failures in the generator's samples (0)


## 2026-10-27 20:54 UTC: flash sale, 10000 users, 500 units

Users arrive uniformly within 10 s of the sale opening; pay ratio 0.6, of which 0.05 pay only after the link expired and 0.05 pay 150 ms before it, racing the expiration worker; poll every 2000 ms; payment window 30 s; 50 users got their token from POST /v1/session, the rest were signed locally. Everything on one machine (20 cores): the generator, NGINX, three Order Service replicas, one order processor, one expiration worker, PostgreSQL, Redis and Kafka in Docker. Each user has its own X-Forwarded-For (gateway started with GATEWAY_TRUST_XFF=1). Real load for 100,000 users is the same script with N=100000 (see load/README.md).

Finished in 50.8s

Outcomes:
  confirmed: 283
  expired: 210
  refunded: 7
  sold_out: 9500

| endpoint | requests | peak req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| POST /v1/session | 50 | 24 | 2.7 | 7.6 | 8.0 | 8.0 | 201: 50 |
| GET /v1/sales/{id} | 10000 | 1041 | 0.2 | 0.5 | 0.8 | 7.2 | 200: 10000 |
| POST /v1/sales/{id}/checkout | 10000 | 1041 | 0.3 | 2.3 | 4.3 | 9.3 | 202: 500, 410: 9500 |
| GET /v1/orders/status/{ticket} | 4522 | 255 | 0.7 | 1.3 | 1.5 | 4.1 | 200: 4522 |
| POST /fake-pay/{session}/pay | 299 | 58 | 0.6 | 1.2 | 1.8 | 6.1 | 303: 290, 410: 9 |

Turned away by rate limits (429): 0; server errors (5xx): 0; network failures: 0
Retries: 0 after 5xx, 0 after network errors; 0 waits on 429
Checkout to payment link: 500 users, p50 1000 ms, p95 1866 ms, max 2034 ms; least time left to pay when seen: 27974 ms
Payments accepted by the provider: 290 (surely in time 277, at the boundary 13, after expiry 0); refused by the provider: 9
In-time payments not CONFIRMED: 0; late payments not REFUNDED: 0

Orders after settling: CONFIRMED 283, EXPIRED 210, REFUNDED 7. Requests turned away by rate limits (429): 0 (the generator waited `Retry-After` and retried).

- [x] the system settled (no PENDING/AWAITING_PAYMENT orders, no queued holds, inbox and outbox drained) (converged=1, orders: CONFIRMED 283, EXPIRED 210, REFUNDED 7)
- [x] CONFIRMED orders equal the payments that arrived in time, and are at most 500 (CONFIRMED 283; payments surely in time 277, at the boundary 13)
- [x] every payment surely in time ended CONFIRMED (generator's own polling) (0)
- [x] every payment accepted after the link expired ended REFUNDED (generator's own polling; 0 such) (0)
- [x] every payment at the boundary of the expiry ended CONFIRMED or REFUNDED (never lost; 13 such) (0)
- [x] CONFIRMED orders equal users the generator saw CONFIRMED (283)
- [x] products.sold equals sold:{sale} (283)
- [x] products.sold equals CONFIRMED orders (283)
- [x] users with two live orders (0)
- [x] orders still PENDING or AWAITING_PAYMENT (0)
- [x] stock:{sale} equals 500 minus sold (every abandoned hold released) (217)
- [x] holds left in holds:pending and holds:expiry (0 + 0)
- [x] server errors (5xx) in the generator's samples (0)
- [x] network failures in the generator's samples (0)


## 2026-10-27 20:56 UTC: backlog, 10000 users, 10000 units, processor throttled to 200/s

All users check out within 10 s of the sale opening; the processor starts 200 messages per second, so the queue needs about 50 s to drain. Payment window 30 s; 0.5 of the users pay, 55-70 % of the window after their link appeared; poll every 2000 ms. One machine, one order processor, three replicas behind NGINX (gateway started with GATEWAY_TRUST_XFF=1). The design point is N=UNITS=100000 at 1,000/s (same script).

Finished in 107.2s

Outcomes:
  confirmed: 4942
  expired: 5058

| endpoint | requests | peak req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| POST /v1/sales/{id}/checkout | 10000 | 1060 | 3.3 | 5.4 | 8.5 | 26.9 | 202: 10000 |
| GET /v1/orders/status/{ticket} | 237456 | 5072 | 0.6 | 1.5 | 2.7 | 25.9 | 200: 237456 |
| POST /fake-pay/{session}/pay | 4942 | 118 | 0.7 | 1.6 | 2.6 | 7.7 | 303: 4942 |

Turned away by rate limits (429): 0; server errors (5xx): 0; network failures: 0
Retries: 0 after 5xx, 0 after network errors; 0 waits on 429
Checkout to payment link: 10000 users, p50 21197 ms, p95 39418 ms, max 43149 ms; least time left to pay when seen: 27805 ms
Payments accepted by the provider: 4942 (surely in time 4942, at the boundary 0, after expiry 0); refused by the provider: 0
In-time payments not CONFIRMED: 0; late payments not REFUNDED: 0

Orders after settling: CONFIRMED 4942, EXPIRED 5058.

- [x] the system settled (converged=1, orders: CONFIRMED 4942, EXPIRED 5058)
- [x] every user got an order (none dropped, none left in the queue) (10000)
- [x] FAILED orders (a PENDING hold released would end FAILED) (0)
- [x] EXPIRED or FAILED orders that never had a payment link (0)
- [x] leaked-hold releases logged by the expiration worker (0)
- [x] generator outcomes failed, timeout or error (0)
- [x] the backlog was real: the last link came after more than the payment window (slowest checkout to link 43149 ms, window 30000 ms)
- [x] every user got the full payment window (time left to pay when the link was first seen at least window - 3400 ms) (least time left 27805 ms of 30000 ms)
- [x] payers who paid in the last half of the window all ended CONFIRMED (in time but not CONFIRMED) (0)
- [x] CONFIRMED orders equal payments accepted in time (the generator pays only in time here) (4942)
- [x] products.sold equals sold:{sale} (4942)
- [x] products.sold equals CONFIRMED orders (4942)
- [x] users with two live orders (0)
- [x] orders still PENDING or AWAITING_PAYMENT (0)
- [x] stock:{sale} equals 10000 minus sold (every abandoned hold released) (5058)
- [x] holds left in holds:pending and holds:expiry (0 + 0)
- [x] server errors (5xx) in the generator's samples (0)
- [x] network failures in the generator's samples (0)


## 2026-10-27 20:57 UTC: oversell demo, 5000 buyers, 500 units

Every buyer is a task that starts at the same moment; PostgreSQL strategies share a pool of 50 connections (latency includes waiting for one). One machine, PostgreSQL 16 and Redis 7 in Docker, default isolation (READ COMMITTED). Own schema `oversell_demo` and Redis database 13.

5000 buyers race for 500 units (PostgreSQL pool of 50 connections; think time 0 us)

| strategy | told yes | sold out | gave up | units taken in the store | oversold | seconds | buyers/s | p50 ms | p95 ms | p99 ms | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| naive SELECT then UPDATE | 5000 | 0 | 0 | 116 | **4500 OVERSOLD** | 4.24 | 1179 | 2115.3 | 4051.0 | 4201.2 |  |
| SELECT ... FOR UPDATE | 500 | 4500 | 0 | 500 | no | 4.61 | 1086 | 2376.4 | 4411.2 | 4566.1 | no lock_timeout; row lock wait mean 44.9 ms, max 528 ms |
| optimistic version column | 500 | 4489 | 11 | 500 | no | 10.26 | 487 | 10201.9 | 10253.0 | 10257.4 | 12800 retries (2.6 per buyer), up to 200 per buyer |
| UPDATE ... WHERE stock > 0 | 500 | 4500 | 0 | 500 | no | 0.57 | 8787 | 501.8 | 559.0 | 564.8 |  |
| Redis Lua script | 500 | 4500 | 0 | 500 | no | 0.02 | 208225 | 16.0 | 19.5 | 19.8 | no PostgreSQL |

naive SELECT then UPDATE: 5000 buyers were told yes but the store lost 4884 of their decrements

With a short lock timeout:

| strategy | told yes | sold out | gave up | units taken in the store | oversold | seconds | buyers/s | p50 ms | p95 ms | p99 ms | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| SELECT ... FOR UPDATE | 500 | 2187 | 2313 | 500 | no | 2.53 | 1976 | 1470.3 | 2420.3 | 2504.5 | lock_timeout 20 ms: 2313 lock timeouts; row lock wait mean 24.2 ms, max 411 ms |


## 2026-10-28 02:42 UTC: flash sale, 100000 users, 500 units

Users arrive uniformly within 10 s of the sale opening; pay ratio 0.6, of which 0.05 pay only after the link expired and 0.05 pay 150 ms before it, racing the expiration worker; poll every 2000 ms; payment window 30 s; 50 users got their token from POST /v1/session, the rest were signed locally. Everything on one machine (20 cores): the generator, NGINX, three Order Service replicas, one order processor, one expiration worker, PostgreSQL, Redis and Kafka in Docker. Each user has its own X-Forwarded-For (gateway started with GATEWAY_TRUST_XFF=1). Real load for 100,000 users is the same script with N=100000 (see load/README.md).

Finished in 57.0s

Outcomes:
  confirmed: 272
  expired: 228
  sold_out: 99500

| endpoint | requests | peak req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| POST /v1/session | 50 | 21 | 3.3 | 8.2 | 8.3 | 8.3 | 201: 50 |
| GET /v1/sales/{id} | 100000 | 10083 | 0.2 | 0.5 | 10.3 | 47.8 | 200: 100000 |
| POST /v1/sales/{id}/checkout | 100000 | 10066 | 0.4 | 0.8 | 18.4 | 76.5 | 202: 500, 410: 99500 |
| GET /v1/orders/status/{ticket} | 4751 | 259 | 0.6 | 1.2 | 1.6 | 44.1 | 200: 4751 |
| POST /fake-pay/{session}/pay | 285 | 49 | 0.7 | 1.0 | 1.5 | 5.9 | 303: 272, 410: 13 |

Turned away by rate limits (429): 0; server errors (5xx): 0; network failures: 0
Retries: 0 after 5xx, 0 after network errors; 0 waits on 429
Checkout to payment link: 500 users, p50 1145 ms, p95 2020 ms, max 2695 ms; least time left to pay when seen: 27763 ms
Payments accepted by the provider: 272 (surely in time 257, at the boundary 15, after expiry 0); refused by the provider: 13
In-time payments not CONFIRMED: 0; late payments not REFUNDED: 0

Orders after settling: CONFIRMED 272, EXPIRED 228. Requests turned away by rate limits (429): 0 (the generator waited `Retry-After` and retried).

- [x] the system settled (no AWAITING_PAYMENT orders, no queued holds, inbox and outbox drained) (converged=1, orders: CONFIRMED 272, EXPIRED 228)
- [x] CONFIRMED orders equal the payments that arrived in time, and are at most 500 (CONFIRMED 272; payments surely in time 257, at the boundary 15)
- [x] every payment surely in time ended CONFIRMED (generator's own polling) (0)
- [x] every payment accepted after the link expired ended REFUNDED (generator's own polling; 0 such) (0)
- [x] every payment at the boundary of the expiry ended CONFIRMED or REFUNDED (never lost; 15 such) (0)
- [x] CONFIRMED orders equal users the generator saw CONFIRMED (272)
- [x] products.sold equals CONFIRMED orders (272)
- [x] users with two live orders (0)
- [x] orders still AWAITING_PAYMENT (0)
- [x] stock:{sale} equals 500 minus sold (every abandoned hold released) (228)
- [x] holds left in holds:pending and holds:expiry (0 + 0)
- [x] server errors (5xx) in the generator's samples (0)
- [x] network failures in the generator's samples (0)


## 2026-10-28 02:44 UTC: flash sale, 10000 users, 500 units

Users arrive uniformly within 10 s of the sale opening; pay ratio 0.6, of which 0.05 pay only after the link expired and 0.05 pay 150 ms before it, racing the expiration worker; poll every 2000 ms; payment window 30 s; 50 users got their token from POST /v1/session, the rest were signed locally. Everything on one machine (20 cores): the generator, NGINX, three Order Service replicas, one order processor, one expiration worker, PostgreSQL, Redis and Kafka in Docker. Each user has its own X-Forwarded-For (gateway started with GATEWAY_TRUST_XFF=1). Real load for 100,000 users is the same script with N=100000 (see load/README.md).

Finished in 56.7s

Outcomes:
  confirmed: 261
  expired: 226
  refunded: 13
  sold_out: 9500

| endpoint | requests | peak req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| POST /v1/session | 50 | 20 | 2.6 | 6.9 | 7.3 | 7.3 | 201: 50 |
| GET /v1/sales/{id} | 10000 | 1040 | 0.2 | 0.5 | 0.7 | 3.3 | 200: 10000 |
| POST /v1/sales/{id}/checkout | 10000 | 1040 | 0.3 | 2.2 | 4.4 | 7.0 | 202: 500, 410: 9500 |
| GET /v1/orders/status/{ticket} | 4760 | 267 | 0.6 | 1.1 | 1.3 | 7.3 | 200: 4760 |
| POST /fake-pay/{session}/pay | 284 | 51 | 0.6 | 1.1 | 1.7 | 6.0 | 303: 274, 410: 10 |

Turned away by rate limits (429): 0; server errors (5xx): 0; network failures: 0
Retries: 0 after 5xx, 0 after network errors; 0 waits on 429
Checkout to payment link: 500 users, p50 1015 ms, p95 1929 ms, max 2020 ms; least time left to pay when seen: 27983 ms
Payments accepted by the provider: 274 (surely in time 254, at the boundary 20, after expiry 0); refused by the provider: 10
In-time payments not CONFIRMED: 0; late payments not REFUNDED: 0

Orders after settling: CONFIRMED 261, EXPIRED 226, REFUNDED 13. Requests turned away by rate limits (429): 0 (the generator waited `Retry-After` and retried).

- [x] the system settled (no AWAITING_PAYMENT orders, no queued holds, inbox and outbox drained) (converged=1, orders: CONFIRMED 261, EXPIRED 226, REFUNDED 13)
- [x] CONFIRMED orders equal the payments that arrived in time, and are at most 500 (CONFIRMED 261; payments surely in time 254, at the boundary 20)
- [x] every payment surely in time ended CONFIRMED (generator's own polling) (0)
- [x] every payment accepted after the link expired ended REFUNDED (generator's own polling; 0 such) (0)
- [x] every payment at the boundary of the expiry ended CONFIRMED or REFUNDED (never lost; 20 such) (0)
- [x] CONFIRMED orders equal users the generator saw CONFIRMED (261)
- [x] products.sold equals CONFIRMED orders (261)
- [x] users with two live orders (0)
- [x] orders still AWAITING_PAYMENT (0)
- [x] stock:{sale} equals 500 minus sold (every abandoned hold released) (239)
- [x] holds left in holds:pending and holds:expiry (0 + 0)
- [x] server errors (5xx) in the generator's samples (0)
- [x] network failures in the generator's samples (0)


## 2026-10-28 02:46 UTC: backlog, 10000 users, 10000 units, processor throttled to 200/s

All users check out within 10 s of the sale opening; the processor starts 200 messages per second, so the queue needs about 50 s to drain. Payment window 30 s; 0.5 of the users pay, 55-70 % of the window after their link appeared; poll every 2000 ms. One machine, one order processor, three replicas behind NGINX (gateway started with GATEWAY_TRUST_XFF=1). The design point is N=UNITS=100000 at 1,000/s (same script).

Finished in 106.9s

Outcomes:
  confirmed: 5003
  expired: 4997

| endpoint | requests | peak req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| POST /v1/sales/{id}/checkout | 10000 | 1035 | 3.4 | 4.9 | 7.0 | 16.4 | 202: 10000 |
| GET /v1/orders/status/{ticket} | 236420 | 5035 | 0.6 | 1.2 | 2.5 | 32.3 | 200: 236420 |
| POST /fake-pay/{session}/pay | 5003 | 129 | 0.7 | 1.5 | 2.9 | 23.3 | 303: 5003 |

Turned away by rate limits (429): 0; server errors (5xx): 0; network failures: 0
Retries: 0 after 5xx, 0 after network errors; 0 waits on 429
Checkout to payment link: 10000 users, p50 21177 ms, p95 39414 ms, max 42842 ms; least time left to pay when seen: 27804 ms
Payments accepted by the provider: 5003 (surely in time 5003, at the boundary 0, after expiry 0); refused by the provider: 0
In-time payments not CONFIRMED: 0; late payments not REFUNDED: 0

Orders after settling: CONFIRMED 5003, EXPIRED 4997.

- [x] the system settled (converged=1, orders: CONFIRMED 5003, EXPIRED 4997)
- [x] every user got an order (none dropped, none left in the queue) (10000)
- [x] FAILED orders (a PENDING hold released would end FAILED) (0)
- [x] EXPIRED or FAILED orders that never had a payment link (0)
- [x] leaked-hold releases logged by the expiration worker (0)
- [x] generator outcomes failed, timeout or error (0)
- [x] the backlog was real: the last link came after more than the payment window (slowest checkout to link 42842 ms, window 30000 ms)
- [x] every user got the full payment window (time left to pay when the link was first seen at least window - 3400 ms) (least time left 27804 ms of 30000 ms)
- [x] payers who paid in the last half of the window all ended CONFIRMED (in time but not CONFIRMED) (0)
- [x] CONFIRMED orders equal payments accepted in time (the generator pays only in time here) (5003)
- [x] products.sold equals CONFIRMED orders (5003)
- [x] users with two live orders (0)
- [x] orders still AWAITING_PAYMENT (0)
- [x] stock:{sale} equals 10000 minus sold (every abandoned hold released) (4997)
- [x] holds left in holds:pending and holds:expiry (0 + 0)
- [x] server errors (5xx) in the generator's samples (0)
- [x] network failures in the generator's samples (0)


## 2026-10-28 15:24 UTC: flash sale, 10000 users, 500 units

Users arrive uniformly within 10 s of the sale opening; pay ratio 0.6, of which 0.05 pay only after the link expired and 0.05 pay 150 ms before it, racing the expiration worker; poll every 2000 ms; payment window 30 s; 50 users got their token from POST /v1/session, the rest were signed locally. Everything on one machine (20 cores): the generator, NGINX, three Order Service replicas, one order processor, one expiration worker, PostgreSQL, Redis and Kafka in Docker. Each user has its own X-Forwarded-For (gateway started with GATEWAY_TRUST_XFF=1). Real load for 100,000 users is the same script with N=100000 (see load/README.md).

Finished in 57.2s

Outcomes:
  confirmed: 295
  expired: 198
  refunded: 7
  sold_out: 9500

| endpoint | requests | peak req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| POST /v1/session | 50 | 29 | 2.9 | 8.0 | 10.2 | 10.2 | 201: 50 |
| GET /v1/sales/{id} | 10000 | 1073 | 0.2 | 0.5 | 1.6 | 7.0 | 200: 10000 |
| POST /v1/sales/{id}/checkout | 10000 | 1073 | 0.3 | 2.9 | 14.3 | 39.0 | 202: 500, 410: 9500 |
| GET /v1/orders/status/{ticket} | 4325 | 254 | 0.7 | 1.2 | 1.5 | 8.0 | 200: 4325 |
| POST /fake-pay/{session}/pay | 312 | 59 | 0.5 | 0.9 | 1.5 | 6.8 | 303: 305, 410: 7 |

Turned away by rate limits (429): 0; server errors (5xx): 0; network failures: 0
Retries: 0 after 5xx, 0 after network errors; 0 waits on 429
Checkout to payment link: 500 users, p50 1047 ms, p95 1940 ms, max 2218 ms; least time left to pay when seen: 27795 ms
Payments accepted by the provider: 305 (surely in time 288, at the boundary 17, after expiry 0); refused by the provider: 7
In-time payments not CONFIRMED: 0; late payments not REFUNDED: 0

Orders after settling: CONFIRMED 295, EXPIRED 195, REFUNDED 10. Requests turned away by rate limits (429): 0 (the generator waited `Retry-After` and retried).

- [x] the system settled (no AWAITING_PAYMENT orders, inbox and outbox drained) (converged=1, orders: CONFIRMED 295, EXPIRED 195, REFUNDED 10)
- [x] CONFIRMED orders equal the payments that arrived in time, and are at most 500 (CONFIRMED 295; payments surely in time 288, at the boundary 17)
- [x] every payment surely in time ended CONFIRMED (generator's own polling) (0)
- [x] every payment accepted after the link expired ended REFUNDED (generator's own polling; 0 such) (0)
- [ ] **FAILED** every payment at the boundary of the expiry ended CONFIRMED or REFUNDED (never lost; 17 such): 3, expected 0
- [x] CONFIRMED orders equal users the generator saw CONFIRMED (295)
- [x] products.sold equals CONFIRMED orders (295)
- [x] users with two live orders (0)
- [x] orders still AWAITING_PAYMENT (0)
- [x] stock:{sale} equals 500 minus products.sold (every unit of an expired order returned once, none of a confirmed one) (205)
- [x] FAILED orders (no publish failed) (0)
- [x] server errors (5xx) in the generator's samples (0)
- [x] network failures in the generator's samples (0)


## 2026-10-28 15:26 UTC: flash sale, 100000 users, 500 units

Users arrive uniformly within 10 s of the sale opening; pay ratio 0.6, of which 0.05 pay only after the link expired and 0.05 pay 150 ms before it, racing the expiration worker; poll every 2000 ms; payment window 30 s; 50 users got their token from POST /v1/session, the rest were signed locally. Everything on one machine (20 cores): the generator, NGINX, three Order Service replicas, one order processor, one expiration worker, PostgreSQL, Redis and Kafka in Docker. Each user has its own X-Forwarded-For (gateway started with GATEWAY_TRUST_XFF=1). Real load for 100,000 users is the same script with N=100000 (see load/README.md).

Finished in 56.9s

Outcomes:
  confirmed: 301
  expired: 199
  sold_out: 99500

| endpoint | requests | peak req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| POST /v1/session | 50 | 22 | 2.7 | 7.3 | 8.0 | 8.0 | 201: 50 |
| GET /v1/sales/{id} | 100000 | 10143 | 0.4 | 2.5 | 48.5 | 129.5 | 200: 100000 |
| POST /v1/sales/{id}/checkout | 100000 | 10111 | 0.6 | 3.3 | 66.9 | 156.1 | 202: 500, 410: 99500 |
| GET /v1/orders/status/{ticket} | 4226 | 267 | 0.8 | 1.8 | 19.5 | 76.3 | 200: 4226 |
| POST /fake-pay/{session}/pay | 318 | 60 | 0.8 | 1.7 | 182.9 | 364.8 | 303: 301, 410: 17 |

Turned away by rate limits (429): 0; server errors (5xx): 0; network failures: 0
Retries: 0 after 5xx, 0 after network errors; 0 waits on 429
Checkout to payment link: 500 users, p50 1092 ms, p95 2066 ms, max 2709 ms; least time left to pay when seen: 27849 ms
Payments accepted by the provider: 301 (surely in time 297, at the boundary 4, after expiry 0); refused by the provider: 17
In-time payments not CONFIRMED: 0; late payments not REFUNDED: 0

Orders after settling: CONFIRMED 301, EXPIRED 199. Requests turned away by rate limits (429): 0 (the generator waited `Retry-After` and retried).

- [x] the system settled (no AWAITING_PAYMENT orders, inbox and outbox drained) (converged=1, orders: CONFIRMED 301, EXPIRED 199)
- [x] CONFIRMED orders equal the payments that arrived in time, and are at most 500 (CONFIRMED 301; payments surely in time 297, at the boundary 4)
- [x] every payment surely in time ended CONFIRMED (generator's own polling) (0)
- [x] every payment accepted after the link expired ended REFUNDED (generator's own polling; 0 such) (0)
- [x] every payment at the boundary of the expiry ended CONFIRMED or REFUNDED (never lost; 4 such) (0)
- [x] CONFIRMED orders equal users the generator saw CONFIRMED (301)
- [x] products.sold equals CONFIRMED orders (301)
- [x] users with two live orders (0)
- [x] orders still AWAITING_PAYMENT (0)
- [x] stock:{sale} equals 500 minus products.sold (every unit of an expired order returned once, none of a confirmed one) (199)
- [x] FAILED orders (no publish failed) (0)
- [x] server errors (5xx) in the generator's samples (0)
- [x] network failures in the generator's samples (0)


## 2026-10-28 15:28 UTC: backlog, 10000 users, 10000 units, processor throttled to 200/s

All users check out within 10 s of the sale opening; the processor starts 200 messages per second, so the queue needs about 50 s to drain. Payment window 30 s; 0.5 of the users pay, 55-70 % of the window after their link appeared; poll every 2000 ms. One machine, one order processor, three replicas behind NGINX (gateway started with GATEWAY_TRUST_XFF=1). The design point is N=UNITS=100000 at 1,000/s (same script).

Finished in 92.3s

Outcomes:
  confirmed: 4986
  expired: 5014

| endpoint | requests | peak req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| POST /v1/sales/{id}/checkout | 10000 | 1069 | 3.4 | 4.5 | 5.2 | 9.8 | 202: 10000 |
| GET /v1/orders/status/{ticket} | 236274 | 5103 | 0.5 | 0.9 | 1.3 | 7.6 | 200: 236274 |
| POST /fake-pay/{session}/pay | 4986 | 122 | 0.5 | 0.8 | 1.2 | 4.6 | 303: 4986 |

Turned away by rate limits (429): 0; server errors (5xx): 0; network failures: 0
Retries: 0 after 5xx, 0 after network errors; 0 waits on 429
Checkout to payment link: 10000 users, p50 21202 ms, p95 39425 ms, max 42605 ms; least time left to pay when seen: 27800 ms
Payments accepted by the provider: 4986 (surely in time 4986, at the boundary 0, after expiry 0); refused by the provider: 0
In-time payments not CONFIRMED: 0; late payments not REFUNDED: 0

Orders after settling: CONFIRMED 4986, EXPIRED 5014.

- [x] the system settled (converged=1, orders: CONFIRMED 4986, EXPIRED 5014)
- [x] every user got an order (none dropped, none left in the queue) (10000)
- [x] FAILED orders (none: no publish failed, nothing queued was given up) (0)
- [x] EXPIRED or FAILED orders that never had a payment link (0)
- [x] generator outcomes failed, timeout or error (0)
- [x] the backlog was real: the last link came after more than the payment window (slowest checkout to link 42605 ms, window 30000 ms)
- [x] every user got the full payment window (time left to pay when the link was first seen at least window - 3400 ms) (least time left 27800 ms of 30000 ms)
- [x] payers who paid in the last half of the window all ended CONFIRMED (in time but not CONFIRMED) (0)
- [x] CONFIRMED orders equal payments accepted in time (the generator pays only in time here) (4986)
- [x] products.sold equals CONFIRMED orders (4986)
- [x] users with two live orders (0)
- [x] orders still AWAITING_PAYMENT (0)
- [x] stock:{sale} equals 10000 minus products.sold (every unit of an expired order returned once) (5014)
- [x] server errors (5xx) in the generator's samples (0)
- [x] network failures in the generator's samples (0)


## 2026-10-28 15:36 UTC: flash sale, 10000 users, 500 units

Users arrive uniformly within 10 s of the sale opening; pay ratio 0.6, of which 0.05 pay only after the link expired and 0.05 pay 150 ms before it, racing the expiration worker; poll every 2000 ms; payment window 30 s; 50 users got their token from POST /v1/session, the rest were signed locally. Everything on one machine (20 cores): the generator, NGINX, three Order Service replicas, one order processor, one expiration worker, PostgreSQL, Redis and Kafka in Docker. Each user has its own X-Forwarded-For (gateway started with GATEWAY_TRUST_XFF=1). Real load for 100,000 users is the same script with N=100000 (see load/README.md).

Finished in 57.3s

Outcomes:
  confirmed: 274
  expired: 223
  refunded: 3
  sold_out: 9500

| endpoint | requests | peak req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| POST /v1/session | 50 | 24 | 3.0 | 7.5 | 8.6 | 8.6 | 201: 50 |
| GET /v1/sales/{id} | 10000 | 1022 | 0.2 | 0.4 | 0.5 | 1.8 | 200: 10000 |
| POST /v1/sales/{id}/checkout | 10000 | 1023 | 0.3 | 1.0 | 4.4 | 12.0 | 202: 500, 410: 9500 |
| GET /v1/orders/status/{ticket} | 4618 | 277 | 0.6 | 1.2 | 1.4 | 2.1 | 200: 4618 |
| POST /fake-pay/{session}/pay | 289 | 58 | 0.5 | 0.9 | 1.2 | 7.0 | 303: 277, 410: 12 |

Turned away by rate limits (429): 0; server errors (5xx): 0; network failures: 0
Retries: 0 after 5xx, 0 after network errors; 0 waits on 429
Checkout to payment link: 500 users, p50 913 ms, p95 1881 ms, max 1996 ms; least time left to pay when seen: 28007 ms
Payments accepted by the provider: 277 (surely in time 265, at the boundary 12, after expiry 0); refused by the provider: 12
In-time payments not CONFIRMED: 0; late payments not REFUNDED: 0

Orders after settling: CONFIRMED 274, EXPIRED 223, REFUNDED 3. Requests turned away by rate limits (429): 0 (the generator waited `Retry-After` and retried).

- [x] the system settled (no AWAITING_PAYMENT orders, inbox and outbox drained) (converged=1, orders: CONFIRMED 274, EXPIRED 223, REFUNDED 3)
- [x] CONFIRMED orders equal the payments that arrived in time, and are at most 500 (CONFIRMED 274; payments surely in time 265, at the boundary 12)
- [x] every payment surely in time ended CONFIRMED (generator's own polling) (0)
- [x] every payment accepted after the link expired ended REFUNDED (generator's own polling; 0 such) (0)
- [x] every payment at the boundary of the expiry ended CONFIRMED or REFUNDED (never lost; 12 such) (0)
- [x] CONFIRMED orders equal users the generator saw CONFIRMED (274)
- [x] products.sold equals CONFIRMED orders (274)
- [x] users with two live orders (0)
- [x] orders still AWAITING_PAYMENT (0)
- [x] stock:{sale} equals 500 minus products.sold (every unit of an expired order returned once, none of a confirmed one) (226)
- [x] FAILED orders (no publish failed) (0)
- [x] server errors (5xx) in the generator's samples (0)
- [x] network failures in the generator's samples (0)


## 2026-10-28 16:28 UTC: flash sale, 10000 users, 500 units

Users arrive uniformly within 10 s of the sale opening; pay ratio 0.6, of which 0.05 pay only after the link expired and 0.05 pay 150 ms before it, racing the expiration worker; poll every 2000 ms; payment window 30 s; 50 users got their token from POST /v1/session, the rest were signed locally. Everything on one machine (20 cores): the generator, NGINX, three Order Service replicas, one order processor, one expiration worker, PostgreSQL, Redis and Kafka in Docker. Each user has its own X-Forwarded-For (gateway started with GATEWAY_TRUST_XFF=1). Real load for 100,000 users is the same script with N=100000 (see load/README.md).

Finished in 56.8s

Outcomes:
  confirmed: 289
  expired: 206
  refunded: 5
  sold_out: 9500

| endpoint | requests | peak req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| POST /v1/session | 50 | 19 | 2.4 | 7.3 | 7.7 | 7.7 | 201: 50 |
| GET /v1/sales/{id} | 10000 | 1027 | 0.2 | 0.5 | 1.2 | 7.1 | 200: 10000 |
| POST /v1/sales/{id}/checkout | 10000 | 1028 | 0.3 | 3.3 | 5.9 | 15.3 | 202: 500, 410: 9500 |
| GET /v1/orders/status/{ticket} | 4430 | 259 | 0.7 | 1.3 | 1.7 | 7.5 | 200: 4430 |
| POST /fake-pay/{session}/pay | 307 | 57 | 0.6 | 1.2 | 2.6 | 7.0 | 303: 294, 410: 13 |

Turned away by rate limits (429): 0; server errors (5xx): 0; network failures: 0
Retries: 0 after 5xx, 0 after network errors; 0 waits on 429
Checkout to payment link: 500 users, p50 1030 ms, p95 1879 ms, max 1999 ms; least time left to pay when seen: 28004 ms
Payments accepted by the provider: 294 (surely in time 279, at the boundary 15, after expiry 0); refused by the provider: 13
In-time payments not CONFIRMED: 0; late payments not REFUNDED: 0

Orders after settling: CONFIRMED 289, EXPIRED 206, REFUNDED 5. Requests turned away by rate limits (429): 0 (the generator waited `Retry-After` and retried).

- [x] the system settled (no AWAITING_PAYMENT orders, inbox and outbox drained) (converged=1, orders: CONFIRMED 289, EXPIRED 206, REFUNDED 5)
- [x] CONFIRMED orders equal the payments that arrived in time, and are at most 500 (CONFIRMED 289; payments surely in time 279, at the boundary 15)
- [x] every payment surely in time ended CONFIRMED (generator's own polling) (0)
- [x] every payment accepted after the link expired ended REFUNDED (generator's own polling; 0 such) (0)
- [x] every payment at the boundary of the expiry ended CONFIRMED or REFUNDED (never lost; 15 such) (0)
- [x] CONFIRMED orders equal users the generator saw CONFIRMED (289)
- [x] products.sold equals CONFIRMED orders (289)
- [x] users with two live orders (0)
- [x] orders still AWAITING_PAYMENT (0)
- [x] stock:{sale} equals 500 minus products.sold (every unit of an expired order returned once, none of a confirmed one) (211)
- [x] FAILED orders (no publish failed) (0)
- [x] server errors (5xx) in the generator's samples (0)
- [x] network failures in the generator's samples (0)


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

