# Gateway

NGINX in front of the system (roadmap Stage 7): the only container that publishes a port for the API
(`http://localhost:8000`). It serves the built frontend, balances `/v1/` over three Order Service
replicas, rate-limits, filters bots and hides `/v1/admin/`.

```
docker compose --profile app up -d --build      # postgres, redis, kafka, 3 replicas, processor, worker, gateway
gateway/smoke.sh                                 # PASS/FAIL per check, non-zero exit on any failure
```

The Dockerfile builds `web/` (node) and copies `web/dist` into the NGINX image, with `.gz` copies for
`gzip_static`. The njs module ships with the official image.

## Files

| File | Purpose |
|---|---|
| `nginx.conf` | Everything: capacity sizing, upstream, limits, locations. Comments explain each choice |
| `bots.conf` | User-Agent patterns for `$is_bot`, in its own file so the list is easy to edit |
| `jwt.js` | njs: reads the user id (`sub`) from the bearer token, to pick the per-user rate-limit bucket |
| `payment_provider_ips.conf` | The addresses the payment webhook accepts (locally the provider container's fixed `172.28.0.50`; in production the provider's published webhook IPs) |
| `realip.conf` | Empty by default. Overwritten at start by `docker-entrypoint.d/25-realip.sh` |
| `realip-trusted.conf` | Trust `X-Forwarded-For` from the bridge address (load tests only) |
| `smoke.sh` | End-to-end check of the points below |

## What it does

- **Balancing and failover.** `least_conn` over `order-service-1..3` with upstream `keepalive`
  (HTTP/1.1, `Connection` cleared). A replica that fails twice within 5 s is skipped for 5 s. Retry on
  the next replica only on `error timeout` (a failed or timed-out connect); a request already sent is never
  repeated, so a checkout is never executed twice by the gateway, and an app `503` is passed on as the answer.
  Replicas are re-resolved through Docker's DNS (`resolve`), so a restarted one comes back without a reload.
  Each response carries `X-Replica: <id>`.
- **Rate limits** (answer `429`, header `Retry-After: 1`, body `{"message": ...}`):

  | Route | Key | Limit |
  |---|---|---|
  | `POST /v1/session` | IP | 1 r/s, burst 5 |
  | `POST /v1/sales/{id}/checkout` | user (JWT `sub`, else IP) | 2 r/s, burst 5 |
  | same | IP (backstop against forged tokens) | 20 r/s, burst 40 |
  | `GET /v1/orders/status/*` | user | 2 r/s, burst 10 (clients poll at 0.5 r/s) |
  | `/v1/*` and `/fake-pay/*` | IP | 100 concurrent connections |

  No limit on `/v1/payments/webhook` (only the address check below) or static files. The token is not verified at the gateway: its `sub`
  only picks a bucket, and the Order Service rejects a forged token.
- **Bots.** `403 {"message": ...}` on `/v1/` (not the webhook) for an empty User-Agent and the patterns in
  `bots.conf`. `curl` and `wget` are deliberately allowed because ops scripts use them; the trade-off is that
  a script claiming to be a browser passes, which the rate limits and the session challenge hook
  (`SESSION_CHALLENGE`, `src/challenge.rs`, a no-op by default) are there for.
- **Admin.** `/v1/admin/` is `403` unless the TCP peer is inside the compose network (`172.28.0.0/16`, fixed in
  `docker-compose.yml`) or the gateway itself. Requests from the host appear from the bridge address
  `172.28.0.1` and are refused. The check uses the real peer, so a forged `X-Forwarded-For` cannot open it.
- **Payment provider.** `/fake-pay/` goes to the `fake-payment-provider` container (its own upstream), not to the
  replicas. `POST /v1/payments/webhook` goes to the `order_processor` upstream (port 8081) and is allowed only from
  the addresses in `payment_provider_ips.conf`; everyone else, the host and the gateway container included, gets
  `403 {"message": ...}`. Like the admin check it tests the real TCP peer (`$realip_remote_addr`), so a forged
  `X-Forwarded-For` cannot pass, not even with `GATEWAY_TRUST_XFF=1` (which is why the list is a `geo` block and
  not `allow`/`deny`, which look at the replaced address). In production the file lists the provider's published
  webhook IPs; the signature check in the processor stays as the second layer. The compose file gives the
  provider the fixed address `172.28.0.50` so this works.
- **Static and cache.** `/assets/*` is `public, max-age=31536000, immutable`; `index.html` is `no-cache`;
  `gzip_static` serves pre-compressed files, `gzip` covers other text. `GET /v1/sales/{id}` is cached by
  NGINX for the 1 s the app allows (`X-Cache-Status`); `proxy_cache_lock` and `use_stale updating` make a
  stampede reach the app about once per second. Status polls and everything else are not cached.
- **Zero dropped requests.** `worker_processes auto`, `worker_connections 16384`, `worker_rlimit_nofile 65536`,
  `listen ... backlog=4096` (with `net.core.somaxconn=4096` in compose), upstream `keepalive`, and in the app
  `HTTP_BACKLOG` (4096), `HTTP_MAX_CONNECTIONS` (25000 per worker), `HTTP_KEEP_ALIVE_SECS` (75, longer than the
  gateway's upstream `keepalive_timeout 60s` so NGINX is the side that closes idle connections). A burst waits in
  the listen queues; every legitimate request gets the app's real answer (`202`, `409`, `410`, ...), and only a
  client over its limit gets `429`. A quick local check (15,000 mixed requests, 2,000 at once, each with its own
  client address) completed with no failed or refused connection.

## Operating it

Admin calls from inside (replace the date; busybox `wget` has `--header` and `--post-data`):

```
docker compose exec -T gateway wget -qO- \
  --header 'X-Admin-Token: dev-admin-token' --header 'Content-Type: application/json' \
  --post-data '{"name":"Phone","price_cents":49900,"total_inventory":500,"sale_start_time":"2026-10-28T20:00:00Z"}' \
  http://127.0.0.1/v1/admin/sales
```

The same URL from the host (`curl http://localhost:8000/v1/admin/sales`) answers `403`.

**Load tests (Stage 9).** The generator runs on the host, so every request reaches the gateway from the
bridge address `172.28.0.1` and would share one per-IP bucket. To count simulated clients separately, start the
gateway with

```
GATEWAY_TRUST_XFF=1 docker compose --profile app up -d gateway
```

and send a distinct `X-Forwarded-For` per simulated user. Only the bridge address is trusted, and the admin
check ignores it. Restart without the variable afterwards; the default trusts nothing. A load run without it
sees `429` from the single-IP limits by design. Run the load against port 8000 (replicas publish no port) and
create the sale through `docker compose exec` as above.

Other settings: `docker compose logs gateway` has one line per request (replica, upstream time, cache status,
user id). The compose network and the gateway's `172.28.0.0/16` rule must agree; change both together.
