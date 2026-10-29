# Flash Sale — web

The storefront: a home page listing the sales, and per sale a product page,
countdown, buy button, queue and payment states. Preact + Vite + TypeScript, no UI, routing or state
libraries.

## Development

```bash
npm install
npm run dev
```

Vite serves on <http://localhost:5173> and proxies like the gateway: the site service's routes
(`/v1/session`, `/v1/sales`, `/v1/sales/search`, `/v1/sales/<id>`, `/v1/admin/`) to <http://localhost:8082>, the rest of
`/v1` (checkout, status) to the Order Service at <http://localhost:8080>. The payment page is the provider's own
(`http://localhost:8090/fake-pay/...`), opened directly, so there is no `/fake-pay` proxy. `/` lists the latest sales
(`GET /v1/sales`) with a search box (`GET /v1/sales/search?q=`, typed text trimmed and lowercased, sent 300 ms after the
last key; empty shows the list); `/?sale=<id>` is that sale's page.

## Build

```bash
npm run build
```

`tsc --noEmit` type-checks, then Vite writes `dist/` with hashed, immutable
asset names and `/` as the base path. Locally the gateway image serves `dist/`; for production see "Where the
frontend runs, and how to deploy it" in the root README.

## Browser run-through

With the compose stack up (root README, Quick start):

```bash
npm run demo                 # creates a 5-unit sale opening in 12 s, buys, pays, waits for "Order confirmed"
npm run demo -- <saleId>     # an existing sale that has not sold out, instead of a new one
```

`scripts/demo-run.mjs` drives Chromium through Playwright: the countdown, the buy button, the fake provider's payment
page, and back to "Order confirmed", saving a screenshot of each step to `demo-shots/`. It creates the sale through the
gateway container and sends a normal browser user agent, since the gateway refuses `HeadlessChrome`. `CHROMIUM_PATH`
(default `/usr/bin/chromium`), `DEMO_BASE_URL` (default `http://localhost:8000`) and `DEMO_OUT` (default `demo-shots`)
change the browser, the target and where the screenshots land.

## Tests and budgets

```bash
npm test        # Vitest: response mapping, state, search text, backoff, Retry-After, storage
npm run budgets # raw + gzip size gate, then Lighthouse TTI when available
```

`npm run budgets` fails when the built JavaScript and CSS together exceed
**200 kB raw** or **50 kB gzipped**. It then runs Lighthouse
(`--preset=desktop`) against the build and fails if time to interactive is
**≥ 1.5 s**. Lighthouse is not a dependency: if no `lighthouse` binary or
Chrome is found, that part prints a skip message. Override the lookup with
`LIGHTHOUSE_BIN` and `CHROME_PATH`.

## Behaviour

- A guest JWT from `POST /v1/session` is kept in `sessionStorage` and reused
  until 30 s before it expires; storage access is best-effort and never throws.
- The buy button locks synchronously on the first click. One
  `Idempotency-Key` (`crypto.randomUUID`) is created per attempt and reused by
  every retry, including retries after `429`/`503` and network failures.
- `429` honours `Retry-After`; `503` and network errors use exponential backoff
  with full jitter.
- Status is polled every 2 s from `GET /v1/orders/status/{ticket}` and stops on
  a final state. The ticket (and final state) is kept in `sessionStorage`, so a
  reload — or a return from `/fake-pay/...` — resumes where it left off.
- The payment page is opened automatically at most once per ticket in a tab.
  Coming back to `/?sale=<id>` while the payment webhook is still in flight
  shows a "waiting for the provider" state with a link, instead of bouncing
  straight back to `/fake-pay`.
- The product image and the lazy checkout/status chunk are prefetched before
  the sale opens: the image with `<link rel="preload">`, the chunk with
  `import('./flow')`.
