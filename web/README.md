# Flash Sale — web

The storefront: a home page listing the sales, and per sale a product page,
countdown, buy button, queue and payment states. Preact + Vite + TypeScript, no UI, routing or state
libraries.

## Development

```bash
npm install
npm run dev
```

Vite serves on <http://localhost:5173> and proxies `/v1` and `/fake-pay` to the
Rust API at <http://localhost:8080>. `/` lists the latest sales
(`GET /v1/sales`); `/?sale=<id>` is that sale's page.

## Build

```bash
npm run build
```

`tsc --noEmit` type-checks, then Vite writes `dist/` with hashed, immutable
asset names and `/` as the base path (NGINX serves it, Stage 7).

## Tests and budgets

```bash
npm test        # Vitest: response mapping, state, backoff, Retry-After, storage
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
