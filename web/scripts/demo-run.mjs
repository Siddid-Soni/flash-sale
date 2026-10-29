// End-to-end run of the storefront in a real browser against the compose stack:
// creates a small sale opening in a few seconds, waits for it, buys, pays on the
// fake provider's page, and waits for "Order confirmed". Screenshots of every step
// go to DEMO_OUT (default ./demo-shots).
//
//   npm run demo                 # new sale, opens in 12 s
//   npm run demo -- <saleId>     # an existing sale that has not sold out
//
// Needs the stack up (docker compose --profile app up -d --build) and a Chromium:
// CHROMIUM_PATH, default /usr/bin/chromium.
import { execFileSync } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import { chromium } from 'playwright-core';

const BASE = process.env.DEMO_BASE_URL ?? 'http://localhost:8000';
const OUT = process.env.DEMO_OUT ?? 'demo-shots';
const CHROMIUM = process.env.CHROMIUM_PATH ?? '/usr/bin/chromium';
// The gateway's bot filter refuses the HeadlessChrome user agent.
const UA =
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36';

async function createSale() {
  // The admin route is refused from the host, so post from inside the gateway container.
  // Retried for up to a minute: right after `docker compose up` the site service can take
  // a while to start listening, and until then the gateway answers 502.
  for (let attempt = 1; ; attempt++) {
    const start = new Date(Date.now() + 12_000).toISOString().replace(/\.\d+Z$/, 'Z');
    const body = JSON.stringify({
      name: 'Demo Phone',
      price_cents: 49900,
      total_inventory: 5,
      sale_start_time: start,
    });
    try {
      const res = execFileSync(
        'docker',
        [
          'compose', 'exec', '-T', 'gateway', 'wget', '-qO-',
          '--header', 'X-Admin-Token: dev-admin-token',
          '--header', 'Content-Type: application/json',
          '--post-data', body,
          'http://127.0.0.1/v1/admin/sales',
        ],
        { stdio: ['ignore', 'pipe', 'pipe'] },
      );
      return JSON.parse(res.toString()).id;
    } catch (error) {
      if (attempt >= 30) throw error;
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }
}

const sale = process.argv[2] ?? (await createSale());
mkdirSync(OUT, { recursive: true });
let step = 0;
const shot = async (page, name) => {
  const path = `${OUT}/${++step}-${name}.png`;
  await page.screenshot({ path });
  console.log(`${path}  ${page.url()}`);
};

const browser = await chromium.launch({ executablePath: CHROMIUM });
try {
  const ctx = await browser.newContext({ userAgent: UA, viewport: { width: 1280, height: 900 } });
  const page = await ctx.newPage();

  await page.goto(`${BASE}/`);
  await page.waitForTimeout(1000);
  await shot(page, 'list');

  await page.goto(`${BASE}/?sale=${sale}`);
  await page.waitForTimeout(1000);
  await shot(page, 'countdown');

  const buy = page.locator('button').filter({ hasNotText: /all sales/i }).first();
  await buy.waitFor();
  for (let i = 0; i < 120 && (await buy.isDisabled()); i++) await page.waitForTimeout(500);
  await shot(page, 'open');

  await buy.click();
  // "You're in", then "Unit reserved", then the frontend sends the buyer to the provider's
  // page on its own. On a quick run the first two last only a moment, so each is captured
  // only if the page still shows it; the payment page is what the run waits for.
  for (const [text, name] of [["You're in", 'queued'], ['Unit reserved', 'reserved']]) {
    const seen = await Promise.race([
      page.getByText(text, { exact: true }).waitFor({ timeout: 30_000 }).then(() => true),
      page.waitForURL(/fake-pay/, { timeout: 30_000 }).then(() => false),
    ]).catch(() => false);
    if (seen && !/fake-pay/.test(page.url())) await shot(page, name);
  }
  await page.waitForURL(/fake-pay/, { timeout: 30_000 });
  await shot(page, 'paypage');
  await page.getByRole('button', { name: /^Pay/ }).click();

  await page.waitForURL((url) => url.href.startsWith(BASE), { timeout: 30_000 });
  await page.getByText('Order confirmed', { exact: true }).waitFor({ timeout: 30_000 });
  await shot(page, 'confirmed');
  console.log(`sale ${sale}: order confirmed`);
} finally {
  await browser.close();
}
