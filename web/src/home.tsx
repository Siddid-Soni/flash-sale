//! The home page (`/`): the latest sales, each linking to its sale page
//! (`/?sale=<id>`). Read-only; no session is created here.

import { useEffect, useState } from 'preact/hooks';
import { fetchSales } from './api';
import { formatCountdown, formatMoney, secondsLeft } from './logic/format';
import { parseSales, type Sale } from './logic/state';

type List =
  | { kind: 'loading' }
  | { kind: 'error'; message: string }
  | { kind: 'ready'; sales: Sale[] };

function saleState(sale: Sale, now: number): { label: string; tone: '' | 'live' | 'out' } {
  if (sale.soldOut) return { label: 'Sold out', tone: 'out' };
  if (sale.saleStartMs === null || now >= sale.saleStartMs) return { label: '● Live now', tone: 'live' };
  return { label: `Opens in ${formatCountdown(secondsLeft(sale.saleStartMs, now))}`, tone: '' };
}

export function Home() {
  const [list, setList] = useState<List>({ kind: 'loading' });
  const [now, setNow] = useState(() => Date.now());
  const [reloadNonce, setReloadNonce] = useState(0);

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1_000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    let cancelled = false;
    setList({ kind: 'loading' });
    void fetchSales().then((result) => {
      if (cancelled) return;
      if (!result.ok) {
        setList({ kind: 'error', message: result.message });
        return;
      }
      const sales = parseSales(result.body);
      setList(
        sales === null
          ? { kind: 'error', message: 'The sale list was not in the expected format.' }
          : { kind: 'ready', sales },
      );
    });
    return () => {
      cancelled = true;
    };
  }, [reloadNonce]);

  return (
    <main class="page">
      <nav class="nav" aria-label="Store">
        <a class="nav__cell nav__brand" href="/">VOLT STORE</a>
        <p class="nav__cell">FLASH SALES</p>
      </nav>

      <div class="kicker">
        <p>PRECISION DROPS</p>
        <p>ONE PER PERSON</p>
      </div>
      <h1 class="display display--small">Sales</h1>

      {list.kind === 'loading' && <p class="micro sales__msg">Loading sales_</p>}

      {list.kind === 'error' && (
        <div class="panel panel--bad sales__msg" role="alert">
          <p class="panel__title">Load failed</p>
          <p class="note">{list.message}</p>
          <button type="button" class="button button--ghost" onClick={() => setReloadNonce((n) => n + 1)}>
            Try again →
          </button>
        </div>
      )}

      {list.kind === 'ready' && list.sales.length === 0 && (
        <div class="panel sales__msg">
          <p class="panel__title">No sales yet</p>
          <p class="note">Check back soon.</p>
        </div>
      )}

      {list.kind === 'ready' && list.sales.length > 0 && (
        <ul class="sales">
          {list.sales.map((sale) => {
            const state = saleState(sale, now);
            return (
              <li key={sale.id}>
                <a class="sale" href={`/?sale=${encodeURIComponent(sale.id)}`}>
                  <span class="sale__id">#{sale.id}</span>
                  <span class="sale__name">{sale.name}</span>
                  <span class="sale__meta">
                    {formatMoney(sale.priceCents)} · {sale.totalInventory} units
                  </span>
                  <span class={`sale__state${state.tone ? ` sale__state--${state.tone}` : ''}`}>
                    {state.label}
                  </span>
                </a>
              </li>
            );
          })}
        </ul>
      )}

      <footer class="footer">
        <p>VOLT STORE — FLASH SALES</p>
        <p>THE LATEST 50 SALES</p>
      </footer>
    </main>
  );
}
