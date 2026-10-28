//! The home page (`/`): the latest sales, each linking to its sale page
//! (`/?sale=<id>`), with a search box (debounced, `GET /v1/sales/search`).
//! Read-only; no session is created here.

import { useEffect, useState } from 'preact/hooks';
import { fetchSales, fetchSearch } from './api';
import { formatCountdown, formatMoney, secondsLeft } from './logic/format';
import { normalizeQuery } from './logic/search';
import { parseSales, type Sale } from './logic/state';

/** How long typing must pause before the search runs. */
const SEARCH_DEBOUNCE_MS = 300;

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
  // What is typed, and the normalised text last settled on (empty = the normal list)
  const [typed, setTyped] = useState('');
  const [term, setTerm] = useState('');

  useEffect(() => {
    const timer = window.setTimeout(() => setTerm(normalizeQuery(typed)), SEARCH_DEBOUNCE_MS);
    return () => window.clearTimeout(timer);
  }, [typed]);

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1_000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    let cancelled = false;
    // Keep showing the previous answer while a new search is on its way
    setList((current) => (current.kind === 'ready' ? current : { kind: 'loading' }));
    void (term === '' ? fetchSales() : fetchSearch(term)).then((result) => {
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
  }, [term, reloadNonce]);

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

      <div class="search">
        <label class="micro" for="sale-search">
          Search sales
        </label>
        <input
          id="sale-search"
          class="search__input"
          type="search"
          value={typed}
          maxLength={200}
          autoComplete="off"
          placeholder="Name of a sale"
          onInput={(event) => setTyped(event.currentTarget.value)}
        />
      </div>

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
          {term === '' ? (
            <>
              <p class="panel__title">No sales yet</p>
              <p class="note">Check back soon.</p>
            </>
          ) : (
            <>
              <p class="panel__title">No sales match</p>
              <p class="note">Nothing is called “{term}”. Try a shorter or different name.</p>
            </>
          )}
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
        <p>{term === '' ? 'THE LATEST 50 SALES' : 'UP TO 50 MATCHES'}</p>
      </footer>
    </main>
  );
}
