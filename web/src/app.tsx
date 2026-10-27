//! The single page: product, countdown, buy button and the order states.
//! Checkout and polling live behind `import('./flow')`; this module only has
//! what first paint needs.

import { useCallback, useEffect, useMemo, useRef, useState } from 'preact/hooks';
import { createGuestSession, fetchSale } from './api';
import { formatCountdown, formatDelay, formatMoney, secondsLeft } from './logic/format';
import { currentSession, forgetSession, parseSessionResponse, rememberSession } from './logic/session';
import {
  finalStatusName,
  isFinalStatus,
  parseSale,
  screenForFinalStatus,
  statusToScreen,
  type CheckoutOutcome,
  type Sale,
  type Screen,
  type StatusUpdate,
} from './logic/state';
import { readTicket, writeTicket } from './logic/storage';

type Flow = typeof import('./flow');

const POLL_NOTE = 'We check every 2 seconds. Keep this page open.';
const REDIRECT_DELAY_MS = 1_500;

/** One lazy chunk for checkout and polling, requested while the page loads. */
let flowPromise: Promise<Flow> | null = null;

function loadFlow(): Promise<Flow> {
  flowPromise ??= import('./flow').catch((error: unknown) => {
    // A failed chunk request must be retryable, so the next attempt re-imports.
    flowPromise = null;
    throw error;
  });
  return flowPromise;
}

function resolveSaleId(): string {
  try {
    const fromQuery = new URLSearchParams(window.location.search).get('sale');
    if (fromQuery !== null && fromQuery.trim() !== '') return fromQuery.trim();
  } catch {
    // A broken URL is not worth failing over; fall through to the default.
  }
  const fromEnv = (import.meta.env.VITE_SALE_ID ?? '').trim();
  return fromEnv !== '' ? fromEnv : '1';
}

const SALE_ID = resolveSaleId();

/** One key per purchase attempt; reused by every retry of that attempt. */
function newIdempotencyKey(): string {
  const cryptoApi = globalThis.crypto;
  if (typeof cryptoApi?.randomUUID === 'function') return cryptoApi.randomUUID();

  if (typeof cryptoApi?.getRandomValues === 'function') {
    const bytes = cryptoApi.getRandomValues(new Uint8Array(16));
    return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
  }
  return `attempt-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function preloadProductImage(): void {
  const href = '/product.svg';
  const existing = document.head.querySelector(`link[rel="preload"][href="${href}"]`);
  if (existing !== null) return;

  const link = document.createElement('link');
  link.rel = 'preload';
  link.as = 'image';
  link.href = href;
  link.type = 'image/svg+xml';
  document.head.appendChild(link);
}

function describeScreen(screen: Screen, sale: Sale | null, isOpen: boolean): string {
  switch (screen.kind) {
    case 'loading':
      return 'Loading the sale.';
    case 'error':
      return screen.message;
    case 'ready':
      if (sale === null) return 'Loading the sale.';
      return isOpen
        ? 'The sale is open. The buy button is ready.'
        : 'The sale is not open yet. The buy button unlocks when it starts.';
    case 'reserving':
      return screen.note === ''
        ? 'Reserving your phone.'
        : `Reserving your phone. ${screen.note}`;
    case 'queue':
      return screen.position !== null
        ? `You are in the queue at position ${screen.position}.`
        : 'You are in the queue.';
    case 'payment':
      return 'Your phone is reserved. Continue to payment before the link expires.';
    case 'confirmed':
      return 'Order confirmed.';
    case 'sold-out':
      return 'Sold out. Every phone in this drop has gone.';
    case 'expired':
      return 'Your reservation expired before payment.';
    case 'failed':
      return screen.message;
    case 'refunded':
      return 'Your payment was refunded because the reservation had expired.';
    case 'already':
      return screen.message;
    case 'retry':
      return screen.message;
  }
}

export function App() {
  const [sale, setSale] = useState<Sale | null>(null);
  const [screen, setScreen] = useState<Screen>({ kind: 'loading' });
  const [waitNote, setWaitNote] = useState('');
  const [now, setNow] = useState(() => Date.now());
  const [cooldownUntil, setCooldownUntil] = useState(0);
  const [reloadNonce, setReloadNonce] = useState(0);

  // Refs, not state: the double-click guard and the ticket must be readable
  // synchronously, before a render happens.
  const busyRef = useRef(false);
  const keyRef = useRef<string | null>(null);
  const ticketRef = useRef<string | null>(null);
  const stopPollRef = useRef<(() => void) | null>(null);
  const pollGenerationRef = useRef(0);
  const sessionPromiseRef = useRef<Promise<string> | null>(null);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  // A single ticker drives every visible countdown. One second is enough: the
  // countdowns are shown to the second, not to the frame.
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1_000);
    return () => window.clearInterval(timer);
  }, []);

  const getToken = useCallback((options?: { force?: boolean }): Promise<string> => {
    if (options?.force === true) {
      sessionPromiseRef.current = null;
      forgetSession();
    } else if (sessionPromiseRef.current !== null) {
      return sessionPromiseRef.current;
    }

    const promise = (async () => {
      if (options?.force !== true) {
        const existing = currentSession();
        if (existing !== null) return existing.token;
      }

      const body = await createGuestSession();
      const session = parseSessionResponse(body);
      if (session === null) {
        throw new Error('The server did not return a session token.');
      }
      rememberSession(session);
      return session.token;
    })();

    sessionPromiseRef.current = promise.catch((error: unknown) => {
      sessionPromiseRef.current = null;
      throw error;
    });
    return sessionPromiseRef.current;
  }, []);

  const stopPolling = useCallback(() => {
    pollGenerationRef.current += 1;
    stopPollRef.current?.();
    stopPollRef.current = null;
  }, []);

  const applyUpdate = useCallback((ticket: string, update: StatusUpdate) => {
    const next = statusToScreen(update, ticket);
    if (next === null) {
      if (update.kind === 'retry') setWaitNote(update.message);
      return;
    }

    setWaitNote('');
    setScreen(next);

    if (isFinalStatus(update)) {
      const final = finalStatusName(update);
      if (final !== null) writeTicket(SALE_ID, { ticket, final });
    } else {
      writeTicket(SALE_ID, { ticket, final: null });
    }
  }, []);

  const startPolling = useCallback(
    (ticket: string) => {
      stopPolling();
      const generation = pollGenerationRef.current;
      if (ticketRef.current === null) ticketRef.current = ticket;

      void loadFlow()
        .then((flow) => {
          if (!mountedRef.current || generation !== pollGenerationRef.current) return;
          stopPollRef.current = flow.startStatusPolling({
            ticket,
            getToken,
            onUpdate: (update) => {
              if (generation !== pollGenerationRef.current) return;
              applyUpdate(ticket, update);
            },
            onError: () => {
              if (generation !== pollGenerationRef.current) return;
              setWaitNote('Connection lost. Retrying every 2 seconds.');
            },
          });
        })
        .catch(() => {
          if (!mountedRef.current || generation !== pollGenerationRef.current) return;
          setScreen({
            kind: 'failed',
            message: 'The status client could not be loaded. Reload the page to try again.',
          });
        });
    },
    [applyUpdate, getToken, stopPolling],
  );

  const applyOutcome = useCallback(
    (outcome: CheckoutOutcome) => {
      switch (outcome.type) {
        case 'ticketed': {
          ticketRef.current = outcome.ticket;
          keyRef.current = null;
          writeTicket(SALE_ID, { ticket: outcome.ticket, final: null });
          setWaitNote('');
          setScreen({
            kind: 'queue',
            ticket: outcome.ticket,
            position: null,
            note: 'Your request is in. Looking for your place in line…',
          });
          startPolling(outcome.ticket);
          return;
        }

        case 'not-started': {
          keyRef.current = null;
          // The server's clock is authoritative. If our copy of the start time
          // is already past, wait a moment instead of letting a click loop.
          const start = sale?.saleStartMs ?? null;
          setCooldownUntil(Math.max(Date.now() + 1_000, start ?? 0));
          setWaitNote('The sale has not started yet. The button unlocks at the start time.');
          setScreen({ kind: 'ready' });
          return;
        }

        case 'already':
          keyRef.current = null;
          if (ticketRef.current !== null) {
            setScreen({
              kind: 'queue',
              ticket: ticketRef.current,
              position: null,
              note: 'You already have an order in this sale. Checking its status…',
            });
            startPolling(ticketRef.current);
          } else {
            setScreen({
              kind: 'already',
              message: 'You already hold or have bought a phone in this sale.',
            });
          }
          return;

        case 'sold-out':
          keyRef.current = null;
          setScreen({ kind: 'sold-out' });
          return;

        case 'unauthorized':
          keyRef.current = null;
          setScreen({
            kind: 'error',
            message: 'Your session expired. Reload to start a new one and try again.',
          });
          return;

        case 'rate-limited':
          setScreen({
            kind: 'retry',
            message: 'The server is rate limiting this sale. Your request is still safe to retry.',
            sameAttempt: true,
          });
          return;

        case 'retry':
          setScreen({ kind: 'retry', message: outcome.message, sameAttempt: true });
          return;

        case 'exhausted':
          setScreen({ kind: 'retry', message: outcome.message, sameAttempt: true });
          return;

        case 'error':
          keyRef.current = null;
          setScreen({ kind: 'failed', message: outcome.message });
          return;
      }
    },
    [sale?.saleStartMs, startPolling],
  );

  const runCheckout = useCallback(
    async (key: string) => {
      try {
        const flow = await loadFlow();
        const outcome = await flow.checkoutWithRetry({
          saleId: SALE_ID,
          idempotencyKey: key,
          getToken,
          onWait: (delayMs, attempt, reason) => {
            if (!mountedRef.current) return;
            setScreen({
              kind: 'reserving',
              note: `${reason} Retrying in ${formatDelay(delayMs)} (attempt ${attempt}).`,
            });
          },
        });
        if (!mountedRef.current) return;
        applyOutcome(outcome);
      } catch {
        if (!mountedRef.current) return;
        setScreen({
          kind: 'retry',
          message: 'The request could not be completed. Your place is still safe to retry.',
          sameAttempt: true,
        });
      } finally {
        busyRef.current = false;
      }
    },
    [applyOutcome, getToken],
  );

  // The synchronous lock happens before the first `await`, so two clicks in
  // the same tick can never produce two checkout requests.
  const beginAttempt = useCallback(
    (event: MouseEvent) => {
      if (busyRef.current) return;
      busyRef.current = true;
      // Lock the DOM node in the same tick, before any state render or await.
      if (event.currentTarget instanceof HTMLButtonElement) {
        event.currentTarget.disabled = true;
      }
      const key = newIdempotencyKey();
      keyRef.current = key;
      setWaitNote('');
      setScreen({ kind: 'reserving', note: '' });
      void runCheckout(key);
    },
    [runCheckout],
  );

  const retrySameAttempt = useCallback(
    (event: MouseEvent) => {
      if (busyRef.current || keyRef.current === null) return;
      busyRef.current = true;
      if (event.currentTarget instanceof HTMLButtonElement) {
        event.currentTarget.disabled = true;
      }
      setScreen({ kind: 'reserving', note: 'Sending your request again…' });
      void runCheckout(keyRef.current);
    },
    [runCheckout],
  );

  useEffect(() => {
    let cancelled = false;

    preloadProductImage();
    // Prefetch the checkout/status chunk while the page is idle, so the sale
    // opening does not wait on a network round trip for code.
    void loadFlow().catch(() => {});

    setSale(null);
    setScreen({ kind: 'loading' });
    setWaitNote('');

    const saved = readTicket(SALE_ID);

    void (async () => {
      const [saleResult, sessionOpened] = await Promise.all([
        fetchSale(SALE_ID),
        getToken().then(
          () => true,
          () => false,
        ),
      ]);
      if (cancelled) return;

      if (!saleResult.ok) {
        setScreen({ kind: 'error', message: saleResult.message });
        return;
      }

      const parsed = parseSale(saleResult.body, SALE_ID);
      if (parsed === null) {
        setScreen({
          kind: 'error',
          message: 'The sale data was not in the expected format.',
        });
        return;
      }

      setSale(parsed);
      if (!sessionOpened) {
        setWaitNote('We could not start a session yet; we will try again when you buy.');
      }

      if (saved !== null) {
        if (saved.final !== null) {
          setScreen(screenForFinalStatus(saved.final));
          return;
        }
        ticketRef.current = saved.ticket;
        setScreen({
          kind: 'queue',
          ticket: saved.ticket,
          position: null,
          note: 'Resuming your place in line…',
        });
        startPolling(saved.ticket);
        return;
      }

      setScreen(parsed.soldOut ? { kind: 'sold-out' } : { kind: 'ready' });
    })();

    return () => {
      cancelled = true;
      stopPolling();
    };
  }, [reloadNonce, getToken, startPolling, stopPolling]);

  // Send the user to the fake provider, but keep a visible link in case the
  // redirect is blocked or the user comes back with the back button.
  const paymentUrl = screen.kind === 'payment' ? screen.paymentUrl : '';
  useEffect(() => {
    if (paymentUrl === '') return;
    const timer = window.setTimeout(() => {
      window.location.assign(paymentUrl);
    }, REDIRECT_DELAY_MS);
    return () => window.clearTimeout(timer);
  }, [paymentUrl]);

  const startMs = sale?.saleStartMs ?? null;
  const secondsToStart = startMs !== null && now < startMs ? secondsLeft(startMs, now) : 0;
  const isOpen = now >= cooldownUntil && (startMs === null || now >= startMs);

  // The "not started yet" note has served its purpose once the button unlocks.
  useEffect(() => {
    if (!isOpen) return;
    setWaitNote((note) =>
      note.startsWith('The sale has not started') ? '' : note,
    );
  }, [isOpen]);
  const busy = screen.kind === 'loading' || screen.kind === 'reserving';
  const liveText = useMemo(
    () => describeScreen(screen, sale, isOpen),
    [screen, sale, isOpen],
  );

  const reload = useCallback(() => setReloadNonce((value) => value + 1), []);

  const renderAction = () => {
    switch (screen.kind) {
      case 'loading':
        return <p class="muted">Loading the sale…</p>;

      case 'error':
        return (
          <div class="block block--bad" role="alert">
            <p>{screen.message}</p>
            <button type="button" class="button button--ghost" onClick={reload}>
              Try again
            </button>
          </div>
        );

      case 'ready':
        return (
          <div class="buy-area">
            <button
              type="button"
              class="button button--buy"
              onClick={beginAttempt}
              disabled={!isOpen}
              aria-busy={busy ? 'true' : 'false'}
            >
              {isOpen
                ? 'Buy now'
                : secondsToStart > 0
                  ? `Buy — opens in ${formatCountdown(secondsToStart)}`
                  : 'Buy — opening…'}
            </button>
            {!isOpen && secondsToStart > 0 && startMs !== null && (
              <p class="note">
                The sale opens at{' '}
                <time dateTime={new Date(startMs).toISOString()}>
                  {new Date(startMs).toLocaleTimeString([], {
                    hour: '2-digit',
                    minute: '2-digit',
                  })}
                </time>
                . You can leave this page open.
              </p>
            )}
            {!isOpen && secondsToStart === 0 && (
              <p class="note">The sale is about to open. The button unlocks in a moment.</p>
            )}
            {isOpen && (
              <p class="note">One phone per person. Your unit is held for five minutes.</p>
            )}
          </div>
        );

      case 'reserving':
        return (
          <div class="block" aria-busy="true">
            <p class="state-title">
              <span class="spinner" aria-hidden="true" /> Reserving your phone…
            </p>
            {screen.note !== '' && <p class="note">{screen.note}</p>}
          </div>
        );

      case 'queue':
        return (
          <div class="block">
            <p class="state-title">
              {screen.position !== null && screen.position > 0
                ? `You're number ${screen.position} in line`
                : "You're in the queue"}
            </p>
            <p class="note">{screen.note || POLL_NOTE}</p>
          </div>
        );

      case 'payment': {
        const paymentSeconds =
          screen.expiresAtMs === null ? null : secondsLeft(screen.expiresAtMs, now);
        return (
          <div class="block">
            <p class="state-title">Your phone is reserved</p>
            {paymentSeconds !== null && paymentSeconds > 0 ? (
              <p class="note">
                Complete payment within <strong>{formatCountdown(paymentSeconds)}</strong>.
              </p>
            ) : (
              <p class="note">The payment window has closed; waiting for the final status.</p>
            )}
            {screen.paymentUrl !== '' ? (
              <>
                <a class="button button--buy" href={screen.paymentUrl}>
                  Continue to payment
                </a>
                <p class="note">Taking you to payment… If nothing happens, use the link above.</p>
              </>
            ) : (
              <p class="note">Preparing your payment link…</p>
            )}
          </div>
        );
      }

      case 'confirmed':
        return (
          <div class="block block--good">
            <p class="state-title">Order confirmed</p>
            <p class="note">Your phone is on its way. Keep this tab for your receipt.</p>
          </div>
        );

      case 'sold-out':
        return (
          <div class="block block--bad">
            <p class="state-title">Sold out</p>
            <p class="note">Every phone in this drop has gone. Better luck next time.</p>
          </div>
        );

      case 'expired':
        return (
          <div class="block block--bad">
            <p class="state-title">Reservation expired</p>
            <p class="note">
              The five-minute payment window closed, so the phone went back on sale.
            </p>
          </div>
        );

      case 'refunded':
        return (
          <div class="block block--bad">
            <p class="state-title">Payment refunded</p>
            <p class="note">
              Your payment arrived after the reservation expired, so it has been refunded.
            </p>
          </div>
        );

      case 'failed':
        return (
          <div class="block block--bad" role="alert">
            <p class="state-title">Something went wrong</p>
            <p class="note">{screen.message}</p>
            <button type="button" class="button button--ghost" onClick={beginAttempt}>
              Start again
            </button>
          </div>
        );

      case 'already':
        return (
          <div class="block">
            <p class="state-title">You already have one</p>
            <p class="note">{screen.message}</p>
          </div>
        );

      case 'retry':
        return (
          <div class="block block--bad" role="alert">
            <p class="state-title">Still trying</p>
            <p class="note">{screen.message}</p>
            <button type="button" class="button button--ghost" onClick={retrySameAttempt}>
              Try again
            </button>
            <p class="note">
              Retrying reuses the same request, so you will never be charged twice.
            </p>
          </div>
        );
    }
  };

  return (
    <main class="page">
      <header class="masthead">
        <p class="brand">
          <span class="dot" aria-hidden="true" /> Flash Sale
        </p>
        <p class="meta">{sale ? `${sale.totalInventory} units · one per person` : 'Limited drop'}</p>
      </header>

      <section class="card" aria-busy={busy ? 'true' : 'false'}>
        <div class="media">
          <img
            src="/product.svg"
            alt=""
            width="640"
            height="640"
            fetchpriority="high"
            decoding="async"
          />
        </div>

        <div class="info">
          <p class="eyebrow">{sale ? 'Limited drop' : 'Loading'}</p>
          <h1>{sale?.name ?? 'Flash sale'}</h1>
          {sale !== null && (
            <>
              <p class="price">{formatMoney(sale.priceCents)}</p>
              <p class="lede">
                One phone per person. Reserve it here, then pay within five minutes.
              </p>
            </>
          )}

          <div class="action">{renderAction()}</div>
          {waitNote !== '' && <p class="note note--wait">{waitNote}</p>}
        </div>
      </section>

      <footer class="footer">
        <p>Your place is kept in this browser only. Closing it does not cancel an order.</p>
      </footer>

      <p class="visually-hidden" role="status" aria-live="polite">
        {liveText}
      </p>
    </main>
  );
}
