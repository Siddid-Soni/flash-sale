//! Checkout and status client, loaded as its own chunk. The page imports this
//! module during the countdown (`import('./flow')`) so the code is already in
//! memory when the buy button unlocks; nothing here is needed for first paint.

import { backoffDelayMs, parseRetryAfterMs } from './logic/backoff';
import { readJsonResponse } from './logic/json';
import { startStatusPolling as startPollingCore, POLL_INTERVAL_MS } from './logic/poll';
import {
  mapCheckoutResponse,
  mapStatusResponse,
  type CheckoutOutcome,
  type StatusUpdate,
} from './logic/state';

/** Retries for `503`, `429` and network failures before handing control back. */
export const MAX_TRANSIENT_ATTEMPTS = 6;
/** Re-mints before giving up on an expired token. */
const MAX_AUTH_RETRIES = 2;

export type TokenGetter = (options?: { force?: boolean }) => Promise<string>;

export type CheckoutOptions = {
  saleId: string;
  /** One key per purchase attempt, reused for every retry of that attempt. */
  idempotencyKey: string;
  getToken: TokenGetter;
  /** Called before each retry wait, for a visible "retrying in ..." note. */
  onWait?: (delayMs: number, attempt: number, reason: string) => void;
  signal?: AbortSignal;
  maxTransientAttempts?: number;
  /** Injected by tests; defaults to a real timer. */
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
};

function isAbort(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'name' in error &&
    (error as { name?: unknown }).name === 'AbortError';
}

function wait(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason);
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

/**
 * `POST /v1/sales/{id}/checkout`, retried until the answer is terminal or the
 * transient budget runs out. The idempotency key never changes, so a retried
 * request can only ever return the ticket created by the first one.
 */
export async function checkoutWithRetry(
  options: CheckoutOptions,
): Promise<CheckoutOutcome> {
  const maxTransient = options.maxTransientAttempts ?? MAX_TRANSIENT_ATTEMPTS;
  const sleep = options.sleep ?? wait;

  let token = await options.getToken();
  let transient = 0;
  let authRetries = 0;
  const url = `/v1/sales/${encodeURIComponent(options.saleId)}/checkout`;

  for (;;) {
    let outcome: CheckoutOutcome;

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          Accept: 'application/json',
          Authorization: `Bearer ${token}`,
          'Idempotency-Key': options.idempotencyKey,
        },
        credentials: 'same-origin',
        cache: 'no-store',
        signal: options.signal,
      });

      const retryAfterMs = parseRetryAfterMs(response.headers.get('Retry-After'));
      const body = await readJsonResponse(response);
      outcome = mapCheckoutResponse(response.status, body, retryAfterMs);
    } catch (error) {
      if (isAbort(error)) throw error;
      outcome = { type: 'retry', retryAfterMs: null, message: 'Network unavailable.' };
    }

    if (outcome.type === 'unauthorized') {
      if (authRetries >= MAX_AUTH_RETRIES) return outcome;
      authRetries += 1;
      token = await options.getToken({ force: true });
      continue;
    }

    if (
      outcome.type !== 'retry' &&
      outcome.type !== 'rate-limited' &&
      outcome.type !== 'exhausted'
    ) {
      return outcome;
    }

    if (outcome.type === 'exhausted') return outcome;

    if (transient >= maxTransient) {
      return {
        type: 'exhausted',
        message:
          outcome.type === 'rate-limited'
            ? 'The server is rate limiting this sale. Your request is still safe to retry.'
            : 'The server is very busy. Your request is still safe to retry.',
        sameAttempt: true,
      };
    }

    const delay =
      outcome.type === 'rate-limited'
        ? outcome.retryAfterMs
        : (outcome.retryAfterMs ?? backoffDelayMs(transient));
    const reason =
      outcome.type === 'rate-limited' ? 'The server is rate limiting requests.' : outcome.message;

    options.onWait?.(delay, transient + 1, reason);
    await sleep(delay, options.signal);
    transient += 1;
  }
}

export type StatusPollingOptions = {
  ticket: string;
  getToken: TokenGetter;
  /** Returning a boolean decides whether polling stops (see `logic/poll`). */
  onUpdate: (update: StatusUpdate) => boolean | void;
  onError?: (error: unknown) => void;
  intervalMs?: number;
};

/**
 * Polls every 2 s until the order is final. The first poll runs at once, so a
 * page reload (or a return from the fake payment page) resolves immediately.
 */
export function startStatusPolling(options: StatusPollingOptions): () => void {
  return startPollingCore({
    intervalMs: options.intervalMs ?? POLL_INTERVAL_MS,
    onError: options.onError,
    onUpdate: options.onUpdate,
    fetchOnce: async () => {
      const token = await options.getToken();
      const response = await fetch(
        `/v1/orders/status/${encodeURIComponent(options.ticket)}`,
        {
          headers: { Accept: 'application/json', Authorization: `Bearer ${token}` },
          credentials: 'same-origin',
          cache: 'no-store',
        },
      );

      // A stale token is re-minted and the next poll picks it up. Anything else
      // is mapped, so a 404/403 stops polling and a 5xx keeps it going.
      if (response.status === 401) {
        await options.getToken({ force: true });
        return { kind: 'retry', message: 'Refreshing your session.' };
      }

      return mapStatusResponse(response.status, await readJsonResponse(response));
    },
  });
}
