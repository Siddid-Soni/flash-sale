//! The pure part of the UI: API responses in, screen state out. Nothing here
//! touches `fetch`, timers or the DOM, so every branch is unit tested and the
//! module stays cheap to ship.

import { asBool, asNumber, asRecord, asString, errorMessage } from './json';
import { parseTimestampMs } from './format';
import type { FinalStatusName } from './storage';

export type Sale = {
  id: string;
  name: string;
  priceCents: number;
  saleStartMs: number | null;
  totalInventory: number;
  soldOut: boolean;
};

/** What the page shows. `ready` derives "before the sale" from the clock. */
export type Screen =
  | { kind: 'loading' }
  | { kind: 'error'; message: string }
  | { kind: 'ready' }
  | { kind: 'reserving'; note: string }
  | { kind: 'queue'; ticket: string; position: number | null; note: string }
  | { kind: 'payment'; ticket: string; paymentUrl: string; expiresAtMs: number | null }
  | { kind: 'confirmed' }
  | { kind: 'sold-out' }
  | { kind: 'expired' }
  | { kind: 'failed'; message: string }
  | { kind: 'refunded' }
  | { kind: 'already'; message: string }
  /** Transient failure after retries ran out; the same attempt can resume. */
  | { kind: 'retry'; message: string; sameAttempt: boolean };

/**
 * The result of one checkout attempt. `exhausted` means the attempt is still
 * open: the caller may retry with the *same* idempotency key.
 */
export type CheckoutOutcome =
  | { type: 'ticketed'; ticket: string; statusUrl: string | null }
  | { type: 'not-started' }
  | { type: 'already' }
  | { type: 'sold-out' }
  | { type: 'rate-limited'; retryAfterMs: number }
  | { type: 'retry'; retryAfterMs: number | null; message: string }
  | { type: 'unauthorized' }
  | { type: 'exhausted'; message: string; sameAttempt: true }
  | { type: 'error'; message: string };

/** A polled status, narrowed to what the page renders. */
export type StatusUpdate =
  | { kind: 'pending'; position: number | null }
  | { kind: 'awaiting-payment'; paymentUrl: string | null; expiresAtMs: number | null }
  | { kind: 'confirmed' }
  | { kind: 'expired' }
  | { kind: 'failed'; message: string }
  | { kind: 'refunded' }
  /** Transient (5xx, network, unknown body): keep polling. */
  | { kind: 'retry'; message: string };

export function isFinalStatus(update: StatusUpdate): boolean {
  return (
    update.kind === 'confirmed' ||
    update.kind === 'expired' ||
    update.kind === 'failed' ||
    update.kind === 'refunded'
  );
}

/**
 * How long a user who went to pay keeps polling after seeing `EXPIRED`. A
 * payment made at the deadline is refunded only once the order has expired,
 * so `REFUNDED` can follow `EXPIRED` a few seconds later.
 */
export const REFUND_GRACE_MS = 60_000;

/**
 * Whether polling may stop on this update. `EXPIRED` is final at once for a
 * user who never went to pay; one who did keeps polling for `REFUND_GRACE_MS`
 * after it was first seen (`expiredSinceMs`).
 */
export function stopsPolling(
  update: StatusUpdate,
  wentToPay: boolean,
  expiredSinceMs: number | null,
  nowMs: number,
): boolean {
  if (!isFinalStatus(update)) return false;
  if (update.kind !== 'expired' || !wentToPay) return true;
  return expiredSinceMs !== null && nowMs - expiredSinceMs >= REFUND_GRACE_MS;
}

export function finalStatusName(update: StatusUpdate): FinalStatusName | null {
  switch (update.kind) {
    case 'confirmed':
      return 'CONFIRMED';
    case 'expired':
      return 'EXPIRED';
    case 'failed':
      return 'FAILED';
    case 'refunded':
      return 'REFUNDED';
    default:
      return null;
  }
}

/** Parses `GET /v1/sales/{id}`. Returns `null` when the body is unusable. */
export function parseSale(body: unknown, id: string): Sale | null {
  const record = asRecord(body);
  if (record === null) return null;

  const name = asString(record.name);
  if (name === null) return null;

  return {
    id,
    name,
    priceCents: asNumber(record.price_cents) ?? 0,
    saleStartMs: parseTimestampMs(record.sale_start_time),
    totalInventory: asNumber(record.total_inventory) ?? 0,
    soldOut: asBool(record.sold_out),
  };
}

/** Parses `GET /v1/sales`, skipping entries without an id or a name. */
export function parseSales(body: unknown): Sale[] | null {
  if (!Array.isArray(body)) return null;
  const sales: Sale[] = [];
  for (const item of body) {
    const id = asNumber(asRecord(item)?.id);
    const sale = id === null ? null : parseSale(item, String(id));
    if (sale !== null) sales.push(sale);
  }
  return sales;
}

/**
 * Maps a checkout response to an outcome. `retryAfterMs` is the already-parsed
 * `Retry-After` header, or `null` when the server sent none.
 */
export function mapCheckoutResponse(
  status: number,
  body: unknown,
  retryAfterMs: number | null,
): CheckoutOutcome {
  if (status === 200 || status === 202) {
    const record = asRecord(body);
    const ticket = asString(record?.queue_ticket_id);
    if (ticket === null) {
      return { type: 'error', message: 'The server sent an incomplete checkout response.' };
    }
    return { type: 'ticketed', ticket, statusUrl: asString(record?.status_url) };
  }

  if (status === 403) return { type: 'not-started' };
  if (status === 409) return { type: 'already' };
  if (status === 410) return { type: 'sold-out' };
  if (status === 401) return { type: 'unauthorized' };
  if (status === 429) {
    return { type: 'rate-limited', retryAfterMs: retryAfterMs ?? 1_000 };
  }
  if (status === 503) {
    return { type: 'retry', retryAfterMs, message: 'The queue is busy.' };
  }
  if (status >= 500) {
    return { type: 'retry', retryAfterMs: null, message: 'The server had a problem.' };
  }

  return {
    type: 'error',
    message: errorMessage(body, `Unexpected response (${status}).`),
  };
}

/** Maps `GET /v1/orders/status/{ticket}`. Unknown statuses keep polling. */
export function mapStatusResponse(status: number, body: unknown): StatusUpdate {
  if (status === 401) {
    return { kind: 'retry', message: 'Your session expired; signing in again.' };
  }
  if (status === 403) {
    return { kind: 'failed', message: 'This order belongs to a different session.' };
  }
  if (status === 404) {
    return { kind: 'failed', message: 'We could not find this order.' };
  }
  if (status >= 500) {
    return { kind: 'retry', message: 'The server is busy; checking again.' };
  }
  if (status !== 200) {
    return { kind: 'retry', message: `Unexpected status (${status}).` };
  }

  const record = asRecord(body);
  const name = asString(record?.status)?.toUpperCase();
  if (record === null || name === undefined) {
    return { kind: 'retry', message: 'The status response was incomplete.' };
  }

  switch (name) {
    case 'PENDING':
      return { kind: 'pending', position: asNumber(record.position) };
    case 'AWAITING_PAYMENT':
      return {
        kind: 'awaiting-payment',
        paymentUrl: asString(record.payment_url),
        expiresAtMs: parseTimestampMs(record.expires_at),
      };
    case 'CONFIRMED':
      return { kind: 'confirmed' };
    case 'EXPIRED':
      return { kind: 'expired' };
    case 'FAILED':
      return { kind: 'failed', message: 'Your reservation could not be completed.' };
    case 'REFUNDED':
      return { kind: 'refunded' };
    default:
      return { kind: 'retry', message: `Unknown order status (${name}).` };
  }
}

/** The screen for a polled update; `null` means "no change, keep polling". */
export function statusToScreen(update: StatusUpdate, ticket: string): Screen | null {
  switch (update.kind) {
    case 'pending':
      return { kind: 'queue', ticket, position: update.position, note: 'You are in line.' };
    case 'awaiting-payment':
      return {
        kind: 'payment',
        ticket,
        paymentUrl: update.paymentUrl ?? '',
        expiresAtMs: update.expiresAtMs,
      };
    case 'confirmed':
      return { kind: 'confirmed' };
    case 'expired':
      return { kind: 'expired' };
    case 'failed':
      return { kind: 'failed', message: update.message };
    case 'refunded':
      return { kind: 'refunded' };
    case 'retry':
      return null;
  }
}

/** Restores a finished purchase from `sessionStorage` after a reload. */
export function screenForFinalStatus(name: FinalStatusName): Screen {
  switch (name) {
    case 'CONFIRMED':
      return { kind: 'confirmed' };
    case 'EXPIRED':
      return { kind: 'expired' };
    case 'FAILED':
      return { kind: 'failed', message: 'Your reservation could not be completed.' };
    case 'REFUNDED':
      return { kind: 'refunded' };
  }
}
