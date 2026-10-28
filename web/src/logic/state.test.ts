import { describe, expect, it } from 'vitest';
import {
  finalStatusName,
  isFinalStatus,
  mapCheckoutResponse,
  mapStatusResponse,
  parseSale,
  parseSales,
  REFUND_GRACE_MS,
  stopsPolling,
  type StatusUpdate,
  screenForFinalStatus,
  statusToScreen,
} from './state';

describe('mapCheckoutResponse', () => {
  it('maps 202 and 200 to a ticket', () => {
    const body = { queue_ticket_id: '42.abc', status_url: '/v1/orders/status/42.abc' };
    expect(mapCheckoutResponse(202, body, null)).toEqual({
      type: 'ticketed',
      ticket: '42.abc',
      statusUrl: '/v1/orders/status/42.abc',
    });
    expect(mapCheckoutResponse(200, body, null)).toEqual({
      type: 'ticketed',
      ticket: '42.abc',
      statusUrl: '/v1/orders/status/42.abc',
    });
  });

  it('treats a 2xx without a ticket as an error', () => {
    expect(mapCheckoutResponse(202, {}, null)).toEqual({
      type: 'error',
      message: 'The server sent an incomplete checkout response.',
    });
    expect(mapCheckoutResponse(202, null, null).type).toBe('error');
  });

  it('maps the documented refusals', () => {
    expect(mapCheckoutResponse(403, null, null)).toEqual({ type: 'not-started' });
    expect(mapCheckoutResponse(409, null, null)).toEqual({ type: 'already' });
    expect(mapCheckoutResponse(410, null, null)).toEqual({ type: 'sold-out' });
    expect(mapCheckoutResponse(401, null, null)).toEqual({ type: 'unauthorized' });
  });

  it('maps 429 to a rate limit, defaulting to one second', () => {
    expect(mapCheckoutResponse(429, null, 4_000)).toEqual({
      type: 'rate-limited',
      retryAfterMs: 4_000,
    });
    expect(mapCheckoutResponse(429, null, null)).toEqual({
      type: 'rate-limited',
      retryAfterMs: 1_000,
    });
  });

  it('maps 503 and other 5xx to a retry', () => {
    expect(mapCheckoutResponse(503, null, 2_000)).toEqual({
      type: 'retry',
      retryAfterMs: 2_000,
      message: 'The queue is busy.',
    });
    expect(mapCheckoutResponse(500, null, null)).toEqual({
      type: 'retry',
      retryAfterMs: null,
      message: 'The server had a problem.',
    });
  });

  it('maps other 4xx to a terminal error with the server message', () => {
    expect(mapCheckoutResponse(400, { message: 'bad key' }, null)).toEqual({
      type: 'error',
      message: 'bad key',
    });
    expect(mapCheckoutResponse(418, null, null)).toEqual({
      type: 'error',
      message: 'Unexpected response (418).',
    });
  });
});

describe('mapStatusResponse', () => {
  it('maps PENDING with a queue position', () => {
    expect(mapStatusResponse(200, { status: 'PENDING', position: 7 })).toEqual({
      kind: 'pending',
      position: 7,
    });
    expect(mapStatusResponse(200, { status: 'pending' })).toEqual({
      kind: 'pending',
      position: null,
    });
  });

  it('maps AWAITING_PAYMENT with the payment link and expiry', () => {
    expect(
      mapStatusResponse(200, {
        status: 'AWAITING_PAYMENT',
        payment_url: '/fake-pay/sess_1',
        expires_at: '2026-10-28T20:05:00Z',
      }),
    ).toEqual({
      kind: 'awaiting-payment',
      paymentUrl: '/fake-pay/sess_1',
      expiresAtMs: Date.parse('2026-10-28T20:05:00Z'),
    });
  });

  it('maps every final status', () => {
    expect(mapStatusResponse(200, { status: 'CONFIRMED' })).toEqual({ kind: 'confirmed' });
    expect(mapStatusResponse(200, { status: 'EXPIRED' })).toEqual({ kind: 'expired' });
    expect(mapStatusResponse(200, { status: 'REFUNDED' })).toEqual({ kind: 'refunded' });
    expect(mapStatusResponse(200, { status: 'FAILED' })).toEqual({
      kind: 'failed',
      message: 'Your reservation could not be completed.',
    });
  });

  it('stops polling on 403 and 404', () => {
    expect(isFinalStatus(mapStatusResponse(403, null))).toBe(true);
    expect(isFinalStatus(mapStatusResponse(404, null))).toBe(true);
  });

  it('keeps polling on 5xx, unknown statuses and malformed bodies', () => {
    for (const update of [
      mapStatusResponse(500, null),
      mapStatusResponse(200, { status: 'SOMETHING_NEW' }),
      mapStatusResponse(200, {}),
      mapStatusResponse(200, null),
      mapStatusResponse(302, null),
    ]) {
      expect(update.kind).toBe('retry');
      expect(isFinalStatus(update)).toBe(false);
    }
  });
});

describe('statusToScreen', () => {
  it('maps a queue update and keeps the ticket', () => {
    expect(statusToScreen({ kind: 'pending', position: 3 }, '42.a')).toEqual({
      kind: 'queue',
      ticket: '42.a',
      position: 3,
      note: 'You are in line.',
    });
  });

  it('maps a payment update and tolerates a missing URL', () => {
    expect(
      statusToScreen(
        { kind: 'awaiting-payment', paymentUrl: null, expiresAtMs: null },
        '42.a',
      ),
    ).toEqual({ kind: 'payment', ticket: '42.a', paymentUrl: '', expiresAtMs: null });
  });

  it('returns null for transient updates so the current screen stays', () => {
    expect(statusToScreen({ kind: 'retry', message: 'try later' }, '42.a')).toBeNull();
  });

  it('maps terminal updates', () => {
    expect(statusToScreen({ kind: 'confirmed' }, '42.a')).toEqual({ kind: 'confirmed' });
    expect(statusToScreen({ kind: 'expired' }, '42.a')).toEqual({ kind: 'expired' });
    expect(statusToScreen({ kind: 'refunded' }, '42.a')).toEqual({ kind: 'refunded' });
  });
});

describe('final states', () => {
  it('recognises final statuses and names them for storage', () => {
    expect(isFinalStatus({ kind: 'confirmed' })).toBe(true);
    expect(isFinalStatus({ kind: 'retry', message: 'x' })).toBe(false);

    expect(finalStatusName({ kind: 'confirmed' })).toBe('CONFIRMED');
    expect(finalStatusName({ kind: 'expired' })).toBe('EXPIRED');
    expect(finalStatusName({ kind: 'failed', message: 'x' })).toBe('FAILED');
    expect(finalStatusName({ kind: 'refunded' })).toBe('REFUNDED');
    expect(finalStatusName({ kind: 'pending', position: 1 })).toBeNull();
  });

  it('restores a screen from a stored final status', () => {
    expect(screenForFinalStatus('CONFIRMED')).toEqual({ kind: 'confirmed' });
    expect(screenForFinalStatus('EXPIRED')).toEqual({ kind: 'expired' });
    expect(screenForFinalStatus('REFUNDED')).toEqual({ kind: 'refunded' });
    expect(screenForFinalStatus('FAILED').kind).toBe('failed');
  });
});

describe('parseSale', () => {
  it('reads the roadmap contract', () => {
    expect(
      parseSale(
        {
          name: 'Aurora X1',
          price_cents: 129_900,
          sale_start_time: '2026-10-28T20:00:00Z',
          total_inventory: 500,
          sold_out: false,
        },
        '42',
      ),
    ).toEqual({
      id: '42',
      name: 'Aurora X1',
      priceCents: 129_900,
      saleStartMs: Date.parse('2026-10-28T20:00:00Z'),
      totalInventory: 500,
      soldOut: false,
    });
  });

  it('accepts epoch seconds for the start time', () => {
    const sale = parseSale(
      { name: 'X', sale_start_time: 1_893_456_000, total_inventory: 1, sold_out: true },
      '1',
    );
    expect(sale?.saleStartMs).toBe(1_893_456_000_000);
    expect(sale?.soldOut).toBe(true);
  });

  it('rejects bodies without a name', () => {
    expect(parseSale(null, '1')).toBeNull();
    expect(parseSale({}, '1')).toBeNull();
  });
});

describe('parseSales', () => {
  it('reads each sale with its id, skipping unusable entries', () => {
    const sales = parseSales([
      { id: 7, name: 'Aurora X1', price_cents: 100, sale_start_time: '2026-10-28T20:00:00Z', total_inventory: 5, sold_out: true },
      { name: 'no id' },
      { id: 8 },
      { id: 9, name: 'Beta', price_cents: 200, sale_start_time: '2026-10-29T20:00:00Z', total_inventory: 3, sold_out: false },
    ]);
    expect(sales?.map((s) => [s.id, s.name, s.soldOut])).toEqual([
      ['7', 'Aurora X1', true],
      ['9', 'Beta', false],
    ]);
  });

  it('rejects a body that is not a list', () => {
    expect(parseSales({ sales: [] })).toBeNull();
    expect(parseSales([])).toEqual([]);
  });
});

describe('stopsPolling', () => {
  const expired: StatusUpdate = { kind: 'expired' };

  it('stops on a final state, and on EXPIRED at once for a user who never went to pay', () => {
    expect(stopsPolling({ kind: 'confirmed' }, true, null, 0)).toBe(true);
    expect(stopsPolling({ kind: 'refunded' }, true, null, 0)).toBe(true);
    expect(stopsPolling(expired, false, 0, 0)).toBe(true);
    expect(stopsPolling({ kind: 'pending', position: 3 }, true, null, 0)).toBe(false);
  });

  it('keeps a payer polling on EXPIRED for the refund grace period', () => {
    expect(stopsPolling(expired, true, 1_000, 1_000)).toBe(false);
    expect(stopsPolling(expired, true, 1_000, 1_000 + REFUND_GRACE_MS - 1)).toBe(false);
    expect(stopsPolling(expired, true, 1_000, 1_000 + REFUND_GRACE_MS)).toBe(true);
  });
});
