import { afterEach, describe, expect, it, vi } from 'vitest';
import { checkoutWithRetry } from './flow';

function jsonResponse(
  status: number,
  body: unknown,
  headers: Record<string, string> = {},
): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json', ...headers },
  });
}

const originalFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = originalFetch;
  vi.restoreAllMocks();
});

describe('checkoutWithRetry', () => {
  it('reuses one idempotency key across a 503 and honours Retry-After', async () => {
    const calls: RequestInit[] = [];
    globalThis.fetch = vi.fn(async (_url: string | URL | Request, init?: RequestInit) => {
      calls.push(init ?? {});
      return calls.length === 1
        ? jsonResponse(503, { message: 'busy' }, { 'Retry-After': '2' })
        : jsonResponse(202, { queue_ticket_id: '42.a', status_url: '/v1/orders/status/42.a' });
    }) as unknown as typeof fetch;

    const waits: number[] = [];
    const outcome = await checkoutWithRetry({
      saleId: '42',
      idempotencyKey: 'key-1',
      getToken: async () => 'jwt',
      sleep: async (ms) => {
        waits.push(ms);
      },
    });

    expect(outcome).toEqual({
      type: 'ticketed',
      ticket: '42.a',
      statusUrl: '/v1/orders/status/42.a',
    });
    expect(calls).toHaveLength(2);
    expect(new Headers(calls[0]?.headers).get('Idempotency-Key')).toBe('key-1');
    expect(new Headers(calls[1]?.headers).get('Idempotency-Key')).toBe('key-1');
    expect(waits).toEqual([2_000]);
  });

  it('waits the Retry-After on 429, defaulting to one second', async () => {
    let call = 0;
    globalThis.fetch = vi.fn(async () => {
      call += 1;
      return call === 1
        ? jsonResponse(429, null, { 'Retry-After': '3' })
        : jsonResponse(202, { queue_ticket_id: '42.b' });
    }) as unknown as typeof fetch;

    const waits: number[] = [];
    const outcome = await checkoutWithRetry({
      saleId: '42',
      idempotencyKey: 'key-2',
      getToken: async () => 'jwt',
      sleep: async (ms) => {
        waits.push(ms);
      },
    });

    expect(outcome.type).toBe('ticketed');
    expect(waits).toEqual([3_000]);
  });

  it('re-mints the token once on 401 and retries with the same key', async () => {
    let call = 0;
    globalThis.fetch = vi.fn(async () => {
      call += 1;
      return call === 1 ? jsonResponse(401, { message: 'expired' }) : jsonResponse(200, { queue_ticket_id: '42.c' });
    }) as unknown as typeof fetch;

    const getToken = vi.fn(async (options?: { force?: boolean }) =>
      options?.force === true ? 'fresh' : 'stale',
    );
    const outcome = await checkoutWithRetry({
      saleId: '42',
      idempotencyKey: 'key-3',
      getToken,
      sleep: async () => {},
    });

    expect(outcome.type).toBe('ticketed');
    expect(getToken).toHaveBeenCalledWith({ force: true });
    expect(call).toBe(2);
  });

  it('gives up as an exhausted (still retryable) attempt after the budget', async () => {
    globalThis.fetch = vi.fn(async () => jsonResponse(500, { message: 'boom' })) as unknown as typeof fetch;

    const waits: number[] = [];
    const outcome = await checkoutWithRetry({
      saleId: '42',
      idempotencyKey: 'key-4',
      getToken: async () => 'jwt',
      maxTransientAttempts: 1,
      sleep: async (ms) => {
        waits.push(ms);
      },
    });

    expect(outcome.type).toBe('exhausted');
    expect(outcome.type === 'exhausted' && outcome.sameAttempt).toBe(true);
    expect(waits).toHaveLength(1);
  });

  it('does not retry terminal refusals', async () => {
    globalThis.fetch = vi.fn(async () => jsonResponse(410, null)) as unknown as typeof fetch;

    const outcome = await checkoutWithRetry({
      saleId: '42',
      idempotencyKey: 'key-5',
      getToken: async () => 'jwt',
      sleep: async () => {
        throw new Error('must not wait');
      },
    });

    expect(outcome).toEqual({ type: 'sold-out' });
    expect(globalThis.fetch).toHaveBeenCalledTimes(1);
  });
});
