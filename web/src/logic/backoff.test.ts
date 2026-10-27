import { describe, expect, it } from 'vitest';
import { backoffDelayMs, parseRetryAfterMs } from './backoff';

describe('backoffDelayMs', () => {
  it('is zero when the jitter sample is zero', () => {
    for (const attempt of [0, 1, 5, 20]) {
      expect(backoffDelayMs(attempt, () => 0)).toBe(0);
    }
  });

  it('stays inside the doubling window', () => {
    // attempt 0: 250 ms, attempt 3: 2000 ms, capped at 8000 ms.
    expect(backoffDelayMs(0, () => 0.999999)).toBe(249);
    expect(backoffDelayMs(3, () => 0.999999)).toBe(1999);
    expect(backoffDelayMs(4, () => 0.999999)).toBe(3999);
    expect(backoffDelayMs(20, () => 0.999999)).toBe(7999);
  });

  it('uses full jitter, not a fixed curve', () => {
    const short = backoffDelayMs(6, () => 0.1);
    const long = backoffDelayMs(6, () => 0.9);
    expect(short).toBeLessThan(long);
    expect(long).toBeLessThanOrEqual(8_000);
  });

  it('clamps odd inputs rather than producing a negative or huge delay', () => {
    expect(backoffDelayMs(-3, () => 0.5)).toBe(125);
    expect(backoffDelayMs(2.8, () => 0.5)).toBe(500);
    expect(backoffDelayMs(3, () => 2)).toBe(1_999);
    expect(backoffDelayMs(3, () => -1)).toBe(0);
  });

  it('honours custom base and cap', () => {
    expect(backoffDelayMs(0, () => 1, 100, 1_000)).toBe(99);
    expect(backoffDelayMs(9, () => 1, 100, 1_000)).toBe(999);
  });
});

describe('parseRetryAfterMs', () => {
  const now = Date.parse('2026-10-28T20:00:00Z');

  it('parses delta-seconds', () => {
    expect(parseRetryAfterMs('2', now)).toBe(2_000);
    expect(parseRetryAfterMs('0', now)).toBe(0);
    expect(parseRetryAfterMs(' 3 ', now)).toBe(3_000);
  });

  it('parses fractional seconds defensively', () => {
    expect(parseRetryAfterMs('1.5', now)).toBe(1_500);
  });

  it('parses an HTTP date', () => {
    expect(parseRetryAfterMs('Wed, 28 Oct 2026 20:00:07 GMT', now)).toBe(7_000);
  });

  it('never returns a negative delay', () => {
    expect(parseRetryAfterMs('Wed, 28 Oct 2026 19:59:00 GMT', now)).toBe(0);
  });

  it('returns null for missing or malformed values', () => {
    expect(parseRetryAfterMs(null, now)).toBeNull();
    expect(parseRetryAfterMs(undefined, now)).toBeNull();
    expect(parseRetryAfterMs('', now)).toBeNull();
    expect(parseRetryAfterMs('soon', now)).toBeNull();
  });
});
