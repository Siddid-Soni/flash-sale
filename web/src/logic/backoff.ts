//! Retry timing for checkout. `503` and network failures use exponential
//! backoff with *full jitter* (a uniform delay in `[0, capped window]`), which
//! spreads a crowd of clients that failed together instead of retrying in
//! lockstep. `429` instead honours the server's `Retry-After` header.

export const BACKOFF_BASE_MS = 250;
export const BACKOFF_CAP_MS = 8_000;
export const DEFAULT_RETRY_AFTER_MS = 1_000;

/**
 * Delay before retry number `attempt` (zero-based).
 *
 * The window doubles per attempt up to `capMs`; the returned delay is uniform
 * in `[0, window]`, so identical clients do not stampede. `random` is injected
 * so the behaviour can be tested deterministically.
 */
export function backoffDelayMs(
  attempt: number,
  random: () => number = Math.random,
  baseMs: number = BACKOFF_BASE_MS,
  capMs: number = BACKOFF_CAP_MS,
): number {
  const safeAttempt = Math.max(0, Math.floor(attempt));
  const window = Math.min(capMs, baseMs * 2 ** safeAttempt);
  const sample = Math.min(0.999999, Math.max(0, random()));
  return Math.floor(sample * window);
}

/**
 * Parses a `Retry-After` header into milliseconds. Both forms from RFC 9110
 * are accepted: delta-seconds (`"2"`, `"1.5"`) and an HTTP date. Returns
 * `null` when the header is missing or unparseable, and never a negative
 * delay.
 */
export function parseRetryAfterMs(
  value: string | null | undefined,
  nowMs: number = Date.now(),
): number | null {
  if (value === null || value === undefined) return null;
  const text = value.trim();
  if (text === '') return null;

  if (/^\d+(\.\d+)?$/.test(text)) {
    return Math.max(0, Math.ceil(Number(text) * 1000));
  }

  const date = Date.parse(text);
  if (Number.isNaN(date)) return null;
  return Math.max(0, date - nowMs);
}
