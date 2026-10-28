//! Time and money formatting shared by the page. All timestamps are converted
//! to epoch milliseconds here so the rest of the code never guesses units.

/**
 * Parses an API timestamp into epoch milliseconds. Accepts ISO-8601 strings,
 * epoch seconds and epoch milliseconds, in numbers or numeric strings. A
 * seconds value is recognised by magnitude (anything below 10^12 is seconds).
 */
export function parseTimestampMs(value: unknown): number | null {
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return null;
    return value < 1e12 ? Math.round(value * 1000) : Math.round(value);
  }

  if (typeof value === 'string') {
    const text = value.trim();
    if (text === '') return null;

    const numeric = Number(text);
    if (Number.isFinite(numeric)) {
      return numeric < 1e12 ? Math.round(numeric * 1000) : Math.round(numeric);
    }

    const parsed = Date.parse(text);
    return Number.isNaN(parsed) ? null : parsed;
  }

  return null;
}

/** Whole seconds left, never negative. */
export function secondsLeft(targetMs: number | null, nowMs: number): number {
  if (targetMs === null) return 0;
  return Math.max(0, Math.ceil((targetMs - nowMs) / 1000));
}

/**
 * A compact countdown: `1h 03m 07s`, `3m 07s`, `7s`. Used in the sale
 * countdown, the button label and the payment-window timer.
 */
export function formatCountdown(totalSeconds: number): string {
  const seconds = Math.max(0, Math.floor(totalSeconds));
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const rest = seconds % 60;
  const pad = (n: number) => String(n).padStart(2, '0');

  if (hours > 0) return `${hours}h ${pad(minutes)}m ${pad(rest)}s`;
  if (minutes > 0) return `${minutes}m ${pad(rest)}s`;
  return `${rest}s`;
}

/** Giant wall-clock countdown: `01:03:07`, `59:56`, `00:07`. */
export function formatClock(totalSeconds: number): string {
  const seconds = Math.max(0, Math.floor(totalSeconds));
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const rest = seconds % 60;
  const pad = (n: number) => String(n).padStart(2, '0');

  if (hours > 0) return `${pad(hours)}:${pad(minutes)}:${pad(rest)}`;
  return `${pad(minutes)}:${pad(rest)}`;
}

/** Integer cents as a currency string. */
export function formatMoney(cents: number): string {
  if (!Number.isFinite(cents)) return '—';
  try {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
    }).format(cents / 100);
  } catch {
    return `$${(cents / 100).toFixed(2)}`;
  }
}

/** A retry delay for the "retrying in ..." note; rounds up to a readable value. */
export function formatDelay(ms: number): string {
  if (ms < 1000) return 'a moment';
  return `${Math.ceil(ms / 1000)} s`;
}
