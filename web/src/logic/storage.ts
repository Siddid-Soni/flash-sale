//! `sessionStorage` access that never throws. Privacy modes, disabled storage
//! and quota errors must not take the sale page down, so every call is
//! wrapped; the app simply loses persistence when storage is unavailable.

const SESSION_KEY = 'flash-sale.session';
const ticketKey = (saleId: string) => `flash-sale.ticket:${saleId}`;

export type Session = { token: string; expiresAtMs: number };

/** Final order states worth remembering so a reload shows the outcome. */
export type FinalStatusName = 'CONFIRMED' | 'EXPIRED' | 'FAILED' | 'REFUNDED';

export type TicketRecord = {
  ticket: string;
  /** Set once the order reached a final state; then no polling is needed. */
  final: FinalStatusName | null;
};

export function safeGet(key: string): string | null {
  try {
    return globalThis.sessionStorage?.getItem(key) ?? null;
  } catch {
    return null;
  }
}

export function safeSet(key: string, value: string): void {
  try {
    globalThis.sessionStorage?.setItem(key, value);
  } catch {
    // Storage is best-effort: a failed write only costs resume-after-reload.
  }
}

export function safeRemove(key: string): void {
  try {
    globalThis.sessionStorage?.removeItem(key);
  } catch {
    // Nothing to do.
  }
}

/** Reads and validates a JSON value; invalid or foreign data reads as `null`. */
export function readJson<T>(
  key: string,
  parse: (value: unknown) => T | null,
): T | null {
  const raw = safeGet(key);
  if (raw === null) return null;
  try {
    return parse(JSON.parse(raw));
  } catch {
    return null;
  }
}

export function writeJson(key: string, value: unknown): void {
  try {
    safeSet(key, JSON.stringify(value));
  } catch {
    // A circular or otherwise unserialisable value: nothing to persist.
  }
}

function parseSession(value: unknown): Session | null {
  if (value === null || typeof value !== 'object') return null;
  const record = value as Record<string, unknown>;
  const token = record.token;
  const expiresAtMs = record.expiresAtMs;
  if (typeof token !== 'string' || token === '') return null;
  if (typeof expiresAtMs !== 'number' || !Number.isFinite(expiresAtMs)) return null;
  return { token, expiresAtMs };
}

/**
 * The stored session while it still has more than 30 s to live. The margin
 * keeps a token from expiring mid-checkout.
 */
export function readSession(nowMs: number = Date.now()): Session | null {
  const session = readJson(SESSION_KEY, parseSession);
  if (session === null) return null;
  return session.expiresAtMs > nowMs + 30_000 ? session : null;
}

export function writeSession(session: Session): void {
  writeJson(SESSION_KEY, session);
}

export function clearSession(): void {
  safeRemove(SESSION_KEY);
}

function parseTicketRecord(value: unknown): TicketRecord | null {
  if (value === null || typeof value !== 'object') return null;
  const record = value as Record<string, unknown>;
  const ticket = record.ticket;
  if (typeof ticket !== 'string' || ticket === '') return null;

  const rawFinal = record.final;
  const final =
    rawFinal === 'CONFIRMED' ||
    rawFinal === 'EXPIRED' ||
    rawFinal === 'FAILED' ||
    rawFinal === 'REFUNDED'
      ? rawFinal
      : null;

  return { ticket, final };
}

/** The ticket of the in-flight (or finished) purchase for this sale. */
export function readTicket(saleId: string): TicketRecord | null {
  return readJson(ticketKey(saleId), parseTicketRecord);
}

export function writeTicket(saleId: string, record: TicketRecord): void {
  writeJson(ticketKey(saleId), record);
}

export function clearTicket(saleId: string): void {
  safeRemove(ticketKey(saleId));
}
