//! Guest sessions. `POST /v1/session` returns a JWT; the app keeps it in
//! `sessionStorage` and reuses it until shortly before it expires, so the
//! opening spike does not create a session per page view.

import { asRecord, asString } from './json';
import { parseTimestampMs } from './format';
import { type Session, readSession, writeSession, clearSession } from './storage';

const FALLBACK_TTL_MS = 60 * 60 * 1000;

/** Decodes the `exp` claim of a JWT without verifying it (we only read our own token). */
export function jwtExpiryMs(token: string): number | null {
  const parts = token.split('.');
  if (parts.length < 2) return null;

  try {
    const base64 = parts[1].replace(/-/g, '+').replace(/_/g, '/');
    const padded = base64.padEnd(base64.length + ((4 - (base64.length % 4)) % 4), '=');
    const payload: unknown = JSON.parse(atob(padded));
    const exp = asRecord(payload)?.exp;
    if (typeof exp !== 'number' || !Number.isFinite(exp)) return null;
    return exp * 1000;
  } catch {
    return null;
  }
}

/**
 * Turns the session response into a stored session. The API contract is
 * `{token}`; an optional `expires_at` and the JWT's own `exp` are honoured
 * when present.
 */
export function parseSessionResponse(
  body: unknown,
  nowMs: number = Date.now(),
): Session | null {
  const record = asRecord(body);
  if (record === null) return null;

  const token = asString(record.token) ?? asString(record.access_token);
  if (token === null) return null;

  const expiresAtMs =
    parseTimestampMs(record.expires_at) ?? jwtExpiryMs(token) ?? nowMs + FALLBACK_TTL_MS;

  return { token, expiresAtMs };
}

/** A usable session, or `null` when there is none or it is about to expire. */
export function currentSession(nowMs: number = Date.now()): Session | null {
  return readSession(nowMs);
}

export function rememberSession(session: Session): void {
  writeSession(session);
}

export function forgetSession(): void {
  clearSession();
}
