import { describe, expect, it } from 'vitest';
import { jwtExpiryMs, parseSessionResponse } from './session';

function fakeJwt(payload: Record<string, unknown>): string {
  const encode = (value: unknown) =>
    btoa(JSON.stringify(value)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  return `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode(payload)}.signature`;
}

describe('jwtExpiryMs', () => {
  it('reads the exp claim in milliseconds', () => {
    const token = fakeJwt({ exp: 1_893_456_000 });
    expect(jwtExpiryMs(token)).toBe(1_893_456_000_000);
  });

  it('returns null for malformed tokens', () => {
    expect(jwtExpiryMs('not-a-jwt')).toBeNull();
    expect(jwtExpiryMs('a.b.c')).toBeNull();
    expect(jwtExpiryMs(fakeJwt({ sub: '1' }))).toBeNull();
  });
});

describe('parseSessionResponse', () => {
  const now = Date.parse('2026-10-28T20:00:00Z');

  it('accepts the API shape {token}', () => {
    const token = fakeJwt({ exp: 1_893_456_000 });
    const session = parseSessionResponse({ token }, now);
    expect(session).toEqual({ token, expiresAtMs: 1_893_456_000_000 });
  });

  it('prefers an explicit expires_at', () => {
    const session = parseSessionResponse(
      { token: 'opaque', expires_at: '2026-10-29T20:00:00Z' },
      now,
    );
    expect(session).toEqual({
      token: 'opaque',
      expiresAtMs: Date.parse('2026-10-29T20:00:00Z'),
    });
  });

  it('falls back to an hour for opaque tokens', () => {
    expect(parseSessionResponse({ token: 'opaque' }, now)).toEqual({
      token: 'opaque',
      expiresAtMs: now + 3_600_000,
    });
  });

  it('rejects a response without a token', () => {
    expect(parseSessionResponse({}, now)).toBeNull();
    expect(parseSessionResponse(null, now)).toBeNull();
  });
});
