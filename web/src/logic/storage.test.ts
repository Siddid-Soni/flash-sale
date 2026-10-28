import { afterEach, describe, expect, it } from 'vitest';
import {
  clearSentToPay,
  hasSentToPay,
  markSentToPay,
  readSession,
  readTicket,
  safeGet,
  safeRemove,
  safeSet,
  writeSession,
  writeTicket,
} from './storage';

type StorageLike = {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
};

const original = Object.getOwnPropertyDescriptor(globalThis, 'sessionStorage');

function installStorage(storage: StorageLike | undefined): void {
  Object.defineProperty(globalThis, 'sessionStorage', {
    value: storage,
    configurable: true,
    writable: true,
  });
}

afterEach(() => {
  if (original === undefined) {
    Reflect.deleteProperty(globalThis, 'sessionStorage');
  } else {
    Object.defineProperty(globalThis, 'sessionStorage', original);
  }
});

describe('safe storage', () => {
  it('never throws when storage is missing or broken', () => {
    installStorage(undefined);
    expect(safeGet('k')).toBeNull();
    expect(() => safeSet('k', 'v')).not.toThrow();
    expect(() => safeRemove('k')).not.toThrow();

    installStorage({
      getItem: () => {
        throw new Error('denied');
      },
      setItem: () => {
        throw new Error('quota');
      },
      removeItem: () => {
        throw new Error('denied');
      },
    });
    expect(safeGet('k')).toBeNull();
    expect(() => safeSet('k', 'v')).not.toThrow();
  });

  it('round-trips a session and refuses an expired one', () => {
    const store = new Map<string, string>();
    installStorage({
      getItem: (key) => store.get(key) ?? null,
      setItem: (key, value) => void store.set(key, value),
      removeItem: (key) => void store.delete(key),
    });

    writeSession({ token: 'abc', expiresAtMs: 100_000 });
    expect(readSession(5_000)).toEqual({ token: 'abc', expiresAtMs: 100_000 });
    // Inside the 30 s safety margin, the session is treated as expired.
    expect(readSession(80_000)).toBeNull();
  });

  it('round-trips a ticket and ignores foreign data', () => {
    const store = new Map<string, string>();
    installStorage({
      getItem: (key) => store.get(key) ?? null,
      setItem: (key, value) => void store.set(key, value),
      removeItem: (key) => void store.delete(key),
    });

    writeTicket('42', { ticket: '42.abc', final: 'CONFIRMED' });
    expect(readTicket('42')).toEqual({ ticket: '42.abc', final: 'CONFIRMED' });

    writeTicket('43', { ticket: '43.abc', final: null });
    expect(readTicket('43')).toEqual({ ticket: '43.abc', final: null });

    store.set('flash-sale.ticket:44', '{"ticket":42}');
    expect(readTicket('44')).toBeNull();

    store.set('flash-sale.ticket:45', 'not json');
    expect(readTicket('45')).toBeNull();
  });

  it('remembers that a ticket was already sent to payment', () => {
    const store = new Map<string, string>();
    installStorage({
      getItem: (key) => store.get(key) ?? null,
      setItem: (key, value) => void store.set(key, value),
      removeItem: (key) => void store.delete(key),
    });

    expect(hasSentToPay('42.a')).toBe(false);
    markSentToPay('42.a');
    expect(hasSentToPay('42.a')).toBe(true);
    // One ticket's marker does not cover another ticket.
    expect(hasSentToPay('42.b')).toBe(false);

    clearSentToPay('42.a');
    expect(hasSentToPay('42.a')).toBe(false);
  });
});
