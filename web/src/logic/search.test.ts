import { describe, expect, it } from 'vitest';
import { MAX_QUERY_CHARS, normalizeQuery } from './search';

describe('normalizeQuery', () => {
  it('trims and lowercases', () => {
    expect(normalizeQuery('  PhOnE \t')).toBe('phone');
    expect(normalizeQuery('Blue  Phone')).toBe('blue  phone');
  });

  it('turns blank input into the empty search', () => {
    expect(normalizeQuery('')).toBe('');
    expect(normalizeQuery(' \n\t ')).toBe('');
  });

  it('cuts to 64 characters, counting characters and not UTF-16 units', () => {
    expect(normalizeQuery('a'.repeat(100))).toBe('a'.repeat(MAX_QUERY_CHARS));
    expect(normalizeQuery('😀'.repeat(100))).toBe('😀'.repeat(MAX_QUERY_CHARS));
    expect(normalizeQuery(`${'a'.repeat(63)} z`)).toBe('a'.repeat(63));
  });
});
