import { describe, expect, it } from 'vitest';
import {
  formatClock,
  formatCountdown,
  formatDelay,
  formatMoney,
  parseTimestampMs,
  secondsLeft,
} from './format';

describe('parseTimestampMs', () => {
  it('reads epoch milliseconds', () => {
    expect(parseTimestampMs(1_893_456_000_000)).toBe(1_893_456_000_000);
    expect(parseTimestampMs('1893456000000')).toBe(1_893_456_000_000);
  });

  it('reads epoch seconds', () => {
    expect(parseTimestampMs(1_893_456_000)).toBe(1_893_456_000_000);
    expect(parseTimestampMs('1893456000')).toBe(1_893_456_000_000);
  });

  it('reads ISO-8601 strings', () => {
    expect(parseTimestampMs('2026-10-28T20:00:00Z')).toBe(Date.parse('2026-10-28T20:00:00Z'));
    expect(parseTimestampMs('2026-10-28T20:00:00+02:00')).toBe(
      Date.parse('2026-10-28T20:00:00+02:00'),
    );
  });

  it('rejects unusable values', () => {
    expect(parseTimestampMs(null)).toBeNull();
    expect(parseTimestampMs(undefined)).toBeNull();
    expect(parseTimestampMs('')).toBeNull();
    expect(parseTimestampMs('not a time')).toBeNull();
    expect(parseTimestampMs(Number.NaN)).toBeNull();
    expect(parseTimestampMs({})).toBeNull();
  });
});

describe('secondsLeft', () => {
  it('rounds up and never goes below zero', () => {
    expect(secondsLeft(10_000, 8_001)).toBe(2);
    expect(secondsLeft(10_000, 10_000)).toBe(0);
    expect(secondsLeft(10_000, 20_000)).toBe(0);
    expect(secondsLeft(null, 0)).toBe(0);
  });
});

describe('formatCountdown', () => {
  it('formats hours, minutes and seconds', () => {
    expect(formatCountdown(0)).toBe('0s');
    expect(formatCountdown(7)).toBe('7s');
    expect(formatCountdown(67)).toBe('1m 07s');
    expect(formatCountdown(3_661)).toBe('1h 01m 01s');
  });

  it('floors fractions and clamps negatives', () => {
    expect(formatCountdown(59.9)).toBe('59s');
    expect(formatCountdown(-5)).toBe('0s');
  });
});

describe('formatClock', () => {
  it('formats a wall-clock countdown', () => {
    expect(formatClock(0)).toBe('00:00');
    expect(formatClock(7)).toBe('00:07');
    expect(formatClock(3596)).toBe('59:56');
    expect(formatClock(3727)).toBe('01:02:07');
  });

  it('floors fractions and clamps negatives', () => {
    expect(formatClock(59.9)).toBe('00:59');
    expect(formatClock(-5)).toBe('00:00');
  });
});

describe('formatMoney', () => {
  it('formats integer cents', () => {
    expect(formatMoney(129_900)).toBe('$1,299.00');
    expect(formatMoney(0)).toBe('$0.00');
    expect(formatMoney(5)).toBe('$0.05');
  });

  it('degrades gracefully on bad input', () => {
    expect(formatMoney(Number.NaN)).toBe('—');
  });
});

describe('formatDelay', () => {
  it('keeps short delays readable', () => {
    expect(formatDelay(0)).toBe('a moment');
    expect(formatDelay(999)).toBe('a moment');
    expect(formatDelay(1_000)).toBe('1 s');
    expect(formatDelay(2_500)).toBe('3 s');
  });
});
