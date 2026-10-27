import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { startStatusPolling } from './poll';
import type { StatusUpdate } from './state';

describe('startStatusPolling', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('polls at once, every interval, and stops on a final state', async () => {
    const updates: string[] = [];
    const fetchOnce = vi
      .fn<() => Promise<StatusUpdate>>()
      .mockResolvedValueOnce({ kind: 'pending', position: 7 })
      .mockResolvedValueOnce({ kind: 'confirmed' });

    const stop = startStatusPolling({
      fetchOnce,
      onUpdate: (update) => updates.push(update.kind),
      intervalMs: 2_000,
    });

    await vi.advanceTimersByTimeAsync(0);
    expect(updates).toEqual(['pending']);
    expect(fetchOnce).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(2_000);
    expect(updates).toEqual(['pending', 'confirmed']);

    // Final state: no further polls, even long after.
    await vi.advanceTimersByTimeAsync(20_000);
    expect(fetchOnce).toHaveBeenCalledTimes(2);

    stop();
  });

  it('keeps polling after an error and the stop function is idempotent', async () => {
    const updates: string[] = [];
    const onError = vi.fn();
    const fetchOnce = vi
      .fn<() => Promise<StatusUpdate>>()
      .mockRejectedValueOnce(new Error('offline'))
      .mockResolvedValueOnce({ kind: 'pending', position: 2 });

    const stop = startStatusPolling({
      fetchOnce,
      onUpdate: (update) => updates.push(update.kind),
      onError,
      intervalMs: 1_000,
    });

    await vi.advanceTimersByTimeAsync(0);
    expect(onError).toHaveBeenCalledTimes(1);
    expect(updates).toEqual([]);

    await vi.advanceTimersByTimeAsync(1_000);
    expect(updates).toEqual(['pending']);

    stop();
    stop();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(fetchOnce).toHaveBeenCalledTimes(2);
  });

  it('stops without polling when cancelled before the first response', async () => {
    let resolve: (update: StatusUpdate) => void = () => {};
    const fetchOnce = vi.fn(
      () =>
        new Promise<StatusUpdate>((settle) => {
          resolve = settle;
        }),
    );

    const updates: string[] = [];
    const stop = startStatusPolling({
      fetchOnce,
      onUpdate: (update) => updates.push(update.kind),
      intervalMs: 1_000,
    });

    stop();
    resolve({ kind: 'pending', position: 1 });
    await vi.advanceTimersByTimeAsync(50);

    expect(updates).toEqual([]);
    expect(fetchOnce).toHaveBeenCalledTimes(1);
  });
});
