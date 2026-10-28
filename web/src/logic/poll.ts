//! Status polling that stops on its own once the order is final. Polling is
//! chained with `setTimeout` (never `setInterval`) so a slow response cannot
//! stack requests on top of each other.

import { isFinalStatus, type StatusUpdate } from './state';

export const POLL_INTERVAL_MS = 2_000;

export type PollOptions = {
  /** One polled status, already mapped from the HTTP response. */
  fetchOnce: () => Promise<StatusUpdate>;
  /**
   * Called with each update. Returning a boolean decides whether polling
   * stops; otherwise it stops on a final state.
   */
  onUpdate: (update: StatusUpdate) => boolean | void;
  /** Called when a poll throws; polling continues. */
  onError?: (error: unknown) => void;
  intervalMs?: number;
};

/**
 * Starts polling immediately (so a reload or a return from the payment page
 * resolves without a two-second wait) and every `intervalMs` afterwards.
 * Returns a stop function; it is safe to call more than once.
 */
export function startStatusPolling(options: PollOptions): () => void {
  const intervalMs = options.intervalMs ?? POLL_INTERVAL_MS;
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;

  const stop = () => {
    stopped = true;
    if (timer !== undefined) {
      clearTimeout(timer);
      timer = undefined;
    }
  };

  const schedule = () => {
    if (!stopped) timer = setTimeout(run, intervalMs);
  };

  const run = async () => {
    if (stopped) return;

    let update: StatusUpdate;
    try {
      update = await options.fetchOnce();
    } catch (error) {
      if (stopped) return;
      options.onError?.(error);
      schedule();
      return;
    }

    if (stopped) return;
    const decided = options.onUpdate(update);
    const final = typeof decided === 'boolean' ? decided : isFinalStatus(update);

    if (final) {
      stop();
      return;
    }
    schedule();
  };

  void run();
  return stop;
}
