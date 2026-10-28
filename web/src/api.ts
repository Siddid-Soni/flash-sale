//! The few network calls the page makes outside the lazy checkout chunk.

import { errorMessage, readJsonResponse } from './logic/json';

export type SaleResult =
  | { ok: true; body: unknown }
  | { ok: false; message: string };

/** `GET /v1/sales/{id}`. Public, cacheable at the edge; the browser asks fresh. */
export async function fetchSale(saleId: string): Promise<SaleResult> {
  try {
    const response = await fetch(`/v1/sales/${encodeURIComponent(saleId)}`, {
      headers: { Accept: 'application/json' },
      credentials: 'same-origin',
      cache: 'no-store',
    });
    const body = await readJsonResponse(response);

    if (!response.ok) {
      const fallback =
        response.status === 404
          ? 'This sale does not exist.'
          : `Could not load the sale (${response.status}).`;
      return { ok: false, message: errorMessage(body, fallback) };
    }
    return { ok: true, body };
  } catch {
    return { ok: false, message: 'Could not reach the server. Check your connection.' };
  }
}

/** `GET /v1/sales`: the latest sales for the home page. */
export async function fetchSales(): Promise<SaleResult> {
  try {
    const response = await fetch('/v1/sales', {
      headers: { Accept: 'application/json' },
      credentials: 'same-origin',
      cache: 'no-store',
    });
    const body = await readJsonResponse(response);
    if (!response.ok) {
      return { ok: false, message: errorMessage(body, `Could not load the sales (${response.status}).`) };
    }
    return { ok: true, body };
  } catch {
    return { ok: false, message: 'Could not reach the server. Check your connection.' };
  }
}

/** `POST /v1/session`; throws with a readable message on failure. */
export async function createGuestSession(): Promise<unknown> {
  let response: Response;
  try {
    response = await fetch('/v1/session', {
      method: 'POST',
      headers: { Accept: 'application/json' },
      credentials: 'same-origin',
    });
  } catch {
    throw new Error('Could not reach the server.');
  }

  const body = await readJsonResponse(response);
  if (!response.ok) {
    throw new Error(errorMessage(body, `Could not start a session (${response.status}).`));
  }
  return body;
}
