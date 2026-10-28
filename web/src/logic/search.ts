//! The text sent to `GET /v1/sales/search?q=`.

/** The longest search text the server uses, in characters. */
export const MAX_QUERY_CHARS = 64;

/**
 * Trims, lowercases and cuts the text to 64 characters, as the server does, so equal searches share
 * one cached answer. Empty means "no search": show the normal list.
 */
export function normalizeQuery(input: string): string {
  return Array.from(input.trim().toLowerCase()).slice(0, MAX_QUERY_CHARS).join('').trim();
}
