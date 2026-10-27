//! Small, dependency-free readers for untrusted JSON. Every API response is
//! `unknown` at the boundary, so the UI narrows it here instead of casting.

/** A JSON object, or `null` for anything else (arrays, scalars, `null`). */
export function asRecord(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

/** A non-empty string, or `null`. */
export function asString(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

/** A finite number, or `null`. Accepts numeric strings (some APIs send them). */
export function asNumber(value: unknown): number | null {
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

/** A boolean from `true`/`false`/`"true"`/`1`-style JSON values. */
export function asBool(value: unknown): boolean {
  return value === true || value === 1 || value === 'true' || value === '1';
}

/** Parses the body of a `fetch` response, tolerating empty or malformed JSON. */
export async function readJsonResponse(response: Response): Promise<unknown> {
  try {
    const text = await response.text();
    return text === '' ? null : JSON.parse(text);
  } catch {
    return null;
  }
}

/** The `message` field of an error body, when the server sent one. */
export function errorMessage(body: unknown, fallback: string): string {
  return asString(asRecord(body)?.message) ?? fallback;
}
