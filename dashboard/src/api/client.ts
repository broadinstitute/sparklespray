const API_KEY_STORAGE_KEY = "sparkles-api-key";

// BASE_PATH is the URL path prefix (e.g. "/sparkles", or "" at the root)
// everything -- API requests and react-router's basename (see App.tsx) -- is
// served under. The server (see --prefix on "sparkles serve") injects a
// matching <base href> into index.html at serve time; reading it back here
// means neither this file nor App.tsx need to know the prefix at build time.
export const BASE_PATH = (
  document.querySelector("base")?.getAttribute("href") ?? "/"
).replace(/\/$/, "");

const INVALID_KEY_PATH = BASE_PATH + "/invalid-api-key";

// unauthorized is set once a request comes back 403, and cleared when the
// user saves a new key (setApiKey). While set, apiFetch short-circuits
// instead of hitting the network — several components poll /api/* on
// intervals that keep running in the background (e.g. EventProvider's job
// poll, which lives above the router and never unmounts), and without this
// guard they'd keep re-requesting a backend they already know will 403.
let unauthorized = false;

export function getApiKey(): string | null {
  return localStorage.getItem(API_KEY_STORAGE_KEY);
}

export function setApiKey(key: string): void {
  localStorage.setItem(API_KEY_STORAGE_KEY, key);
  unauthorized = false;
}

// apiFetch is a drop-in replacement for fetch() for all /api/* requests: it
// prefixes url with BASE_PATH so requests land under the configured --prefix,
// attaches the stored API key as a Bearer token, and redirects to
// /invalid-api-key on a 403 (Forbidden) response.
export async function apiFetch(
  url: string,
  init: RequestInit = {}
): Promise<Response> {
  if (unauthorized) {
    throw new Error("Forbidden: invalid API key");
  }

  const key = getApiKey();
  const headers = new Headers(init.headers);
  if (key) headers.set("Authorization", `Bearer ${key}`);

  const response = await fetch(BASE_PATH + url, { ...init, headers });

  if (response.status === 403) {
    unauthorized = true;
    // Avoid re-assigning location.href when already on this page — doing so
    // unconditionally forces a full page reload even for a same-URL
    // assignment, which (combined with background pollers that keep firing
    // regardless of route) produced a reload loop.
    if (window.location.pathname !== INVALID_KEY_PATH) {
      window.location.href = INVALID_KEY_PATH;
    }
    throw new Error("Forbidden: invalid API key");
  }

  return response;
}
