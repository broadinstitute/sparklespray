import { useState, useEffect } from "react";
import { apiFetch } from "../api/client";

// The backend's version doesn't change while it's running, so this is a
// one-shot fetch on mount rather than a poll.
export function useVersion(): string | undefined {
  const [version, setVersion] = useState<string | undefined>();
  useEffect(() => {
    let cancelled = false;
    apiFetch("/api/v1/version")
      .then((r) => (r.ok ? r.json() : undefined))
      .then((data) => {
        if (!cancelled && data?.version) setVersion(data.version);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, []);
  return version;
}
