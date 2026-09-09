import { useEffect, useState } from "react";
import type { MetricMetadata } from "../types";
import { apiFetch } from "../api/client";

// The metric metadata table is static for the lifetime of the page (it
// describes what the backend is capable of collecting, not any particular
// task's data), so one fetch is shared across every component that needs
// it instead of each task detail page re-fetching it.
let cached: MetricMetadata[] | null = null;
let inFlight: Promise<MetricMetadata[]> | null = null;

async function fetchMetricMetadata(): Promise<MetricMetadata[]> {
  if (cached) return cached;
  if (!inFlight) {
    inFlight = apiFetch("/api/v1/metrics")
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data: { metrics: MetricMetadata[] }) => {
        cached = data.metrics;
        return cached;
      })
      .finally(() => {
        inFlight = null;
      });
  }
  return inFlight;
}

// useMetricMetadata returns the full metric metadata table, fetching it once
// (shared across callers) on first use.
export function useMetricMetadata(): {
  metadata: MetricMetadata[];
  error: string | null;
} {
  const [metadata, setMetadata] = useState<MetricMetadata[]>(cached ?? []);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (cached) {
      setMetadata(cached);
      return;
    }
    fetchMetricMetadata()
      .then(setMetadata)
      .catch((e) => setError(String(e)));
  }, []);

  return { metadata, error };
}
