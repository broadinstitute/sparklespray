import { useState, useEffect, useRef } from "react";
import type { MetricMetadata, MetricValue, ResourceDataPoint } from "../types";
import { apiFetch } from "../api/client";
import { useMetricMetadata } from "./useMetricMetadata";

// MetricSampleDTO mirrors v100.MetricSample's JSON shape: one optional field
// per metric (nil server-side is simply absent, not a sentinel), nested
// under a metric_update entry's "metric" key. host_volumes is the one
// exception -- an array, expanded into per-volume MetricValues below rather
// than being a metadata key itself.
interface MetricSampleDTO {
  host_volumes?: {
    location: string;
    total_bytes: number;
    used_bytes: number;
  }[];
  [key: string]: unknown;
}

interface TaskLogEntry {
  type: "metric_update" | "log_update";
  task_id: string;
  timestamp: string;
  content?: string;
  metric?: MetricSampleDTO;
}

// toResourceDataPoint builds one point generically from the fetched metric
// metadata list: any metadata key present and non-null on entry.metric
// becomes one MetricValue; a key that's nil server-side is simply omitted
// (which is what later lets a chart render it as a gap, rather than a dip to
// zero, with no special-casing needed here). host_volumes expands into two
// MetricValues per volume, tagged with a location prop so multiple volumes
// can share the same metric key and still render as distinct series.
function toResourceDataPoint(
  entry: TaskLogEntry,
  metadata: MetricMetadata[]
): ResourceDataPoint {
  const t = new Date(entry.timestamp).getTime();
  const raw = entry.metric ?? {};
  const metrics: MetricValue[] = [];

  for (const m of metadata) {
    if (!m.in_metric_sample) continue; // e.g. elapsed_seconds -- ResourceUsage-only
    if (
      m.key === "host_volume_total_bytes" ||
      m.key === "host_volume_used_bytes"
    ) {
      continue; // synthesized from host_volumes below, not a top-level field
    }
    const value = raw[m.key];
    if (typeof value === "number") {
      metrics.push({ key: m.key, value });
    }
  }
  for (const v of raw.host_volumes ?? []) {
    metrics.push({
      key: "host_volume_total_bytes",
      value: v.total_bytes,
      props: { location: v.location },
    });
    metrics.push({
      key: "host_volume_used_bytes",
      value: v.used_bytes,
      props: { location: v.location },
    });
  }

  return {
    time: t,
    label: new Date(t).toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    }),
    metrics,
  };
}

function mergeResourceData(
  prev: ResourceDataPoint[],
  incoming: ResourceDataPoint[]
): ResourceDataPoint[] {
  const byTime = new Map<number, ResourceDataPoint>();
  for (const p of prev) byTime.set(p.time, p);
  for (const p of incoming) byTime.set(p.time, p);
  return Array.from(byTime.values()).sort((a, b) => a.time - b.time);
}

const MAX_FAILURES = 10;

export function useTaskLog(
  taskId: string,
  isActive: boolean,
  paused = false
): {
  resourceData: ResourceDataPoint[];
  logContent: string;
  error: string | null;
  lastUpdatedAt: number | null;
} {
  const { metadata } = useMetricMetadata();
  const [resourceData, setResourceData] = useState<ResourceDataPoint[]>([]);
  const [logContent, setLogContent] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | null>(null);
  const cancelledRef = useRef(false);
  const cursorRef = useRef<string | null>(null);
  const streamActivatedRef = useRef(false);
  const pausedRef = useRef(paused);
  pausedRef.current = paused;

  useEffect(() => {
    // Metadata is needed to know which keys on entry.metric to read; wait
    // for it rather than starting a poll loop that would parse nothing.
    if (metadata.length === 0) return;

    cancelledRef.current = false;
    setError(null);

    async function start() {
      // Activate live streaming on the worker (best-effort). Only needed
      // while the task is still running; finished tasks have no more
      // metrics to stream, we just need to fetch what's already stored.
      if (isActive && !streamActivatedRef.current) {
        streamActivatedRef.current = true;
        apiFetch(`/api/v1/task/${taskId}/stream`, {
          method: "POST",
        }).catch(() => {});
      }

      let failures = 0;

      while (!cancelledRef.current) {
        if (pausedRef.current) {
          await new Promise<void>((r) => setTimeout(r, 5_000));
          continue;
        }
        try {
          const params = new URLSearchParams({
            types: "log_update,metric_update",
          });
          if (cursorRef.current) params.set("after", cursorRef.current);

          const res = await apiFetch(`/api/v1/task/${taskId}/log?${params}`);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);

          const data: {
            entries: TaskLogEntry[];
            next_after?: string;
          } = await res.json();

          failures = 0;
          const newMetrics: ResourceDataPoint[] = [];
          let newLog = "";

          for (const entry of data.entries) {
            if (entry.type === "metric_update") {
              newMetrics.push(toResourceDataPoint(entry, metadata));
            } else if (entry.type === "log_update") {
              if (!entry.content) continue;
              const ts = new Date(entry.timestamp).toLocaleTimeString("en-US", {
                hour: "2-digit",
                minute: "2-digit",
                second: "2-digit",
              });
              newLog += `[${ts}] ${entry.content}`;
            }
          }

          if (data.next_after) cursorRef.current = data.next_after;

          if (newMetrics.length > 0) {
            setResourceData((prev) => mergeResourceData(prev, newMetrics));
          }
          if (newLog) {
            setLogContent((prev) => prev + newLog);
          }
          setLastUpdatedAt(Date.now());

          // Finished tasks won't produce any more entries — fetch once and stop.
          if (!isActive) break;
        } catch {
          failures++;
          if (failures > MAX_FAILURES) {
            setError(
              `Stopped polling after ${MAX_FAILURES} consecutive errors.`
            );
            break;
          }
        }

        if (!cancelledRef.current) {
          await new Promise<void>((r) => setTimeout(r, 5_000));
        }
      }
    }

    start();

    return () => {
      cancelledRef.current = true;
    };
  }, [taskId, isActive, metadata]);

  useEffect(() => {
    setResourceData([]);
    setLogContent("");
    setError(null);
    cursorRef.current = null;
    streamActivatedRef.current = false;
  }, [taskId]);

  return { resourceData, logContent, error, lastUpdatedAt };
}
