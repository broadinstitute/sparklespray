import { useState, useEffect, useRef } from "react";
import type { ResourceDataPoint, VolumeDataPoint } from "../types";
import { apiFetch } from "../api/client";

interface ResourceUsageUpdate {
  type: "metric_update";
  task_id: string;
  timestamp: string;
  process_count: number;
  total_memory: number;
  total_data: number;
  total_shared: number;
  total_resident: number;
  cpu_user: number;
  cpu_system: number;
  cpu_idle: number;
  cpu_iowait: number;
  mem_total: number;
  mem_available: number;
  mem_free: number;
  mem_pressure_some_avg10: number;
  mem_pressure_full_avg10: number;
  volumes?: { location: string; total_gb: number; used_gb: number }[];
}

interface LogStreamUpdate {
  type: "log_update";
  task_id: string;
  timestamp: string;
  content: string;
}

const GB = 1_073_741_824;

// The server now reports cpu_user/cpu_system/cpu_idle/cpu_iowait as
// percentages of total CPU time across all cores (computed server-side from
// consecutive /proc/stat snapshots), so no client-side delta math is needed.
function toResourceDataPoint(msg: ResourceUsageUpdate): ResourceDataPoint {
  const t = new Date(msg.timestamp).getTime();

  return {
    time: t,
    label: new Date(t).toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    }),
    processCount: msg.process_count,
    totalMemoryGb: Math.round((msg.total_memory / GB) * 100) / 100,
    totalDataGb: Math.round((msg.total_data / GB) * 100) / 100,
    totalSharedGb: Math.round((msg.total_shared / GB) * 100) / 100,
    totalResidentGb: Math.round((msg.total_resident / GB) * 100) / 100,
    cpuUser: Math.round(msg.cpu_user * 10) / 10,
    cpuSystem: Math.round(msg.cpu_system * 10) / 10,
    cpuIdle: Math.round(msg.cpu_idle * 10) / 10,
    cpuIowait: Math.round(msg.cpu_iowait * 10) / 10,
    memTotalGb: Math.round((msg.mem_total / GB) * 100) / 100,
    memAvailableGb: Math.round((msg.mem_available / GB) * 100) / 100,
    memFreeGb: Math.round((msg.mem_free / GB) * 100) / 100,
    memPressureSomeAvg10: msg.mem_pressure_some_avg10,
    memPressureFullAvg10: msg.mem_pressure_full_avg10,
    volumes: (msg.volumes ?? []).map(
      (v): VolumeDataPoint => ({
        location: v.location,
        totalGb: v.total_gb,
        usedGb: v.used_gb,
      })
    ),
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
            entries: (ResourceUsageUpdate | LogStreamUpdate)[];
            next_after?: string;
          } = await res.json();

          failures = 0;
          const newMetrics: ResourceDataPoint[] = [];
          let newLog = "";

          for (const entry of data.entries) {
            if (entry.type === "metric_update") {
              newMetrics.push(
                toResourceDataPoint(entry as ResourceUsageUpdate)
              );
            } else if (entry.type === "log_update") {
              const lu = entry as LogStreamUpdate;
              if (!lu.content) continue;
              const ts = new Date(lu.timestamp).toLocaleTimeString("en-US", {
                hour: "2-digit",
                minute: "2-digit",
                second: "2-digit",
              });
              newLog += `[${ts}] ${lu.content}`;
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
  }, [taskId, isActive]);

  useEffect(() => {
    setResourceData([]);
    setLogContent("");
    setError(null);
    cursorRef.current = null;
    streamActivatedRef.current = false;
  }, [taskId]);

  return { resourceData, logContent, error, lastUpdatedAt };
}
