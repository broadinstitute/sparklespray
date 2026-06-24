import { useState, useEffect, useRef } from "react";
import type { ResourceDataPoint, VolumeDataPoint } from "./simulate";

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

interface RawCpuSnapshot {
  time: number;
  cpuUser: number;
  cpuSystem: number;
  cpuIdle: number;
  cpuIowait: number;
}

function toResourceDataPoint(
  msg: ResourceUsageUpdate,
  prev: RawCpuSnapshot | null
): ResourceDataPoint {
  const t = new Date(msg.timestamp).getTime();

  let cpuUser = 0,
    cpuSystem = 0,
    cpuIdle = 0,
    cpuIowait = 0;
  if (prev !== null) {
    const dt = (t - prev.time) / 1000;
    if (dt > 0) {
      cpuUser = Math.max(0, ((msg.cpu_user - prev.cpuUser) / dt) * 100);
      cpuSystem = Math.max(0, ((msg.cpu_system - prev.cpuSystem) / dt) * 100);
      cpuIdle = Math.max(0, ((msg.cpu_idle - prev.cpuIdle) / dt) * 100);
      cpuIowait = Math.max(0, ((msg.cpu_iowait - prev.cpuIowait) / dt) * 100);
    }
  }

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
    cpuUser: Math.round(cpuUser * 10) / 10,
    cpuSystem: Math.round(cpuSystem * 10) / 10,
    cpuIdle: Math.round(cpuIdle * 10) / 10,
    cpuIowait: Math.round(cpuIowait * 10) / 10,
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

const MAX_FAILURES = 10;

export function useTaskLog(
  taskId: string,
  isActive: boolean
): {
  resourceData: ResourceDataPoint[];
  logContent: string;
  error: string | null;
} {
  const [resourceData, setResourceData] = useState<ResourceDataPoint[]>([]);
  const [logContent, setLogContent] = useState("");
  const [error, setError] = useState<string | null>(null);
  const cancelledRef = useRef(false);
  const cursorRef = useRef<string | null>(null);
  const lastRawCpuRef = useRef<RawCpuSnapshot | null>(null);
  const streamActivatedRef = useRef(false);

  useEffect(() => {
    if (!isActive) return;

    cancelledRef.current = false;
    setError(null);

    async function start() {
      // Activate live streaming on the worker (best-effort).
      if (!streamActivatedRef.current) {
        streamActivatedRef.current = true;
        fetch(`/api/v1/task/${taskId}/stream`, {
          method: "POST",
        }).catch(() => {});
      }

      let failures = 0;

      while (!cancelledRef.current) {
        try {
          const params = new URLSearchParams({
            types: "log_update,metric_update",
          });
          if (cursorRef.current) params.set("after", cursorRef.current);

          const res = await fetch(`/api/v1/task/${taskId}/log?${params}`);
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
              const raw = entry as ResourceUsageUpdate;
              const prev = lastRawCpuRef.current;
              lastRawCpuRef.current = {
                time: new Date(raw.timestamp).getTime(),
                cpuUser: raw.cpu_user,
                cpuSystem: raw.cpu_system,
                cpuIdle: raw.cpu_idle,
                cpuIowait: raw.cpu_iowait,
              };
              if (prev !== null) {
                newMetrics.push(toResourceDataPoint(raw, prev));
              }
            } else if (entry.type === "log_update") {
              const lu = entry as LogStreamUpdate;
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
            setResourceData((prev) => [...prev, ...newMetrics]);
          }
          if (newLog) {
            setLogContent((prev) => prev + newLog);
          }
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
          await new Promise<void>((r) => setTimeout(r, 2_000));
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
    lastRawCpuRef.current = null;
    streamActivatedRef.current = false;
  }, [taskId]);

  return { resourceData, logContent, error };
}
