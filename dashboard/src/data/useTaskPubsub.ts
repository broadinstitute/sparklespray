import { useState, useEffect, useRef } from "react";
import type { ResourceDataPoint } from "./simulate";

interface SubscriptionCreds {
  subscriptionId: string;
  pullUrl: string;
  ackUrl: string;
  token: string;
}

interface ResourceUsageUpdate {
  type: "metric_update";
  req_id: string;
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
}

interface LogStreamUpdate {
  type: "log_update";
  req_id: string;
  task_id: string;
  timestamp: string;
  content: string;
}

const GB = 1_073_741_824;

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
    cpuUser: msg.cpu_user,
    cpuSystem: msg.cpu_system,
    cpuIdle: msg.cpu_idle,
    cpuIowait: msg.cpu_iowait,
    memTotalGb: Math.round((msg.mem_total / GB) * 100) / 100,
    memAvailableGb: Math.round((msg.mem_available / GB) * 100) / 100,
    memFreeGb: Math.round((msg.mem_free / GB) * 100) / 100,
    memPressureSomeAvg10: msg.mem_pressure_some_avg10,
    memPressureFullAvg10: msg.mem_pressure_full_avg10,
  };
}

async function pullMessages(creds: SubscriptionCreds) {
  const res = await fetch(creds.pullUrl, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${creds.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ maxMessages: 20 }),
  });
  if (!res.ok) return [];
  const data = await res.json();
  return (data.receivedMessages ?? []) as Array<{
    ackId: string;
    message: { data: string; attributes: Record<string, string> };
  }>;
}

async function ackMessages(creds: SubscriptionCreds, ackIds: string[]) {
  if (ackIds.length === 0) return;
  await fetch(creds.ackUrl, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${creds.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ ackIds }),
  });
}

const MAX_FAILURES = 10;

export function useTaskPubsub(
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
  const credsRef = useRef<SubscriptionCreds | null>(null);
  const cancelledRef = useRef(false);

  useEffect(() => {
    if (!isActive) return;

    cancelledRef.current = false;
    setError(null);
    let creds: SubscriptionCreds | null = null;

    async function start() {
      try {
        const res = await fetch(`/api/v1/task/${taskId}/subscription`, {
          method: "POST",
        });
        if (!res.ok || cancelledRef.current) {
          setError("Failed to create task subscription.");
          return;
        }
        const body = await res.json();
        creds = {
          subscriptionId: body.subscription_id,
          pullUrl: body.pull_url,
          ackUrl: body.ack_url,
          token: body.authorization_token,
        };
        credsRef.current = creds;
      } catch {
        if (!cancelledRef.current) setError("Failed to connect to backend.");
        return;
      }

      let failures = 0;

      while (!cancelledRef.current && creds) {
        try {
          const messages = await pullMessages(creds);
          failures = 0;
          const ackIds: string[] = [];
          const newMetrics: ResourceDataPoint[] = [];
          let newLog = "";

          for (const m of messages) {
            ackIds.push(m.ackId);
            try {
              const payload = JSON.parse(atob(m.message.data));
              if (payload.type === "metric_update") {
                newMetrics.push(
                  toResourceDataPoint(payload as ResourceUsageUpdate)
                );
              } else if (payload.type === "log_update") {
                newLog += (payload as LogStreamUpdate).content;
              }
            } catch {
              /* skip malformed messages */
            }
          }

          if (newMetrics.length > 0) {
            setResourceData((prev) => [...prev, ...newMetrics]);
          }
          if (newLog) {
            setLogContent((prev) => prev + newLog);
          }

          await ackMessages(creds, ackIds);
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
      const c = credsRef.current;
      credsRef.current = null;
      if (c) {
        fetch(
          `/api/v1/task/${taskId}/subscription/${c.subscriptionId}/unsubscribe`,
          {
            method: "POST",
          }
        ).catch(() => {});
      }
    };
  }, [taskId, isActive]);

  // Reset data when taskId changes
  useEffect(() => {
    setResourceData([]);
    setLogContent("");
    setError(null);
  }, [taskId]);

  return { resourceData, logContent, error };
}
