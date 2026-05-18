import { useState, useEffect, useMemo, useRef } from "react";
import { useParams, Link } from "react-router-dom";
import { mergeEvents } from "../data/EventProvider";
import { computeClusterTimeSeries } from "../data/clusterTimeSeries";
import type { AnyEvent } from "../types";
import MultiLineChart from "../components/MultiLineChart";

const CLUSTER_EVENT_TYPES =
  "worker_started,worker_stopped,cluster_started,cluster_stopped";
const POLL_MS = 5_000;
const PAGE_LIMIT = 1000;

export default function ClusterDetail() {
  const { clusterId } = useParams<{ clusterId: string }>();
  const [localEvents, setLocalEvents] = useState<AnyEvent[]>([]);
  const cursorRef = useRef<string | null>(null);

  useEffect(() => {
    if (!clusterId) return;
    let cancelled = false;

    async function poll() {
      while (!cancelled) {
        try {
          const params = new URLSearchParams({
            cluster_id: clusterId!,
            types: CLUSTER_EVENT_TYPES,
            limit: String(PAGE_LIMIT),
          });
          if (cursorRef.current) params.set("after", cursorRef.current);

          const res = await fetch(`/api/v1/events?${params}`);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data: {
            events: AnyEvent[];
            next_after?: string;
          } = await res.json();

          if (data.events.length > 0) {
            setLocalEvents((prev) => mergeEvents(prev, data.events));
            if (data.next_after) cursorRef.current = data.next_after;
            if (data.events.length >= PAGE_LIMIT) continue;
          }
        } catch (err) {
          console.error("[ClusterDetail] poll error:", err);
        }
        await new Promise<void>((r) => setTimeout(r, POLL_MS));
      }
    }

    poll();
    return () => {
      cancelled = true;
    };
  }, [clusterId]);

  const { counts, rates } = useMemo(
    () =>
      clusterId
        ? computeClusterTimeSeries(localEvents, clusterId)
        : { counts: [], rates: [] },
    [localEvents, clusterId]
  );

  if (!clusterId) {
    return (
      <div style={{ padding: "2rem", fontFamily: "monospace" }}>
        Invalid cluster ID.
      </div>
    );
  }

  if (counts.length === 0) {
    return (
      <div style={{ padding: "2rem", fontFamily: "monospace" }}>
        <p style={{ marginTop: "1rem" }}>
          No worker events found for cluster: <strong>{clusterId}</strong>
        </p>
      </div>
    );
  }

  const currentWorkers = counts[counts.length - 1]?.running ?? 0;
  const peakWorkers = Math.max(...counts.map((p) => p.running));

  return (
    <div
      style={{
        maxWidth: 960,
        margin: "0 auto",
        padding: "2rem",
        fontFamily: "monospace",
      }}
    >
      {/* Header */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: "1.5rem",
        }}
      >
        <h1 style={{ margin: 0, fontSize: "1.3rem", fontWeight: 700 }}>
          {clusterId}
        </h1>
        <Link
          to={`/clusters/${clusterId}/logs`}
          style={{
            fontSize: "0.8rem",
            padding: "0.35rem 0.85rem",
            border: "1px solid #c5cae9",
            borderRadius: 6,
            background: "#f5f5ff",
            color: "#1a237e",
            textDecoration: "none",
            fontWeight: 500,
          }}
        >
          View logs
        </Link>
      </div>

      {/* Summary bar */}
      <div
        style={{
          display: "flex",
          gap: "2rem",
          background: "#f8f9fa",
          border: "1px solid #e0e0e0",
          borderRadius: 8,
          padding: "1rem 1.5rem",
          marginBottom: "1.5rem",
          fontSize: "0.85rem",
        }}
      >
        <span>
          <span style={{ color: "#888" }}>workers running </span>
          <span style={{ fontWeight: 600 }}>{currentWorkers}</span>
        </span>
        <span>
          <span style={{ color: "#888" }}>peak </span>
          <span style={{ fontWeight: 600 }}>{peakWorkers}</span>
        </span>
      </div>

      {/* Charts */}
      <div
        style={{
          background: "#f8f9fa",
          border: "1px solid #e0e0e0",
          borderRadius: 8,
          padding: "1rem 1.5rem",
        }}
      >
        <MultiLineChart
          data={counts}
          title="Workers Running"
          yLabel="workers"
          stacked={false}
          series={[{ key: "running", label: "Running", color: "#2e7d32" }]}
        />
        <div style={{ height: "1.25rem" }} />
        <MultiLineChart
          data={rates}
          title="Worker Change Rate"
          yLabel="workers/min"
          series={[
            { key: "started", label: "Started", color: "#1565c0" },
            { key: "stopped", label: "Stopped", color: "#e53935" },
          ]}
        />
      </div>
    </div>
  );
}
