import { useState, useEffect, useMemo } from "react";
import { useParams, Link } from "react-router-dom";
import { useEvents, mergeEvents } from "../data/EventProvider";
import { computeClusterTimeSeries } from "../data/clusterTimeSeries";
import type { AnyEvent } from "../types";
import MultiLineChart from "../components/MultiLineChart";

export default function ClusterDetail() {
  const { clusterId } = useParams<{ clusterId: string }>();
  const { addEventListener } = useEvents();
  const [localEvents, setLocalEvents] = useState<AnyEvent[]>([]);

  useEffect(() => {
    if (!clusterId) return;
    return addEventListener((newEvents) => {
      const relevant = newEvents.filter(
        (e) =>
          "cluster_id" in e &&
          (e as any).cluster_id === clusterId &&
          (e.type === "worker_started" ||
            e.type === "worker_stopped" ||
            e.type === "cluster_started" ||
            e.type === "cluster_stopped")
      );
      if (relevant.length > 0)
        setLocalEvents((prev) => mergeEvents(prev, relevant));
    });
  }, [addEventListener, clusterId]);

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
        <Link
          to="/"
          style={{
            color: "#2e7d32",
            textDecoration: "none",
            fontSize: "0.85rem",
          }}
        >
          ← All Clusters
        </Link>
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
      <div style={{ marginBottom: "1.5rem" }}>
        <div style={{ fontSize: "0.8rem", marginBottom: "0.25rem" }}>
          <Link to="/" style={{ color: "#2e7d32", textDecoration: "none" }}>
            ← All Clusters
          </Link>
        </div>
        <h1 style={{ margin: 0, fontSize: "1.3rem", fontWeight: 700 }}>
          {clusterId}
        </h1>
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
