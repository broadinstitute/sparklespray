import { useState, useEffect, useRef } from "react";
import type { TaskSummaryRecord, MetricMetadata } from "../types";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Label,
} from "recharts";
import { computeDistribution, computeCategoryCounts } from "../data/jobPerf";
import { useMetricMetadata } from "../data/useMetricMetadata";
import { apiFetch } from "../api/client";

const TICK = { fontSize: 11, fontFamily: "monospace" };
const MARGIN = { top: 4, right: 16, left: 8, bottom: 24 };
const TOOLTIP_STYLE = { fontFamily: "monospace", fontSize: 11 };

function fmt(n: number, decimals = 2) {
  return n.toFixed(decimals);
}

function StatsGrid({
  stats,
  unit,
}: {
  stats: {
    min: number;
    p25: number;
    median: number;
    p75: number;
    p95: number;
    max: number;
  };
  unit: string;
}) {
  const cells: [string, number][] = [
    ["min", stats.min],
    ["p25", stats.p25],
    ["median", stats.median],
    ["p75", stats.p75],
    ["p95", stats.p95],
    ["max", stats.max],
  ];
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "1fr 1fr 1fr",
        gap: 8,
        marginBottom: 16,
      }}
    >
      {cells.map(([k, v]) => (
        <div
          key={k}
          style={{
            padding: "6px 10px",
            background: "#f7f7f7",
            borderRadius: 4,
            fontFamily: "monospace",
          }}
        >
          <div style={{ fontSize: "0.65rem", color: "#999", marginBottom: 2 }}>
            {k}
          </div>
          <div style={{ fontSize: "0.9rem", fontWeight: 600, color: "#111" }}>
            {fmt(v)}{" "}
            <span style={{ fontSize: "0.7rem", color: "#aaa" }}>{unit}</span>
          </div>
        </div>
      ))}
    </div>
  );
}

function HistogramBlock({
  title,
  count,
  histData,
  color,
}: {
  title: string;
  count: number;
  histData: { label: string; count: number }[];
  color: string;
}) {
  return (
    <div style={{ height: 220 }}>
      <div style={{ fontSize: "0.78rem", color: "#aaa", marginBottom: 4 }}>
        {title} &mdash; {count} tasks
      </div>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={histData} margin={MARGIN}>
          <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
          <XAxis
            dataKey="label"
            tick={TICK}
            interval={Math.max(0, Math.floor(histData.length / 10) - 1)}
          >
            <Label
              value={title}
              offset={-8}
              position="insideBottom"
              style={TICK}
            />
          </XAxis>
          <YAxis tick={TICK} />
          <Tooltip
            contentStyle={TOOLTIP_STYLE}
            /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
            formatter={(v: any) => [`${v} tasks`, "count"]}
          />
          <Bar dataKey="count" fill={color} isAnimationActive={false} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// DrillDown renders one metric's block: a percentile stats grid + histogram
// for gauge/counter metrics, or just a count-per-category bar chart for
// categorical ones (percentiles don't apply to a label like an exit code).
function DrillDown({
  metadata,
  tasks,
  color,
}: {
  metadata: MetricMetadata;
  tasks: TaskSummaryRecord[];
  color: string;
}) {
  if (metadata.type === "categorical") {
    const counts = computeCategoryCounts(tasks, metadata);
    const total = counts.reduce((sum, c) => sum + c.count, 0);
    return (
      <div style={{ marginBottom: "1.5rem" }}>
        <HistogramBlock
          title={metadata.name}
          count={total}
          histData={counts}
          color={color}
        />
        {metadata.description && (
          <div
            style={{ marginTop: "0.5rem", fontSize: "0.75rem", color: "#888" }}
          >
            {metadata.description}
          </div>
        )}
      </div>
    );
  }

  const { stats, histData, unit } = computeDistribution(tasks, metadata, 20);
  return (
    <div style={{ marginBottom: "1.5rem" }}>
      <div
        style={{
          display: "flex",
          alignItems: "baseline",
          gap: "0.5rem",
          marginBottom: "0.5rem",
        }}
      >
        <div
          style={{ width: 4, height: 20, background: color, borderRadius: 2 }}
        />
        <h3 style={{ margin: 0, fontSize: "0.95rem", fontFamily: "monospace" }}>
          {metadata.name}
        </h3>
      </div>
      <StatsGrid stats={stats} unit={unit} />
      <HistogramBlock
        title={unit}
        count={stats.count}
        histData={histData}
        color={color}
      />
      {metadata.description && (
        <div
          style={{ marginTop: "0.5rem", fontSize: "0.75rem", color: "#888" }}
        >
          {metadata.description}
        </div>
      )}
    </div>
  );
}

const PALETTE = [
  "#7c4dff",
  "#00897b",
  "#6a1b9a",
  "#ad1457",
  "#0277bd",
  "#01579b",
  "#c62828",
  "#2e7d32",
];

// Renders the "Completed Summary" tab's content (checkboxes + distributions).
// The job header and TabBar are owned by JobDetail, which hosts this tab
// alongside Overview/Tasks/Events so all tabs share the same top section.
export default function PerfOverview({ jobId }: { jobId: string }) {
  const [tasks, setTasks] = useState<TaskSummaryRecord[]>([]);
  const { metadata } = useMetricMetadata();
  const [visible, setVisible] = useState<Set<string>>(new Set());
  const seededRef = useRef(false);

  const applicable = metadata.filter((m) => m.in_resource_usage);

  useEffect(() => {
    if (seededRef.current || applicable.length === 0) return;
    seededRef.current = true;
    setVisible(
      new Set(
        applicable
          .filter((m) => m.resource_usage_default_position != null)
          .map((m) => m.key)
      )
    );
  }, [applicable]);

  useEffect(() => {
    if (!jobId) return;
    let cancelled = false;
    const POLL_MS = 10_000;

    async function poll() {
      while (!cancelled) {
        try {
          const res = await apiFetch(
            `/api/v1/job/${jobId}/tasks?status=success,error,failed,killed`
          );
          if (res.ok) {
            const data: TaskSummaryRecord[] = await res.json();
            if (!cancelled) setTasks(data);
          }
        } catch {
          // transient
        }
        await new Promise<void>((r) => setTimeout(r, POLL_MS));
      }
    }

    poll();
    return () => {
      cancelled = true;
    };
  }, [jobId]);

  const completed = tasks.filter((t) => t.resource_usage);
  const sortedMetadata = applicable.slice().sort((a, b) => {
    const ap = a.resource_usage_default_position ?? Infinity;
    const bp = b.resource_usage_default_position ?? Infinity;
    if (ap !== bp) return ap - bp;
    return a.name.localeCompare(b.name);
  });

  function toggle(key: string) {
    setVisible((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

  return (
    <div style={{ fontFamily: "monospace" }}>
      <div
        style={{
          display: "flex",
          alignItems: "baseline",
          gap: "1rem",
          marginBottom: "1.25rem",
        }}
      >
        <span style={{ fontWeight: 700 }}>Completed Task Metrics</span>
        <span style={{ color: "#aaa", fontSize: "0.82rem" }}>
          {completed.length} tasks
        </span>
      </div>

      {completed.length === 0 ? (
        <p style={{ color: "#888" }}>No completed tasks yet.</p>
      ) : (
        <div
          style={{ display: "flex", gap: "1.5rem", alignItems: "flex-start" }}
        >
          <div
            style={{
              display: "flex",
              flexDirection: "column",
              gap: "0.6rem",
              flex: "0 0 auto",
              minWidth: "12rem",
              fontSize: "0.8rem",
              color: "#666",
            }}
          >
            {sortedMetadata.map((m) => (
              <label
                key={m.key}
                title={m.description}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "0.35rem",
                  cursor: "pointer",
                }}
              >
                <input
                  type="checkbox"
                  checked={visible.has(m.key)}
                  onChange={() => toggle(m.key)}
                />
                {m.name}
              </label>
            ))}
          </div>

          <div style={{ flex: "1 1 auto", minWidth: 0, maxWidth: 640 }}>
            {sortedMetadata
              .filter((m) => visible.has(m.key))
              .map((m, i) => (
                <DrillDown
                  key={m.key}
                  metadata={m}
                  tasks={completed}
                  color={PALETTE[i % PALETTE.length]}
                />
              ))}
          </div>
        </div>
      )}
    </div>
  );
}
