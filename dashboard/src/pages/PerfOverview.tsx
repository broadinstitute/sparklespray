import { useState, useEffect, useMemo } from "react";
import { useParams, Link } from "react-router-dom";
import { useEvents, mergeEvents } from "../data/EventProvider";
import type { AnyEvent } from "../types";
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
import { computeJobPerf, makeHistogram } from "../data/jobPerf";
import type { PerfStats } from "../data/jobPerf";

const TICK = { fontSize: 11, fontFamily: "monospace" };
const MARGIN = { top: 4, right: 16, left: 8, bottom: 24 };
const TOOLTIP_STYLE = { fontFamily: "monospace", fontSize: 11 };

function fmt(n: number, decimals = 2) {
  return n.toFixed(decimals);
}

function scaleStats(stats: PerfStats, factor: number): PerfStats {
  return {
    count: stats.count,
    min: stats.min * factor,
    p25: stats.p25 * factor,
    median: stats.median * factor,
    p75: stats.p75 * factor,
    p95: stats.p95 * factor,
    max: stats.max * factor,
  };
}

interface MetricDef {
  key: string;
  label: string;
  unit: string;
  color: string;
  stats: PerfStats;
  histData: { label: string; count: number }[];
  caption?: string;
}

interface MetricGroup {
  label: string;
  metrics: MetricDef[];
}

function SummaryCard({
  metric,
  selected,
  onClick,
}: {
  metric: MetricDef;
  selected: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      style={{
        all: "unset",
        display: "block",
        cursor: "pointer",
        background: selected ? "#f0f0ff" : "#fff",
        border: "1px solid",
        borderColor: selected ? metric.color : "#e0e0e0",
        borderLeft: `4px solid ${metric.color}`,
        borderRadius: 8,
        padding: "0.75rem 1rem",
        fontFamily: "monospace",
        width: "100%",
        boxSizing: "border-box",
      }}
    >
      <div
        style={{ fontSize: "0.72rem", color: "#888", marginBottom: "0.3rem" }}
      >
        {metric.label}
      </div>
      <div
        style={{
          fontSize: "1.2rem",
          fontWeight: 700,
          color: "#1a1a1a",
          lineHeight: 1,
        }}
      >
        {fmt(metric.stats.median)}
        <span
          style={{
            fontSize: "0.7rem",
            fontWeight: 400,
            color: "#888",
            marginLeft: "0.25rem",
          }}
        >
          {metric.unit}
        </span>
      </div>
      <div style={{ fontSize: "0.72rem", color: "#aaa", marginTop: "0.3rem" }}>
        p95 {fmt(metric.stats.p95)} {metric.unit}
      </div>
    </button>
  );
}

function StatsTable({ stats, unit }: { stats: PerfStats; unit: string }) {
  const rows: [string, number][] = [
    ["min", stats.min],
    ["p25", stats.p25],
    ["median", stats.median],
    ["p75", stats.p75],
    ["p95", stats.p95],
    ["max", stats.max],
  ];
  return (
    <table style={{ borderCollapse: "collapse", fontFamily: "monospace" }}>
      <tbody>
        {rows.map(([name, val]) => (
          <tr key={name}>
            <td
              style={{
                padding: "4px 16px 4px 0",
                color: "#888",
                fontSize: "0.85rem",
              }}
            >
              {name}
            </td>
            <td style={{ padding: "4px 0", color: "#222", fontWeight: 600 }}>
              {fmt(val)} {unit}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function DrillDown({ metric }: { metric: MetricDef }) {
  return (
    <div>
      <div
        style={{
          display: "flex",
          alignItems: "baseline",
          gap: "0.75rem",
          marginBottom: "1.25rem",
        }}
      >
        <div
          style={{
            width: 4,
            height: 24,
            background: metric.color,
            borderRadius: 2,
            flexShrink: 0,
          }}
        />
        <h2 style={{ margin: 0, fontSize: "1rem", fontWeight: 700 }}>
          {metric.label}
        </h2>
        <span style={{ color: "#aaa", fontSize: "0.82rem" }}>
          {metric.stats.count} tasks
        </span>
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "160px 1fr",
          gap: "1.5rem",
          alignItems: "start",
        }}
      >
        <StatsTable stats={metric.stats} unit={metric.unit} />
        <div
          style={{
            background: "#f8f9fa",
            border: "1px solid #e0e0e0",
            borderRadius: 8,
            padding: "1rem 1.25rem",
          }}
        >
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={metric.histData} margin={MARGIN}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
              <XAxis dataKey="label" tick={TICK} interval={4}>
                <Label
                  value={metric.unit}
                  offset={-8}
                  position="insideBottom"
                  style={TICK}
                />
              </XAxis>
              <YAxis tick={TICK} />
              {/* eslint-disable-next-line @typescript-eslint/no-explicit-any */}
              <Tooltip
                contentStyle={TOOLTIP_STYLE}
                formatter={(v: any) => [`${v} tasks`, "count"]}
              />
              <Bar
                dataKey="count"
                fill={metric.color}
                isAnimationActive={false}
              />
            </BarChart>
          </ResponsiveContainer>
          {metric.caption && (
            <div
              style={{
                marginTop: "0.5rem",
                fontSize: "0.78rem",
                color: "#888",
                lineHeight: 1.4,
              }}
            >
              {metric.caption}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default function PerfOverview() {
  const { jobId } = useParams<{ jobId: string }>();
  const { addEventListener } = useEvents();
  const [localEvents, setLocalEvents] = useState<AnyEvent[]>([]);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);

  useEffect(() => {
    if (!jobId) return;
    return addEventListener((newEvents) => {
      const relevant = newEvents.filter(
        (e) =>
          "job_id" in e &&
          (e as any).job_id === jobId &&
          (e.type === "task_exec_started" ||
            e.type === "task_exec_complete" ||
            e.type === "task_complete")
      );
      if (relevant.length > 0)
        setLocalEvents((prev) => mergeEvents(prev, relevant));
    });
  }, [addEventListener, jobId]);

  const perf = useMemo(() => computeJobPerf(localEvents, jobId ?? ""), [
    localEvents,
    jobId,
  ]);

  if (!jobId) {
    return (
      <div style={{ padding: "2rem", fontFamily: "monospace" }}>
        Invalid job ID.
      </div>
    );
  }

  const {
    entries,
    execStats,
    locStats,
    uploadTimeStats,
    memStats,
    userCpuStats,
    systemCpuStats,
    cpuEffStats,
    sharedMemStats,
    unsharedMemStats,
    blockInputStats,
    blockOutputStats,
    downloadStats,
    uploadBytesStats,
  } = perf;

  const header = (
    <div style={{ marginBottom: "1.5rem" }}>
      <div
        style={{ fontSize: "0.8rem", color: "#888", marginBottom: "0.25rem" }}
      >
        <Link
          to={`/job/${jobId}`}
          style={{ color: "#1565c0", textDecoration: "none" }}
        >
          ← Job {jobId}
        </Link>
      </div>
      <h1 style={{ margin: 0, fontSize: "1.3rem", fontWeight: 700 }}>
        Completed Task Metrics
      </h1>
      <div style={{ color: "#888", fontSize: "0.85rem", marginTop: "0.25rem" }}>
        {entries.length} tasks with complete execution data
      </div>
    </div>
  );

  if (entries.length === 0) {
    return (
      <div
        style={{
          maxWidth: 960,
          margin: "0 auto",
          padding: "2rem",
          fontFamily: "monospace",
        }}
      >
        {header}
        <p style={{ color: "#888" }}>No completed tasks yet.</p>
      </div>
    );
  }

  const uploadEntries = entries.filter((e) => e.uploadMin !== undefined);

  const groups: MetricGroup[] = [
    {
      label: "Timing",
      metrics: [
        {
          key: "execTime",
          label: "Execution Time",
          unit: "sec",
          color: "#7c4dff",
          stats: scaleStats(execStats, 60),
          histData: makeHistogram(
            entries.map((e) => e.executionMin * 60),
            20
          ),
        },
        {
          key: "locTime",
          label: "Localization Time",
          unit: "sec",
          color: "#1565c0",
          stats: scaleStats(locStats, 60),
          histData: makeHistogram(
            entries.map((e) => e.localizationMin * 60),
            20
          ),
        },
        ...(uploadTimeStats
          ? [
              {
                key: "uploadTime",
                label: "Upload Time",
                unit: "sec",
                color: "#e65100",
                stats: scaleStats(uploadTimeStats, 60),
                histData: makeHistogram(
                  uploadEntries.map((e) => (e.uploadMin as number) * 60),
                  20
                ),
              },
            ]
          : []),
      ],
    },
    {
      label: "Memory",
      metrics: [
        {
          key: "peakMem",
          label: "Peak Memory",
          unit: "GB",
          color: "#00897b",
          stats: memStats,
          histData: makeHistogram(
            entries.map((e) => e.maxMemGb),
            20
          ),
        },
        {
          key: "sharedMem",
          label: "Shared Memory",
          unit: "GB",
          color: "#00695c",
          stats: sharedMemStats,
          histData: makeHistogram(
            entries.map((e) => e.sharedMemoryBytes / 1e9),
            20
          ),
        },
        {
          key: "unsharedMem",
          label: "Unshared Memory",
          unit: "GB",
          color: "#2e7d32",
          stats: unsharedMemStats,
          histData: makeHistogram(
            entries.map((e) => e.unsharedMemoryBytes / 1e9),
            20
          ),
        },
      ],
    },
    {
      label: "CPU",
      metrics: [
        {
          key: "userCpu",
          label: "User CPU Time",
          unit: "sec",
          color: "#6a1b9a",
          stats: userCpuStats,
          histData: makeHistogram(
            entries.map((e) => e.userCpuSec),
            20
          ),
        },
        {
          key: "sysCpu",
          label: "System CPU Time",
          unit: "sec",
          color: "#ad1457",
          stats: systemCpuStats,
          histData: makeHistogram(
            entries.map((e) => e.systemCpuSec),
            20
          ),
        },
        {
          key: "cpuEff",
          label: "CPU Efficiency",
          unit: "cpu/wall",
          color: "#4527a0",
          stats: cpuEffStats,
          histData: makeHistogram(
            entries.map(
              (e) => (e.userCpuSec + e.systemCpuSec) / (e.executionMin * 60)
            ),
            20
          ),
          caption:
            "Ratio of total CPU time (user + system) to wall-clock execution time. Values >1 indicate multiple cores in use; values <1 suggest the task was idle or waiting on I/O.",
        },
      ],
    },
    {
      label: "I/O",
      metrics: [
        {
          key: "blockIn",
          label: "Block Input Ops",
          unit: "ops",
          color: "#0277bd",
          stats: blockInputStats,
          histData: makeHistogram(
            entries.map((e) => e.blockInputOps),
            20
          ),
        },
        {
          key: "blockOut",
          label: "Block Output Ops",
          unit: "ops",
          color: "#01579b",
          stats: blockOutputStats,
          histData: makeHistogram(
            entries.map((e) => e.blockOutputOps),
            20
          ),
        },
        {
          key: "download",
          label: "Download",
          unit: "GB",
          color: "#c62828",
          stats: downloadStats,
          histData: makeHistogram(
            entries.map((e) => e.downloadBytes / 1e9),
            20
          ),
        },
        {
          key: "uploadBytes",
          label: "Upload Bytes",
          unit: "GB",
          color: "#b71c1c",
          stats: uploadBytesStats,
          histData: makeHistogram(
            entries.map((e) => e.uploadBytes / 1e9),
            20
          ),
        },
      ],
    },
  ];

  const allMetrics = groups.flatMap((g) => g.metrics);
  const selected = selectedKey
    ? allMetrics.find((m) => m.key === selectedKey) ?? null
    : null;

  const sidebar = (
    <div
      style={{
        width: 220,
        flexShrink: 0,
        position: "sticky",
        top: 0,
        maxHeight: "100vh",
        overflowY: "auto",
        paddingBottom: "1rem",
      }}
    >
      {groups.map((group) => (
        <div key={group.label} style={{ marginBottom: "1.5rem" }}>
          <div
            style={{
              fontSize: "0.68rem",
              fontWeight: 700,
              letterSpacing: "0.08em",
              color: "#bbb",
              textTransform: "uppercase",
              marginBottom: "0.5rem",
            }}
          >
            {group.label}
          </div>
          <div
            style={{ display: "flex", flexDirection: "column", gap: "0.4rem" }}
          >
            {group.metrics.map((metric) => (
              <SummaryCard
                key={metric.key}
                metric={metric}
                selected={metric.key === selectedKey}
                onClick={() => setSelectedKey(metric.key)}
              />
            ))}
          </div>
        </div>
      ))}
    </div>
  );

  return (
    <div
      style={{
        maxWidth: 1100,
        margin: "0 auto",
        padding: "2rem",
        fontFamily: "monospace",
      }}
    >
      {header}
      <div style={{ display: "flex", gap: "2rem", alignItems: "flex-start" }}>
        {sidebar}
        <div style={{ flex: 1, minWidth: 0 }}>
          {selected ? (
            <DrillDown metric={selected} />
          ) : (
            <div
              style={{
                color: "#bbb",
                fontSize: "0.85rem",
                marginTop: "0.5rem",
              }}
            >
              Select a metric to see its distribution.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
