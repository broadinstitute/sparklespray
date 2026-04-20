import { useState, useEffect, useMemo } from "react";
import { useParams } from "react-router-dom";
import { useEvents, mergeEvents } from "../data/EventProvider";
import type { AnyEvent } from "../types";
import TabBar from "../components/TabBar";
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

function MetricRow({
  metric,
  selected,
  onClick,
}: {
  metric: MetricDef;
  selected: boolean;
  onClick: () => void;
}) {
  return (
    <div
      onClick={onClick}
      style={{
        display: "flex",
        alignItems: "center",
        padding: "7px 14px",
        gap: 10,
        borderBottom: "1px solid #f0f0f0",
        cursor: "pointer",
        background: selected ? "#f0f7ff" : "#fff",
        userSelect: "none",
      }}
    >
      <div
        style={{
          width: 4,
          height: 28,
          background: metric.color,
          borderRadius: 2,
          flexShrink: 0,
        }}
      />
      <div
        style={{
          flex: 1,
          minWidth: 0,
          fontSize: "0.82rem",
          fontFamily: "monospace",
          color: "#333",
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
      >
        {metric.label}
      </div>
      {([
        ["median", metric.stats.median],
        ["p95", metric.stats.p95],
        ["max", metric.stats.max],
      ] as [string, number][]).map(([label, val]) => (
        <div
          key={label}
          style={{ textAlign: "right", whiteSpace: "nowrap", minWidth: 64 }}
        >
          <div
            style={{
              fontSize: "0.88rem",
              fontWeight: 700,
              color: "#111",
              lineHeight: 1.2,
            }}
          >
            {fmt(val)}
            <span style={{ fontSize: "0.62rem", color: "#aaa", marginLeft: 2 }}>
              {metric.unit}
            </span>
          </div>
          <div style={{ fontSize: "0.65rem", color: "#999" }}>{label}</div>
        </div>
      ))}
      <div style={{ color: "#ccc", fontSize: 14, flexShrink: 0 }}>›</div>
    </div>
  );
}

function StatsGrid({ stats, unit }: { stats: PerfStats; unit: string }) {
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

function DrillDown({ metric }: { metric: MetricDef }) {
  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column" }}>
      <div style={{ marginBottom: "1rem" }}>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: "0.5rem",
            marginBottom: "0.25rem",
          }}
        >
          <div
            style={{
              width: 4,
              height: 20,
              background: metric.color,
              borderRadius: 2,
              flexShrink: 0,
            }}
          />
          <h2
            style={{
              margin: 0,
              fontSize: "1rem",
              fontWeight: 700,
              fontFamily: "monospace",
            }}
          >
            {metric.label}
          </h2>
          <span
            style={{
              color: "#aaa",
              fontSize: "0.78rem",
              fontFamily: "monospace",
            }}
          >
            {metric.stats.count} tasks
          </span>
        </div>
      </div>

      <StatsGrid stats={metric.stats} unit={metric.unit} />

      <div style={{ flex: 1, minHeight: 180 }}>
        <ResponsiveContainer width="100%" height="100%">
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
            <Tooltip
              contentStyle={TOOLTIP_STYLE}
              /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
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
              fontSize: "0.75rem",
              color: "#888",
              lineHeight: 1.4,
              fontFamily: "monospace",
            }}
          >
            {metric.caption}
          </div>
        )}
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
          (e.type === "task_claimed" ||
            e.type === "task_exec_started" ||
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

  const jobTabs = [
    { label: "Overview", href: `/jobs/${jobId}`, matchExact: true },
    { label: "Tasks", href: `/jobs/${jobId}/tasks` },
    {
      label: "Completed Summary",
      href: `/jobs/${jobId}/summary`,
      matchExact: true,
    },
  ];

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

  return (
    <div
      style={{
        maxWidth: 1100,
        margin: "0 auto",
        padding: "2rem",
        fontFamily: "monospace",
      }}
    >
      {/* Header */}
      <div style={{ marginBottom: "1.5rem" }}>
        <TabBar tabs={jobTabs} />
        <div style={{ display: "flex", alignItems: "baseline", gap: "1rem" }}>
          <h1 style={{ margin: 0, fontSize: "1.3rem", fontWeight: 700 }}>
            Completed Task Metrics
          </h1>
          <span style={{ color: "#aaa", fontSize: "0.82rem" }}>
            {entries.length} tasks
          </span>
        </div>
      </div>

      {entries.length === 0 ? (
        <p style={{ color: "#888" }}>No completed tasks yet.</p>
      ) : (
        <div
          style={{
            display: "flex",
            border: "1px solid #e0e0e0",
            borderRadius: 8,
            overflow: "hidden",
            height: "calc(100vh - 260px)",
            minHeight: 420,
          }}
        >
          {/* Left: metric list */}
          <div
            style={{
              width: 380,
              borderRight: "1px solid #e0e0e0",
              overflowY: "auto",
              flexShrink: 0,
            }}
          >
            {groups.map((group) => (
              <div key={group.label}>
                <div
                  style={{
                    padding: "5px 14px",
                    background: "#f7f7f7",
                    fontSize: "0.65rem",
                    fontWeight: 700,
                    letterSpacing: "0.12em",
                    color: "#999",
                    textTransform: "uppercase",
                    borderBottom: "1px solid #eee",
                    borderTop: "1px solid #eee",
                  }}
                >
                  {group.label}
                </div>
                {group.metrics.map((metric) => (
                  <MetricRow
                    key={metric.key}
                    metric={metric}
                    selected={metric.key === selectedKey}
                    onClick={() => setSelectedKey(metric.key)}
                  />
                ))}
              </div>
            ))}
          </div>

          {/* Right: detail panel */}
          <div
            style={{
              flex: 1,
              padding: "1.25rem 1.5rem",
              overflowY: "auto",
              minWidth: 0,
            }}
          >
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
      )}
    </div>
  );
}
