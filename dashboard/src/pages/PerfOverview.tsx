import { useState, useEffect, useMemo } from "react";
import type { TaskSummaryRecord } from "../types";
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
import { apiFetch } from "../api/client";

const TICK = { fontSize: 11, fontFamily: "monospace" };
const MARGIN = { top: 4, right: 16, left: 8, bottom: 24 };
const TOOLTIP_STYLE = { fontFamily: "monospace", fontSize: 11 };

function fmt(n: number, decimals = 2) {
  return n.toFixed(decimals);
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

// Renders the "Completed Summary" tab's content (metric list + drill-down).
// The job header and TabBar are owned by JobDetail, which hosts this tab
// alongside Overview/Tasks/Events so all tabs share the same top section.
export default function PerfOverview({ jobId }: { jobId: string }) {
  const [tasks, setTasks] = useState<TaskSummaryRecord[]>([]);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);

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

  const perf = useMemo(() => computeJobPerf(tasks), [tasks]);

  const {
    entries,
    execStats,
    memStats,
    userCpuStats,
    systemCpuStats,
    cpuEffStats,
    blockReadStats,
    blockWriteStats,
  } = perf;

  const groups: MetricGroup[] = [
    {
      label: "Timing",
      metrics: [
        {
          key: "execTime",
          label: "Execution Time",
          unit: "sec",
          color: "#7c4dff",
          stats: execStats,
          histData: makeHistogram(
            entries.map((e) => e.executionSec),
            20
          ),
        },
      ],
    },
    {
      label: "Memory",
      metrics: [
        {
          key: "peakMem",
          label: "Peak Memory",
          unit: "MB",
          color: "#00897b",
          stats: memStats,
          histData: makeHistogram(
            entries.map((e) => e.maxMemGb * 1024),
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
            entries.map((e) =>
              e.executionSec > 0
                ? (e.userCpuSec + e.systemCpuSec) / e.executionSec
                : 0
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
          key: "blockRead",
          label: "Block Read",
          unit: "MB",
          color: "#0277bd",
          stats: blockReadStats,
          histData: makeHistogram(
            entries.map((e) => e.blockReadBytes / 1e6),
            20
          ),
        },
        {
          key: "blockWrite",
          label: "Block Write",
          unit: "MB",
          color: "#01579b",
          stats: blockWriteStats,
          histData: makeHistogram(
            entries.map((e) => e.blockWriteBytes / 1e6),
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
          {entries.length} tasks
        </span>
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
          <div
            style={{
              width: 520,
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
