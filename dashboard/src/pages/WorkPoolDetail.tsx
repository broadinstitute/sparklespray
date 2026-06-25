import { useState, useEffect, useMemo } from "react";
import { useParams, useLocation, Link } from "react-router-dom";
import type { WorkPoolDetail, WorkPoolSummaryHistoryEntry } from "../types";
import MultiLineChart from "../components/MultiLineChart";
import TabBar from "../components/TabBar";

const MONO = "'JetBrains Mono', 'Courier New', monospace";

// ── Status colours ────────────────────────────────────────────────────────────

const TASK_COLORS: Record<string, string> = {
  pending: "#1565c0",
  claimed: "#e65100",
  running: "#6a1b9a",
  writing: "#2e7d32",
  success: "#00695c",
  error: "#bf360c",
  failed: "#b71c1c",
  killed: "#555555",
};

const WORKER_COLORS: Record<string, string> = {
  started: "#2e7d32",
  stopped: "#aaaaaa",
};

const BATCH_STATUS_COLORS: Record<string, string> = {
  pending: "#1565c0",
  started: "#2e7d32",
  completed: "#00695c",
  failed: "#b71c1c",
};

// ── API shapes ────────────────────────────────────────────────────────────────

export interface WorkerRecord {
  worker_id: string;
  workpool_id: string;
  batch_id: string;
  instance_name: string;
  status: string;
  expiry: string;
  heartbeat_expiry: string;
}

export interface BatchRecord {
  batch_id: string;
  job_id: string;
  workpool_id: string;
  expected_vm_count: number;
  preemptible: boolean;
  submitted_at: string;
  running_since: string | null;
  registered_worker_count: number;
  status: string;
  unhealthy: boolean;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function formatTime(ms: number): string {
  return new Date(ms).toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
  });
}

// ── Data hooks ────────────────────────────────────────────────────────────────

function useWorkPoolDetail(
  workpoolId: string | undefined
): WorkPoolDetail | undefined {
  const [detail, setDetail] = useState<WorkPoolDetail | undefined>();
  useEffect(() => {
    if (!workpoolId) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await fetch(`/api/v1/workpool/${workpoolId}`);
        if (!r.ok || cancelled) return;
        const data = await r.json();
        if (!cancelled) setDetail(data);
      } catch (_) {}
    };
    poll();
    const id = setInterval(poll, 10000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [workpoolId]);
  return detail;
}

function useWorkPoolSummaryHistory(
  workpoolId: string | undefined
): WorkPoolSummaryHistoryEntry[] {
  const [history, setHistory] = useState<WorkPoolSummaryHistoryEntry[]>([]);
  useEffect(() => {
    if (!workpoolId) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await fetch(`/api/v1/workpool/${workpoolId}/summary-history`);
        if (!r.ok || cancelled) return;
        const data = await r.json();
        if (!cancelled) setHistory(data);
      } catch (_) {}
    };
    poll();
    const id = setInterval(poll, 10000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [workpoolId]);
  return history;
}

function useWorkers(
  workpoolId: string | undefined,
  active: boolean
): WorkerRecord[] {
  const [workers, setWorkers] = useState<WorkerRecord[]>([]);
  useEffect(() => {
    if (!workpoolId || !active) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await fetch(
          `/api/v1/workpool/${workpoolId}/workers?status=all`
        );
        if (!r.ok || cancelled) return;
        const data = await r.json();
        if (!cancelled) setWorkers(data);
      } catch (_) {}
    };
    poll();
    const id = setInterval(poll, 5000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [workpoolId, active]);
  return workers;
}

function useBatches(
  workpoolId: string | undefined,
  active: boolean
): BatchRecord[] {
  const [batches, setBatches] = useState<BatchRecord[]>([]);
  useEffect(() => {
    if (!workpoolId || !active) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await fetch(`/api/v1/workpool/${workpoolId}/batches`);
        if (!r.ok || cancelled) return;
        const data = await r.json();
        if (!cancelled) setBatches(data);
      } catch (_) {}
    };
    poll();
    const id = setInterval(poll, 5000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [workpoolId, active]);
  return batches;
}

// ── Time-series computation ───────────────────────────────────────────────────

interface ChartPoint {
  time: number;
  label: string;
  [key: string]: number | string;
}

function computeWorkPoolTimeSeries(
  history: WorkPoolSummaryHistoryEntry[]
): {
  vmCounts: ChartPoint[];
  taskCounts: ChartPoint[];
  taskTerminalDeltas: ChartPoint[];
  workerCounts: ChartPoint[];
  taskStatuses: string[];
  terminalTaskStatuses: string[];
  workerStatuses: string[];
} {
  const empty = {
    vmCounts: [],
    taskCounts: [],
    taskTerminalDeltas: [],
    workerCounts: [],
    taskStatuses: [],
    terminalTaskStatuses: [],
    workerStatuses: [],
  };
  if (history.length === 0) return empty;

  const TERMINAL_TASK = new Set(["error", "failed", "success", "killed"]);
  const TERMINAL_WORKER = new Set(["stopped"]);

  const taskStatusSet = new Set<string>();
  const terminalTaskStatusSet = new Set<string>();
  const workerStatusSet = new Set<string>();

  for (const h of history) {
    for (const tc of h.tasks) {
      if (TERMINAL_TASK.has(tc.status)) terminalTaskStatusSet.add(tc.status);
      else taskStatusSet.add(tc.status);
    }
    for (const wc of h.workers)
      if (!TERMINAL_WORKER.has(wc.status)) workerStatusSet.add(wc.status);
  }

  const taskStatuses = Array.from(taskStatusSet);
  const terminalTaskStatuses = Array.from(terminalTaskStatusSet);
  const workerStatuses = Array.from(workerStatusSet);

  const vmCounts: ChartPoint[] = history.map((h) => {
    const t = new Date(h.timestamp).getTime();
    return {
      time: t,
      label: formatTime(t),
      preemptible: h.expected_preemptible_vm_count,
      nonPreemptible: h.expected_nonpreemptible_vm_count,
    };
  });

  const taskCounts: ChartPoint[] = history.map((h) => {
    const t = new Date(h.timestamp).getTime();
    const pt: ChartPoint = { time: t, label: formatTime(t) };
    for (const s of taskStatuses) pt[s] = 0;
    for (const tc of h.tasks)
      if (!TERMINAL_TASK.has(tc.status)) pt[tc.status] = tc.count;
    return pt;
  });

  // Deltas of terminal-state counts between consecutive history points
  const taskTerminalDeltas: ChartPoint[] = [];
  for (let i = 1; i < history.length; i++) {
    const prev = history[i - 1];
    const curr = history[i];
    const t = new Date(curr.timestamp).getTime();
    const pt: ChartPoint = { time: t, label: formatTime(t) };
    for (const s of terminalTaskStatuses) {
      const prevCount = prev.tasks.find((tc) => tc.status === s)?.count ?? 0;
      const currCount = curr.tasks.find((tc) => tc.status === s)?.count ?? 0;
      pt[s] = Math.max(0, currCount - prevCount);
    }
    taskTerminalDeltas.push(pt);
  }

  const workerCounts: ChartPoint[] = history.map((h) => {
    const t = new Date(h.timestamp).getTime();
    const pt: ChartPoint = { time: t, label: formatTime(t) };
    for (const s of workerStatuses) pt[s] = 0;
    for (const wc of h.workers) pt[wc.status] = wc.count;
    return pt;
  });

  return {
    vmCounts,
    taskCounts,
    taskTerminalDeltas,
    workerCounts,
    taskStatuses,
    terminalTaskStatuses,
    workerStatuses,
  };
}

// ── Shared UI components ──────────────────────────────────────────────────────

function DetailRow({
  label,
  value,
}: {
  label: string;
  value: React.ReactNode;
}) {
  return (
    <div
      style={{
        display: "flex",
        justifyContent: "space-between",
        gap: "1rem",
        padding: "5px 0",
        borderBottom: "1px solid #f0f0f0",
        fontSize: "0.8rem",
        fontFamily: MONO,
      }}
    >
      <span style={{ color: "#999", flexShrink: 0 }}>{label}</span>
      <span
        style={{ color: "#222", textAlign: "right", wordBreak: "break-all" }}
      >
        {value}
      </span>
    </div>
  );
}

function SectionHeader({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        fontSize: 10,
        letterSpacing: 2,
        color: "#aaa",
        fontFamily: MONO,
        marginTop: 12,
        marginBottom: 6,
        textTransform: "uppercase",
      }}
    >
      {children}
    </div>
  );
}

export function StatusBadge({
  status,
  colorMap,
}: {
  status: string;
  colorMap?: Record<string, string>;
}) {
  const color = colorMap?.[status];
  return (
    <span
      style={{
        background: color ? color + "22" : "#f0f0f0",
        color: color ?? "#555",
        borderRadius: 4,
        padding: "1px 8px",
        fontSize: "0.78rem",
        fontWeight: 600,
        whiteSpace: "nowrap",
      }}
    >
      {status}
    </span>
  );
}

const TH_STYLE: React.CSSProperties = {
  padding: "8px 14px",
  textAlign: "left",
  fontWeight: 600,
  color: "#555",
  fontSize: "0.78rem",
  fontFamily: MONO,
  borderBottom: "1px solid #e0e0e0",
  background: "#f8f9fa",
};

const TD_STYLE: React.CSSProperties = {
  padding: "8px 14px",
  fontSize: "0.8rem",
  fontFamily: MONO,
  borderBottom: "1px solid #f0f0f0",
  verticalAlign: "middle",
};

// ── Overview tab ──────────────────────────────────────────────────────────────

function WorkPoolPropertiesPanel({
  detail,
}: {
  detail: WorkPoolDetail | undefined;
}) {
  const dash = <span style={{ color: "#ccc" }}>—</span>;
  if (!detail) {
    return (
      <div
        style={{
          width: 300,
          flexShrink: 0,
          background: "#f8f9fa",
          border: "1px solid #e0e0e0",
          borderRadius: 8,
          padding: "0.75rem 1rem",
          color: "#ccc",
          fontFamily: MONO,
          fontSize: "0.8rem",
        }}
      >
        Loading…
      </div>
    );
  }

  const statusColorMap: Record<string, string> = {
    ok: "#2e7d32",
    idle: "#2e7d32",
    halted: "#b71c1c",
    unhealthy: "#b71c1c",
  };

  return (
    <div
      style={{
        width: 300,
        flexShrink: 0,
        background: "#f8f9fa",
        border: "1px solid #e0e0e0",
        borderRadius: 8,
        padding: "0.75rem 1rem",
      }}
    >
      <SectionHeader>Status</SectionHeader>
      <DetailRow
        label="status"
        value={
          <StatusBadge
            status={detail.status || "—"}
            colorMap={statusColorMap}
          />
        }
      />
      {detail.status_message && (
        <DetailRow label="message" value={detail.status_message} />
      )}
      <DetailRow label="incidents" value={detail.incident_count} />
      {detail.last_incident_at && (
        <DetailRow
          label="last incident"
          value={new Date(detail.last_incident_at).toLocaleString()}
        />
      )}

      <SectionHeader>Configuration</SectionHeader>
      <DetailRow label="machine type" value={detail.machine_type || dash} />
      <DetailRow label="region" value={detail.region || dash} />
      <DetailRow
        label="zones"
        value={detail.zones?.length ? detail.zones.join(", ") : dash}
      />
      <DetailRow label="root dir" value={detail.root_dir || dash} />
      <DetailRow label="max workers" value={detail.max_worker_count ?? dash} />
      <DetailRow
        label="max preemptible attempts"
        value={detail.max_preemptible_worker_attempts ?? dash}
      />

      {detail.resources?.length > 0 && (
        <>
          <SectionHeader>Resources</SectionHeader>
          {detail.resources.map((r) => (
            <DetailRow key={r.name} label={r.name} value={r.value} />
          ))}
        </>
      )}

      {detail.empty_volumes?.length > 0 && (
        <>
          <SectionHeader>Volumes</SectionHeader>
          {detail.empty_volumes.map((v, i) => (
            <DetailRow
              key={i}
              label={v.mount_point}
              value={`${v.type} · ${v.size_in_gb} GB`}
            />
          ))}
        </>
      )}
    </div>
  );
}

function OverviewTab({
  workpoolId,
  detail,
  history,
}: {
  workpoolId: string;
  detail: WorkPoolDetail | undefined;
  history: WorkPoolSummaryHistoryEntry[];
}) {
  const {
    vmCounts,
    taskCounts,
    taskTerminalDeltas,
    workerCounts,
    taskStatuses,
    terminalTaskStatuses,
    workerStatuses,
  } = useMemo(() => computeWorkPoolTimeSeries(history), [history]);

  const taskSeriesConfig = taskStatuses.map((s) => ({
    key: s,
    label: s,
    color: TASK_COLORS[s] ?? "#888",
  }));

  const terminalDeltaSeriesConfig = terminalTaskStatuses.map((s) => ({
    key: s,
    label: s,
    color: TASK_COLORS[s] ?? "#888",
  }));

  const workerSeriesConfig = workerStatuses.map((s) => ({
    key: s,
    label: s,
    color: WORKER_COLORS[s] ?? "#888",
  }));

  const hasCharts = vmCounts.length > 0;

  // suppress unused warning
  void workpoolId;

  return (
    <div style={{ display: "flex", gap: "1.5rem", alignItems: "flex-start" }}>
      <WorkPoolPropertiesPanel detail={detail} />

      <div style={{ flex: 1, minWidth: 0 }}>
        {hasCharts ? (
          <div
            style={{
              background: "#f8f9fa",
              border: "1px solid #e0e0e0",
              borderRadius: 8,
              padding: "1rem 1.5rem",
            }}
          >
            <MultiLineChart
              data={vmCounts}
              title="Expected VMs"
              yLabel="VMs"
              stacked
              series={[
                { key: "preemptible", label: "Preemptible", color: "#6a1b9a" },
                {
                  key: "nonPreemptible",
                  label: "Non-preemptible",
                  color: "#1565c0",
                },
              ]}
            />
            <div style={{ height: "1.25rem" }} />
            {taskSeriesConfig.length > 0 && (
              <>
                <MultiLineChart
                  data={taskCounts}
                  title="Tasks by Status"
                  yLabel="tasks"
                  stacked
                  series={taskSeriesConfig}
                />
                <div style={{ height: "1.25rem" }} />
              </>
            )}
            {terminalDeltaSeriesConfig.length > 0 &&
              taskTerminalDeltas.length > 0 && (
                <>
                  <MultiLineChart
                    data={taskTerminalDeltas}
                    title="Task Completions per Interval"
                    yLabel="tasks"
                    stacked
                    series={terminalDeltaSeriesConfig}
                  />
                  <div style={{ height: "1.25rem" }} />
                </>
              )}
            {workerSeriesConfig.length > 0 && (
              <MultiLineChart
                data={workerCounts}
                title="Workers by Status"
                yLabel="workers"
                stacked
                series={workerSeriesConfig}
              />
            )}
          </div>
        ) : (
          <div
            style={{
              background: "#f8f9fa",
              border: "1px solid #e0e0e0",
              borderRadius: 8,
              padding: "2rem",
              color: "#aaa",
              fontFamily: MONO,
              fontSize: "0.85rem",
              textAlign: "center",
            }}
          >
            No history data yet. Charts will appear once the workpool summary
            history is populated.
          </div>
        )}
      </div>
    </div>
  );
}

// ── Workers tab ───────────────────────────────────────────────────────────────

function WorkersTab({ workpoolId }: { workpoolId: string }) {
  const workers = useWorkers(workpoolId, true);

  return (
    <div
      style={{
        border: "1px solid #e0e0e0",
        borderRadius: 8,
        overflow: "hidden",
      }}
    >
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            <th style={TH_STYLE}>Worker ID</th>
            <th style={TH_STYLE}>Status</th>
            <th style={TH_STYLE}>Instance</th>
            <th style={TH_STYLE}>Batch</th>
            <th style={TH_STYLE}>Heartbeat Expiry</th>
          </tr>
        </thead>
        <tbody>
          {workers.length === 0 && (
            <tr>
              <td
                colSpan={5}
                style={{
                  ...TD_STYLE,
                  color: "#aaa",
                  textAlign: "center",
                  padding: "2rem",
                }}
              >
                No workers found.
              </td>
            </tr>
          )}
          {workers.map((w, i) => (
            <tr
              key={w.worker_id}
              style={{ background: i % 2 === 0 ? "#fff" : "#fafafa" }}
            >
              <td style={TD_STYLE}>
                <Link
                  to={`/workpools/${workpoolId}/workers/${w.worker_id}`}
                  style={{ color: "#1565c0", textDecoration: "none" }}
                >
                  {w.worker_id}
                </Link>
              </td>
              <td style={TD_STYLE}>
                <StatusBadge status={w.status} colorMap={WORKER_COLORS} />
              </td>
              <td style={{ ...TD_STYLE, color: "#666" }}>
                {w.instance_name || "—"}
              </td>
              <td style={TD_STYLE}>
                {w.batch_id ? (
                  <Link
                    to={`/workpools/${workpoolId}/batches/${w.batch_id}`}
                    style={{ color: "#1565c0", textDecoration: "none" }}
                  >
                    {w.batch_id}
                  </Link>
                ) : (
                  <span style={{ color: "#ccc" }}>—</span>
                )}
              </td>
              <td style={{ ...TD_STYLE, color: "#777" }}>
                {w.heartbeat_expiry
                  ? new Date(w.heartbeat_expiry).toLocaleString()
                  : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Batches tab ───────────────────────────────────────────────────────────────

function BatchesTab({ workpoolId }: { workpoolId: string }) {
  const batches = useBatches(workpoolId, true);

  return (
    <div
      style={{
        border: "1px solid #e0e0e0",
        borderRadius: 8,
        overflow: "hidden",
      }}
    >
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            <th style={TH_STYLE}>Batch ID</th>
            <th style={TH_STYLE}>Status</th>
            <th style={TH_STYLE}>Submitted</th>
            <th style={{ ...TH_STYLE, textAlign: "right" }}>Expected VMs</th>
            <th style={{ ...TH_STYLE, textAlign: "right" }}>Registered</th>
            <th style={TH_STYLE}>Preemptible</th>
            <th style={TH_STYLE}>Unhealthy</th>
          </tr>
        </thead>
        <tbody>
          {batches.length === 0 && (
            <tr>
              <td
                colSpan={7}
                style={{
                  ...TD_STYLE,
                  color: "#aaa",
                  textAlign: "center",
                  padding: "2rem",
                }}
              >
                No batch requests found.
              </td>
            </tr>
          )}
          {batches.map((b, i) => (
            <tr
              key={b.batch_id}
              style={{ background: i % 2 === 0 ? "#fff" : "#fafafa" }}
            >
              <td style={TD_STYLE}>
                <Link
                  to={`/workpools/${workpoolId}/batches/${b.batch_id}`}
                  style={{ color: "#1565c0", textDecoration: "none" }}
                >
                  {b.batch_id}
                </Link>
              </td>
              <td style={TD_STYLE}>
                <StatusBadge status={b.status} colorMap={BATCH_STATUS_COLORS} />
              </td>
              <td style={{ ...TD_STYLE, color: "#777" }}>
                {new Date(b.submitted_at).toLocaleString()}
              </td>
              <td style={{ ...TD_STYLE, textAlign: "right", color: "#333" }}>
                {b.expected_vm_count}
              </td>
              <td style={{ ...TD_STYLE, textAlign: "right", color: "#333" }}>
                {b.registered_worker_count}
              </td>
              <td
                style={{
                  ...TD_STYLE,
                  color: b.preemptible ? "#6a1b9a" : "#555",
                }}
              >
                {b.preemptible ? "yes" : "no"}
              </td>
              <td
                style={{ ...TD_STYLE, color: b.unhealthy ? "#b71c1c" : "#aaa" }}
              >
                {b.unhealthy ? "⚠ yes" : "no"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function WorkPoolDetailPage() {
  const { workpoolId } = useParams<{ workpoolId: string }>();
  const location = useLocation();

  const isWorkers = location.pathname.endsWith("/workers");
  const isBatches = location.pathname.endsWith("/batches");
  const isOverview = !isWorkers && !isBatches;

  const detail = useWorkPoolDetail(workpoolId);
  const history = useWorkPoolSummaryHistory(workpoolId);

  if (!workpoolId) {
    return (
      <div style={{ padding: "2rem", fontFamily: "monospace" }}>
        Invalid workpool ID.
      </div>
    );
  }

  const tabs = [
    { label: "Overview", href: `/workpools/${workpoolId}`, matchExact: true },
    {
      label: "Workers",
      href: `/workpools/${workpoolId}/workers`,
      matchExact: true,
    },
    {
      label: "Batch API Requests",
      href: `/workpools/${workpoolId}/batches`,
      matchExact: true,
    },
  ];

  return (
    <div style={{ padding: "2rem", fontFamily: "monospace" }}>
      <h1 style={{ margin: "0 0 1.5rem", fontSize: "1.3rem", fontWeight: 700 }}>
        {workpoolId}
      </h1>

      <TabBar tabs={tabs} />

      {isOverview && (
        <OverviewTab
          workpoolId={workpoolId}
          detail={detail}
          history={history}
        />
      )}
      {isWorkers && <WorkersTab workpoolId={workpoolId} />}
      {isBatches && <BatchesTab workpoolId={workpoolId} />}
    </div>
  );
}
