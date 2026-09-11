import { useState, useEffect, useMemo } from "react";
import { useParams, useLocation, Link } from "react-router-dom";
import type { WorkPoolDetail, WorkPoolSummaryHistoryEntry } from "../types";
import MultiLineChart from "../components/MultiLineChart";
import TabBar from "../components/TabBar";
import { RangeRefreshBar } from "../components/RefreshControls";
import type { RangeChange } from "../components/RefreshControls";
import JobsTable from "../components/JobsTable";
import type { JobsTableRow } from "../components/JobsTable";
import EventsPanel from "../components/EventsPanel";
import { apiFetch } from "../api/client";

const MONO = "'IBM Plex Mono', monospace";

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
  /**
   * Project this batch's job and VMs live in, pinned at submission time.
   * Empty means the backend's own project.
   */
  project_id: string;
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
  workpoolId: string | undefined,
  active: boolean
): [WorkPoolDetail | undefined, () => void] {
  const [detail, setDetail] = useState<WorkPoolDetail | undefined>();
  // refreshNonce lets callers (e.g. after a successful edit) force an
  // immediate re-fetch instead of waiting for the next 10s poll tick.
  const [refreshNonce, setRefreshNonce] = useState(0);
  useEffect(() => {
    if (!workpoolId || !active) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await apiFetch(`/api/v1/workpool/${workpoolId}`);
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
  }, [workpoolId, active, refreshNonce]);
  const refetch = () => setRefreshNonce((n) => n + 1);
  return [detail, refetch];
}

function useWorkPoolSummaryHistory(
  workpoolId: string | undefined,
  active: boolean
): { history: WorkPoolSummaryHistoryEntry[]; lastUpdatedAt: number | null } {
  const [history, setHistory] = useState<WorkPoolSummaryHistoryEntry[]>([]);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | null>(null);
  useEffect(() => {
    if (!workpoolId || !active) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await apiFetch(
          `/api/v1/workpool/${workpoolId}/summary-history`
        );
        if (!r.ok || cancelled) return;
        const data = await r.json();
        if (!cancelled) {
          setHistory(data);
          setLastUpdatedAt(Date.now());
        }
      } catch (_) {}
    };
    poll();
    const id = setInterval(poll, 10000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [workpoolId, active]);
  return { history, lastUpdatedAt };
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
        const r = await apiFetch(
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

interface JobRecord {
  job_id: string;
  workpool_id: string;
  created_at: string;
  state: string;
  tasks: { state: string; count: number }[];
  labels: { name: string; value: string }[];
}

function useJobs(workpoolId: string | undefined, active: boolean): JobRecord[] {
  const [jobs, setJobs] = useState<JobRecord[]>([]);
  useEffect(() => {
    if (!workpoolId || !active) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await apiFetch(
          `/api/v1/jobs?workpool_id=${encodeURIComponent(workpoolId)}`
        );
        if (!r.ok || cancelled) return;
        const data = await r.json();
        if (!cancelled) setJobs(data);
      } catch (_) {}
    };
    poll();
    const id = setInterval(poll, 10000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [workpoolId, active]);
  return jobs;
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
        const r = await apiFetch(`/api/v1/workpool/${workpoolId}/batches`);
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
  workerCounts: ChartPoint[];
  taskCounts: ChartPoint[];
  taskTerminalDeltas: ChartPoint[];
  taskStatuses: string[];
  terminalTaskStatuses: string[];
} {
  const empty = {
    workerCounts: [],
    taskCounts: [],
    taskTerminalDeltas: [],
    taskStatuses: [],
    terminalTaskStatuses: [],
  };
  if (history.length === 0) return empty;

  const TERMINAL_TASK = new Set(["error", "failed", "success", "killed"]);

  const taskStatusSet = new Set<string>();
  const terminalTaskStatusSet = new Set<string>();

  for (const h of history) {
    for (const tc of h.tasks ?? []) {
      if (TERMINAL_TASK.has(tc.state)) terminalTaskStatusSet.add(tc.state);
      else taskStatusSet.add(tc.state);
    }
  }

  const taskStatuses = Array.from(taskStatusSet);
  const terminalTaskStatuses = Array.from(terminalTaskStatusSet);

  // Combined worker chart: started_preemptible, started_nonpreemptible,
  // pending_preemptible (expected - started), pending_nonpreemptible
  const workerCounts: ChartPoint[] = history.map((h) => {
    const t = new Date(h.timestamp).getTime();
    const startedP =
      (h.preemptible_workers ?? []).find((w) => w.state === "started")?.count ??
      0;
    const startedNP =
      (h.nonpreemptible_workers ?? []).find((w) => w.state === "started")
        ?.count ?? 0;
    const pendingP = Math.max(0, h.expected_preemptible_workers - startedP);
    const pendingNP = Math.max(
      0,
      h.expected_nonpreemptible_workers - startedNP
    );
    return {
      time: t,
      label: formatTime(t),
      started_preemptible: startedP,
      started_nonpreemptible: startedNP,
      pending_preemptible: pendingP,
      pending_nonpreemptible: pendingNP,
    };
  });

  const taskCounts: ChartPoint[] = history.map((h) => {
    const t = new Date(h.timestamp).getTime();
    const pt: ChartPoint = { time: t, label: formatTime(t) };
    for (const s of taskStatuses) pt[s] = 0;
    for (const tc of h.tasks ?? [])
      if (!TERMINAL_TASK.has(tc.state)) pt[tc.state] = tc.count;
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
      const prevCount =
        (prev.tasks ?? []).find((tc) => tc.state === s)?.count ?? 0;
      const currCount =
        (curr.tasks ?? []).find((tc) => tc.state === s)?.count ?? 0;
      pt[s] = Math.max(0, currCount - prevCount);
    }
    taskTerminalDeltas.push(pt);
  }

  return {
    workerCounts,
    taskCounts,
    taskTerminalDeltas,
    taskStatuses,
    terminalTaskStatuses,
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

// Fields editable via PATCH /api/v1/workpool/{workpool_id}. `label` is the
// display name used both in the read-only DetailRow and as the edit-form
// field label.
const PROVISIONING_FIELDS: {
  key:
    | "max_worker_count"
    | "max_preemptible_worker_attempts"
    | "max_workers_per_request"
    | "max_zombies_before_abort"
    | "max_consecutive_failed_batches";
  label: string;
}[] = [
  { key: "max_worker_count", label: "max worker count" },
  {
    key: "max_preemptible_worker_attempts",
    label: "max preemptible worker attempts",
  },
  { key: "max_workers_per_request", label: "max workers per request" },
  { key: "max_zombies_before_abort", label: "max zombies before abort" },
  {
    key: "max_consecutive_failed_batches",
    label: "max consecutive failed batches",
  },
];

function EditIconButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      title="Edit"
      style={{
        all: "unset",
        cursor: "pointer",
        color: "#1565c0",
        fontSize: "0.7rem",
        fontFamily: MONO,
        letterSpacing: 1,
        textTransform: "uppercase",
      }}
    >
      edit
    </button>
  );
}

function ProvisioningSection({
  workpoolId,
  detail,
  onWorkpoolUpdated,
}: {
  workpoolId: string;
  detail: WorkPoolDetail;
  onWorkpoolUpdated: () => void;
}) {
  const [editing, setEditing] = useState(false);
  const [values, setValues] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const startEditing = () => {
    const initial: Record<string, string> = {};
    for (const f of PROVISIONING_FIELDS) {
      initial[f.key] = String(detail[f.key] ?? 0);
    }
    setValues(initial);
    setError(null);
    setEditing(true);
  };

  const cancelEditing = () => {
    setEditing(false);
    setError(null);
  };

  const save = async () => {
    const body: Record<string, number> = {};
    for (const f of PROVISIONING_FIELDS) {
      const raw = (values[f.key] ?? "").trim();
      const n = Number(raw);
      if (raw === "" || !Number.isInteger(n) || n < 0) {
        setError(`"${f.label}" must be a non-negative whole number`);
        return;
      }
      body[f.key] = n;
    }
    setSaving(true);
    setError(null);
    try {
      const r = await apiFetch(`/api/v1/workpool/${workpoolId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const errBody = await r.json().catch(() => null);
        setError(errBody?.error || `Save failed (status ${r.status})`);
        return;
      }
      setEditing(false);
      onWorkpoolUpdated();
    } catch (_) {
      setError("Save failed: could not reach the server");
    } finally {
      setSaving(false);
    }
  };

  return (
    <>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "baseline",
        }}
      >
        <SectionHeader>Provisioning &amp; Watchdog</SectionHeader>
        {!editing && <EditIconButton onClick={startEditing} />}
      </div>

      {!editing ? (
        PROVISIONING_FIELDS.map((f) => (
          <DetailRow key={f.key} label={f.label} value={detail[f.key] ?? 0} />
        ))
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
          {PROVISIONING_FIELDS.map((f) => (
            <div
              key={f.key}
              style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                gap: "0.75rem",
                fontSize: "0.8rem",
                fontFamily: MONO,
              }}
            >
              <label style={{ color: "#999", flexShrink: 0 }}>{f.label}</label>
              <input
                type="number"
                min={0}
                step={1}
                value={values[f.key] ?? ""}
                onChange={(e) =>
                  setValues((v) => ({ ...v, [f.key]: e.target.value }))
                }
                style={{
                  width: 90,
                  padding: "3px 6px",
                  fontFamily: MONO,
                  fontSize: "0.8rem",
                  border: "1px solid #ccc",
                  borderRadius: 4,
                  textAlign: "right",
                }}
              />
            </div>
          ))}

          {error && (
            <div
              style={{
                color: "#b71c1c",
                fontSize: "0.75rem",
                fontFamily: MONO,
              }}
            >
              {error}
            </div>
          )}

          <div style={{ display: "flex", gap: 8, marginTop: 4 }}>
            <button
              onClick={save}
              disabled={saving}
              style={{
                padding: "4px 10px",
                fontFamily: MONO,
                fontSize: "0.75rem",
                color: "#fff",
                background: saving ? "#90caf9" : "#1565c0",
                border: "none",
                borderRadius: 4,
                cursor: saving ? "default" : "pointer",
              }}
            >
              {saving ? "saving…" : "save"}
            </button>
            <button
              onClick={cancelEditing}
              disabled={saving}
              style={{
                padding: "4px 10px",
                fontFamily: MONO,
                fontSize: "0.75rem",
                color: "#555",
                background: "#fff",
                border: "1px solid #ccc",
                borderRadius: 4,
                cursor: saving ? "default" : "pointer",
              }}
            >
              cancel
            </button>
          </div>
        </div>
      )}
    </>
  );
}

function WorkPoolPropertiesPanel({
  workpoolId,
  detail,
  onWorkpoolUpdated,
}: {
  workpoolId: string;
  detail: WorkPoolDetail | undefined;
  onWorkpoolUpdated: () => void;
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
          <StatusBadge status={detail.state || "—"} colorMap={statusColorMap} />
        }
      />
      {detail.state_message && (
        <DetailRow label="message" value={detail.state_message} />
      )}
      <DetailRow label="incidents" value={detail.incident_count} />
      {detail.last_incident_at && (
        <DetailRow
          label="last incident"
          value={new Date(detail.last_incident_at).toLocaleString()}
        />
      )}

      <SectionHeader>Configuration</SectionHeader>
      <DetailRow
        label="project"
        value={
          detail.project_id || (
            // Empty means the workpool never overrode it, so its VMs run in
            // whichever project the backend was started with.
            <span style={{ color: "#999" }}>backend default</span>
          )
        }
      />
      <DetailRow label="machine type" value={detail.machine_type || dash} />
      <DetailRow label="region" value={detail.region || dash} />
      <DetailRow
        label="zones"
        value={detail.zones?.length ? detail.zones.join(", ") : dash}
      />
      <DetailRow label="root dir" value={detail.root_dir || dash} />

      <ProvisioningSection
        workpoolId={workpoolId}
        detail={detail}
        onWorkpoolUpdated={onWorkpoolUpdated}
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

      {detail.labels?.length > 0 && (
        <>
          <SectionHeader>Labels</SectionHeader>
          {detail.labels.map((l) => (
            <DetailRow key={l.name} label={l.name} value={l.value} />
          ))}
        </>
      )}
    </div>
  );
}

function OverviewTab({ workpoolId }: { workpoolId: string }) {
  const [range, setRange] = useState<RangeChange | null>(null);

  const [detail, refetchDetail] = useWorkPoolDetail(
    workpoolId,
    range?.live ?? true
  );
  const { history: fullHistory, lastUpdatedAt } = useWorkPoolSummaryHistory(
    workpoolId,
    range?.live ?? true
  );

  const earliestMs = useMemo(
    () =>
      fullHistory.length > 0
        ? new Date(fullHistory[0].timestamp).getTime()
        : null,
    [fullHistory]
  );

  // The most recent history entry's timestamp mirrors WorkPoolSummary.last_updated
  // (the monitor writes both from the same value whenever it recomputes the summary).
  const poolLastUpdatedMs = useMemo(
    () =>
      fullHistory.length > 0
        ? new Date(fullHistory[fullHistory.length - 1].timestamp).getTime()
        : null,
    [fullHistory]
  );

  const history = useMemo(() => {
    if (!range) return fullHistory;
    return fullHistory.filter((h) => {
      const t = new Date(h.timestamp).getTime();
      return t >= range.startMs && t <= range.endMs;
    });
  }, [fullHistory, range]);

  const {
    workerCounts,
    taskCounts,
    taskTerminalDeltas,
    taskStatuses,
    terminalTaskStatuses,
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

  const hasCharts = fullHistory.length > 0;
  const xDomain: [number, number] | undefined = range
    ? [range.startMs, range.endMs]
    : undefined;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1.25rem" }}>
      <RangeRefreshBar
        anchorLabel="Pool start"
        anchorMs={earliestMs}
        endAnchorLabel="Pool end"
        endAnchorMs={poolLastUpdatedMs}
        lastUpdatedAt={lastUpdatedAt}
        onChange={setRange}
      />

      <div style={{ display: "flex", gap: "1.5rem", alignItems: "flex-start" }}>
        <WorkPoolPropertiesPanel
          workpoolId={workpoolId}
          detail={detail}
          onWorkpoolUpdated={refetchDetail}
        />

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
                data={workerCounts}
                title="Workers"
                yLabel="workers"
                stacked
                xDomain={xDomain}
                series={[
                  {
                    key: "started_preemptible",
                    label: "Started (preemptible)",
                    color: "#6a1b9a",
                  },
                  {
                    key: "started_nonpreemptible",
                    label: "Started (non-preemptible)",
                    color: "#1565c0",
                  },
                  {
                    key: "pending_preemptible",
                    label: "Pending (preemptible)",
                    color: "#ce93d8",
                  },
                  {
                    key: "pending_nonpreemptible",
                    label: "Pending (non-preemptible)",
                    color: "#90caf9",
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
                    xDomain={xDomain}
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
                      xDomain={xDomain}
                      series={terminalDeltaSeriesConfig}
                    />
                    <div style={{ height: "1.25rem" }} />
                  </>
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

// ── Jobs tab ──────────────────────────────────────────────────────────────────

function JobsTab({ workpoolId }: { workpoolId: string }) {
  const jobs = useJobs(workpoolId, true);

  const rows: JobsTableRow[] = jobs.map((j) => ({
    job_id: j.job_id,
    workpool_id: j.workpool_id,
    created_at: j.created_at,
    tasks: j.tasks,
    metadata: Object.fromEntries(j.labels.map((l) => [l.name, l.value])),
  }));

  return <JobsTable jobs={rows} emptyMessage="No jobs found." />;
}

// ── Events tab ────────────────────────────────────────────────────────────────

function EventsTab({ workpoolId }: { workpoolId: string }) {
  return <EventsPanel filterField="workpool_id" filterValue={workpoolId} />;
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function WorkPoolDetailPage() {
  const { workpoolId } = useParams<{ workpoolId: string }>();
  const location = useLocation();

  const isWorkers = location.pathname.endsWith("/workers");
  const isBatches = location.pathname.endsWith("/batches");
  const isJobs = location.pathname.endsWith("/jobs");
  const isEvents = location.pathname.endsWith("/events");
  const isOverview = !isWorkers && !isBatches && !isJobs && !isEvents;

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
    {
      label: "Jobs",
      href: `/workpools/${workpoolId}/jobs`,
      matchExact: true,
    },
    {
      label: "Events",
      href: `/workpools/${workpoolId}/events`,
      matchExact: true,
    },
  ];

  return (
    <div style={{ padding: "2rem", fontFamily: "monospace" }}>
      <h1 style={{ margin: "0 0 1.5rem", fontSize: "1.3rem", fontWeight: 700 }}>
        {workpoolId}
      </h1>

      <TabBar tabs={tabs} />

      {isOverview && <OverviewTab workpoolId={workpoolId} />}
      {isWorkers && <WorkersTab workpoolId={workpoolId} />}
      {isBatches && <BatchesTab workpoolId={workpoolId} />}
      {isJobs && <JobsTab workpoolId={workpoolId} />}
      {isEvents && <EventsTab workpoolId={workpoolId} />}
    </div>
  );
}
