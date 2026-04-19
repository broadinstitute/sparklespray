import { useState, useEffect, useMemo } from "react";
import { useParams, Link } from "react-router-dom";
import { getJobTasks, getJobTaskCount } from "../data/events";
import type { TaskStatus } from "../data/events";
import { computeJobTimeSeries } from "../data/jobTimeSeries";
import { useEvents } from "../data/EventProvider";
import type { AnyEvent } from "../types";
import MultiLineChart from "../components/MultiLineChart";

const STATUS_COLORS: Record<TaskStatus, { bg: string; text: string }> = {
  pending: { bg: "#e3f2fd", text: "#1565c0" },
  claimed: { bg: "#fff3e0", text: "#e65100" },
  exec_started: { bg: "#f3e5f5", text: "#6a1b9a" },
  exec_complete: { bg: "#e8f5e9", text: "#2e7d32" },
  complete: { bg: "#e0f2f1", text: "#00695c" },
  orphaned: { bg: "#fbe9e7", text: "#bf360c" },
  failed: { bg: "#ffebee", text: "#b71c1c" },
  killed: { bg: "#eeeeee", text: "#555555" },
};

function StatusBadge({ status }: { status: TaskStatus }) {
  const { bg, text } = STATUS_COLORS[status];
  return (
    <span
      style={{
        background: bg,
        color: text,
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

export default function JobDetail() {
  const { jobId } = useParams<{ jobId: string }>();
  const { addEventListener } = useEvents();
  const [localEvents, setLocalEvents] = useState<AnyEvent[]>([]);

  useEffect(() => {
    if (!jobId) return;
    return addEventListener((newEvents) => {
      const relevant = newEvents.filter(
        (e) => "job_id" in e && (e as any).job_id === jobId
      );
      if (relevant.length > 0) setLocalEvents((prev) => [...prev, ...relevant]);
    });
  }, [addEventListener, jobId]);

  const tasks = useMemo(() => (jobId ? getJobTasks(localEvents, jobId) : []), [
    localEvents,
    jobId,
  ]);
  const { counts, rates } = useMemo(
    () =>
      jobId
        ? computeJobTimeSeries(localEvents, jobId)
        : { counts: [], rates: [] },
    [localEvents, jobId]
  );
  const totalTasks = useMemo(
    () => (jobId ? getJobTaskCount(localEvents, jobId) : 0),
    [localEvents, jobId]
  );

  if (!jobId) {
    return (
      <div style={{ padding: "2rem", fontFamily: "monospace" }}>
        Invalid job ID.
      </div>
    );
  }

  if (tasks.length === 0) {
    return (
      <div style={{ padding: "2rem", fontFamily: "monospace" }}>
        <Link
          to="/"
          style={{
            color: "#1565c0",
            textDecoration: "none",
            fontSize: "0.85rem",
          }}
        >
          ← All Jobs
        </Link>
        <p style={{ marginTop: "1rem" }}>
          Waiting for tasks for job: <strong>{jobId}</strong>
        </p>
      </div>
    );
  }

  const statusCounts: Partial<Record<TaskStatus, number>> = {};
  for (const t of tasks)
    statusCounts[t.status] = (statusCounts[t.status] ?? 0) + 1;

  const statusOrder: TaskStatus[] = [
    "complete",
    "failed",
    "killed",
    "exec_complete",
    "exec_started",
    "claimed",
    "orphaned",
    "pending",
  ];

  const doneTasks =
    (statusCounts.complete ?? 0) +
    (statusCounts.failed ?? 0) +
    (statusCounts.killed ?? 0);
  const jobStarted = localEvents.find((e) => e.type === "job_started");
  const elapsedMin = jobStarted
    ? (Date.now() - new Date(jobStarted.timestamp).getTime()) / 60_000
    : 0;
  const ratePerMin = elapsedMin > 0 ? doneTasks / elapsedMin : 0;
  const remaining = totalTasks - doneTasks;
  const etaDate =
    ratePerMin > 0
      ? new Date(Date.now() + (remaining / ratePerMin) * 60_000)
      : null;

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
          marginBottom: "1.5rem",
          display: "flex",
          alignItems: "baseline",
          justifyContent: "space-between",
        }}
      >
        <div>
          <div style={{ fontSize: "0.8rem", marginBottom: "0.25rem" }}>
            <Link to="/" style={{ color: "#1565c0", textDecoration: "none" }}>
              ← All Jobs
            </Link>
          </div>
          <h1 style={{ margin: 0, fontSize: "1.3rem", fontWeight: 700 }}>
            {jobId}
          </h1>
        </div>
        <Link
          to={`/job/${jobId}/perf-overview`}
          style={{
            color: "#1565c0",
            textDecoration: "none",
            fontSize: "0.85rem",
          }}
        >
          Completed Task Metrics →
        </Link>
      </div>

      {/* Status summary */}
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: "0.75rem",
          background: "#f8f9fa",
          border: "1px solid #e0e0e0",
          borderRadius: 8,
          padding: "1rem 1.5rem",
          marginBottom: "1.5rem",
        }}
      >
        <span style={{ color: "#888", alignSelf: "center" }}>
          {tasks.length} tasks
        </span>
        {statusOrder.map((s) =>
          statusCounts[s] ? (
            <span
              key={s}
              style={{ display: "flex", alignItems: "center", gap: 6 }}
            >
              <StatusBadge status={s} />
              <span style={{ color: "#555", fontSize: "0.85rem" }}>
                {statusCounts[s]}
              </span>
            </span>
          ) : null
        )}
        {ratePerMin > 0 && (
          <span
            style={{
              marginLeft: "auto",
              color: "#555",
              fontSize: "0.85rem",
              display: "flex",
              gap: "1.25rem",
            }}
          >
            <span>
              <span style={{ color: "#888" }}>rate </span>
              {ratePerMin.toFixed(2)} tasks/min
            </span>
            {etaDate && doneTasks < totalTasks && (
              <span>
                <span style={{ color: "#888" }}>ETA </span>
                {etaDate.toLocaleTimeString("en-US", {
                  hour: "2-digit",
                  minute: "2-digit",
                  second: "2-digit",
                })}
              </span>
            )}
          </span>
        )}
      </div>

      {/* Time-series charts */}
      {counts.length > 0 && (
        <div
          style={{
            background: "#f8f9fa",
            border: "1px solid #e0e0e0",
            borderRadius: 8,
            padding: "1rem 1.5rem",
            marginBottom: "1.5rem",
          }}
        >
          <MultiLineChart
            data={counts}
            title="Tasks in Queue"
            yLabel="tasks"
            stacked
            series={[
              { key: "pending", label: "Pending", color: "#1565c0" },
              { key: "running", label: "Running", color: "#e65100" },
            ]}
          />
          <div style={{ height: "1.25rem" }} />
          <MultiLineChart
            data={rates}
            title="Completion Rate"
            yLabel="tasks/min"
            series={[
              {
                key: "completedSuccess",
                label: "Completed (success)",
                color: "#2e7d32",
              },
              {
                key: "completedError",
                label: "Completed (error)",
                color: "#f44336",
              },
              { key: "orphaned", label: "Orphaned", color: "#f59e0b" },
              { key: "failed", label: "Failed", color: "#b71c1c" },
            ]}
          />
        </div>
      )}

      {/* Task table */}
      <div
        style={{
          border: "1px solid #e0e0e0",
          borderRadius: 8,
          overflow: "hidden",
          fontSize: "0.85rem",
        }}
      >
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr
              style={{
                background: "#f8f9fa",
                borderBottom: "1px solid #e0e0e0",
              }}
            >
              <th
                style={{
                  padding: "8px 16px",
                  textAlign: "left",
                  fontWeight: 600,
                  color: "#555",
                }}
              >
                Task ID
              </th>
              <th
                style={{
                  padding: "8px 16px",
                  textAlign: "left",
                  fontWeight: 600,
                  color: "#555",
                }}
              >
                Status
              </th>
              <th
                style={{
                  padding: "8px 16px",
                  textAlign: "left",
                  fontWeight: 600,
                  color: "#555",
                }}
              >
                Events
              </th>
              <th
                style={{
                  padding: "8px 16px",
                  textAlign: "left",
                  fontWeight: 600,
                  color: "#555",
                }}
              >
                Last Event
              </th>
            </tr>
          </thead>
          <tbody>
            {tasks.map((task, i) => {
              const lastEvent = task.events[task.events.length - 1];
              return (
                <tr
                  key={task.taskId}
                  style={{
                    borderBottom:
                      i < tasks.length - 1 ? "1px solid #f0f0f0" : "none",
                    background: i % 2 === 0 ? "#fff" : "#fafafa",
                  }}
                >
                  <td style={{ padding: "8px 16px" }}>
                    <Link
                      to={`/job/${jobId}/task/${task.taskId}`}
                      style={{ color: "#1565c0", textDecoration: "none" }}
                    >
                      {task.taskId}
                    </Link>
                  </td>
                  <td style={{ padding: "8px 16px" }}>
                    <StatusBadge status={task.status} />
                  </td>
                  <td style={{ padding: "8px 16px", color: "#777" }}>
                    {task.events.length}
                  </td>
                  <td
                    style={{
                      padding: "8px 16px",
                      color: "#999",
                      fontSize: "0.8rem",
                    }}
                  >
                    {new Date(lastEvent.timestamp).toLocaleString()}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
