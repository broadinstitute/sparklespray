import { useState, useEffect, useMemo } from "react";
import { useParams, Link, useLocation } from "react-router-dom";
import { getJobTasks, getJobTaskCount, extractTimings } from "../data/events";
import type { TaskStatus } from "../data/events";
import { computeJobTimeSeries } from "../data/jobTimeSeries";
import { useEvents, mergeEvents } from "../data/EventProvider";
import type { AnyEvent } from "../types";
import MultiLineChart from "../components/MultiLineChart";
import TabBar from "../components/TabBar";

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
  const location = useLocation();
  const { addEventListener } = useEvents();
  const [localEvents, setLocalEvents] = useState<AnyEvent[]>([]);

  const isTasksTab = location.pathname.endsWith("/tasks");

  useEffect(() => {
    if (!jobId) return;
    return addEventListener((newEvents) => {
      const relevant = newEvents.filter(
        (e) => "job_id" in e && (e as any).job_id === jobId
      );
      if (relevant.length > 0)
        setLocalEvents((prev) => mergeEvents(prev, relevant));
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

  const jobTabs = [
    { label: "Overview", href: `/jobs/${jobId}`, matchExact: true },
    { label: "Tasks", href: `/jobs/${jobId}/tasks` },
    {
      label: "Completed Summary",
      href: `/jobs/${jobId}/summary`,
      matchExact: true,
    },
  ];

  if (tasks.length === 0) {
    return (
      <div
        style={{
          maxWidth: 960,
          margin: "0 auto",
          padding: "2rem",
          fontFamily: "monospace",
        }}
      >
        <h1
          style={{ margin: "0 0 1.5rem", fontSize: "1.3rem", fontWeight: 700 }}
        >
          {jobId}
        </h1>
        <TabBar tabs={jobTabs} />
        <p style={{ marginTop: "1rem", color: "#888" }}>Waiting for tasks…</p>
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
      <h1 style={{ margin: "0 0 1.5rem", fontSize: "1.3rem", fontWeight: 700 }}>
        {jobId}
      </h1>

      <TabBar tabs={jobTabs} />

      {/* Overview tab */}
      {!isTasksTab && (
        <>
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
        </>
      )}

      {/* Tasks tab */}
      {isTasksTab && (
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
                  title="The number of times this task was started. Values > 1 are signs that the job was re-run or the worker was preempted and the task was reattempted"
                  style={{
                    padding: "8px 16px",
                    textAlign: "left",
                    fontWeight: 600,
                    color: "#555",
                    cursor: "help",
                  }}
                >
                  Attempts
                </th>
                <th
                  style={{
                    padding: "8px 16px",
                    textAlign: "left",
                    fontWeight: 600,
                    color: "#555",
                  }}
                >
                  Exit Code
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
                const timings = extractTimings(task.events);
                const exitCode = timings.exitCode;
                const exitCodeDefined = exitCode !== undefined;
                const exitOk = exitCode === 0;
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
                        to={`/jobs/${jobId}/tasks/${task.taskId}`}
                        style={{ color: "#1565c0", textDecoration: "none" }}
                      >
                        {task.taskId}
                      </Link>
                    </td>
                    <td style={{ padding: "8px 16px" }}>
                      <StatusBadge status={task.status} />
                    </td>
                    <td style={{ padding: "8px 16px", color: "#777" }}>
                      {
                        task.events.filter((e) => e.type === "task_claimed")
                          .length
                      }
                    </td>
                    <td style={{ padding: "8px 16px" }}>
                      {exitCodeDefined ? (
                        <span
                          style={{
                            fontFamily: "monospace",
                            fontWeight: 600,
                            color: exitOk ? "#2e7d32" : "#c62828",
                          }}
                        >
                          {exitCode}
                        </span>
                      ) : (
                        <span style={{ color: "#ccc" }}>—</span>
                      )}
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
      )}
    </div>
  );
}
