import { useState, useEffect, useMemo } from "react";
import { useParams, Link, useLocation } from "react-router-dom";
import { getJobTasks, getJobTaskCount, extractTimings } from "../data/events";
import type { TaskStatus } from "../data/events";
import { computeJobTimeSeries } from "../data/jobTimeSeries";
import { useEvents, mergeEvents } from "../data/EventProvider";
import type { AnyEvent, JobDetail } from "../types";
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

const HIDDEN_LABEL_KEYS = new Set([
  "UUID",
  "job-env-sha256",
  "job-spec-sha256",
]);

const MONO = "'JetBrains Mono', 'Courier New', monospace";

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

interface JobDetailsPanelProps {
  jobDetail: JobDetail | undefined;
  statusCounts: Partial<Record<TaskStatus, number>>;
  statusOrder: TaskStatus[];
  ratePerMin: number;
  etaDate: Date | null;
  totalTasks: number;
  doneTasks: number;
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

function JobDetailsPanel({
  jobDetail,
  statusCounts,
  statusOrder,
  ratePerMin,
  etaDate,
  totalTasks,
  doneTasks,
}: JobDetailsPanelProps) {
  const dash = <span style={{ color: "#ccc" }}>—</span>;
  const labels = jobDetail?.metadata
    ? Object.entries(jobDetail.metadata).filter(
        ([k]) => !HIDDEN_LABEL_KEYS.has(k)
      )
    : [];
  const clusterId = jobDetail?.cluster_id;

  return (
    <div
      style={{
        width: 280,
        flexShrink: 0,
        background: "#f8f9fa",
        border: "1px solid #e0e0e0",
        borderRadius: 8,
        padding: "0.75rem 1rem",
      }}
    >
      <SectionHeader>Status</SectionHeader>
      {statusOrder.map((s) =>
        statusCounts[s] ? (
          <DetailRow
            key={s}
            label={s}
            value={
              <span
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 6,
                  justifyContent: "flex-end",
                }}
              >
                <StatusBadge status={s} />
                <span style={{ color: "#555" }}>{statusCounts[s]}</span>
              </span>
            }
          />
        ) : null
      )}
      {ratePerMin > 0 && (
        <>
          <DetailRow
            label="rate"
            value={`${ratePerMin.toFixed(2)} tasks/min`}
          />
          {etaDate && doneTasks < totalTasks && (
            <DetailRow
              label="ETA"
              value={etaDate.toLocaleTimeString("en-US", {
                hour: "2-digit",
                minute: "2-digit",
                second: "2-digit",
              })}
            />
          )}
        </>
      )}

      <SectionHeader>Job Details</SectionHeader>
      <DetailRow
        label="submitted"
        value={
          jobDetail ? new Date(jobDetail.submit_time).toLocaleString() : dash
        }
      />
      <DetailRow
        label="tasks"
        value={jobDetail ? jobDetail.task_count : dash}
      />
      <DetailRow
        label="max workers to start"
        value={jobDetail ? jobDetail.target_node_count : dash}
      />
      <DetailRow
        label="max preemptable"
        value={jobDetail ? jobDetail.max_preemptable_attempts : dash}
      />
      <DetailRow
        label="cluster"
        value={
          clusterId ? (
            <Link
              to={`/clusters/${clusterId}`}
              style={{ color: "#1565c0", textDecoration: "none" }}
            >
              {clusterId}
            </Link>
          ) : (
            dash
          )
        }
      />

      {labels.length > 0 && (
        <>
          <SectionHeader>Labels</SectionHeader>
          {labels.map(([k, v]) => (
            <DetailRow key={k} label={k} value={v} />
          ))}
        </>
      )}
    </div>
  );
}

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
  const { addJobEventListener, jobCache } = useEvents();
  const [localEvents, setLocalEvents] = useState<AnyEvent[]>([]);

  const isTasksTab = location.pathname.endsWith("/tasks");

  useEffect(() => {
    if (!jobId) return;
    return addJobEventListener(jobId, (newEvents) =>
      setLocalEvents((prev) => mergeEvents(prev, newEvents))
    );
  }, [addJobEventListener, jobId]);

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
      <div style={{ padding: "2rem", fontFamily: "monospace" }}>
        <h1
          style={{ margin: "0 0 1.5rem", fontSize: "1.3rem", fontWeight: 700 }}
        >
          {jobId}
        </h1>
        <TabBar tabs={jobTabs} />
        <div
          style={{
            display: "flex",
            gap: "1.5rem",
            alignItems: "flex-start",
            marginTop: "1rem",
          }}
        >
          <JobDetailsPanel
            jobDetail={jobCache[jobId]}
            statusCounts={{}}
            statusOrder={[]}
            ratePerMin={0}
            etaDate={null}
            totalTasks={0}
            doneTasks={0}
          />
          <div
            style={{
              flex: 1,
              minWidth: 0,
              color: "#888",
              fontFamily: "monospace",
              paddingTop: "0.5rem",
            }}
          >
            Waiting for tasks…
          </div>
        </div>
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
        <div
          style={{ display: "flex", gap: "1.5rem", alignItems: "flex-start" }}
        >
          {/* Left: job details + status */}
          <JobDetailsPanel
            jobDetail={jobId ? jobCache[jobId] : undefined}
            statusCounts={statusCounts}
            statusOrder={statusOrder}
            ratePerMin={ratePerMin}
            etaDate={etaDate}
            totalTasks={totalTasks}
            doneTasks={doneTasks}
          />

          {/* Right: charts */}
          <div style={{ flex: 1, minWidth: 0 }}>
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
          </div>
        </div>
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
