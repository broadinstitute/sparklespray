import { useState, useEffect, useMemo, useRef } from "react";
import { useParams, useLocation } from "react-router-dom";
import { getTaskEvents, deriveStatus, extractTimings } from "../data/events";
import { useEvents, mergeEvents } from "../data/EventProvider";
import { useTaskPubsub } from "../data/useTaskPubsub";
import type { AnyEvent } from "../types";
import TaskProperties from "../components/TaskProperties";
import MultiLineChart from "../components/MultiLineChart";
import EventLog from "../components/EventLog";
import TabBar from "../components/TabBar";

const STATUS_COLORS: Record<string, { bg: string; text: string }> = {
  pending: { bg: "#e3f2fd", text: "#1565c0" },
  claimed: { bg: "#fff3e0", text: "#e65100" },
  exec_started: { bg: "#f3e5f5", text: "#6a1b9a" },
  exec_complete: { bg: "#e8f5e9", text: "#2e7d32" },
  complete: { bg: "#e0f2f1", text: "#00695c" },
  orphaned: { bg: "#fbe9e7", text: "#bf360c" },
  failed: { bg: "#ffebee", text: "#b71c1c" },
  killed: { bg: "#eeeeee", text: "#555555" },
};

export default function TaskDetail() {
  const { jobId, taskId } = useParams<{ jobId: string; taskId: string }>();
  const location = useLocation();
  const { addEventListener } = useEvents();
  const [localEvents, setLocalEvents] = useState<AnyEvent[]>([]);
  const [taskInfo, setTaskInfo] = useState<{
    command: string;
    dockerImage: string;
  } | null>(null);
  const logBottomRef = useRef<HTMLDivElement>(null);

  const taskBase = `/jobs/${jobId}/tasks/${taskId}`;
  const activeTab = location.pathname.endsWith("/metrics")
    ? "metrics"
    : location.pathname.endsWith("/log")
    ? "log"
    : "overview";

  useEffect(() => {
    if (!jobId || !taskId) return;
    return addEventListener((newEvents) => {
      const relevant = newEvents.filter(
        (e) =>
          "task_id" in e &&
          (e as any).task_id === taskId &&
          (e as any).job_id === jobId
      );
      if (relevant.length > 0)
        setLocalEvents((prev) => mergeEvents(prev, relevant));
    });
  }, [addEventListener, jobId, taskId]);

  useEffect(() => {
    if (!taskId) return;
    fetch(`/api/v1/task/${taskId}`)
      .then((r) => r.json())
      .then((d) =>
        setTaskInfo({
          command: d.command ?? "missing",
          dockerImage: d.docker_image ?? "missing",
        })
      )
      .catch(() => setTaskInfo({ command: "missing", dockerImage: "missing" }));
  }, [taskId]);

  const taskEvents = useMemo(
    () => (jobId && taskId ? getTaskEvents(localEvents, jobId, taskId) : []),
    [localEvents, jobId, taskId]
  );

  const status = useMemo(() => deriveStatus(taskEvents), [taskEvents]);
  const timings = useMemo(() => extractTimings(taskEvents), [taskEvents]);

  const isActive = ["claimed", "exec_started", "exec_complete"].includes(
    status
  );
  const { resourceData, logContent, error: pubsubError } = useTaskPubsub(
    taskId ?? "",
    isActive
  );

  useEffect(() => {
    if (activeTab === "log")
      logBottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [logContent, activeTab]);

  if (!jobId || !taskId) {
    return (
      <div style={{ padding: "2rem", fontFamily: "monospace" }}>
        Invalid URL parameters.
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

  const taskTabs = [
    { label: "Overview", href: taskBase, matchExact: true },
    { label: "Metrics", href: `${taskBase}/metrics`, matchExact: true },
    { label: "Log", href: `${taskBase}/log`, matchExact: true },
  ];

  if (taskEvents.length === 0) {
    return (
      <div
        style={{
          maxWidth: 960,
          margin: "0 auto",
          padding: "2rem",
          fontFamily: "monospace",
        }}
      >
        <TabBar tabs={jobTabs} />
        <p>
          Task not found or not yet started: <strong>{taskId}</strong>
        </p>
      </div>
    );
  }

  const statusStyle = STATUS_COLORS[status] ?? { bg: "#eee", text: "#333" };
  const command = taskInfo?.command ?? "…";
  const dockerImage = taskInfo?.dockerImage ?? "…";

  return (
    <div
      style={{
        maxWidth: 960,
        margin: "0 auto",
        padding: "2rem",
        fontFamily: "monospace",
      }}
    >
      {/* Job-level tabs */}
      <TabBar tabs={jobTabs} />

      {/* Task header */}
      <div style={{ marginBottom: "1.5rem" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "1rem" }}>
          <h1 style={{ margin: 0, fontSize: "1.4rem", fontWeight: 700 }}>
            {taskId}
          </h1>
          <span
            style={{
              background: statusStyle.bg,
              color: statusStyle.text,
              borderRadius: 6,
              padding: "2px 12px",
              fontWeight: 600,
              fontSize: "0.85rem",
            }}
          >
            {status}
          </span>
        </div>
      </div>

      {/* Task-level tabs */}
      <TabBar tabs={taskTabs} />

      {/* Pubsub error banner */}
      {pubsubError && (
        <div
          style={{
            background: "#ffebee",
            border: "1px solid #ef9a9a",
            borderRadius: 6,
            color: "#b71c1c",
            fontFamily: "monospace",
            fontSize: "0.85rem",
            padding: "0.6rem 1rem",
            marginBottom: "1rem",
          }}
        >
          Live data unavailable: {pubsubError}
        </div>
      )}

      {/* Overview tab: properties + events */}
      {activeTab === "overview" && (
        <>
          <TaskProperties
            command={command}
            dockerImage={dockerImage}
            timings={timings}
            status={status}
          />
          <div style={{ marginBottom: "2rem" }}>
            <EventLog events={taskEvents} />
          </div>
        </>
      )}

      {/* Metrics tab */}
      {activeTab === "metrics" && resourceData.length > 0 && (
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            gap: "2rem",
            marginBottom: "2rem",
          }}
        >
          <MultiLineChart
            data={resourceData}
            title="CPU Breakdown"
            yLabel="%"
            stacked
            series={[
              { key: "cpuUser", label: "user", color: "#1976d2" },
              { key: "cpuSystem", label: "system", color: "#e53935" },
              { key: "cpuIowait", label: "iowait", color: "#fb8c00" },
              { key: "cpuIdle", label: "idle", color: "#cfd8dc" },
            ]}
          />
          <MultiLineChart
            data={resourceData}
            title="Process Memory"
            yLabel="GB"
            series={[
              { key: "totalMemoryGb", label: "virtual", color: "#7c4dff" },
              { key: "totalResidentGb", label: "resident", color: "#ab47bc" },
              { key: "totalDataGb", label: "data", color: "#42a5f5" },
              { key: "totalSharedGb", label: "shared", color: "#80cbc4" },
            ]}
          />
          <MultiLineChart
            data={resourceData}
            title="System Memory"
            yLabel="GB"
            series={[
              { key: "memTotalGb", label: "total", color: "#bdbdbd" },
              { key: "memAvailableGb", label: "available", color: "#43a047" },
              { key: "memFreeGb", label: "free", color: "#00acc1" },
            ]}
          />
          <MultiLineChart
            data={resourceData}
            title="Memory Pressure"
            yLabel="%"
            series={[
              {
                key: "memPressureSomeAvg10",
                label: "some avg10",
                color: "#fb8c00",
              },
              {
                key: "memPressureFullAvg10",
                label: "full avg10",
                color: "#e53935",
              },
            ]}
          />
          <MultiLineChart
            data={resourceData}
            title="Process Count"
            yLabel="procs"
            series={[
              { key: "processCount", label: "processes", color: "#5c6bc0" },
            ]}
          />
        </div>
      )}
      {activeTab === "metrics" && resourceData.length === 0 && (
        <p
          style={{
            color: "#aaa",
            fontFamily: "monospace",
            fontSize: "0.85rem",
          }}
        >
          {isActive
            ? "Waiting for metrics…"
            : "No metrics available — task is not currently running."}
        </p>
      )}

      {/* Log tab */}
      {activeTab === "log" && (
        <div style={{ marginBottom: "2rem" }}>
          <div
            style={{
              background: "#282c34",
              borderRadius: 8,
              padding: "1rem",
              fontFamily: '"JetBrains Mono", "Fira Mono", monospace',
              fontSize: "0.8rem",
              lineHeight: 1.6,
              height: 420,
              overflowY: "auto",
              boxSizing: "border-box",
              whiteSpace: "pre-wrap",
              color: "#abb2bf",
            }}
          >
            {logContent || (
              <span style={{ color: "#5c6370", fontStyle: "italic" }}>
                No output yet…
              </span>
            )}
            <div ref={logBottomRef} />
          </div>
        </div>
      )}
    </div>
  );
}
