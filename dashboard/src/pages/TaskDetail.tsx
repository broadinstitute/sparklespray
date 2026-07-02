import { useState, useEffect, useMemo, useRef, Fragment } from "react";
import { useParams, useLocation } from "react-router-dom";
import { getTaskEvents, deriveStatus, extractTimings } from "../data/events";
import { useEvents, mergeEvents } from "../data/EventProvider";
import { useTaskLog } from "../data/useTaskLog";
import type { AnyEvent } from "../types";
import TaskProperties from "../components/TaskProperties";
import MultiLineChart from "../components/MultiLineChart";
import EventLog from "../components/EventLog";
import TabBar from "../components/TabBar";

const MONO = "'IBM Plex Mono', monospace";
const SANS = "'IBM Plex Sans', sans-serif";

const STATUS_PILL: Record<string, { bg: string; text: string; dot: string }> = {
  success: { bg: "#e7f5ec", text: "#1a7f44", dot: "#2ea05f" },
  error: { bg: "#fdecea", text: "#c62828", dot: "#f44336" },
  failed: { bg: "#fdecea", text: "#c62828", dot: "#f44336" },
  killed: { bg: "#f5f5f5", text: "#616161", dot: "#9e9e9e" },
  running: { bg: "#e8f0fe", text: "#1565c0", dot: "#2f6fdb" },
  writing: { bg: "#e8f5e9", text: "#2e7d32", dot: "#4caf50" },
  claimed: { bg: "#fff3e0", text: "#e65100", dot: "#fb8c00" },
  pending: { bg: "#f5f5f5", text: "#757575", dot: "#bdbdbd" },
};

export default function TaskDetail() {
  const { jobId, taskId } = useParams<{ jobId: string; taskId: string }>();
  const location = useLocation();
  const { addJobEventListener } = useEvents();
  const [localEvents, setLocalEvents] = useState<AnyEvent[]>([]);
  const [taskInfo, setTaskInfo] = useState<{
    command: string;
    dockerImage: string;
    logPath: string;
    resultPath: string;
    exitCode: number | null;
    failureReason: string;
    labels: { name: string; value: string }[];
    resourceUsage: {
      elapsed_seconds: number;
      max_memory_bytes: number;
      cpu_user_usec: number;
      cpu_system_usec: number;
      block_read_bytes: number;
      block_write_bytes: number;
      oom_killed: boolean;
    } | null;
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
    return addJobEventListener(jobId, (newEvents) => {
      const relevant = newEvents.filter(
        (e) => "task_id" in e && (e as any).task_id === taskId
      );
      if (relevant.length > 0)
        setLocalEvents((prev) => mergeEvents(prev, relevant));
    });
  }, [addJobEventListener, jobId, taskId]);

  useEffect(() => {
    if (!taskId) return;
    fetch(`/api/v1/task/${taskId}`)
      .then((r) => r.json())
      .then((d) =>
        setTaskInfo({
          command: Array.isArray(d.command)
            ? d.command.join(" ")
            : d.command ?? "",
          dockerImage: d.docker_image ?? "",
          logPath: d.log_path ?? "",
          resultPath: d.result_path ?? "",
          exitCode: d.exit_code != null ? d.exit_code : null,
          failureReason: d.failure_reason ?? "",
          labels: Array.isArray(d.labels) ? d.labels : [],
          resourceUsage: d.resource_usage ?? null,
        })
      )
      .catch(() =>
        setTaskInfo({
          command: "",
          dockerImage: "",
          logPath: "",
          resultPath: "",
          exitCode: null,
          failureReason: "",
          labels: [],
          resourceUsage: null,
        })
      );
  }, [taskId]);

  const taskEvents = useMemo(
    () => (jobId && taskId ? getTaskEvents(localEvents, jobId, taskId) : []),
    [localEvents, jobId, taskId]
  );

  const status = useMemo(() => deriveStatus(taskEvents), [taskEvents]);
  const timings = useMemo(() => extractTimings(taskEvents), [taskEvents]);

  const isActive = ["claimed", "running", "writing"].includes(status);
  const { resourceData, logContent, error: pubsubError } = useTaskLog(
    taskId ?? "",
    isActive
  );

  const volumeSeries = useMemo(() => {
    const locations = Array.from(
      new Set(resourceData.flatMap((p) => p.volumes.map((v) => v.location)))
    );
    return locations.map((loc) => ({
      location: loc,
      data: resourceData.map((p) => {
        const v = p.volumes.find((v) => v.location === loc);
        return {
          time: p.time,
          label: p.label,
          usedGb: v ? Math.round(v.usedGb * 100) / 100 : 0,
          totalGb: v ? Math.round(v.totalGb * 100) / 100 : 0,
        };
      }),
    }));
  }, [resourceData]);

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

  const pill = STATUS_PILL[status] ?? {
    bg: "#f5f5f5",
    text: "#555",
    dot: "#9e9e9e",
  };

  return (
    <div style={{ padding: "2rem", fontFamily: MONO }}>
      {/* Job-level tabs */}
      <TabBar tabs={jobTabs} />

      {/* Task header */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 14,
          marginBottom: "1.5rem",
          flexWrap: "wrap",
        }}
      >
        <h1
          style={{
            margin: 0,
            font: `500 22px ${MONO}`,
            color: "#16191d",
            letterSpacing: "-0.01em",
          }}
        >
          {taskId}
        </h1>
        <span
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: 7,
            height: 24,
            padding: "0 11px",
            borderRadius: 999,
            background: pill.bg,
            color: pill.text,
            font: `600 12px ${SANS}`,
          }}
        >
          <span
            style={{
              width: 7,
              height: 7,
              borderRadius: "50%",
              background: pill.dot,
              flexShrink: 0,
            }}
          />
          {status}
        </span>
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
            command={taskInfo?.command ?? ""}
            dockerImage={taskInfo?.dockerImage ?? ""}
            logPath={taskInfo?.logPath ?? ""}
            resultPath={taskInfo?.resultPath ?? ""}
            exitCode={taskInfo?.exitCode ?? null}
            failureReason={taskInfo?.failureReason ?? ""}
            labels={taskInfo?.labels ?? []}
            resourceUsage={taskInfo?.resourceUsage ?? null}
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
            title="CPU Breakdown (% of one core)"
            yLabel="%/core"
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
          {volumeSeries.map((vs) => (
            <Fragment key={vs.location}>
              <MultiLineChart
                data={vs.data}
                title={`Disk: ${vs.location}`}
                yLabel="GB"
                series={[
                  { key: "totalGb", label: "total", color: "#bdbdbd" },
                  { key: "usedGb", label: "used", color: "#f4511e" },
                ]}
              />
            </Fragment>
          ))}
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
          No metrics available for this task.
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
