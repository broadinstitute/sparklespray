import { useState, useEffect, useMemo, useRef } from "react";
import { useParams, useLocation } from "react-router-dom";
import { getTaskEvents, deriveStatus, extractTimings } from "../data/events";
import { useEvents, mergeEvents } from "../data/EventProvider";
import { useTaskLog } from "../data/useTaskLog";
import type { AnyEvent, ResourceUsageSummary } from "../types";
import TaskProperties from "../components/TaskProperties";
import MetricsPanel from "../components/MetricsPanel";
import EventLog from "../components/EventLog";
import TabBar from "../components/TabBar";
import { RangeRefreshBar, RefreshToggle } from "../components/RefreshControls";
import type { RangeChange } from "../components/RefreshControls";
import { apiFetch } from "../api/client";

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
    vmConsoleUrl: string;
    exitCode: number | null;
    failureReason: string;
    labels: { name: string; value: string }[];
    workpoolId: string;
    resourceUsage: ResourceUsageSummary | null;
  } | null>(null);
  const logBottomRef = useRef<HTMLDivElement>(null);
  const isNearBottomRef = useRef(true);

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
    apiFetch(`/api/v1/task/${taskId}`)
      .then((r) => r.json())
      .then((d) =>
        setTaskInfo({
          command: Array.isArray(d.command)
            ? d.command.join(" ")
            : d.command ?? "",
          dockerImage: d.docker_image ?? "",
          logPath: d.log_path ?? "",
          resultPath: d.result_path ?? "",
          vmConsoleUrl: d.vm_console_url ?? "",
          exitCode: d.exit_code != null ? d.exit_code : null,
          failureReason: d.failure_reason ?? "",
          labels: Array.isArray(d.labels) ? d.labels : [],
          workpoolId: d.workpool_id ?? "",
          resourceUsage: d.resource_usage ?? null,
        })
      )
      .catch(() =>
        setTaskInfo({
          command: "",
          dockerImage: "",
          logPath: "",
          resultPath: "",
          vmConsoleUrl: "",
          exitCode: null,
          failureReason: "",
          labels: [],
          workpoolId: "",
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
  const [range, setRange] = useState<RangeChange | null>(null);
  const [live, setLive] = useState(true);
  const paused = !live;
  const {
    resourceData: fullResourceData,
    logContent,
    error: pubsubError,
    lastUpdatedAt,
  } = useTaskLog(taskId ?? "", isActive, paused);

  const taskStartMs =
    timings.running?.getTime() ?? timings.claimed?.getTime() ?? null;

  const resourceData = useMemo(() => {
    if (!range) return fullResourceData;
    return fullResourceData.filter(
      (p) => p.time >= range.startMs && p.time <= range.endMs
    );
  }, [fullResourceData, range]);

  const xDomain: [number, number] | undefined = range
    ? [range.startMs, range.endMs]
    : undefined;

  useEffect(() => {
    if (activeTab === "log" && isNearBottomRef.current)
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
            vmConsoleUrl={taskInfo?.vmConsoleUrl ?? ""}
            exitCode={taskInfo?.exitCode ?? null}
            failureReason={taskInfo?.failureReason ?? ""}
            labels={taskInfo?.labels ?? []}
            workpoolId={taskInfo?.workpoolId ?? ""}
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
      {activeTab === "metrics" && (
        <div style={{ marginBottom: "1.25rem" }}>
          <RangeRefreshBar
            anchorLabel="Task start"
            anchorMs={taskStartMs}
            lastUpdatedAt={lastUpdatedAt}
            onChange={setRange}
            live={live}
            onLiveChange={setLive}
          />
        </div>
      )}
      {activeTab === "metrics" && resourceData.length > 0 && (
        <MetricsPanel resourceData={resourceData} xDomain={xDomain} />
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
          <div style={{ marginBottom: "0.75rem" }}>
            <RefreshToggle
              live={live}
              onToggle={() => setLive(!live)}
              lastUpdatedAt={lastUpdatedAt}
            />
          </div>
          <div
            onScroll={(e) => {
              const el = e.currentTarget;
              isNearBottomRef.current =
                el.scrollHeight - el.scrollTop - el.clientHeight < 40;
            }}
            style={{
              background: "#282c34",
              borderRadius: 8,
              padding: "1rem",
              fontFamily: MONO,
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
