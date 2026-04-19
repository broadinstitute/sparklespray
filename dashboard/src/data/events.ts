import type {
  AnyEvent,
  AnyTaskEvent,
  JobStartedEvent,
  TaskCompleteEvent,
  TaskExecCompleteEvent,
} from "../types";

export function getJobTaskCount(events: AnyEvent[], jobId: string): number {
  const started = events.find(
    (e) => e.type === "job_started" && (e as JobStartedEvent).job_id === jobId
  );
  if (started) return (started as JobStartedEvent).task_count;
  const ids = new Set(
    events
      .filter((e) => "task_id" in e && (e as any).job_id === jobId)
      .map((e) => (e as any).task_id as string)
  );
  return ids.size;
}

export interface TaskSummary {
  taskId: string;
  status: TaskStatus;
  events: AnyTaskEvent[];
}

export interface JobSummary {
  jobId: string;
  startTime: Date;
}

export function getJobs(events: AnyEvent[]): JobSummary[] {
  return events
    .filter((e) => e.type === "job_started")
    .map((e) => ({
      jobId: (e as JobStartedEvent).job_id,
      startTime: new Date(e.timestamp),
    }))
    .sort((a, b) => a.startTime.getTime() - b.startTime.getTime());
}

export interface ClusterSummary {
  clusterId: string;
  startTime: Date;
}

export function getClusters(events: AnyEvent[]): ClusterSummary[] {
  return events
    .filter((e) => e.type === "cluster_started")
    .map((e) => ({
      clusterId: (e as any).cluster_id as string,
      startTime: new Date(e.timestamp),
    }))
    .sort((a, b) => a.startTime.getTime() - b.startTime.getTime());
}

export function getJobTasks(events: AnyEvent[], jobId: string): TaskSummary[] {
  const jobEvents = events.filter(
    (e) => "task_id" in e && (e as any).job_id === jobId
  ) as AnyTaskEvent[];
  const byTask = new Map<string, AnyTaskEvent[]>();
  for (const e of jobEvents) {
    const list = byTask.get(e.task_id) ?? [];
    list.push(e);
    byTask.set(e.task_id, list);
  }
  return Array.from(byTask.entries())
    .map(([taskId, evts]) => {
      const sorted = evts.sort((a, b) =>
        a.timestamp.localeCompare(b.timestamp)
      );
      return { taskId, status: deriveStatus(sorted), events: sorted };
    })
    .sort((a, b) => a.taskId.localeCompare(b.taskId));
}

export function getTaskEvents(
  events: AnyEvent[],
  jobId: string,
  taskId: string
): AnyTaskEvent[] {
  return events
    .filter(
      (e) =>
        "task_id" in e &&
        (e as any).task_id === taskId &&
        (e as any).job_id === jobId
    )
    .sort((a, b) => a.timestamp.localeCompare(b.timestamp)) as AnyTaskEvent[];
}

export type TaskStatus =
  | "pending"
  | "claimed"
  | "exec_started"
  | "exec_complete"
  | "complete"
  | "orphaned"
  | "failed"
  | "killed";

export function deriveStatus(events: AnyTaskEvent[]): TaskStatus {
  if (events.length === 0) return "pending";
  const last = events[events.length - 1];
  switch (last.type) {
    case "task_claimed":
      return "claimed";
    case "task_exec_started":
      return "exec_started";
    case "task_exec_complete":
      return "exec_complete";
    case "task_complete":
      return "complete";
    case "task_orphaned":
      return "orphaned";
    case "task_failed":
      return "failed";
    case "task_killed":
      return "killed";
    default:
      return "pending";
  }
}

export interface TimingWindows {
  claimed?: Date;
  exec_started?: Date;
  exec_complete?: Date;
  complete?: Date;
  maxMemInGb?: number;
  exitCode?: number;
}

export function extractTimings(events: AnyTaskEvent[]): TimingWindows {
  const result: TimingWindows = {};
  for (const e of events) {
    if (e.type === "task_claimed") result.claimed = new Date(e.timestamp);
    if (e.type === "task_exec_started")
      result.exec_started = new Date(e.timestamp);
    if (e.type === "task_exec_complete") {
      result.exec_complete = new Date(e.timestamp);
      result.exitCode = (e as TaskExecCompleteEvent).exit_code;
    }
    if (e.type === "task_complete") {
      result.complete = new Date(e.timestamp);
      result.maxMemInGb = (e as TaskCompleteEvent).max_mem_in_gb;
    }
  }
  return result;
}
