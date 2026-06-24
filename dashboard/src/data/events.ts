import type {
  AnyEvent,
  AnyTaskEvent,
  BackendJobSummary,
  TaskStateUpdateEvent,
} from "../types";

export function getJobTaskCount(events: AnyEvent[], jobId: string): number {
  const ids = new Set(
    events
      .filter((e) => "task_id" in e && (e as AnyTaskEvent).job_id === jobId)
      .map((e) => (e as AnyTaskEvent).task_id)
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

export function getJobs(jobs: BackendJobSummary[]): JobSummary[] {
  return jobs
    .map((j) => ({ jobId: j.job_id, startTime: new Date(j.created_at) }))
    .sort((a, b) => b.startTime.getTime() - a.startTime.getTime());
}

export interface ClusterSummary {
  clusterId: string;
  startTime: Date;
}

export function getClusters(jobs: BackendJobSummary[]): ClusterSummary[] {
  const earliest = new Map<string, Date>();
  for (const j of jobs) {
    const t = new Date(j.created_at);
    const prev = earliest.get(j.workpool_id);
    if (!prev || t < prev) earliest.set(j.workpool_id, t);
  }
  return Array.from(earliest.entries())
    .map(([clusterId, startTime]) => ({ clusterId, startTime }))
    .sort((a, b) => b.startTime.getTime() - a.startTime.getTime());
}

export function getJobTasks(events: AnyEvent[], jobId: string): TaskSummary[] {
  const jobEvents = events.filter(
    (e) => "task_id" in e && (e as AnyTaskEvent).job_id === jobId
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
        (e as AnyTaskEvent).task_id === taskId &&
        (e as AnyTaskEvent).job_id === jobId
    )
    .sort((a, b) => a.timestamp.localeCompare(b.timestamp)) as AnyTaskEvent[];
}

export type TaskStatus =
  | "pending"
  | "claimed"
  | "running"
  | "writing"
  | "success"
  | "error"
  | "failed"
  | "killed";

export function deriveStatus(events: AnyTaskEvent[]): TaskStatus {
  if (events.length === 0) return "pending";
  const last = events[events.length - 1] as TaskStateUpdateEvent;
  if (last.type === "task_state_update") {
    const s = last.new_state;
    if (s === "claimed") return "claimed";
    if (s === "running") return "running";
    if (s === "writing") return "writing";
    if (s === "success") return "success";
    if (s === "error") return "error";
    if (s === "failed") return "failed";
    if (s === "killed") return "killed";
  }
  return "pending";
}

export interface TimingWindows {
  claimed?: Date;
  running?: Date;
  writing?: Date;
  done?: Date;
}

export interface JobTaskStats {
  total: number;
  success: number;
  failure: number;
}

export function getJobTaskStats(
  events: AnyEvent[],
  jobId: string
): JobTaskStats {
  const total = getJobTaskCount(events, jobId);
  const tasks = getJobTasks(events, jobId);
  let success = 0;
  let failure = 0;
  for (const task of tasks) {
    if (task.status === "success") success++;
    else if (
      task.status === "error" ||
      task.status === "failed" ||
      task.status === "killed"
    )
      failure++;
  }
  return { total, success, failure };
}

export function extractTimings(events: AnyTaskEvent[]): TimingWindows {
  const result: TimingWindows = {};
  for (const e of events) {
    if (e.type === "task_state_update") {
      const tsu = e as TaskStateUpdateEvent;
      const t = new Date(e.timestamp);
      if (tsu.new_state === "claimed") result.claimed = t;
      else if (tsu.new_state === "running") result.running = t;
      else if (tsu.new_state === "writing") result.writing = t;
      else if (
        tsu.new_state === "success" ||
        tsu.new_state === "error" ||
        tsu.new_state === "failed" ||
        tsu.new_state === "killed"
      ) {
        result.done = t;
      }
    }
  }
  return result;
}
