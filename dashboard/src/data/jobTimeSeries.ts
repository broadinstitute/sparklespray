import type {
  AnyEvent,
  AnyTaskEvent,
  TaskCompleteEvent,
  TaskExecCompleteEvent,
} from "../types";
import { getJobTaskCount } from "./events";

function formatTime(ms: number): string {
  return new Date(ms).toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
  });
}

export interface CountPoint {
  time: number;
  label: string;
  pending: number;
  running: number;
}

export interface RatePoint {
  time: number;
  label: string;
  completedSuccess: number; // tasks/min
  completedError: number;
  orphaned: number;
  failed: number;
}

type TaskState = "pending" | "running" | "done";

const NUM_BUCKETS = 60;

export function computeJobTimeSeries(
  events: AnyEvent[],
  jobId: string
): { counts: CountPoint[]; rates: RatePoint[] } {
  const allJobEvents = events
    .filter((e) => "job_id" in e && (e as any).job_id === jobId)
    .sort((a, b) => a.timestamp.localeCompare(b.timestamp)) as AnyTaskEvent[];

  if (allJobEvents.length === 0) return { counts: [], rates: [] };

  const totalTasks = getJobTaskCount(events, jobId);

  const minTime = new Date(allJobEvents[0].timestamp).getTime();
  const maxTime = new Date(
    allJobEvents[allJobEvents.length - 1].timestamp
  ).getTime();

  if (maxTime <= minTime) return { counts: [], rates: [] };

  const bucketSize = (maxTime - minTime) / NUM_BUCKETS;
  const bucketSizeMin = bucketSize / 60_000;

  // Track exit codes for classifying task_complete as success/error
  const taskExitCodes = new Map<string, number>();

  interface Transition {
    time: number;
    taskId: string;
    newState: TaskState;
  }
  const transitions: Transition[] = [];

  for (const event of allJobEvents) {
    const t = new Date(event.timestamp).getTime();
    if (event.type === "task_claimed") {
      transitions.push({ time: t, taskId: event.task_id, newState: "running" });
    } else if (event.type === "task_exec_complete") {
      taskExitCodes.set(
        event.task_id,
        (event as TaskExecCompleteEvent).exit_code ?? 0
      );
    } else if (event.type === "task_complete") {
      taskExitCodes.set(event.task_id, (event as TaskCompleteEvent).exit_code);
      transitions.push({ time: t, taskId: event.task_id, newState: "done" });
    } else if (event.type === "task_failed") {
      transitions.push({ time: t, taskId: event.task_id, newState: "done" });
    } else if (event.type === "task_orphaned") {
      transitions.push({ time: t, taskId: event.task_id, newState: "pending" });
    }
  }
  transitions.sort((a, b) => a.time - b.time);

  const taskState = new Map<string, TaskState>();
  let transIdx = 0;
  const counts: CountPoint[] = [];

  for (let i = 0; i <= NUM_BUCKETS; i++) {
    const t = minTime + i * bucketSize;
    while (transIdx < transitions.length && transitions[transIdx].time <= t) {
      taskState.set(
        transitions[transIdx].taskId,
        transitions[transIdx].newState
      );
      transIdx++;
    }
    let pending = totalTasks - taskState.size; // tasks not yet seen in events
    let running = 0;
    for (const s of taskState.values()) {
      if (s === "pending") pending++;
      else if (s === "running") running++;
    }
    counts.push({ time: t, label: formatTime(t), pending, running });
  }

  const rates: RatePoint[] = Array.from({ length: NUM_BUCKETS }, (_, i) => ({
    time: minTime + (i + 0.5) * bucketSize,
    label: formatTime(minTime + (i + 0.5) * bucketSize),
    completedSuccess: 0,
    completedError: 0,
    orphaned: 0,
    failed: 0,
  }));

  for (const event of allJobEvents) {
    const t = new Date(event.timestamp).getTime();
    const bi = Math.min(
      Math.floor((t - minTime) / bucketSize),
      NUM_BUCKETS - 1
    );
    if (event.type === "task_complete") {
      const exitCode = taskExitCodes.get(event.task_id) ?? 0;
      if (exitCode === 0) rates[bi].completedSuccess += 1 / bucketSizeMin;
      else rates[bi].completedError += 1 / bucketSizeMin;
    } else if (event.type === "task_orphaned") {
      rates[bi].orphaned += 1 / bucketSizeMin;
    } else if (event.type === "task_failed") {
      rates[bi].failed += 1 / bucketSizeMin;
    }
  }

  for (const r of rates) {
    r.completedSuccess = Math.round(r.completedSuccess * 100) / 100;
    r.completedError = Math.round(r.completedError * 100) / 100;
    r.orphaned = Math.round(r.orphaned * 100) / 100;
    r.failed = Math.round(r.failed * 100) / 100;
  }

  return { counts, rates };
}
