import type { AnyEvent, AnyTaskEvent, TaskStateUpdateEvent } from "../types";
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
  completedSuccess: number;
  completedError: number;
  failed: number;
}

type TaskState = "pending" | "running" | "done";

const NUM_BUCKETS = 60;

export function computeJobTimeSeries(
  events: AnyEvent[],
  jobId: string,
  maxTimeMs?: number
): { counts: CountPoint[]; rates: RatePoint[] } {
  const allJobEvents = events
    .filter((e) => "job_id" in e && (e as AnyTaskEvent).job_id === jobId)
    .sort((a, b) => a.timestamp.localeCompare(b.timestamp)) as AnyTaskEvent[];

  if (allJobEvents.length === 0) return { counts: [], rates: [] };

  const totalTasks = getJobTaskCount(events, jobId);

  const minTime = new Date(allJobEvents[0].timestamp).getTime();
  const maxTime = Math.max(
    new Date(allJobEvents[allJobEvents.length - 1].timestamp).getTime(),
    maxTimeMs ?? 0
  );

  if (maxTime <= minTime) return { counts: [], rates: [] };

  const bucketSize = (maxTime - minTime) / NUM_BUCKETS;
  const bucketSizeMin = bucketSize / 60_000;

  interface Transition {
    time: number;
    taskId: string;
    newState: TaskState;
  }
  const transitions: Transition[] = [];

  for (const event of allJobEvents) {
    if (event.type !== "task_state_update") continue;
    const tsu = event as TaskStateUpdateEvent;
    const t = new Date(event.timestamp).getTime();
    if (
      tsu.new_state === "claimed" ||
      tsu.new_state === "running" ||
      tsu.new_state === "writing"
    ) {
      transitions.push({ time: t, taskId: tsu.task_id, newState: "running" });
    } else if (
      tsu.new_state === "success" ||
      tsu.new_state === "error" ||
      tsu.new_state === "failed" ||
      tsu.new_state === "killed"
    ) {
      transitions.push({ time: t, taskId: tsu.task_id, newState: "done" });
    } else if (tsu.old_state !== "pending" && tsu.new_state === "pending") {
      transitions.push({ time: t, taskId: tsu.task_id, newState: "pending" });
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
    let pending = totalTasks - taskState.size;
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
    failed: 0,
  }));

  for (const event of allJobEvents) {
    if (event.type !== "task_state_update") continue;
    const tsu = event as TaskStateUpdateEvent;
    const t = new Date(event.timestamp).getTime();
    const bi = Math.min(
      Math.floor((t - minTime) / bucketSize),
      NUM_BUCKETS - 1
    );
    if (tsu.new_state === "success") {
      rates[bi].completedSuccess += 1 / bucketSizeMin;
    } else if (tsu.new_state === "error") {
      rates[bi].completedError += 1 / bucketSizeMin;
    } else if (tsu.new_state === "failed" || tsu.new_state === "killed") {
      rates[bi].failed += 1 / bucketSizeMin;
    }
  }

  for (const r of rates) {
    r.completedSuccess = Math.round(r.completedSuccess * 100) / 100;
    r.completedError = Math.round(r.completedError * 100) / 100;
    r.failed = Math.round(r.failed * 100) / 100;
  }

  return { counts, rates };
}
