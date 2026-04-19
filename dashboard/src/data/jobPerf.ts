import type { AnyEvent, AnyTaskEvent, TaskCompleteEvent } from "../types";

export interface TaskPerfEntry {
  taskId: string;
  localizationMin: number;
  executionMin: number;
  uploadMin: number | undefined;
  maxMemGb: number;
  userCpuSec: number;
  systemCpuSec: number;
  maxMemoryBytes: number;
  sharedMemoryBytes: number;
  unsharedMemoryBytes: number;
  blockInputOps: number;
  blockOutputOps: number;
  downloadBytes: number;
  uploadBytes: number;
}

export interface PerfStats {
  count: number;
  min: number;
  p25: number;
  median: number;
  p75: number;
  p95: number;
  max: number;
}

export interface JobPerfData {
  entries: TaskPerfEntry[];
  execStats: PerfStats;
  memStats: PerfStats;
  locStats: PerfStats;
  uploadTimeStats: PerfStats | null;
  userCpuStats: PerfStats;
  systemCpuStats: PerfStats;
  cpuEffStats: PerfStats;
  blockInputStats: PerfStats;
  blockOutputStats: PerfStats;
  downloadStats: PerfStats;
  uploadBytesStats: PerfStats;
  sharedMemStats: PerfStats;
  unsharedMemStats: PerfStats;
}

function quantile(sorted: number[], q: number): number {
  const idx = q * (sorted.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
}

function computeStats(values: number[]): PerfStats {
  const sorted = [...values].sort((a, b) => a - b);
  return {
    count: sorted.length,
    min: sorted[0],
    p25: quantile(sorted, 0.25),
    median: quantile(sorted, 0.5),
    p75: quantile(sorted, 0.75),
    p95: quantile(sorted, 0.95),
    max: sorted[sorted.length - 1],
  };
}

export function computeJobPerf(events: AnyEvent[], jobId: string): JobPerfData {
  const jobEvents = events.filter(
    (e) => "job_id" in e && (e as any).job_id === jobId
  ) as AnyTaskEvent[];

  const byTask = new Map<string, AnyTaskEvent[]>();
  for (const e of jobEvents) {
    if (!("task_id" in e)) continue;
    const list = byTask.get(e.task_id) ?? [];
    list.push(e);
    byTask.set(e.task_id, list);
  }

  const entries: TaskPerfEntry[] = [];

  for (const [taskId, evs] of byTask) {
    evs.sort((a, b) => a.timestamp.localeCompare(b.timestamp));

    // Find the task_complete event — this is the authoritative rusage source
    const completeEv = evs.findLast(
      (e: AnyTaskEvent) => e.type === "task_complete"
    ) as TaskCompleteEvent | undefined;
    if (!completeEv) continue;

    // Find the last exec cycle: task_exec_started → task_exec_complete before the task_complete
    let execCompleteIdx = -1;
    for (let i = evs.length - 1; i >= 0; i--) {
      if (evs[i].type === "task_exec_complete") {
        execCompleteIdx = i;
        break;
      }
    }
    if (execCompleteIdx < 0) continue;

    let execStartedIdx = -1;
    for (let i = execCompleteIdx - 1; i >= 0; i--) {
      if (evs[i].type === "task_exec_started") {
        execStartedIdx = i;
        break;
      }
    }
    if (execStartedIdx < 0) continue;

    let claimedIdx = -1;
    for (let i = execStartedIdx - 1; i >= 0; i--) {
      if (evs[i].type === "task_claimed") {
        claimedIdx = i;
        break;
      }
    }

    const tExecStarted = new Date(evs[execStartedIdx].timestamp).getTime();
    const tExecComplete = new Date(evs[execCompleteIdx].timestamp).getTime();
    const executionMin = (tExecComplete - tExecStarted) / 60_000;
    if (executionMin < 0) continue;

    const localizationMin =
      claimedIdx >= 0
        ? (tExecStarted - new Date(evs[claimedIdx].timestamp).getTime()) /
          60_000
        : 0;

    const tComplete = new Date(completeEv.timestamp).getTime();
    const uploadMin = (tComplete - tExecComplete) / 60_000;

    entries.push({
      taskId,
      localizationMin,
      executionMin,
      uploadMin,
      maxMemGb: completeEv.max_mem_in_gb,
      userCpuSec: completeEv.user_cpu_sec,
      systemCpuSec: completeEv.system_cpu_sec,
      maxMemoryBytes: completeEv.max_memory_bytes,
      sharedMemoryBytes: completeEv.shared_memory_bytes,
      unsharedMemoryBytes: completeEv.unshared_memory_bytes,
      blockInputOps: completeEv.block_input_ops,
      blockOutputOps: completeEv.block_output_ops,
      downloadBytes: completeEv.download_bytes,
      uploadBytes: completeEv.upload_bytes,
    });
  }

  const uploadTimeEntries = entries.filter((e) => e.uploadMin !== undefined);

  return {
    entries,
    execStats: computeStats(entries.map((e) => e.executionMin)),
    memStats: computeStats(entries.map((e) => e.maxMemGb)),
    locStats: computeStats(entries.map((e) => e.localizationMin)),
    uploadTimeStats:
      uploadTimeEntries.length > 0
        ? computeStats(uploadTimeEntries.map((e) => e.uploadMin as number))
        : null,
    userCpuStats: computeStats(entries.map((e) => e.userCpuSec)),
    systemCpuStats: computeStats(entries.map((e) => e.systemCpuSec)),
    cpuEffStats: computeStats(
      entries.map(
        (e) => (e.userCpuSec + e.systemCpuSec) / (e.executionMin * 60)
      )
    ),
    blockInputStats: computeStats(entries.map((e) => e.blockInputOps)),
    blockOutputStats: computeStats(entries.map((e) => e.blockOutputOps)),
    downloadStats: computeStats(entries.map((e) => e.downloadBytes / 1e9)),
    uploadBytesStats: computeStats(entries.map((e) => e.uploadBytes / 1e9)),
    sharedMemStats: computeStats(entries.map((e) => e.sharedMemoryBytes / 1e9)),
    unsharedMemStats: computeStats(
      entries.map((e) => e.unsharedMemoryBytes / 1e9)
    ),
  };
}

export function makeHistogram(
  values: number[],
  numBins: number
): { label: string; count: number }[] {
  if (values.length === 0) return [];
  const min = Math.min(...values);
  const max = Math.max(...values);
  const binWidth = (max - min) / numBins || 1;
  const bins = Array.from({ length: numBins }, (_, i) => ({
    label: (min + i * binWidth).toFixed(2),
    count: 0,
  }));
  for (const v of values) {
    const bi = Math.min(Math.floor((v - min) / binWidth), numBins - 1);
    bins[bi].count++;
  }
  return bins;
}
