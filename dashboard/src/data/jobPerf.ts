import type { TaskSummaryRecord } from "../types";

export interface TaskPerfEntry {
  taskId: string;
  executionSec: number;
  maxMemGb: number;
  userCpuSec: number;
  systemCpuSec: number;
  blockReadBytes: number;
  blockWriteBytes: number;
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
  userCpuStats: PerfStats;
  systemCpuStats: PerfStats;
  cpuEffStats: PerfStats;
  blockReadStats: PerfStats;
  blockWriteStats: PerfStats;
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

export function computeJobPerf(tasks: TaskSummaryRecord[]): JobPerfData {
  const entries: TaskPerfEntry[] = [];

  for (const task of tasks) {
    const ru = task.resource_usage;
    if (!ru) continue;
    entries.push({
      taskId: task.task_id,
      executionSec: ru.elapsed_seconds,
      maxMemGb: ru.max_memory_bytes / 1e9,
      userCpuSec: ru.cpu_user_usec / 1e6,
      systemCpuSec: ru.cpu_system_usec / 1e6,
      blockReadBytes: ru.block_read_bytes,
      blockWriteBytes: ru.block_write_bytes,
    });
  }

  if (entries.length === 0) {
    const empty: PerfStats = {
      count: 0,
      min: 0,
      p25: 0,
      median: 0,
      p75: 0,
      p95: 0,
      max: 0,
    };
    return {
      entries,
      execStats: empty,
      memStats: empty,
      userCpuStats: empty,
      systemCpuStats: empty,
      cpuEffStats: empty,
      blockReadStats: empty,
      blockWriteStats: empty,
    };
  }

  return {
    entries,
    execStats: computeStats(entries.map((e) => e.executionSec)),
    memStats: computeStats(entries.map((e) => e.maxMemGb * 1024)),
    userCpuStats: computeStats(entries.map((e) => e.userCpuSec)),
    systemCpuStats: computeStats(entries.map((e) => e.systemCpuSec)),
    cpuEffStats: computeStats(
      entries.map((e) =>
        e.executionSec > 0
          ? (e.userCpuSec + e.systemCpuSec) / e.executionSec
          : 0
      )
    ),
    blockReadStats: computeStats(entries.map((e) => e.blockReadBytes / 1e6)),
    blockWriteStats: computeStats(entries.map((e) => e.blockWriteBytes / 1e6)),
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
