import type { MetricMetadata, TaskSummaryRecord } from "../types";
import { adaptiveDistributionScale } from "./units";

export interface PerfStats {
  count: number;
  min: number;
  p25: number;
  median: number;
  p75: number;
  p95: number;
  max: number;
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

// extractResourceUsageValues pulls one metric's raw values across every task
// that has a resource_usage with that field present -- generically by
// metadata.key, the same nil-is-absent pattern useTaskLog.ts uses for
// MetricSample. ResourceUsage's container_* counters are optional/nullable
// now (not the old -1 sentinel), so a task with no value for this metric
// simply has no entry in raw.resource_usage, and is skipped here.
function extractResourceUsageValues(
  tasks: TaskSummaryRecord[],
  metadata: MetricMetadata
): number[] {
  const values: number[] = [];
  for (const task of tasks) {
    const v = task.resource_usage?.[metadata.key];
    if (typeof v === "number") values.push(v);
  }
  return values;
}

// computeDistribution is for "gauge"/"counter" metrics: percentile stats
// plus a histogram of raw values. Unlike a MetricSample time series, a
// ResourceUsage value is one-shot per task, so a "counter" here is just its
// cumulative total for that task's whole run -- there's no second reading
// to difference against, so no rate conversion (see MetricMetadata.type's
// doc comment on the Go side).
export function computeDistribution(
  tasks: TaskSummaryRecord[],
  metadata: MetricMetadata,
  numBins: number
): {
  stats: PerfStats;
  histData: { label: string; count: number }[];
  unit: string;
} {
  const raw = extractResourceUsageValues(tasks, metadata);
  const { scale, label: unit } = adaptiveDistributionScale(metadata.units, raw);
  if (raw.length === 0) {
    const empty: PerfStats = {
      count: 0,
      min: 0,
      p25: 0,
      median: 0,
      p75: 0,
      p95: 0,
      max: 0,
    };
    return { stats: empty, histData: [], unit };
  }
  const values = raw.map((v) => v / scale);
  return {
    stats: computeStats(values),
    histData: makeHistogram(values, numBins),
    unit,
  };
}

// computeCategoryCounts is for "categorical" metrics (exit_code, oom_killed):
// a count of tasks per distinct value rather than a continuous distribution.
export function computeCategoryCounts(
  tasks: TaskSummaryRecord[],
  metadata: MetricMetadata
): { label: string; count: number }[] {
  const counts = new Map<string, number>();
  for (const task of tasks) {
    const v = task.resource_usage?.[metadata.key];
    if (v === undefined || v === null) continue;
    const label = String(v);
    counts.set(label, (counts.get(label) ?? 0) + 1);
  }
  return Array.from(counts.entries())
    .map(([label, count]) => ({ label, count }))
    .sort((a, b) => {
      const an = Number(a.label);
      const bn = Number(b.label);
      if (!Number.isNaN(an) && !Number.isNaN(bn)) return an - bn;
      return a.label.localeCompare(b.label);
    });
}
