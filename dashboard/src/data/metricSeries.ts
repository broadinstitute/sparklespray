import type { MetricMetadata, ResourceDataPoint } from "../types";
import type { SeriesConfig } from "../components/MultiLineChart";
import { unitScale } from "./units";

export interface MetricSeriesGroup {
  /** Distinguishes this series from others for the same metric key, e.g.
   * one distinct disk volume's location -- "" when there's only one. */
  propsKey: string;
  label: string;
  data: { time: number; value: number }[];
}

export interface MetricSeriesResult {
  groups: MetricSeriesGroup[];
  yLabel: string;
}

function propsSignature(props: Record<string, string> | undefined): string {
  if (!props) return "";
  return Object.entries(props)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}=${v}`)
    .join(",");
}

// buildSeries extracts one metric's values across every sample, split into
// one group per distinct props signature (e.g. one line per disk volume).
// A "gauge" metric's raw values are used directly; a "counter" metric's
// cumulative values are turned into a per-second rate by differencing
// consecutive points within the same group -- a decrease (the only way that
// can happen is the counted thing resetting, which a running task's cgroup
// counters don't do) is dropped as a gap rather than plotted as a negative
// rate.
export function buildSeries(
  points: ResourceDataPoint[],
  metadata: MetricMetadata
): MetricSeriesResult {
  const isRate = metadata.type === "counter";
  const { scale, label: yLabel } = unitScale(metadata.units, isRate);

  const raw = new Map<
    string,
    { label: string; points: { time: number; value: number }[] }
  >();
  for (const p of points) {
    for (const m of p.metrics) {
      if (m.key !== metadata.key) continue;
      const sig = propsSignature(m.props);
      let group = raw.get(sig);
      if (!group) {
        group = {
          label: Object.values(m.props ?? {}).join(", ") || metadata.name,
          points: [],
        };
        raw.set(sig, group);
      }
      group.points.push({ time: p.time, value: m.value });
    }
  }

  const groups: MetricSeriesGroup[] = [];
  for (const [sig, group] of raw) {
    const sorted = group.points.slice().sort((a, b) => a.time - b.time);
    const data = metadata.type === "counter" ? toRate(sorted) : sorted;
    groups.push({
      propsKey: sig,
      label: group.label,
      data: data.map((d) => ({
        time: d.time,
        value: Math.round((d.value / scale) * 100) / 100,
      })),
    });
  }

  return { groups, yLabel };
}

const PALETTE = [
  "#1976d2",
  "#e53935",
  "#43a047",
  "#fb8c00",
  "#8e24aa",
  "#00acc1",
  "#5c6bc0",
  "#bdbdbd",
];

// forChart merges a metric's (possibly per-volume) groups into the single
// data array + series config MultiLineChart expects: one row per distinct
// timestamp seen across all groups, with each group's value under its own
// synthetic key, plus a matching SeriesConfig entry (cycling a fixed
// palette -- a metric with props has no natural per-group color).
export function forChart(
  result: MetricSeriesResult
): {
  data: { time: number; label: string }[];
  series: SeriesConfig[];
} {
  const times = new Set<number>();
  for (const g of result.groups) for (const d of g.data) times.add(d.time);
  const sortedTimes = Array.from(times).sort((a, b) => a - b);

  const series: SeriesConfig[] = result.groups.map((g, i) => ({
    key: `s${i}`,
    label: g.label,
    color: PALETTE[i % PALETTE.length],
  }));

  const data = sortedTimes.map((time) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const row: any = {
      time,
      label: new Date(time).toLocaleTimeString("en-US", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      }),
    };
    result.groups.forEach((g, i) => {
      const point = g.data.find((d) => d.time === time);
      if (point) row[`s${i}`] = point.value;
    });
    return row;
  });

  return { data, series };
}

function toRate(
  sorted: { time: number; value: number }[]
): { time: number; value: number }[] {
  const rates: { time: number; value: number }[] = [];
  for (let i = 1; i < sorted.length; i++) {
    const dtSeconds = (sorted[i].time - sorted[i - 1].time) / 1000;
    const dValue = sorted[i].value - sorted[i - 1].value;
    if (dtSeconds <= 0 || dValue < 0) continue; // reset or duplicate timestamp: gap, not a plotted point
    rates.push({ time: sorted[i].time, value: dValue / dtSeconds });
  }
  return rates;
}
