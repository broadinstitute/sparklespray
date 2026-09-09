import type { MetricMetadata } from "../types";

const GB = 1_073_741_824;

// unitScale maps a raw units value to how to scale it for display and what
// label to show, for a gauge/one-shot value (the raw value) or a counter
// turned into a per-second rate (see metricSeries.ts -- jobPerf.ts always
// passes isRate: false, since a one-shot ResourceUsage value has no second
// reading to difference against). Only "bytes" needs a non-1 scale today;
// everything else is already in a display-ready unit.
export function unitScale(
  units: MetricMetadata["units"],
  isRate: boolean
): { scale: number; label: string } {
  if (units === "bytes") {
    return isRate ? { scale: GB, label: "GB/s" } : { scale: GB, label: "GB" };
  }
  // A usec counter turned into a per-second rate is a stalled-time-per-second
  // figure -- e.g. 500,000 usec/s means half of every second was spent
  // stalled -- so it reads far more naturally as a percentage of wall-clock
  // time than as a raw usec/s rate.
  if (units === "usec" && isRate) {
    return { scale: 10_000, label: "%" };
  }
  const labels: Record<Exclude<MetricMetadata["units"], "bytes">, string> = {
    percent: "%",
    usec: "usec",
    count: "count",
    seconds: "s",
    none: "",
  };
  const label = labels[units];
  return { scale: 1, label: isRate ? `${label}/s` : label };
}

const KB = 1024;
const MB = KB * 1024;
const TB = GB * 1024;

const SECOND_USEC = 1_000_000;
const MINUTE_USEC = SECOND_USEC * 60;
const HOUR_USEC = MINUTE_USEC * 60;

// adaptiveDistributionScale picks a display unit for a one-shot per-task
// distribution (jobPerf.ts's histograms/stats) based on the mean of its raw
// values, rather than unitScale's single fixed unit -- e.g. a usec-typed
// duration whose tasks average a couple hours reads far better in hours than
// in raw microseconds, and a bytes-typed field averaging a few hundred KB
// shouldn't be squashed to "0.00 GB". Only usec and bytes have more than one
// natural display unit; everything else falls back to unitScale's fixed
// (non-rate, since a one-shot value has nothing to rate against) scale.
export function adaptiveDistributionScale(
  units: MetricMetadata["units"],
  values: number[]
): { scale: number; label: string } {
  if (values.length === 0) return unitScale(units, false);
  const mean = values.reduce((a, b) => a + b, 0) / values.length;

  if (units === "usec") {
    const meanSeconds = mean / SECOND_USEC;
    if (meanSeconds > 60 * 60 * 2) return { scale: HOUR_USEC, label: "hr" };
    if (meanSeconds > 600) return { scale: MINUTE_USEC, label: "min" };
    if (meanSeconds > 1) return { scale: SECOND_USEC, label: "s" };
    return { scale: 1, label: "usec" };
  }

  if (units === "bytes") {
    if (mean >= TB) return { scale: TB, label: "TB" };
    if (mean >= GB) return { scale: GB, label: "GB" };
    if (mean >= MB) return { scale: MB, label: "MB" };
    if (mean >= KB) return { scale: KB, label: "KB" };
    return { scale: 1, label: "B" };
  }

  return unitScale(units, false);
}
