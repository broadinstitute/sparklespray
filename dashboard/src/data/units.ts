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
