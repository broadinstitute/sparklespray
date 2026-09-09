import { useEffect, useRef, useState } from "react";
import type { ResourceDataPoint } from "../types";
import { useMetricMetadata } from "../data/useMetricMetadata";
import { buildSeries, forChart } from "../data/metricSeries";
import MultiLineChart from "./MultiLineChart";

interface Props {
  resourceData: ResourceDataPoint[];
  xDomain?: [number, number];
}

// MetricsPanel renders one chart per metric key the operator has chosen to
// see, driven entirely by the backend's metric metadata (GET /api/v1/metrics)
// rather than a hardcoded chart list -- so adding/removing/reordering
// metrics server-side needs no frontend change. Two metrics are visible by
// default (whichever metadata marks with the lowest default_position); a
// checkbox per remaining metric lets the operator add more time series.
export default function MetricsPanel({ resourceData, xDomain }: Props) {
  const { metadata } = useMetricMetadata();
  const [visible, setVisible] = useState<Set<string>>(new Set());
  const seededRef = useRef(false);

  const applicable = metadata.filter((m) => m.in_metric_sample);

  useEffect(() => {
    if (seededRef.current || applicable.length === 0) return;
    seededRef.current = true;
    const defaults = applicable
      .filter((m) => m.default_position != null)
      .map((m) => m.key);
    setVisible(new Set(defaults));
  }, [applicable]);

  const sortedMetadata = applicable.slice().sort((a, b) => {
    const ap = a.default_position ?? Infinity;
    const bp = b.default_position ?? Infinity;
    if (ap !== bp) return ap - bp;
    return a.name.localeCompare(b.name);
  });

  function toggle(key: string) {
    setVisible((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

  if (resourceData.length === 0) {
    return null;
  }

  return (
    <div>
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: "0.75rem",
          marginBottom: "1.5rem",
          fontFamily: "monospace",
          fontSize: "0.8rem",
          color: "#666",
        }}
      >
        {sortedMetadata.map((m) => (
          <label
            key={m.key}
            title={m.description}
            style={{
              display: "flex",
              alignItems: "center",
              gap: "0.35rem",
              cursor: "pointer",
            }}
          >
            <input
              type="checkbox"
              checked={visible.has(m.key)}
              onChange={() => toggle(m.key)}
            />
            {m.name}
          </label>
        ))}
      </div>
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          gap: "2rem",
          marginBottom: "2rem",
        }}
      >
        {sortedMetadata
          .filter((m) => visible.has(m.key))
          .map((m) => {
            const result = buildSeries(resourceData, m);
            const { data, series } = forChart(result);
            if (data.length === 0) return null;
            return (
              <MultiLineChart
                key={m.key}
                data={data}
                title={m.name}
                yLabel={result.yLabel}
                xDomain={xDomain}
                series={series}
              />
            );
          })}
      </div>
    </div>
  );
}
