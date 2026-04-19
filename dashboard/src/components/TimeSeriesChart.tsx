import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import type { TimeSeriesPoint } from "../data/simulate";

interface Props {
  data: TimeSeriesPoint[];
  title: string;
  yLabel: string;
  color: string;
  domain?: [number, number];
}

export default function TimeSeriesChart({
  data,
  title,
  yLabel,
  color,
  domain,
}: Props) {
  return (
    <div style={{ marginBottom: "2rem" }}>
      <h3
        style={{
          marginBottom: "0.5rem",
          fontFamily: "monospace",
          fontSize: "0.9rem",
          color: "#666",
        }}
      >
        {title}
      </h3>
      <ResponsiveContainer width="100%" height={200}>
        <LineChart
          data={data}
          margin={{ top: 4, right: 16, left: 0, bottom: 4 }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
          <XAxis
            dataKey="label"
            tick={{ fontSize: 11, fontFamily: "monospace" }}
            interval="preserveStartEnd"
          />
          <YAxis
            tickFormatter={(v) => `${v}`}
            label={{
              value: yLabel,
              angle: -90,
              position: "insideLeft",
              style: { fontSize: 11, fontFamily: "monospace" },
            }}
            domain={domain}
            tick={{ fontSize: 11 }}
          />
          <Tooltip
            formatter={(v) => [`${v} ${yLabel}`, ""]}
            labelStyle={{ fontFamily: "monospace", fontSize: 11 }}
            contentStyle={{ fontFamily: "monospace", fontSize: 11 }}
          />
          <Line
            type="linear"
            dataKey="value"
            stroke={color}
            dot={false}
            strokeWidth={2}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
