import {
  LineChart,
  Line,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

export interface SeriesConfig {
  key: string;
  label: string;
  color: string;
}

interface Props {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data: any[];
  series: SeriesConfig[];
  title: string;
  yLabel: string;
  xKey?: string;
  stacked?: boolean;
  yAxisWidth?: number;
  /** When set, renders a time-scaled x-axis (epoch ms `time` field) spanning
   * this domain, so gaps in the data show as proportional blank space instead
   * of being evenly spaced like a categorical axis. */
  xDomain?: [number, number];
}

const CHART_MARGIN = { top: 4, right: 16, left: 8, bottom: 4 };
const TICK_STYLE = { fontSize: 11, fontFamily: "monospace" };
const TOOLTIP_STYLE = { fontFamily: "monospace", fontSize: 11 };

function formatTimeTick(ms: number): string {
  return new Date(ms).toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatTooltipLabel(label: unknown): string {
  const ms = typeof label === "number" ? label : Number(label);
  if (Number.isNaN(ms)) return String(label);
  return new Date(ms).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export default function MultiLineChart({
  data,
  series,
  title,
  yLabel,
  xKey = "label",
  stacked = false,
  yAxisWidth = 56,
  xDomain,
}: Props) {
  const axes = (
    <>
      <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
      {xDomain ? (
        <XAxis
          dataKey="time"
          type="number"
          domain={xDomain}
          scale="time"
          tick={TICK_STYLE}
          tickFormatter={formatTimeTick}
        />
      ) : (
        <XAxis dataKey={xKey} tick={TICK_STYLE} interval="preserveStartEnd" />
      )}
      <YAxis
        width={yAxisWidth}
        label={{
          value: yLabel,
          angle: -90,
          position: "insideLeft",
          offset: 10,
          style: TICK_STYLE,
        }}
        tick={{ fontSize: 11 }}
      />
      <Tooltip
        labelStyle={TOOLTIP_STYLE}
        contentStyle={TOOLTIP_STYLE}
        labelFormatter={xDomain ? formatTooltipLabel : undefined}
      />
      <Legend wrapperStyle={TOOLTIP_STYLE} />
    </>
  );

  return (
    <div style={{ marginBottom: 0 }}>
      <h3
        style={{
          margin: "0 0 0.5rem",
          fontFamily: "monospace",
          fontSize: "0.9rem",
          color: "#666",
        }}
      >
        {title}
      </h3>
      <ResponsiveContainer width="100%" height={200}>
        {stacked ? (
          <AreaChart data={data} margin={CHART_MARGIN}>
            {axes}
            {series.map((s) => (
              <Area
                key={s.key}
                type="linear"
                dataKey={s.key}
                name={s.label}
                stroke={s.color}
                fill={s.color}
                fillOpacity={0.4}
                stackId="stack"
                dot={false}
                strokeWidth={2}
                isAnimationActive={false}
              />
            ))}
          </AreaChart>
        ) : (
          <LineChart data={data} margin={CHART_MARGIN}>
            {axes}
            {series.map((s) => (
              <Line
                key={s.key}
                type="linear"
                dataKey={s.key}
                name={s.label}
                stroke={s.color}
                dot={false}
                strokeWidth={2}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        )}
      </ResponsiveContainer>
    </div>
  );
}
