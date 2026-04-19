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
}

const CHART_MARGIN = { top: 4, right: 16, left: 8, bottom: 4 };
const TICK_STYLE = { fontSize: 11, fontFamily: "monospace" };
const TOOLTIP_STYLE = { fontFamily: "monospace", fontSize: 11 };

export default function MultiLineChart({
  data,
  series,
  title,
  yLabel,
  xKey = "label",
  stacked = false,
  yAxisWidth = 56,
}: Props) {
  const axes = (
    <>
      <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
      <XAxis dataKey={xKey} tick={TICK_STYLE} interval="preserveStartEnd" />
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
      <Tooltip labelStyle={TOOLTIP_STYLE} contentStyle={TOOLTIP_STYLE} />
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
