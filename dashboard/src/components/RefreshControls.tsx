import { useEffect, useMemo, useState } from "react";

const MONO = "'IBM Plex Mono', monospace";

let styleInjected = false;
function ensurePulseKeyframes() {
  if (styleInjected || typeof document === "undefined") return;
  styleInjected = true;
  const el = document.createElement("style");
  el.textContent = `@keyframes livePulse { 0%,100% { opacity:1; } 50% { opacity:0.25; } }`;
  document.head.appendChild(el);
}

function formatUpdatedText(
  lastUpdatedAt: number | null,
  live: boolean,
  nowTick: number
): string {
  void nowTick;
  if (!live) return "auto-refresh off";
  if (lastUpdatedAt === null) return "updated —";
  const secs = Math.max(0, Math.round((Date.now() - lastUpdatedAt) / 1000));
  return `updated ${secs}s ago`;
}

// ── Live/Paused pill ─────────────────────────────────────────────────────────

export function LiveToggle({
  live,
  onToggle,
}: {
  live: boolean;
  onToggle: () => void;
}) {
  ensurePulseKeyframes();
  return (
    <div
      onClick={onToggle}
      style={{
        cursor: "pointer",
        display: "flex",
        alignItems: "center",
        gap: 7,
        border: "1px solid #e5e7eb",
        borderRadius: 16,
        padding: "5px 12px",
        background: "#fff",
        fontFamily: MONO,
        userSelect: "none",
      }}
    >
      <span
        style={{
          width: 7,
          height: 7,
          borderRadius: "50%",
          display: "inline-block",
          background: live ? "#22c55e" : "#9ca3af",
          animation: live ? "livePulse 1.4s ease-in-out infinite" : "none",
        }}
      />
      <span style={{ fontSize: 12, color: "#1f2328", fontWeight: 600 }}>
        {live ? "Live" : "Paused"}
      </span>
    </div>
  );
}

/** Ticks once per second while `live` so "updated Ns ago" text stays fresh. */
function useNowTick(live: boolean): number {
  const [tick, setTick] = useState(0);
  useEffect(() => {
    if (!live) return;
    const id = setInterval(() => setTick((t) => t + 1), 1000);
    return () => clearInterval(id);
  }, [live]);
  return tick;
}

// ── Snapshot-page toggle (JobList) ───────────────────────────────────────────

export function RefreshToggle({
  live,
  onToggle,
  lastUpdatedAt,
}: {
  live: boolean;
  onToggle: () => void;
  lastUpdatedAt: number | null;
}) {
  const tick = useNowTick(live);
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
      <LiveToggle live={live} onToggle={onToggle} />
      <span
        style={{
          fontSize: 11,
          color: "#9095a0",
          fontFamily: MONO,
          whiteSpace: "nowrap",
        }}
      >
        {formatUpdatedText(lastUpdatedAt, live, tick)}
      </span>
    </div>
  );
}

// ── Timeline-page range + refresh bar (WorkPoolDetail Overview) ─────────────

export interface RangeChange {
  startMs: number;
  endMs: number;
  live: boolean;
}

const OFFSET_PRESETS = [
  { value: "60m", label: "60m ago", offsetMin: 60 },
  { value: "30m", label: "30m ago", offsetMin: 30 },
  { value: "10m", label: "10m ago", offsetMin: 10 },
] as const;

type StartPresetValue = typeof OFFSET_PRESETS[number]["value"] | "anchor";
type EndModeValue = "now" | "custom" | "endAnchor";

function segStyle(active: boolean, isFirst: boolean): React.CSSProperties {
  return {
    cursor: "pointer",
    fontSize: 12,
    fontWeight: active ? 600 : 400,
    padding: "6px 12px",
    background: active ? "#111827" : "#ffffff",
    color: active ? "#ffffff" : "#4b5563",
    borderLeft: isFirst ? "none" : "1px solid #e5e7eb",
    whiteSpace: "nowrap",
    fontFamily: MONO,
  };
}

export function RangeRefreshBar({
  anchorLabel,
  anchorMs,
  endAnchorLabel,
  endAnchorMs,
  lastUpdatedAt,
  onChange,
  live: controlledLive,
  onLiveChange,
}: {
  /** Label for the "since the beginning" Start preset, e.g. "Pool start", "Job start", "Task start". */
  anchorLabel: string;
  /** Timestamp that preset resolves to (earliest available data). */
  anchorMs: number | null;
  /** Optional label for a fixed End option, e.g. "Pool end". Omit to hide that option. */
  endAnchorLabel?: string;
  /** Timestamp that the End option above resolves to. */
  endAnchorMs?: number | null;
  /** Timestamp of the most recent successful poll, for the "updated Ns ago" text. */
  lastUpdatedAt: number | null;
  onChange: (range: RangeChange) => void;
  /** Optional: control the Live/Paused state from outside (e.g. to share it
   * with another control on a different tab). Uncontrolled by default. */
  live?: boolean;
  onLiveChange?: (live: boolean) => void;
}) {
  const [start, setStart] = useState<StartPresetValue>("10m");
  const [endMode, setEndMode] = useState<EndModeValue>("now");
  const [customOffsetMin, setCustomOffsetMin] = useState(15);
  const [liveState, setLiveState] = useState(true);
  const live = controlledLive ?? liveState;
  const setLive = onLiveChange ?? setLiveState;

  const tick = useNowTick(live);

  const selectEnd = (mode: EndModeValue) => {
    setEndMode(mode);
    setLive(mode === "now");
  };

  const toggleLive = () => setLive(!live);

  const nudge = (deltaMin: number) =>
    setCustomOffsetMin((m) => Math.max(5, m + deltaMin));

  const startPresets = useMemo(
    () => [
      ...OFFSET_PRESETS,
      { value: "anchor" as const, label: anchorLabel, offsetMin: null },
    ],
    [anchorLabel]
  );

  const endOptions = useMemo(() => {
    const opts: { value: EndModeValue; label: string }[] = [
      { value: "now", label: "Now" },
      { value: "custom", label: "Custom" },
    ];
    if (endAnchorLabel)
      opts.push({ value: "endAnchor", label: endAnchorLabel });
    return opts;
  }, [endAnchorLabel]);

  const { startMs, endMs, startLabel, endLabel } = useMemo(() => {
    // While live, advance "now" only when new data has actually arrived
    // (lastUpdatedAt), rather than on every 1s tick — the timeline should
    // only redraw in step with real data updates, not the clock.
    const now = live ? lastUpdatedAt ?? Date.now() : Date.now();
    const e =
      endMode === "now"
        ? now
        : endMode === "endAnchor"
        ? endAnchorMs ?? now
        : now - customOffsetMin * 60_000;
    const preset = startPresets.find((p) => p.value === start)!;
    const s =
      preset.offsetMin !== null
        ? e - preset.offsetMin * 60_000
        : anchorMs ?? now;
    const eLabel =
      endMode === "now"
        ? "now"
        : endMode === "endAnchor"
        ? endAnchorLabel ?? "now"
        : `${customOffsetMin}m ago`;
    return { startMs: s, endMs: e, startLabel: preset.label, endLabel: eLabel };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    start,
    endMode,
    customOffsetMin,
    anchorMs,
    endAnchorMs,
    endAnchorLabel,
    startPresets,
    live,
    lastUpdatedAt,
  ]);

  useEffect(() => {
    onChange({ startMs, endMs, live });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [startMs, endMs, live]);

  const isCustomEnd = endMode === "custom";
  const rangeSummary = `${startLabel} → ${endLabel}`;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          alignItems: "center",
          gap: 18,
          background: "#fafafa",
          border: "1px solid #eceef1",
          borderRadius: 6,
          padding: "12px 16px",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span
            style={{
              fontSize: 10,
              letterSpacing: "0.08em",
              color: "#9095a0",
              textTransform: "uppercase",
              fontWeight: 600,
              fontFamily: MONO,
            }}
          >
            Start
          </span>
          <div
            style={{
              display: "flex",
              border: "1px solid #e5e7eb",
              borderRadius: 5,
              overflow: "hidden",
            }}
          >
            {startPresets.map((p, i) => (
              <div
                key={p.value}
                onClick={() => setStart(p.value)}
                style={segStyle(start === p.value, i === 0)}
              >
                {p.label}
              </div>
            ))}
          </div>
        </div>

        <div style={{ width: 1, height: 20, background: "#e5e7eb" }} />

        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span
            style={{
              fontSize: 10,
              letterSpacing: "0.08em",
              color: "#9095a0",
              textTransform: "uppercase",
              fontWeight: 600,
              fontFamily: MONO,
            }}
          >
            End
          </span>
          <div
            style={{
              display: "flex",
              border: "1px solid #e5e7eb",
              borderRadius: 5,
              overflow: "hidden",
            }}
          >
            {endOptions.map((d, i) => (
              <div
                key={d.value}
                onClick={() => selectEnd(d.value)}
                style={segStyle(endMode === d.value, i === 0)}
              >
                {d.label}
              </div>
            ))}
          </div>

          {isCustomEnd && (
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: 4,
                marginLeft: 4,
              }}
            >
              <div
                onClick={() => nudge(15)}
                style={{
                  cursor: "pointer",
                  fontSize: 12,
                  color: "#4b5563",
                  border: "1px solid #e5e7eb",
                  borderRadius: 4,
                  padding: "5px 8px",
                  background: "#fff",
                  fontFamily: MONO,
                }}
              >
                « 15m
              </div>
              <div
                style={{
                  fontSize: 12,
                  color: "#1f2328",
                  padding: "0 6px",
                  whiteSpace: "nowrap",
                  fontFamily: MONO,
                }}
              >
                {customOffsetMin}m ago
              </div>
              <div
                onClick={() => nudge(-15)}
                style={{
                  cursor: "pointer",
                  fontSize: 12,
                  color: "#4b5563",
                  border: "1px solid #e5e7eb",
                  borderRadius: 4,
                  padding: "5px 8px",
                  background: "#fff",
                  fontFamily: MONO,
                }}
              >
                15m »
              </div>
            </div>
          )}
        </div>

        <div style={{ width: 1, height: 20, background: "#e5e7eb" }} />

        <div
          style={{
            marginLeft: "auto",
            display: "flex",
            alignItems: "center",
            gap: 12,
          }}
        >
          <LiveToggle live={live} onToggle={toggleLive} />
          <span
            style={{
              fontSize: 11,
              color: "#9095a0",
              fontFamily: MONO,
              whiteSpace: "nowrap",
            }}
          >
            {formatUpdatedText(lastUpdatedAt, live, tick)}
          </span>
        </div>
      </div>

      <div style={{ fontSize: 11, color: "#9095a0", fontFamily: MONO }}>
        Showing{" "}
        <span style={{ color: "#1f2328", fontWeight: 600 }}>
          {rangeSummary}
        </span>
      </div>
    </div>
  );
}
