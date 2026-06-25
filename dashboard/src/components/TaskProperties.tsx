import { useState } from "react";
import type { TimingWindows } from "../data/events";

const MONO = "'IBM Plex Mono', monospace";
const SANS = "'IBM Plex Sans', sans-serif";

interface ResourceUsage {
  elapsed_seconds: number;
  max_memory_bytes: number;
  cpu_user_usec: number;
  cpu_system_usec: number;
  block_read_bytes: number;
  block_write_bytes: number;
  oom_killed: boolean;
}

interface Props {
  command: string;
  dockerImage: string;
  logPath: string;
  resultPath: string;
  exitCode: number | null;
  failureReason: string;
  parameters: { name: string; value: string }[];
  resourceUsage: ResourceUsage | null;
  timings: TimingWindows;
  status: string;
}

// ── colour tokens ──────────────────────────────────────────────────────────

const STATUS_STYLE: Record<
  string,
  { bg: string; text: string; dot: string }
> = {
  success: { bg: "#e7f5ec", text: "#1a7f44", dot: "#2ea05f" },
  error: { bg: "#fdecea", text: "#c62828", dot: "#f44336" },
  failed: { bg: "#fdecea", text: "#c62828", dot: "#f44336" },
  killed: { bg: "#f5f5f5", text: "#616161", dot: "#9e9e9e" },
  running: { bg: "#e8f0fe", text: "#1565c0", dot: "#2f6fdb" },
  writing: { bg: "#e8f5e9", text: "#2e7d32", dot: "#4caf50" },
  claimed: { bg: "#fff3e0", text: "#e65100", dot: "#fb8c00" },
  pending: { bg: "#f5f5f5", text: "#757575", dot: "#bdbdbd" },
};

function formatDuration(ms: number): string {
  if (ms < 60_000) return `${Math.round(ms / 1000)}s`;
  const m = Math.floor(ms / 60_000);
  const s = Math.round((ms % 60_000) / 1000);
  return `${m}m ${s}s`;
}

function formatBytes(n: number): string {
  if (n >= 1_073_741_824) return `${(n / 1_073_741_824).toFixed(2)} GB`;
  if (n >= 1_048_576) return `${(n / 1_048_576).toFixed(1)} MB`;
  if (n >= 1_024) return `${(n / 1_024).toFixed(0)} KB`;
  return `${n} B`;
}

function fmtTime(d: Date): string {
  return d.toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

// ── small reusable pieces ──────────────────────────────────────────────────

const ExternalLinkIcon = () => (
  <svg
    width="11"
    height="11"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2.2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <path d="M7 17 17 7" />
    <path d="M8 7h9v9" />
  </svg>
);

const CopyIcon = () => (
  <svg
    width="13"
    height="13"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <rect x="9" y="9" width="11" height="11" rx="2" />
    <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
  </svg>
);

const CheckIcon = ({ color = "#1a7f44" }: { color?: string }) => (
  <svg
    width="13"
    height="13"
    viewBox="0 0 24 24"
    fill="none"
    stroke={color}
    strokeWidth="2.6"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <polyline points="20 6 9 17 4 12" />
  </svg>
);

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      onClick={() => {
        navigator.clipboard.writeText(text).catch(() => {});
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      }}
      title="Copy URL"
      style={{
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        width: 28,
        height: 28,
        border: "1px solid #e6e8ec",
        borderRadius: 7,
        background: "#fff",
        color: "#8a909a",
        cursor: "pointer",
        flexShrink: 0,
      }}
    >
      {copied ? <CheckIcon /> : <CopyIcon />}
    </button>
  );
}

function GcsPathRow({ label, path }: { label: string; path: string }) {
  const href = path.replace(
    /^gs:\/\/([^/]+)\/(.+)$/,
    "https://storage.cloud.google.com/$1/$2"
  );
  const name = path.split("/").pop() || path;
  return (
    <>
      <span style={{ font: `500 12.5px ${MONO}`, color: "#9aa1ac" }}>
        {label}
      </span>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <a
          href={href}
          target="_blank"
          rel="noopener noreferrer"
          title={path}
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: 6,
            color: "#2f6fdb",
            textDecoration: "none",
            font: `500 13px ${MONO}`,
          }}
        >
          {name}
          <ExternalLinkIcon />
        </a>
        <CopyButton text={path} />
      </div>
    </>
  );
}

const SectionLabel = ({ children }: { children: React.ReactNode }) => (
  <div
    style={{
      font: `600 11px ${MONO}`,
      letterSpacing: ".14em",
      textTransform: "uppercase",
      color: "#9aa1ac",
      marginBottom: 18,
    }}
  >
    {children}
  </div>
);

const CARD: React.CSSProperties = {
  background: "#fbfcfd",
  border: "1px solid #eef1f4",
  borderRadius: 12,
  padding: "22px 24px",
};

// ── Lifecycle stepper ──────────────────────────────────────────────────────

interface LifecycleStep {
  label: string;
  subtitle: string;
  time: Date | undefined;
  deltaMs: number | null;
  terminal: boolean;
  terminalOk: boolean;
  terminalReached: boolean;
}

function LifecycleDot({ step }: { step: LifecycleStep }) {
  if (step.terminal && step.terminalReached) {
    const bg = step.terminalOk ? "#1a7f44" : "#c62828";
    return (
      <span
        style={{
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          width: 16,
          height: 16,
          borderRadius: "50%",
          background: bg,
          border: `3px solid ${bg}`,
          flexShrink: 0,
          justifySelf: "center",
        }}
      >
        {step.terminalOk ? (
          <svg
            width="9"
            height="9"
            viewBox="0 0 24 24"
            fill="none"
            stroke="#fff"
            strokeWidth="3.4"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <polyline points="20 6 9 17 4 12" />
          </svg>
        ) : (
          <svg
            width="9"
            height="9"
            viewBox="0 0 24 24"
            fill="none"
            stroke="#fff"
            strokeWidth="3.4"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <line x1="18" y1="6" x2="6" y2="18" />
            <line x1="6" y1="6" x2="18" y2="18" />
          </svg>
        )}
      </span>
    );
  }
  const happened = step.time != null;
  return (
    <span
      style={{
        display: "inline-block",
        width: 16,
        height: 16,
        borderRadius: "50%",
        background: "#fbfcfd",
        border: `3px solid ${happened ? "#2f6fdb" : "#d1d5db"}`,
        flexShrink: 0,
        justifySelf: "center",
      }}
    />
  );
}

function LifecycleStepper({
  timings,
  status,
}: {
  timings: TimingWindows;
  status: string;
}) {
  const { claimed, running, writing, done } = timings;

  const terminalLabel =
    status === "success"
      ? "Completed"
      : status === "error"
      ? "Error"
      : status === "failed"
      ? "Failed"
      : status === "killed"
      ? "Killed"
      : "Completed";

  const steps: LifecycleStep[] = [
    {
      label: "Claimed",
      subtitle: "queued",
      time: claimed,
      deltaMs: null,
      terminal: false,
      terminalOk: false,
      terminalReached: false,
    },
    {
      label: "Running",
      subtitle: "localized",
      time: running,
      deltaMs:
        claimed && running ? running.getTime() - claimed.getTime() : null,
      terminal: false,
      terminalOk: false,
      terminalReached: false,
    },
    {
      label: "Writing",
      subtitle: "executed",
      time: writing,
      deltaMs:
        running && writing ? writing.getTime() - running.getTime() : null,
      terminal: false,
      terminalOk: false,
      terminalReached: false,
    },
    {
      label: terminalLabel,
      subtitle: "uploaded",
      time: done,
      deltaMs:
        writing && done
          ? done.getTime() - writing.getTime()
          : claimed && done && !writing
          ? done.getTime() - claimed.getTime()
          : null,
      terminal: true,
      terminalReached: ["success", "error", "failed", "killed"].includes(
        status
      ),
      terminalOk: status === "success",
    },
  ];

  const totalMs = claimed && done ? done.getTime() - claimed.getTime() : null;

  return (
    <div style={{ ...CARD, display: "flex", flexDirection: "column" }}>
      <SectionLabel>Lifecycle</SectionLabel>
      <div style={{ position: "relative", flex: 1 }}>
        <div
          style={{
            position: "absolute",
            left: 8,
            top: 9,
            bottom: 26,
            width: 2,
            background: "#e1e5ea",
          }}
        />
        {steps.map((step, i) => (
          <div
            key={step.label}
            style={{
              position: "relative",
              display: "grid",
              gridTemplateColumns: "18px 1fr auto",
              gap: 13,
              alignItems: "start",
              paddingBottom: i < steps.length - 1 ? 22 : 0,
            }}
          >
            <LifecycleDot step={step} />
            <div>
              <div
                style={{
                  font: `600 13.5px ${SANS}`,
                  color: step.time ? "#16191d" : "#9aa1ac",
                }}
              >
                {step.label}
              </div>
              {step.time && (
                <div
                  style={{
                    font: `400 11.5px ${MONO}`,
                    color: "#9aa1ac",
                    marginTop: 3,
                  }}
                >
                  {step.subtitle} · {fmtTime(step.time)}
                </div>
              )}
            </div>
            <span
              style={{
                font: `600 12px ${MONO}`,
                color: step.terminal
                  ? step.terminalOk
                    ? "#1a7f44"
                    : "#c62828"
                  : "#2f6fdb",
                whiteSpace: "nowrap",
              }}
            >
              {i === 0
                ? "start"
                : step.deltaMs != null
                ? `+${formatDuration(step.deltaMs)}`
                : ""}
            </span>
          </div>
        ))}
      </div>
      <div
        style={{
          marginTop: 18,
          paddingTop: 16,
          borderTop: "1px solid #e8ebef",
          display: "flex",
          alignItems: "baseline",
          justifyContent: "space-between",
        }}
      >
        <span
          style={{
            font: `600 11px ${MONO}`,
            letterSpacing: ".14em",
            textTransform: "uppercase",
            color: "#6e7681",
          }}
        >
          Total
        </span>
        <span style={{ font: `600 19px ${MONO}`, color: "#16191d" }}>
          {totalMs != null ? formatDuration(totalMs) : "—"}
        </span>
      </div>
    </div>
  );
}

// ── Resource tile ──────────────────────────────────────────────────────────

function ResourceTile({ label, value }: { label: string; value: string }) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        background: "#fbfcfd",
        padding: "16px 18px",
      }}
    >
      <span
        style={{
          font: `500 11px ${MONO}`,
          letterSpacing: ".06em",
          textTransform: "uppercase",
          color: "#9aa1ac",
        }}
      >
        {label}
      </span>
      <span style={{ font: `600 14px ${MONO}`, color: "#2b3138" }}>
        {value}
      </span>
    </div>
  );
}

// ── Main component ─────────────────────────────────────────────────────────

export default function TaskProperties({
  command,
  dockerImage,
  logPath,
  resultPath,
  exitCode,
  failureReason,
  parameters,
  resourceUsage,
  timings,
  status,
}: Props) {
  const st = STATUS_STYLE[status] ?? {
    bg: "#f5f5f5",
    text: "#555",
    dot: "#9e9e9e",
  };

  const detailRows: { label: string; value: React.ReactNode }[] = [];

  if (command) {
    detailRows.push({
      label: "command",
      value: (
        <code style={{ font: `500 13px ${MONO}`, color: "#2b3138" }}>
          {command}
        </code>
      ),
    });
  }
  if (dockerImage) {
    detailRows.push({
      label: "image",
      value: (
        <span style={{ font: `500 13px ${MONO}`, color: "#2b3138" }}>
          {dockerImage}
        </span>
      ),
    });
  }
  detailRows.push({
    label: "status",
    value: (
      <span
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 6,
          height: 22,
          padding: "0 10px",
          borderRadius: 999,
          background: st.bg,
          color: st.text,
          font: `600 12px ${SANS}`,
        }}
      >
        <span
          style={{
            width: 7,
            height: 7,
            borderRadius: "50%",
            background: st.dot,
            flexShrink: 0,
          }}
        />
        {status}
      </span>
    ),
  });
  if (exitCode != null) {
    detailRows.push({
      label: "exit code",
      value: (
        <span style={{ font: `500 13px ${MONO}`, color: "#2b3138" }}>
          {exitCode}
        </span>
      ),
    });
  }
  if (failureReason) {
    detailRows.push({
      label: "failure",
      value: (
        <span style={{ font: `500 13px ${MONO}`, color: "#c62828" }}>
          {failureReason}
        </span>
      ),
    });
  }
  if (parameters.length > 0) {
    detailRows.push({
      label: "parameters",
      value: (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "auto 1fr",
            gap: "4px 12px",
          }}
        >
          {parameters.map((p) => (
            <>
              <span
                key={p.name + "-k"}
                style={{ font: `500 12px ${MONO}`, color: "#9aa1ac" }}
              >
                {p.name}
              </span>
              <code
                key={p.name + "-v"}
                style={{ font: `500 12px ${MONO}`, color: "#2b3138" }}
              >
                {p.value}
              </code>
            </>
          ))}
        </div>
      ),
    });
  }

  return (
    <div>
      {/* two-column grid: details | lifecycle */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1.25fr 1fr",
          gap: 24,
          marginBottom: 24,
        }}
      >
        {/* Details */}
        <div style={CARD}>
          <SectionLabel>Details</SectionLabel>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "108px 1fr",
              rowGap: 15,
              columnGap: 14,
              alignItems: "center",
            }}
          >
            {detailRows.map((r) => (
              <>
                <span
                  key={String(r.label) + "-lbl"}
                  style={{ font: `500 12.5px ${MONO}`, color: "#9aa1ac" }}
                >
                  {r.label}
                </span>
                <div key={String(r.label) + "-val"}>{r.value}</div>
              </>
            ))}
            {resultPath && <GcsPathRow label="result" path={resultPath} />}
            {logPath && <GcsPathRow label="log" path={logPath} />}
          </div>
        </div>

        {/* Lifecycle */}
        <LifecycleStepper timings={timings} status={status} />
      </div>

      {/* Resource usage */}
      {resourceUsage && (
        <div style={{ marginBottom: 24 }}>
          <div
            style={{
              font: `600 11px ${MONO}`,
              letterSpacing: ".14em",
              textTransform: "uppercase",
              color: "#9aa1ac",
              marginBottom: 14,
            }}
          >
            Resource Usage
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(3, 1fr)",
              gap: 1,
              background: "#eceef1",
              border: "1px solid #eceef1",
              borderRadius: 10,
              overflow: "hidden",
            }}
          >
            <ResourceTile
              label="peak mem"
              value={formatBytes(resourceUsage.max_memory_bytes)}
            />
            <ResourceTile
              label="cpu user"
              value={`${(resourceUsage.cpu_user_usec / 1_000_000).toFixed(2)}s`}
            />
            <ResourceTile
              label="cpu sys"
              value={`${(resourceUsage.cpu_system_usec / 1_000_000).toFixed(
                2
              )}s`}
            />
            <ResourceTile
              label="block read"
              value={formatBytes(resourceUsage.block_read_bytes)}
            />
            <ResourceTile
              label="block write"
              value={formatBytes(resourceUsage.block_write_bytes)}
            />
            <ResourceTile
              label="elapsed"
              value={`${resourceUsage.elapsed_seconds.toFixed(1)}s`}
            />
            {resourceUsage.oom_killed && (
              <ResourceTile label="oom killed" value="yes" />
            )}
          </div>
        </div>
      )}
    </div>
  );
}
