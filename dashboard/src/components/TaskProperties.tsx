import { useState } from "react";
import type { TimingWindows } from "../data/events";

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

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const handleCopy = () => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  };
  return (
    <span
      onClick={handleCopy}
      title="Copy to clipboard"
      style={{
        marginLeft: 8,
        cursor: "pointer",
        fontSize: "1rem",
        color: copied ? "#2e7d32" : "#444",
        userSelect: "none",
      }}
    >
      {copied ? "✓" : "⎘"}
    </span>
  );
}

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

function GcsLink({ path }: { path: string }) {
  const href = path.replace(
    /^gs:\/\/([^/]+)\/(.+)$/,
    "https://storage.cloud.google.com/$1/$2"
  );
  return (
    <span style={{ display: "flex", alignItems: "center" }}>
      <a
        href={href}
        target="_blank"
        rel="noopener noreferrer"
        style={{ wordBreak: "break-all" }}
      >
        {path}
      </a>
      <CopyButton text={path} />
    </span>
  );
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <tr>
      <td
        style={{
          padding: "6px 16px 6px 0",
          color: "#888",
          fontWeight: 500,
          whiteSpace: "nowrap",
          verticalAlign: "top",
        }}
      >
        {label}
      </td>
      <td style={{ padding: "6px 0", wordBreak: "break-all" }}>{value}</td>
    </tr>
  );
}

const cardStyle: React.CSSProperties = {
  background: "#f8f9fa",
  border: "1px solid #e0e0e0",
  borderRadius: 8,
  padding: "1rem 1.5rem",
  fontFamily: "monospace",
  fontSize: "0.875rem",
  marginBottom: "1.5rem",
};

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
  const { claimed, running, writing, done } = timings;

  let duration: string | undefined;
  if (running && writing) {
    duration = formatDuration(writing.getTime() - running.getTime());
  } else if (claimed && done) {
    duration = formatDuration(done.getTime() - claimed.getTime());
  }

  const localizeDuration =
    running && claimed
      ? formatDuration(running.getTime() - claimed.getTime())
      : undefined;

  const uploadDuration =
    done && writing
      ? formatDuration(done.getTime() - writing.getTime())
      : undefined;

  const code = (s: string) => (
    <code style={{ background: "#eee", padding: "2px 6px", borderRadius: 4 }}>
      {s}
    </code>
  );

  return (
    <>
      <div style={cardStyle}>
        <table style={{ borderCollapse: "collapse", width: "100%" }}>
          <tbody>
            {command && <Row label="Command" value={code(command)} />}
            {dockerImage && (
              <Row label="Docker Image" value={code(dockerImage)} />
            )}
            <Row label="Status" value={status} />
            {exitCode != null && <Row label="Exit Code" value={exitCode} />}
            {failureReason && (
              <Row label="Failure Reason" value={failureReason} />
            )}
            {resultPath && (
              <Row label="Result Path" value={<GcsLink path={resultPath} />} />
            )}
            {logPath && (
              <Row label="Log Path" value={<GcsLink path={logPath} />} />
            )}
            {parameters.length > 0 && (
              <Row
                label="Parameters"
                value={
                  <table style={{ borderCollapse: "collapse" }}>
                    <tbody>
                      {parameters.map((p) => (
                        <tr key={p.name}>
                          <td style={{ paddingRight: 12, color: "#666" }}>
                            {p.name}
                          </td>
                          <td>{code(p.value)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                }
              />
            )}
            {localizeDuration && (
              <Row label="Localization" value={localizeDuration} />
            )}
            {duration && <Row label="Execution Time" value={duration} />}
            {uploadDuration && (
              <Row label="Upload Time" value={uploadDuration} />
            )}
            {claimed && (
              <Row
                label="Claimed At"
                value={new Date(claimed).toLocaleString()}
              />
            )}
            {running && (
              <Row
                label="Running At"
                value={new Date(running).toLocaleString()}
              />
            )}
            {writing && (
              <Row
                label="Writing At"
                value={new Date(writing).toLocaleString()}
              />
            )}
            {done && (
              <Row
                label="Completed At"
                value={new Date(done).toLocaleString()}
              />
            )}
          </tbody>
        </table>
      </div>

      {resourceUsage && (
        <div style={cardStyle}>
          <div
            style={{
              fontSize: "0.75rem",
              letterSpacing: 1.5,
              color: "#aaa",
              textTransform: "uppercase",
              marginBottom: "0.75rem",
            }}
          >
            Resource Usage
          </div>
          <table style={{ borderCollapse: "collapse", width: "100%" }}>
            <tbody>
              <Row
                label="Elapsed"
                value={`${resourceUsage.elapsed_seconds.toFixed(1)}s`}
              />
              <Row
                label="Peak Memory"
                value={formatBytes(resourceUsage.max_memory_bytes)}
              />
              <Row
                label="CPU User"
                value={`${(resourceUsage.cpu_user_usec / 1_000_000).toFixed(
                  2
                )}s`}
              />
              <Row
                label="CPU System"
                value={`${(resourceUsage.cpu_system_usec / 1_000_000).toFixed(
                  2
                )}s`}
              />
              <Row
                label="Block Read"
                value={formatBytes(resourceUsage.block_read_bytes)}
              />
              <Row
                label="Block Write"
                value={formatBytes(resourceUsage.block_write_bytes)}
              />
              {resourceUsage.oom_killed && (
                <Row
                  label="OOM Killed"
                  value={
                    <span style={{ color: "#b71c1c", fontWeight: 600 }}>
                      yes
                    </span>
                  }
                />
              )}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}
