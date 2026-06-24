import { useState } from "react";
import type { TimingWindows } from "../data/events";

interface Props {
  command: string;
  dockerImage: string;
  logUrl: string;
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

export default function TaskProperties({
  command,
  dockerImage,
  logUrl,
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

  return (
    <div
      style={{
        background: "#f8f9fa",
        border: "1px solid #e0e0e0",
        borderRadius: 8,
        padding: "1rem 1.5rem",
        fontFamily: "monospace",
        fontSize: "0.875rem",
        marginBottom: "1.5rem",
      }}
    >
      <table style={{ borderCollapse: "collapse", width: "100%" }}>
        <tbody>
          <Row
            label="Command"
            value={
              <code
                style={{
                  background: "#eee",
                  padding: "2px 6px",
                  borderRadius: 4,
                }}
              >
                {command}
              </code>
            }
          />
          <Row
            label="Docker Image"
            value={
              <code
                style={{
                  background: "#eee",
                  padding: "2px 6px",
                  borderRadius: 4,
                }}
              >
                {dockerImage}
              </code>
            }
          />
          {logUrl && (
            <Row
              label="Output folder"
              value={(() => {
                const outputPath = logUrl.replace(/\/[^/]+$/, "");
                const href = outputPath.replace(
                  /^gs:\/\/([^/]+)\/(.+)$/,
                  "https://console.cloud.google.com/storage/browser/$1/$2"
                );
                return (
                  <span style={{ display: "flex", alignItems: "center" }}>
                    <a
                      href={href}
                      target="_blank"
                      rel="noopener noreferrer"
                      style={{ wordBreak: "break-all" }}
                    >
                      {outputPath}
                    </a>
                    <CopyButton text={outputPath} />
                  </span>
                );
              })()}
            />
          )}
          {logUrl && (
            <Row
              label="Output log"
              value={
                <span style={{ display: "flex", alignItems: "center" }}>
                  <a
                    href={logUrl.replace(
                      /^gs:\/\/([^/]+)\/(.+)$/,
                      "https://storage.cloud.google.com/$1/$2"
                    )}
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{ wordBreak: "break-all" }}
                  >
                    {logUrl}
                  </a>
                  <CopyButton text={logUrl} />
                </span>
              }
            />
          )}
          <Row label="Status" value={status} />
          {localizeDuration && (
            <Row label="Localization" value={localizeDuration} />
          )}
          {duration && <Row label="Execution Time" value={duration} />}
          {uploadDuration && <Row label="Upload Time" value={uploadDuration} />}
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
            <Row label="Completed At" value={new Date(done).toLocaleString()} />
          )}
        </tbody>
      </table>
    </div>
  );
}
