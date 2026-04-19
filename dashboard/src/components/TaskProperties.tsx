import type { TimingWindows } from "../data/events";

interface Props {
  command: string;
  dockerImage: string;
  timings: TimingWindows;
  status: string;
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
  timings,
  status,
}: Props) {
  const {
    claimed,
    exec_started,
    exec_complete,
    complete,
    exitCode,
    maxMemInGb,
  } = timings;

  let duration: string | undefined;
  if (exec_started && exec_complete) {
    duration = formatDuration(exec_complete.getTime() - exec_started.getTime());
  } else if (claimed && complete) {
    duration = formatDuration(complete.getTime() - claimed.getTime());
  }

  const localizeDuration =
    exec_started && claimed
      ? formatDuration(exec_started.getTime() - claimed.getTime())
      : undefined;

  const uploadDuration =
    complete && exec_complete
      ? formatDuration(complete.getTime() - exec_complete.getTime())
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
          <Row label="Status" value={status} />
          {exitCode !== undefined && (
            <Row
              label="Exit Code"
              value={exitCode === 0 ? "0 (success)" : `${exitCode} (error)`}
            />
          )}
          {maxMemInGb !== undefined && (
            <Row label="Peak Memory" value={`${maxMemInGb.toFixed(3)} GB`} />
          )}
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
          {exec_started && (
            <Row
              label="Exec Started At"
              value={new Date(exec_started).toLocaleString()}
            />
          )}
          {exec_complete && (
            <Row
              label="Exec Complete At"
              value={new Date(exec_complete).toLocaleString()}
            />
          )}
          {complete && (
            <Row
              label="Completed At"
              value={new Date(complete).toLocaleString()}
            />
          )}
        </tbody>
      </table>
    </div>
  );
}
