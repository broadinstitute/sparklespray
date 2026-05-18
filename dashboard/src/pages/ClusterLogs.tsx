import { useState, useEffect } from "react";
import { useParams, Link } from "react-router-dom";

interface LogSummaryEntry {
  source: "batchapi" | "log";
  timestamp: string;
  jobId: string;
  content: string;
}

interface BatchJobInfo {
  state: string;
  url: string;
}

interface LogSummaryResponse {
  entries: LogSummaryEntry[];
  googleBatchJobs: BatchJobInfo[];
}

function formatEntry(e: LogSummaryEntry): string {
  const shortJob = e.jobId.split("/").pop() ?? e.jobId;
  return `[${e.timestamp}] [${e.source.padEnd(8)}] [${shortJob}]\n${e.content}`;
}

export default function ClusterLogs() {
  const { clusterId } = useParams<{ clusterId: string }>();
  const [response, setResponse] = useState<LogSummaryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!clusterId) return;
    setResponse(null);
    setError(null);

    fetch(`/api/v1/cluster/${clusterId}/log-summary`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json() as Promise<LogSummaryResponse>;
      })
      .then(setResponse)
      .catch((err) => setError(String(err)));
  }, [clusterId]);

  if (!clusterId) {
    return (
      <div style={{ padding: "2rem", fontFamily: "monospace" }}>
        Invalid cluster ID.
      </div>
    );
  }

  return (
    <div
      style={{
        maxWidth: 960,
        margin: "0 auto",
        padding: "2rem",
        fontFamily: "monospace",
      }}
    >
      <div style={{ marginBottom: "1rem" }}>
        <Link
          to={`/clusters/${clusterId}`}
          style={{
            color: "#1565c0",
            textDecoration: "none",
            fontSize: "0.85rem",
          }}
        >
          ← {clusterId}
        </Link>
      </div>

      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: "1.5rem",
        }}
      >
        <h1 style={{ margin: 0, fontSize: "1.3rem", fontWeight: 700 }}>
          Cluster logs
        </h1>
        <a
          href={`/api/v1/cluster/${clusterId}/log-summary?download`}
          target="_blank"
          rel="noreferrer"
          style={{
            fontSize: "0.8rem",
            padding: "0.35rem 0.85rem",
            border: "1px solid #c5cae9",
            borderRadius: 6,
            background: "#f5f5ff",
            color: "#1a237e",
            textDecoration: "none",
            fontWeight: 500,
          }}
        >
          Download
        </a>
      </div>

      {error && (
        <div
          style={{
            padding: "0.75rem 1rem",
            background: "#fdecea",
            border: "1px solid #f5c6cb",
            borderRadius: 6,
            color: "#b71c1c",
            marginBottom: "1rem",
          }}
        >
          {error}
        </div>
      )}

      {!error && response === null && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: "0.75rem",
            color: "#555",
            fontSize: "0.9rem",
          }}
        >
          <Spinner />
          Loading logs…
        </div>
      )}

      {response !== null && response.googleBatchJobs.length > 0 && (
        <div style={{ marginBottom: "1.5rem" }}>
          <div
            style={{
              fontSize: "0.8rem",
              color: "#888",
              marginBottom: "0.5rem",
            }}
          >
            Google Batch jobs
          </div>
          <div
            style={{ display: "flex", flexDirection: "column", gap: "0.35rem" }}
          >
            {response.googleBatchJobs.map((job, i) => (
              <div
                key={i}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "0.75rem",
                  fontSize: "0.85rem",
                }}
              >
                <StateChip state={job.state} />
                <a
                  href={job.url}
                  target="_blank"
                  rel="noreferrer"
                  style={{ color: "#1565c0" }}
                >
                  {job.url.split("/jobs/")[1]?.split("/")[0] ?? job.url}
                </a>
              </div>
            ))}
          </div>
        </div>
      )}

      {response !== null && response.entries.length === 0 && (
        <div style={{ color: "#888" }}>
          No log entries found for this cluster.
        </div>
      )}

      {response !== null && response.entries.length > 0 && (
        <pre
          style={{
            background: "#1e1e1e",
            color: "#d4d4d4",
            padding: "1rem 1.25rem",
            borderRadius: 8,
            overflowX: "auto",
            fontSize: "0.78rem",
            lineHeight: 1.55,
            whiteSpace: "pre-wrap",
            wordBreak: "break-all",
          }}
        >
          {response.entries.map((e) => formatEntry(e)).join("\n\n")}
        </pre>
      )}
    </div>
  );
}

const STATE_COLORS: Record<string, { bg: string; fg: string }> = {
  SUCCEEDED: { bg: "#e8f5e9", fg: "#2e7d32" },
  FAILED: { bg: "#fdecea", fg: "#b71c1c" },
  CANCELLED: { bg: "#f3e5f5", fg: "#6a1b9a" },
  RUNNING: { bg: "#e3f2fd", fg: "#1565c0" },
  SCHEDULED: { bg: "#fff8e1", fg: "#f57f17" },
  QUEUED: { bg: "#fff8e1", fg: "#f57f17" },
};

function StateChip({ state }: { state: string }) {
  const colors = STATE_COLORS[state] ?? { bg: "#f5f5f5", fg: "#555" };
  return (
    <span
      style={{
        background: colors.bg,
        color: colors.fg,
        borderRadius: 4,
        padding: "0.1rem 0.45rem",
        fontSize: "0.75rem",
        fontWeight: 600,
        letterSpacing: "0.02em",
        whiteSpace: "nowrap",
      }}
    >
      {state}
    </span>
  );
}

function Spinner() {
  return (
    <svg
      width="18"
      height="18"
      viewBox="0 0 18 18"
      style={{ animation: "spin 0.9s linear infinite" }}
    >
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
      <circle
        cx="9"
        cy="9"
        r="7"
        fill="none"
        stroke="#bbb"
        strokeWidth="2.5"
        strokeDasharray="30 14"
        strokeLinecap="round"
      />
    </svg>
  );
}
