import { useState, useEffect } from "react";
import { useParams, Link } from "react-router-dom";
import type { BatchRecord } from "./WorkPoolDetail";
import { StatusBadge } from "./WorkPoolDetail";
import { RefreshToggle } from "../components/RefreshControls";

const MONO = "'IBM Plex Mono', monospace";

const BATCH_STATUS_COLORS: Record<string, string> = {
  pending: "#1565c0",
  started: "#2e7d32",
  completed: "#00695c",
  failed: "#b71c1c",
};

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div
      style={{
        display: "flex",
        justifyContent: "space-between",
        gap: "1rem",
        padding: "7px 0",
        borderBottom: "1px solid #f0f0f0",
        fontSize: "0.82rem",
        fontFamily: MONO,
      }}
    >
      <span style={{ color: "#999", flexShrink: 0, minWidth: 180 }}>
        {label}
      </span>
      <span
        style={{ color: "#222", textAlign: "right", wordBreak: "break-all" }}
      >
        {value}
      </span>
    </div>
  );
}

function useBatch(
  batchId: string | undefined,
  active: boolean
): { batch: BatchRecord | undefined; lastUpdatedAt: number | null } {
  const [batch, setBatch] = useState<BatchRecord | undefined>();
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | null>(null);
  useEffect(() => {
    if (!batchId || !active) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await fetch(`/api/v1/batch/${batchId}`);
        if (!r.ok || cancelled) return;
        const data = await r.json();
        if (!cancelled) {
          setBatch(data);
          setLastUpdatedAt(Date.now());
        }
      } catch (_) {}
    };
    poll();
    const id = setInterval(poll, 5000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [batchId, active]);
  return { batch, lastUpdatedAt };
}

export default function BatchDetail() {
  const { workpoolId, batchId } = useParams<{
    workpoolId: string;
    batchId: string;
  }>();
  const [live, setLive] = useState(true);
  const { batch, lastUpdatedAt } = useBatch(batchId, live);
  const dash = <span style={{ color: "#ccc" }}>—</span>;

  return (
    <div style={{ padding: "2rem", fontFamily: MONO }}>
      <div style={{ marginBottom: "1rem", fontSize: "0.82rem" }}>
        <Link
          to={`/workpools/${workpoolId}`}
          style={{ color: "#1565c0", textDecoration: "none" }}
        >
          {workpoolId}
        </Link>
        <span style={{ color: "#aaa", margin: "0 6px" }}>›</span>
        <Link
          to={`/workpools/${workpoolId}/batches`}
          style={{ color: "#1565c0", textDecoration: "none" }}
        >
          Batch API Requests
        </Link>
        <span style={{ color: "#aaa", margin: "0 6px" }}>›</span>
        <span style={{ color: "#555" }}>{batchId}</span>
      </div>

      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: "1.5rem",
        }}
      >
        <h1 style={{ margin: 0, fontSize: "1.1rem", fontWeight: 700 }}>
          Batch API Request
        </h1>
        <RefreshToggle
          live={live}
          onToggle={() => setLive(!live)}
          lastUpdatedAt={lastUpdatedAt}
        />
      </div>

      {!batch ? (
        <div style={{ color: "#aaa" }}>Loading…</div>
      ) : (
        <div
          style={{
            maxWidth: 680,
            background: "#f8f9fa",
            border: "1px solid #e0e0e0",
            borderRadius: 8,
            padding: "0.75rem 1rem",
          }}
        >
          <Row label="batch_id" value={batch.batch_id} />
          <Row
            label="status"
            value={
              <StatusBadge
                status={batch.status}
                colorMap={BATCH_STATUS_COLORS}
              />
            }
          />
          <Row
            label="job_id"
            value={
              batch.job_id
                ? (() => {
                    const m = batch.job_id.match(
                      /^projects\/([^/]+)\/locations\/([^/]+)\/jobs\/([^/]+)$/
                    );
                    if (m) {
                      const url = `https://console.cloud.google.com/batch/jobsDetail/regions/${m[2]}/jobs/${m[3]}/details?project=${m[1]}`;
                      return (
                        <a
                          href={url}
                          target="_blank"
                          rel="noopener noreferrer"
                          style={{ color: "#1565c0" }}
                        >
                          {batch.job_id}
                        </a>
                      );
                    }
                    return batch.job_id;
                  })()
                : dash
            }
          />
          <Row label="workpool_id" value={batch.workpool_id || dash} />
          <Row
            label="expected_vm_count"
            value={batch.expected_vm_count ?? dash}
          />
          <Row
            label="preemptible"
            value={
              <span style={{ color: batch.preemptible ? "#6a1b9a" : "#555" }}>
                {batch.preemptible ? "yes" : "no"}
              </span>
            }
          />
          <Row
            label="registered_worker_count"
            value={batch.registered_worker_count ?? dash}
          />
          <Row
            label="submitted_at"
            value={
              batch.submitted_at
                ? new Date(batch.submitted_at).toLocaleString()
                : dash
            }
          />
          <Row
            label="running_since"
            value={
              batch.running_since
                ? new Date(batch.running_since).toLocaleString()
                : dash
            }
          />
          <Row
            label="unhealthy"
            value={
              <span
                style={{
                  color: batch.unhealthy ? "#b71c1c" : "#aaa",
                  fontWeight: batch.unhealthy ? 600 : 400,
                }}
              >
                {batch.unhealthy ? "⚠ yes" : "no"}
              </span>
            }
          />
        </div>
      )}
    </div>
  );
}
