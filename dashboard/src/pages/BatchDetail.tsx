import { useState, useEffect } from "react";
import { useParams, Link } from "react-router-dom";
import type { BatchRecord } from "./WorkPoolDetail";
import { StatusBadge } from "./WorkPoolDetail";

const MONO = "'JetBrains Mono', 'Courier New', monospace";

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

function useBatch(batchId: string | undefined): BatchRecord | undefined {
  const [batch, setBatch] = useState<BatchRecord | undefined>();
  useEffect(() => {
    if (!batchId) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await fetch(`/api/v1/batch/${batchId}`);
        if (!r.ok || cancelled) return;
        const data = await r.json();
        if (!cancelled) setBatch(data);
      } catch (_) {}
    };
    poll();
    const id = setInterval(poll, 5000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [batchId]);
  return batch;
}

export default function BatchDetail() {
  const { workpoolId, batchId } = useParams<{
    workpoolId: string;
    batchId: string;
  }>();
  const batch = useBatch(batchId);
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

      <h1 style={{ margin: "0 0 1.5rem", fontSize: "1.1rem", fontWeight: 700 }}>
        Batch API Request
      </h1>

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
          <Row label="job_id" value={batch.job_id || dash} />
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
