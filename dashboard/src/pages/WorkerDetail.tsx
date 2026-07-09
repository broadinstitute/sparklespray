import { useState, useEffect } from "react";
import { useParams, Link } from "react-router-dom";
import type { WorkerRecord } from "./WorkPoolDetail";
import { StatusBadge } from "./WorkPoolDetail";
import { RefreshToggle } from "../components/RefreshControls";

const MONO = "'IBM Plex Mono', monospace";

const WORKER_COLORS: Record<string, string> = {
  started: "#2e7d32",
  stopped: "#aaaaaa",
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

function useWorker(
  workerId: string | undefined,
  active: boolean
): { worker: WorkerRecord | undefined; lastUpdatedAt: number | null } {
  const [worker, setWorker] = useState<WorkerRecord | undefined>();
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | null>(null);
  useEffect(() => {
    if (!workerId || !active) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await fetch(`/api/v1/worker/${workerId}`);
        if (!r.ok || cancelled) return;
        const data = await r.json();
        if (!cancelled) {
          setWorker(data);
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
  }, [workerId, active]);
  return { worker, lastUpdatedAt };
}

export default function WorkerDetail() {
  const { workpoolId, workerId } = useParams<{
    workpoolId: string;
    workerId: string;
  }>();
  const [live, setLive] = useState(true);
  const { worker, lastUpdatedAt } = useWorker(workerId, live);
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
          to={`/workpools/${workpoolId}/workers`}
          style={{ color: "#1565c0", textDecoration: "none" }}
        >
          Workers
        </Link>
        <span style={{ color: "#aaa", margin: "0 6px" }}>›</span>
        <span style={{ color: "#555" }}>{workerId}</span>
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
          Worker
        </h1>
        <RefreshToggle
          live={live}
          onToggle={() => setLive(!live)}
          lastUpdatedAt={lastUpdatedAt}
        />
      </div>

      {!worker ? (
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
          <Row label="worker_id" value={worker.worker_id} />
          <Row
            label="status"
            value={
              <StatusBadge status={worker.status} colorMap={WORKER_COLORS} />
            }
          />
          <Row label="workpool_id" value={worker.workpool_id || dash} />
          <Row
            label="batch_id"
            value={
              worker.batch_id ? (
                <Link
                  to={`/workpools/${workpoolId}/batches/${worker.batch_id}`}
                  style={{ color: "#1565c0", textDecoration: "none" }}
                >
                  {worker.batch_id}
                </Link>
              ) : (
                dash
              )
            }
          />
          <Row label="instance_name" value={worker.instance_name || dash} />
          <Row
            label="expiry"
            value={
              worker.expiry ? new Date(worker.expiry).toLocaleString() : dash
            }
          />
          <Row
            label="heartbeat_expiry"
            value={
              worker.heartbeat_expiry
                ? new Date(worker.heartbeat_expiry).toLocaleString()
                : dash
            }
          />
        </div>
      )}
    </div>
  );
}
