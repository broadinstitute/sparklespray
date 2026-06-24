import { useState, useEffect } from "react";
import { useParams } from "react-router-dom";

const POLL_MS = 30_000;

interface WorkpoolDetail {
  workpool_id: string;
  machine_type: string;
  region: string;
  status: string;
  status_message: string;
  last_incident_at: string | null;
  incident_count: number;
  expiry: string;
}

interface WorkerInfo {
  worker_id: string;
  status: string;
  instance_name: string;
  heartbeat_expiry: string;
}

export default function ClusterDetail() {
  const { clusterId } = useParams<{ clusterId: string }>();
  const [workpool, setWorkpool] = useState<WorkpoolDetail | null>(null);
  const [workers, setWorkers] = useState<WorkerInfo[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!clusterId) return;
    let cancelled = false;

    async function poll() {
      while (!cancelled) {
        try {
          const [wpRes, wRes] = await Promise.all([
            fetch(`/api/v1/workpool/${clusterId}`),
            fetch(`/api/v1/workpool/${clusterId}/workers?status=started`),
          ]);
          if (wpRes.ok) setWorkpool(await wpRes.json());
          if (wRes.ok) setWorkers(await wRes.json());
          setLoading(false);
        } catch (err) {
          console.error("[ClusterDetail] poll error:", err);
        }
        await new Promise<void>((r) => setTimeout(r, POLL_MS));
      }
    }

    poll();
    return () => {
      cancelled = true;
    };
  }, [clusterId]);

  if (!clusterId) {
    return (
      <div style={{ padding: "2rem", fontFamily: "monospace" }}>
        Invalid workpool ID.
      </div>
    );
  }

  if (loading) {
    return (
      <div style={{ padding: "2rem", fontFamily: "monospace" }}>Loading…</div>
    );
  }

  if (!workpool) {
    return (
      <div style={{ padding: "2rem", fontFamily: "monospace" }}>
        Workpool not found: <strong>{clusterId}</strong>
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
      <h1 style={{ margin: "0 0 1.5rem", fontSize: "1.3rem", fontWeight: 700 }}>
        {clusterId}
      </h1>

      <div
        style={{
          background: "#f8f9fa",
          border: "1px solid #e0e0e0",
          borderRadius: 8,
          padding: "1rem 1.5rem",
          marginBottom: "1.5rem",
          fontSize: "0.85rem",
        }}
      >
        <div style={{ display: "flex", gap: "2rem", flexWrap: "wrap" }}>
          <span>
            <span style={{ color: "#888" }}>status </span>
            <span style={{ fontWeight: 600 }}>{workpool.status || "—"}</span>
          </span>
          <span>
            <span style={{ color: "#888" }}>machine type </span>
            <span style={{ fontWeight: 600 }}>
              {workpool.machine_type || "—"}
            </span>
          </span>
          <span>
            <span style={{ color: "#888" }}>region </span>
            <span style={{ fontWeight: 600 }}>{workpool.region || "—"}</span>
          </span>
          <span>
            <span style={{ color: "#888" }}>active workers </span>
            <span style={{ fontWeight: 600 }}>{workers.length}</span>
          </span>
        </div>
        {workpool.status_message && (
          <div style={{ marginTop: "0.5rem", color: "#666" }}>
            {workpool.status_message}
          </div>
        )}
        {workpool.incident_count > 0 && (
          <div
            style={{ marginTop: "0.5rem", color: "#b71c1c", fontWeight: 600 }}
          >
            ⚠ {workpool.incident_count} incident
            {workpool.incident_count !== 1 ? "s" : ""}
            {workpool.last_incident_at && (
              <span style={{ fontWeight: 400, marginLeft: 8, color: "#888" }}>
                last: {new Date(workpool.last_incident_at).toLocaleString()}
              </span>
            )}
          </div>
        )}
      </div>

      {workers.length > 0 && (
        <div
          style={{
            border: "1px solid #e0e0e0",
            borderRadius: 8,
            overflow: "hidden",
            fontSize: "0.85rem",
          }}
        >
          <div
            style={{
              padding: "8px 16px",
              background: "#f8f9fa",
              borderBottom: "1px solid #e0e0e0",
              fontWeight: 600,
              color: "#555",
              fontSize: "0.75rem",
              letterSpacing: "0.1em",
            }}
          >
            ACTIVE WORKERS
          </div>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr
                style={{
                  background: "#fafafa",
                  borderBottom: "1px solid #eee",
                }}
              >
                <th
                  style={{
                    padding: "6px 16px",
                    textAlign: "left",
                    fontWeight: 500,
                    color: "#777",
                    fontSize: "0.78rem",
                  }}
                >
                  Worker ID
                </th>
                <th
                  style={{
                    padding: "6px 16px",
                    textAlign: "left",
                    fontWeight: 500,
                    color: "#777",
                    fontSize: "0.78rem",
                  }}
                >
                  Instance
                </th>
                <th
                  style={{
                    padding: "6px 16px",
                    textAlign: "left",
                    fontWeight: 500,
                    color: "#777",
                    fontSize: "0.78rem",
                  }}
                >
                  Heartbeat Expires
                </th>
              </tr>
            </thead>
            <tbody>
              {workers.map((w, i) => (
                <tr
                  key={w.worker_id}
                  style={{
                    borderBottom:
                      i < workers.length - 1 ? "1px solid #f0f0f0" : "none",
                  }}
                >
                  <td style={{ padding: "7px 16px", color: "#222" }}>
                    {w.worker_id}
                  </td>
                  <td style={{ padding: "7px 16px", color: "#555" }}>
                    {w.instance_name || "—"}
                  </td>
                  <td style={{ padding: "7px 16px", color: "#888" }}>
                    {new Date(w.heartbeat_expiry).toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
