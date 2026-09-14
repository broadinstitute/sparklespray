import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { apiFetch } from "../api/client";
import { RefreshToggle } from "../components/RefreshControls";
import type { ErrorLogEntry } from "../types";

const MONO = "'IBM Plex Mono', monospace";
const POLL_INTERVAL_MS = 30_000;

export default function ErrorLog() {
  const [errors, setErrors] = useState<ErrorLogEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [paused, setPaused] = useState(false);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | null>(null);

  const bottomRef = useRef<HTMLDivElement>(null);
  const lastKeyRef = useRef<string | null>(null);
  const hasLoadedRef = useRef(false);

  // Poll GET /api/v1/errors every 30s. The endpoint returns the full current
  // list each time (no cursor), so each successful poll just replaces state.
  useEffect(() => {
    if (paused) return;
    let cancelled = false;

    async function poll() {
      while (!cancelled) {
        try {
          const res = await apiFetch("/api/v1/errors");
          if (res.ok && !cancelled) {
            const data: { errors: ErrorLogEntry[] } = await res.json();
            setErrors(data.errors ?? []);
            setLastUpdatedAt(Date.now());
            setLoading(false);
          }
        } catch {
          // network errors are transient; just retry next tick
        }
        await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
      }
    }
    poll();
    return () => {
      cancelled = true;
    };
  }, [paused]);

  // Auto-scroll to the bottom whenever a new error has arrived since the
  // last render: jump instantly into view on first load, then smooth-scroll
  // on later arrivals so a fresh error is never left off-screen.
  useEffect(() => {
    if (errors.length === 0) return;
    const last = errors[errors.length - 1];
    const key = `${last.timestamp}|${last.message}`;
    if (key === lastKeyRef.current) return;
    const isFirstLoad = !hasLoadedRef.current;
    lastKeyRef.current = key;
    hasLoadedRef.current = true;
    bottomRef.current?.scrollIntoView({
      behavior: isFirstLoad ? "auto" : "smooth",
    });
  }, [errors]);

  return (
    <div
      style={{
        padding: "2rem",
        fontFamily: MONO,
        maxWidth: 1100,
        margin: "0 auto",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "baseline",
          justifyContent: "space-between",
          marginBottom: "0.5rem",
        }}
      >
        <h1
          style={{
            fontSize: "1.6rem",
            fontWeight: 700,
            color: "#111",
            margin: 0,
          }}
        >
          Error Log
        </h1>
        <RefreshToggle
          live={!paused}
          onToggle={() => setPaused((p) => !p)}
          lastUpdatedAt={lastUpdatedAt}
        />
      </div>
      <p
        style={{
          color: "#888",
          fontSize: "0.85rem",
          marginTop: 0,
          marginBottom: "1rem",
        }}
      >
        Recent errors from the monitor's background pollers (cluster reconciler,
        provisioning, task recovery, batch startup monitor, summary polls,
        etc.), polled every 30s.
      </p>

      <div
        style={{
          border: "1px solid #e0e0e0",
          borderRadius: 8,
          background: "#282c34",
          height: "70vh",
          overflowY: "auto",
          fontSize: "0.82rem",
          lineHeight: 1.6,
          boxSizing: "border-box",
        }}
      >
        {loading ? (
          <div
            style={{ padding: "1rem", color: "#5c6370", fontStyle: "italic" }}
          >
            Loading…
          </div>
        ) : errors.length === 0 ? (
          <div
            style={{ padding: "1rem", color: "#5c6370", fontStyle: "italic" }}
          >
            No errors recorded.
          </div>
        ) : (
          errors.map((e, i) => (
            <div
              key={`${e.timestamp}-${i}`}
              style={{
                display: "flex",
                gap: "1rem",
                padding: "6px 16px",
                borderBottom:
                  i < errors.length - 1 ? "1px solid #3a3f4b" : "none",
                whiteSpace: "pre-wrap",
              }}
            >
              <span style={{ color: "#5c6370", whiteSpace: "nowrap" }}>
                {new Date(e.timestamp).toLocaleString()}
              </span>
              <span style={{ color: "#e06c75" }}>{e.message}</span>
            </div>
          ))
        )}
        <div ref={bottomRef} />
      </div>

      <div style={{ marginTop: "1.5rem" }}>
        <Link
          to="/"
          style={{
            color: "#1565c0",
            textDecoration: "none",
            fontSize: "0.85rem",
          }}
        >
          ← Back to dashboard
        </Link>
      </div>
    </div>
  );
}
