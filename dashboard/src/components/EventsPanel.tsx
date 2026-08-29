import { useEffect, useState } from "react";
import type { RawEvent } from "../types";
import { apiFetch } from "../api/client";

const MONO = "'IBM Plex Mono', monospace";
const POLL_INTERVAL_MS = 5_000;
// Matches the server's cap in handleListEvents (v100/dev/dashboard_backend.go).
const FETCH_LIMIT = 1000;

const WINDOW_OPTIONS = [
  { value: 10, label: "10m" },
  { value: 60, label: "1h" },
  { value: 360, label: "6h" },
] as const;

const EVENT_TYPE_COLORS: Record<string, string> = {
  worker_started: "#2e7d32",
  worker_stopped: "#757575",
  job_created: "#1565c0",
  job_terminated: "#6a1b9a",
  task_state_update: "#00897b",
  workpool_state_change: "#e65100",
  batch_failed: "#b71c1c",
  batch_succeeded: "#2e7d32",
  workpool_incident: "#c62828",
};

interface Props {
  /** Which field to filter events by. */
  filterField: "workpool_id" | "job_id";
  filterValue: string;
}

function EventTypeBadge({ type }: { type: string }) {
  const color = EVENT_TYPE_COLORS[type] ?? "#555";
  return (
    <span
      style={{
        background: color + "22",
        color,
        borderRadius: 4,
        padding: "1px 8px",
        fontSize: "0.78rem",
        fontWeight: 600,
        whiteSpace: "nowrap",
      }}
    >
      {type}
    </span>
  );
}

function formatValue(key: string, value: unknown): string {
  if (value === undefined || value === null || value === "") return "—";
  if (typeof value === "boolean") return value ? "true" : "false";
  if ((key === "timestamp" || key === "expiry") && typeof value === "string") {
    const d = new Date(value);
    if (!Number.isNaN(d.getTime())) return d.toLocaleString();
  }
  return String(value);
}

function useEventsInWindow(
  filterField: "workpool_id" | "job_id",
  filterValue: string,
  windowMin: number
): { events: RawEvent[]; truncated: boolean; lastUpdatedAt: number | null } {
  const [events, setEvents] = useState<RawEvent[]>([]);
  const [truncated, setTruncated] = useState(false);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | null>(null);

  useEffect(() => {
    if (!filterValue) return;
    let cancelled = false;

    const poll = async () => {
      try {
        const since = new Date(Date.now() - windowMin * 60_000).toISOString();
        const params = new URLSearchParams({
          [filterField]: filterValue,
          after: since,
          order: "desc",
          limit: String(FETCH_LIMIT),
        });
        const res = await apiFetch(`/api/v1/events?${params}`);
        if (!res.ok || cancelled) return;
        const data: { events: RawEvent[] } = await res.json();
        if (cancelled) return;
        setEvents(data.events);
        setTruncated(data.events.length >= FETCH_LIMIT);
        setLastUpdatedAt(Date.now());
      } catch (_) {
        // ignore — keep showing the last successful result until the next poll
      }
    };

    poll();
    const id = setInterval(poll, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [filterField, filterValue, windowMin]);

  return { events, truncated, lastUpdatedAt };
}

export default function EventsPanel({ filterField, filterValue }: Props) {
  const [windowMin, setWindowMin] = useState<number>(10);
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const { events, truncated, lastUpdatedAt } = useEventsInWindow(
    filterField,
    filterValue,
    windowMin
  );

  // Keep the selection valid; default to the newest event when nothing (or a
  // now-stale event) is selected.
  useEffect(() => {
    if (events.length === 0) {
      setSelectedId(null);
      return;
    }
    if (!selectedId || !events.some((e) => e.event_id === selectedId)) {
      setSelectedId(events[0].event_id);
    }
  }, [events, selectedId]);

  const selected = events.find((e) => e.event_id === selectedId) ?? null;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          flexWrap: "wrap",
          gap: "0.75rem",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span
            style={{
              fontSize: 10,
              letterSpacing: "0.08em",
              color: "#9095a0",
              textTransform: "uppercase",
              fontWeight: 600,
              fontFamily: MONO,
            }}
          >
            Window
          </span>
          <div
            style={{
              display: "flex",
              border: "1px solid #e5e7eb",
              borderRadius: 5,
              overflow: "hidden",
            }}
          >
            {WINDOW_OPTIONS.map((opt, i) => (
              <div
                key={opt.value}
                onClick={() => setWindowMin(opt.value)}
                style={{
                  cursor: "pointer",
                  fontSize: 12,
                  fontWeight: windowMin === opt.value ? 600 : 400,
                  padding: "6px 12px",
                  background: windowMin === opt.value ? "#111827" : "#ffffff",
                  color: windowMin === opt.value ? "#ffffff" : "#4b5563",
                  borderLeft: i === 0 ? "none" : "1px solid #e5e7eb",
                  whiteSpace: "nowrap",
                  fontFamily: MONO,
                }}
              >
                {opt.label}
              </div>
            ))}
          </div>
        </div>

        <span
          style={{
            fontSize: 11,
            color: "#9095a0",
            fontFamily: MONO,
            whiteSpace: "nowrap",
          }}
        >
          {lastUpdatedAt
            ? `updated ${Math.max(
                0,
                Math.round((Date.now() - lastUpdatedAt) / 1000)
              )}s ago`
            : "updated —"}
        </span>
      </div>

      {truncated && (
        <div
          style={{
            background: "#fff8e1",
            border: "1px solid #ffe0b2",
            borderRadius: 6,
            padding: "8px 14px",
            fontSize: "0.8rem",
            fontFamily: MONO,
            color: "#8a6d00",
          }}
        >
          Showing the latest {FETCH_LIMIT} events in this window — more may
          exist. Narrow the window to see everything.
        </div>
      )}

      <div style={{ display: "flex", gap: "1.5rem", alignItems: "flex-start" }}>
        {/* Event table */}
        <div
          style={{
            flex: 1,
            minWidth: 0,
            border: "1px solid #e0e0e0",
            borderRadius: 8,
            overflow: "hidden",
          }}
        >
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr>
                <th
                  style={{
                    padding: "8px 14px",
                    textAlign: "left",
                    fontWeight: 600,
                    color: "#555",
                    fontSize: "0.78rem",
                    fontFamily: MONO,
                    borderBottom: "1px solid #e0e0e0",
                    background: "#f8f9fa",
                  }}
                >
                  Timestamp
                </th>
                <th
                  style={{
                    padding: "8px 14px",
                    textAlign: "left",
                    fontWeight: 600,
                    color: "#555",
                    fontSize: "0.78rem",
                    fontFamily: MONO,
                    borderBottom: "1px solid #e0e0e0",
                    background: "#f8f9fa",
                  }}
                >
                  Event Type
                </th>
              </tr>
            </thead>
            <tbody>
              {events.length === 0 && (
                <tr>
                  <td
                    colSpan={2}
                    style={{
                      padding: "2rem",
                      textAlign: "center",
                      color: "#aaa",
                      fontFamily: MONO,
                      fontSize: "0.85rem",
                    }}
                  >
                    No events in this window.
                  </td>
                </tr>
              )}
              {events.map((ev, i) => {
                const isSelected = ev.event_id === selectedId;
                return (
                  <tr
                    key={ev.event_id}
                    onClick={() => setSelectedId(ev.event_id)}
                    style={{
                      cursor: "pointer",
                      background: isSelected
                        ? "#e3f2fd"
                        : i % 2 === 0
                        ? "#fff"
                        : "#fafafa",
                    }}
                  >
                    <td
                      style={{
                        padding: "8px 14px",
                        fontSize: "0.8rem",
                        fontFamily: MONO,
                        borderBottom: "1px solid #f0f0f0",
                        color: "#666",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {new Date(ev.timestamp).toLocaleString()}
                    </td>
                    <td
                      style={{
                        padding: "8px 14px",
                        fontSize: "0.8rem",
                        fontFamily: MONO,
                        borderBottom: "1px solid #f0f0f0",
                      }}
                    >
                      <EventTypeBadge type={ev.type} />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Detail panel */}
        <div
          style={{
            width: 340,
            flexShrink: 0,
            background: "#f8f9fa",
            border: "1px solid #e0e0e0",
            borderRadius: 8,
            padding: "0.75rem 1rem",
          }}
        >
          <div
            style={{
              fontSize: 10,
              letterSpacing: 2,
              color: "#aaa",
              fontFamily: MONO,
              marginBottom: 10,
              textTransform: "uppercase",
            }}
          >
            Event Details
          </div>
          {!selected ? (
            <div
              style={{ color: "#ccc", fontFamily: MONO, fontSize: "0.8rem" }}
            >
              Select an event to see its details.
            </div>
          ) : (
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <tbody>
                {Object.entries(selected).map(([key, value]) => (
                  <tr key={key}>
                    <td
                      style={{
                        padding: "5px 8px 5px 0",
                        fontSize: "0.78rem",
                        fontFamily: MONO,
                        color: "#999",
                        verticalAlign: "top",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {key}
                    </td>
                    <td
                      style={{
                        padding: "5px 0",
                        fontSize: "0.8rem",
                        fontFamily: MONO,
                        color: "#222",
                        wordBreak: "break-all",
                        textAlign: "right",
                      }}
                    >
                      {formatValue(key, value)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}
