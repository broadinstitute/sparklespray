import type { AnyTaskEvent, TaskStateUpdateEvent } from "../types";

interface Props {
  events: AnyTaskEvent[];
}

const STATE_COLORS: Record<string, string> = {
  claimed: "#2196f3",
  running: "#9c27b0",
  writing: "#ff9800",
  success: "#00897b",
  error: "#ff5722",
  failed: "#f44336",
  killed: "#795548",
};

export default function EventLog({ events }: Props) {
  return (
    <div>
      <h3
        style={{
          fontFamily: "monospace",
          fontSize: "0.9rem",
          color: "#666",
          marginBottom: "0.75rem",
        }}
      >
        Event Log
      </h3>
      <div
        style={{
          border: "1px solid #e0e0e0",
          borderRadius: 8,
          overflow: "hidden",
          fontFamily: "monospace",
          fontSize: "0.82rem",
        }}
      >
        {events.map((event, i) => {
          const tsu = event as TaskStateUpdateEvent;
          const newState = tsu.new_state ?? "";
          const color = STATE_COLORS[newState] ?? "#777";
          return (
            <div
              key={i}
              style={{
                display: "flex",
                alignItems: "flex-start",
                gap: "1rem",
                padding: "8px 16px",
                borderBottom:
                  i < events.length - 1 ? "1px solid #f0f0f0" : "none",
                background: i % 2 === 0 ? "#fff" : "#fafafa",
              }}
            >
              <span
                style={{ color: "#aaa", whiteSpace: "nowrap", minWidth: 180 }}
              >
                {new Date(event.timestamp).toLocaleString()}
              </span>
              <span
                style={{
                  color: "white",
                  background: color,
                  borderRadius: 4,
                  padding: "1px 8px",
                  whiteSpace: "nowrap",
                  fontSize: "0.78rem",
                }}
              >
                {newState || event.type}
              </span>
              {tsu.old_state && (
                <span style={{ color: "#999", fontSize: "0.78rem" }}>
                  {tsu.old_state} → {tsu.new_state}
                </span>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
