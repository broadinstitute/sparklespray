import type {
  AnyTaskEvent,
  TaskExecCompleteEvent,
  TaskFailedEvent,
} from "../types";

interface Props {
  events: AnyTaskEvent[];
}

const EVENT_COLORS: Record<string, string> = {
  task_claimed: "#2196f3",
  task_exec_started: "#9c27b0",
  task_exec_complete: "#ff9800",
  task_complete: "#00897b",
  task_orphaned: "#ff5722",
  task_failed: "#f44336",
  task_killed: "#795548",
};

function eventDetails(event: AnyTaskEvent): string | null {
  if (event.type === "task_exec_complete") {
    const e = event as TaskExecCompleteEvent;
    return `exit_code=${e.exit_code}`;
  }
  if (event.type === "task_failed") {
    const e = event as TaskFailedEvent;
    return `failure_reason: ${e.failure_reason}`;
  }
  return null;
}

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
          const color = EVENT_COLORS[event.type] ?? "#777";
          const details = eventDetails(event);
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
                {event.type}
              </span>
              {details && <span style={{ color: "#555" }}>{details}</span>}
            </div>
          );
        })}
      </div>
    </div>
  );
}
