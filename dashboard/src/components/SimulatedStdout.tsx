import { useEffect, useRef } from "react";
import type { StdoutLine } from "../data/simulate";

interface Props {
  lines: StdoutLine[];
}

const LINE_COLORS: Record<string, string> = {
  "[INFO]": "#98c379",
  "[WARN]": "#e5c07b",
  "[ERROR]": "#e06c75",
  "[PROG]": "#61afef",
};

function lineColor(text: string): string {
  for (const [prefix, color] of Object.entries(LINE_COLORS)) {
    if (text.includes(prefix)) return color;
  }
  return "#abb2bf";
}

function formatTime(ms: number): string {
  return new Date(ms).toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

export default function SimulatedStdout({ lines }: Props) {
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [lines.length]);

  return (
    <div
      style={{
        background: "#282c34",
        borderRadius: 8,
        padding: "1rem",
        fontFamily: '"JetBrains Mono", "Fira Mono", "Cascadia Code", monospace',
        fontSize: "0.8rem",
        lineHeight: 1.6,
        height: 420,
        overflowY: "auto",
        boxSizing: "border-box",
      }}
    >
      {lines.length === 0 ? (
        <span style={{ color: "#5c6370", fontStyle: "italic" }}>
          No output yet…
        </span>
      ) : (
        lines.map((line, i) => (
          <div
            key={i}
            style={{ display: "flex", gap: "1rem", alignItems: "baseline" }}
          >
            <span
              style={{
                color: "#5c6370",
                whiteSpace: "nowrap",
                userSelect: "none",
                flexShrink: 0,
              }}
            >
              {formatTime(line.time)}
            </span>
            <span
              style={{ color: lineColor(line.text), wordBreak: "break-all" }}
            >
              {line.text}
            </span>
          </div>
        ))
      )}
      <div ref={bottomRef} />
    </div>
  );
}
