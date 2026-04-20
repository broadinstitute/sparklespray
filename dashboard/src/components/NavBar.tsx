import { useState, useEffect, useMemo, useRef } from "react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { useEvents, mergeEvents } from "../data/EventProvider";
import { getJobs, getClusters } from "../data/events";
import type { AnyEvent } from "../types";

interface BreadcrumbSegment {
  label: string;
  href?: string;
}

function parseBreadcrumbs(pathname: string): BreadcrumbSegment[] {
  const segs = pathname.split("/").filter(Boolean);
  const items: BreadcrumbSegment[] = [{ label: "sparkles", href: "/" }];

  if (segs[0] === "jobs" && segs[1]) {
    const jobId = segs[1];
    if (segs[2] === "tasks" && segs[3]) {
      const taskId = segs[3];
      items.push({ label: jobId, href: `/jobs/${jobId}` });
      items.push({ label: "tasks", href: `/jobs/${jobId}/tasks` });
      if (segs[4] === "metrics") {
        items.push({ label: taskId, href: `/jobs/${jobId}/tasks/${taskId}` });
        items.push({ label: "metrics" });
      } else if (segs[4] === "log") {
        items.push({ label: taskId, href: `/jobs/${jobId}/tasks/${taskId}` });
        items.push({ label: "log" });
      } else {
        items.push({ label: taskId });
      }
    } else if (segs[2] === "tasks") {
      items.push({ label: jobId, href: `/jobs/${jobId}` });
      items.push({ label: "tasks" });
    } else if (segs[2] === "summary") {
      items.push({ label: jobId, href: `/jobs/${jobId}` });
      items.push({ label: "completed summary" });
    } else {
      items.push({ label: jobId });
    }
  } else if (segs[0] === "clusters" && segs[1]) {
    items.push({ label: segs[1] });
  }

  return items;
}

interface PaletteEntry {
  label: string;
  href: string;
  kind: "job" | "cluster";
}

function CommandPalette({
  onClose,
  entries,
}: {
  onClose: () => void;
  entries: PaletteEntry[];
}) {
  const [query, setQuery] = useState("");
  const navigate = useNavigate();
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [onClose]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return entries;
    return entries.filter((e) => e.label.toLowerCase().includes(q));
  }, [query, entries]);

  function selectEntry(entry: PaletteEntry) {
    navigate(entry.href);
    onClose();
  }

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(0,0,0,0.35)",
        zIndex: 1000,
        display: "flex",
        alignItems: "flex-start",
        justifyContent: "center",
        paddingTop: "15vh",
      }}
      onClick={onClose}
    >
      <div
        style={{
          background: "#fff",
          borderRadius: 10,
          boxShadow: "0 8px 32px rgba(0,0,0,0.2)",
          width: 480,
          maxWidth: "90vw",
          overflow: "hidden",
          fontFamily: "'JetBrains Mono', monospace",
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <input
          ref={inputRef}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="jump to job or cluster…"
          style={{
            display: "block",
            width: "100%",
            boxSizing: "border-box",
            border: "none",
            borderBottom: "1px solid #e0e0e0",
            padding: "0.85rem 1rem",
            fontSize: "0.9rem",
            fontFamily: "inherit",
            outline: "none",
            color: "#222",
          }}
        />
        <div style={{ maxHeight: 320, overflowY: "auto" }}>
          {filtered.length === 0 ? (
            <div
              style={{
                padding: "1rem",
                color: "#bbb",
                fontSize: "0.8rem",
                textAlign: "center",
              }}
            >
              no results
            </div>
          ) : (
            filtered.map((entry) => (
              <button
                key={entry.href}
                onClick={() => selectEntry(entry)}
                style={{
                  all: "unset",
                  display: "flex",
                  alignItems: "center",
                  gap: "0.75rem",
                  width: "100%",
                  boxSizing: "border-box",
                  padding: "0.65rem 1rem",
                  cursor: "pointer",
                  borderBottom: "1px solid #f5f5f5",
                  fontSize: "0.82rem",
                }}
                onMouseEnter={(e) =>
                  ((e.currentTarget as HTMLElement).style.background =
                    "#f5f8ff")
                }
                onMouseLeave={(e) =>
                  ((e.currentTarget as HTMLElement).style.background = "")
                }
              >
                <span
                  style={{
                    fontSize: "0.65rem",
                    color: entry.kind === "job" ? "#1565c0" : "#2e7d32",
                    background: entry.kind === "job" ? "#e3f2fd" : "#e8f5e9",
                    borderRadius: 4,
                    padding: "1px 6px",
                    fontWeight: 600,
                    flexShrink: 0,
                  }}
                >
                  {entry.kind}
                </span>
                <span
                  style={{
                    color: "#222",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                  }}
                >
                  {entry.label}
                </span>
              </button>
            ))
          )}
        </div>
      </div>
    </div>
  );
}

export default function NavBar() {
  const location = useLocation();
  const { addEventListener } = useEvents();
  const [allEvents, setAllEvents] = useState<AnyEvent[]>([]);
  const [paletteOpen, setPaletteOpen] = useState(false);

  useEffect(() => {
    return addEventListener((newEvents) =>
      setAllEvents((prev) => mergeEvents(prev, newEvents))
    );
  }, [addEventListener]);

  const entries = useMemo<PaletteEntry[]>(() => {
    const jobs = getJobs(allEvents).map((j) => ({
      label: j.jobId,
      href: `/jobs/${j.jobId}`,
      kind: "job" as const,
    }));
    const clusters = getClusters(allEvents).map((c) => ({
      label: c.clusterId,
      href: `/clusters/${c.clusterId}`,
      kind: "cluster" as const,
    }));
    return [...jobs, ...clusters];
  }, [allEvents]);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        setPaletteOpen((o) => !o);
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  const breadcrumbs = parseBreadcrumbs(location.pathname);

  return (
    <>
      <div
        style={{
          position: "fixed",
          top: 0,
          left: 0,
          right: 0,
          height: 40,
          background: "#fff",
          borderBottom: "1px solid #e8e8e8",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "0 1.25rem",
          zIndex: 100,
          fontFamily: "'JetBrains Mono', monospace",
          boxSizing: "border-box",
        }}
      >
        {/* Breadcrumb */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: "0.3rem",
            fontSize: "0.78rem",
            minWidth: 0,
          }}
        >
          {breadcrumbs.map((seg, i) => {
            const isLast = i === breadcrumbs.length - 1;
            return (
              <span
                key={i}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "0.3rem",
                  minWidth: 0,
                }}
              >
                {i > 0 && (
                  <span style={{ color: "#ccc", flexShrink: 0 }}>›</span>
                )}
                {seg.href && !isLast ? (
                  <Link
                    to={seg.href}
                    style={{
                      color: i === 0 ? "#1565c0" : "#1565c0",
                      textDecoration: "none",
                      fontWeight: i === 0 ? 700 : 400,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {seg.label}
                  </Link>
                ) : (
                  <span
                    style={{
                      color: isLast ? "#222" : "#1565c0",
                      fontWeight: i === 0 ? 700 : isLast ? 600 : 400,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {seg.label}
                  </span>
                )}
              </span>
            );
          })}
        </div>

        {/* ⌘K pill */}
        <button
          onClick={() => setPaletteOpen(true)}
          style={{
            all: "unset",
            cursor: "pointer",
            display: "flex",
            alignItems: "center",
            gap: "0.4rem",
            border: "1px solid #e0e0e0",
            borderRadius: 6,
            padding: "3px 10px",
            fontSize: "0.72rem",
            fontFamily: "inherit",
            color: "#999",
            background: "#fafafa",
            flexShrink: 0,
            whiteSpace: "nowrap",
          }}
        >
          <span style={{ fontWeight: 600, color: "#bbb" }}>⌘K</span>
          <span
            style={{ borderLeft: "1px solid #e0e0e0", paddingLeft: "0.4rem" }}
          >
            jump to job / task…
          </span>
        </button>
      </div>

      {paletteOpen && (
        <CommandPalette
          entries={entries}
          onClose={() => setPaletteOpen(false)}
        />
      )}
    </>
  );
}
