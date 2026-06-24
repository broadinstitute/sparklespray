import { useState, useMemo, useRef, useCallback, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { useEvents } from "../data/EventProvider";
import type { BackendJobSummary } from "../types";

// ── Constants ────────────────────────────────────────────────────────────────

const TIME_PRESETS = [
  { label: "Last day", hours: 24 },
  { label: "Last week", hours: 24 * 7 },
  { label: "All time", hours: 24 * 365 * 10 },
];

const SYSTEM_LABEL_KEYS = new Set([
  "UUID",
  "job-env-sha256",
  "job-spec-sha256",
]);

// ── Colors ───────────────────────────────────────────────────────────────────

const LABEL_HUES = [145, 250, 60, 320, 200, 30, 280, 170];
const labelHueCache = new Map<string, number>();
let labelHueIndex = 0;
function getLabelHue(value: string): number {
  if (!labelHueCache.has(value)) {
    labelHueCache.set(value, LABEL_HUES[labelHueIndex % LABEL_HUES.length]);
    labelHueIndex++;
  }
  return labelHueCache.get(value)!;
}
function labelColors(value: string) {
  const h = getLabelHue(value);
  return {
    bg: `oklch(93% 0.06 ${h})`,
    border: `oklch(72% 0.10 ${h})`,
    text: `oklch(35% 0.10 ${h})`,
  };
}

const POOL_HUES = [30, 80, 350, 240, 170, 300, 120, 200];
const poolHueCache = new Map<string, number>();
let poolHueIndex = 0;
function workerPoolColor(id: string) {
  if (!poolHueCache.has(id)) {
    poolHueCache.set(id, POOL_HUES[poolHueIndex % POOL_HUES.length]);
    poolHueIndex++;
  }
  const h = poolHueCache.get(id)!;
  return {
    bg: `oklch(94% 0.05 ${h})`,
    border: `oklch(72% 0.10 ${h})`,
    text: `oklch(38% 0.12 ${h})`,
  };
}

const C_OK = "oklch(45% 0.14 145)";
const C_BAD = "oklch(48% 0.20 25)";
const C_BAD_SOFT = "oklch(72% 0.16 25)";
const MONO = "'JetBrains Mono', 'Courier New', monospace";

// ── Helpers ──────────────────────────────────────────────────────────────────

const LOCAL_TZ =
  new Intl.DateTimeFormat("en", { timeZoneName: "short" })
    .formatToParts(new Date())
    .find((p) => p.type === "timeZoneName")?.value ?? "";

function formatTimestamp(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}` +
    ` ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(
      d.getSeconds()
    )} ${LOCAL_TZ}`
  );
}

// ── useWorkerPools hook ───────────────────────────────────────────────────────

interface WorkpoolInfo {
  workpool_id: string;
  machine_type: string;
  region: string;
  status: string;
  status_message: string;
  last_incident_at: string | null;
  incident_count: number;
  expiry: string;
}

interface WorkerPool {
  info: WorkpoolInfo;
  activeWorkerCount: number;
  jobs: BackendJobSummary[];
}

function useWorkerPools(jobs: BackendJobSummary[]): WorkerPool[] {
  const [workpools, setWorkpools] = useState<WorkpoolInfo[]>([]);
  const [workerCounts, setWorkerCounts] = useState<Record<string, number>>({});

  useEffect(() => {
    let cancelled = false;
    async function poll() {
      while (!cancelled) {
        try {
          const res = await fetch("/api/v1/workpools");
          if (res.ok) {
            const data: WorkpoolInfo[] = await res.json();
            setWorkpools(data);
            // Fetch worker counts for each workpool in parallel.
            const counts: Record<string, number> = {};
            await Promise.all(
              data.map(async (wp) => {
                try {
                  const r = await fetch(
                    `/api/v1/workpool/${wp.workpool_id}/workers?status=started`
                  );
                  if (r.ok) {
                    const workers: unknown[] = await r.json();
                    counts[wp.workpool_id] = workers.length;
                  }
                } catch {
                  /* ignore */
                }
              })
            );
            setWorkerCounts(counts);
          }
        } catch {
          // network errors are transient; just retry
        }
        await new Promise((r) => setTimeout(r, 30_000));
      }
    }
    poll();
    return () => {
      cancelled = true;
    };
  }, []);

  return useMemo(() => {
    return workpools.map((wp) => ({
      info: wp,
      activeWorkerCount: workerCounts[wp.workpool_id] ?? 0,
      jobs: jobs.filter((j) => j.workpool_id === wp.workpool_id),
    }));
  }, [workpools, workerCounts, jobs]);
}

// ── Worker pool card ──────────────────────────────────────────────────────────

function WorkerPoolCard({ pool }: { pool: WorkerPool }) {
  const col = workerPoolColor(pool.info.workpool_id);
  const wp = pool.info;
  const hasIncident = wp.incident_count > 0;

  return (
    <div
      style={{
        background: "white",
        border: "1.5px solid #ddd",
        borderRadius: 4,
        overflow: "hidden",
      }}
    >
      {/* Header */}
      <div
        style={{
          padding: "8px 10px",
          background: col.bg,
          borderBottom: `1px solid ${col.border}`,
          display: "flex",
          alignItems: "center",
          gap: 6,
        }}
      >
        <span
          style={{
            width: 8,
            height: 8,
            borderRadius: "50%",
            background:
              wp.status === "ok" || wp.status === "active" ? C_OK : C_BAD,
            flexShrink: 0,
          }}
        />
        <span
          style={{
            fontSize: 13,
            fontWeight: 700,
            fontFamily: MONO,
            color: col.text,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            flex: 1,
          }}
        >
          {wp.workpool_id}
        </span>
        {wp.machine_type && (
          <span
            style={{
              fontSize: 11,
              fontFamily: MONO,
              color: col.text,
              opacity: 0.75,
              flexShrink: 0,
            }}
          >
            {wp.machine_type}
          </span>
        )}
      </div>

      {/* Incident alert */}
      {hasIncident && (
        <div
          style={{
            padding: "7px 10px",
            background: "oklch(97% 0.04 25)",
            borderBottom: `1px solid ${C_BAD_SOFT}`,
            fontSize: 12,
            fontFamily: MONO,
            color: C_BAD,
            lineHeight: 1.45,
          }}
        >
          ⚠ {wp.incident_count} incident{wp.incident_count !== 1 ? "s" : ""}
          {wp.status_message && (
            <span style={{ color: "#555", fontWeight: 400 }}>
              {" — "}
              {wp.status_message}
            </span>
          )}
        </div>
      )}

      <div style={{ padding: "10px 12px 12px" }}>
        {/* Stats */}
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: 8,
            marginBottom: 8,
          }}
        >
          {[
            {
              value: pool.activeWorkerCount,
              label: "ACTIVE WORKERS",
              color: pool.activeWorkerCount > 0 ? C_OK : "#bbb",
            },
            {
              value: pool.jobs.length,
              label: "JOBS",
              color: pool.jobs.length > 0 ? "#222" : "#bbb",
            },
          ].map(({ value, label, color }) => (
            <div
              key={label}
              style={{ display: "flex", flexDirection: "column" }}
            >
              <span
                style={{
                  fontSize: 18,
                  lineHeight: 1,
                  fontFamily: MONO,
                  fontWeight: 700,
                  color,
                }}
              >
                {value}
              </span>
              <span
                style={{
                  fontSize: 10,
                  letterSpacing: 1.2,
                  color: "#999",
                  fontFamily: MONO,
                  marginTop: 3,
                }}
              >
                {label}
              </span>
            </div>
          ))}
        </div>

        {/* Status */}
        {wp.status && (
          <div
            style={{
              fontSize: 11,
              fontFamily: MONO,
              color: "#888",
              marginTop: 4,
            }}
          >
            status: <span style={{ color: "#333" }}>{wp.status}</span>
            {wp.region && (
              <span style={{ marginLeft: 8, color: "#aaa" }}>{wp.region}</span>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function ColorSwatch({ color }: { color: string }) {
  return (
    <span
      style={{
        display: "inline-block",
        width: 7,
        height: 7,
        background: color,
        borderRadius: 1,
        verticalAlign: "middle",
        marginRight: 4,
      }}
    />
  );
}

// ── Worker pools sidebar ──────────────────────────────────────────────────────

function WorkerPoolsSidebar({ workerPools }: { workerPools: WorkerPool[] }) {
  return (
    <div>
      <div
        style={{
          display: "flex",
          alignItems: "baseline",
          justifyContent: "space-between",
          marginBottom: 8,
        }}
      >
        <span
          style={{
            fontSize: 11,
            letterSpacing: 2,
            color: "#888",
            fontFamily: MONO,
          }}
        >
          WORKER POOLS
        </span>
        <span style={{ fontSize: 11, color: "#bbb", fontFamily: MONO }}>
          {workerPools.length} active
        </span>
      </div>
      {workerPools.length === 0 ? (
        <div
          style={{
            padding: "16px 12px",
            background: "#fafafa",
            border: "1px dashed #ddd",
            borderRadius: 6,
            fontSize: 12,
            color: "#aaa",
            lineHeight: 1.5,
          }}
        >
          No worker pools.
          <br />
          Worker pools will be automatically created when jobs need workers to
          run tasks.
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {workerPools.map((p) => (
            <WorkerPoolCard key={p.info.workpool_id} pool={p} />
          ))}
        </div>
      )}
    </div>
  );
}

// ── Facet key button with popover ─────────────────────────────────────────────

function FacetKeyButton({
  k,
  values,
  selected,
  onToggle,
  onClear,
}: {
  k: string;
  values: { value: string; count: number }[];
  selected: Set<string>;
  onToggle: (v: string) => void;
  onClear: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const wrapRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    function onDoc(e: MouseEvent) {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node))
        setOpen(false);
    }
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [open]);

  const selectedArr = Array.from(selected);
  const hasSelection = selectedArr.length > 0;

  const q = query.trim().toLowerCase();
  const visible = values
    .filter(({ value }) => !q || value.toLowerCase().includes(q))
    .sort((a, b) => {
      const aSel = selected.has(a.value);
      const bSel = selected.has(b.value);
      if (aSel !== bSel) return aSel ? -1 : 1;
      return b.count - a.count;
    });

  return (
    <div
      ref={wrapRef}
      style={{
        position: "relative",
        display: "inline-flex",
        alignItems: "center",
      }}
    >
      <button
        onClick={() => setOpen((o) => !o)}
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 5,
          border: `1.5px solid ${hasSelection ? "#333" : "#ccc"}`,
          borderRadius: 3,
          padding: "3px 8px",
          fontSize: "0.68rem",
          fontFamily: MONO,
          background: "white",
          color: "#222",
          cursor: "pointer",
          maxWidth: 260,
        }}
      >
        <span style={{ color: "#888" }}>{k}</span>
        {hasSelection && (
          <>
            <span style={{ color: "#ccc" }}>:</span>
            <span
              style={{
                display: "inline-flex",
                gap: 3,
                alignItems: "center",
                overflow: "hidden",
              }}
            >
              {selectedArr.slice(0, 2).map((v) => {
                const c = labelColors(v);
                return (
                  <span
                    key={v}
                    style={{
                      background: c.bg,
                      color: c.text,
                      border: `1px solid ${c.border}`,
                      borderRadius: 2,
                      padding: "0 4px",
                      fontSize: "0.6rem",
                      fontWeight: 600,
                    }}
                  >
                    {v}
                  </span>
                );
              })}
              {selectedArr.length > 2 && (
                <span style={{ color: "#888", fontSize: "0.6rem" }}>
                  +{selectedArr.length - 2}
                </span>
              )}
            </span>
          </>
        )}
        <span style={{ color: "#bbb", fontSize: "0.6rem" }}>▾</span>
      </button>

      {hasSelection && (
        <button
          onClick={(e) => {
            e.stopPropagation();
            onClear();
          }}
          style={{
            marginLeft: 2,
            fontFamily: MONO,
            fontSize: "0.68rem",
            color: "#aaa",
            background: "none",
            border: "none",
            cursor: "pointer",
            padding: "0 3px",
          }}
        >
          ✕
        </button>
      )}

      {open && (
        <div
          style={{
            position: "absolute",
            top: "calc(100% + 4px)",
            left: 0,
            background: "white",
            border: "1.5px solid #333",
            borderRadius: 4,
            boxShadow: "0 4px 16px rgba(0,0,0,0.12)",
            width: 240,
            zIndex: 200,
            fontFamily: MONO,
          }}
        >
          <div
            style={{
              padding: "7px 10px",
              borderBottom: "1px solid #eee",
              display: "flex",
              alignItems: "center",
              gap: 6,
            }}
          >
            <span
              style={{ fontSize: "0.58rem", letterSpacing: 1.5, color: "#aaa" }}
            >
              FILTER
            </span>
            <span
              style={{ fontSize: "0.72rem", color: "#333", fontWeight: 600 }}
            >
              {k}
            </span>
            <span style={{ flex: 1 }} />
            <span style={{ fontSize: "0.6rem", color: "#aaa" }}>
              {values.length} value{values.length !== 1 ? "s" : ""}
            </span>
          </div>
          <div style={{ padding: 8, borderBottom: "1px solid #eee" }}>
            <input
              autoFocus
              placeholder={`search ${k} values…`}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              style={{
                fontFamily: MONO,
                fontSize: "0.68rem",
                border: "1.5px solid #333",
                borderRadius: 3,
                padding: "4px 8px",
                outline: "none",
                background: "white",
                color: "#111",
                width: "100%",
                boxSizing: "border-box",
              }}
            />
          </div>
          <div style={{ maxHeight: 200, overflowY: "auto" }}>
            {visible.length === 0 ? (
              <div
                style={{
                  padding: 12,
                  textAlign: "center",
                  color: "#bbb",
                  fontSize: "0.68rem",
                }}
              >
                no matches
              </div>
            ) : (
              visible.map(({ value, count }) => {
                const isSel = selected.has(value);
                const disabled = count === 0 && !isSel;
                const c = labelColors(value);
                return (
                  <button
                    key={value}
                    onClick={() => onToggle(value)}
                    disabled={disabled}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 8,
                      width: "100%",
                      padding: "5px 10px",
                      border: "none",
                      borderBottom: "1px solid #f5f5f5",
                      background: isSel ? "oklch(96% 0.02 90)" : "white",
                      cursor: disabled ? "not-allowed" : "pointer",
                      opacity: disabled ? 0.4 : 1,
                      textAlign: "left",
                      fontFamily: MONO,
                      fontSize: "0.68rem",
                      boxSizing: "border-box",
                    }}
                  >
                    <span
                      style={{
                        width: 12,
                        height: 12,
                        flexShrink: 0,
                        border: `1.5px solid ${isSel ? "#222" : "#ccc"}`,
                        borderRadius: 2,
                        background: isSel ? "#222" : "white",
                        color: "white",
                        fontSize: "0.55rem",
                        lineHeight: "10px",
                        textAlign: "center",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "center",
                      }}
                    >
                      {isSel ? "✓" : ""}
                    </span>
                    <span style={{ flex: 1, color: c.text, fontWeight: 600 }}>
                      {value}
                    </span>
                    <span style={{ fontSize: "0.6rem", color: "#aaa" }}>
                      {count}
                    </span>
                  </button>
                );
              })
            )}
          </div>
          {hasSelection && (
            <div
              style={{
                padding: 6,
                borderTop: "1px solid #eee",
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
              }}
            >
              <button
                onClick={onClear}
                style={{
                  fontFamily: MONO,
                  fontSize: "0.6rem",
                  color: "#888",
                  background: "none",
                  border: "none",
                  padding: "2px 6px",
                  cursor: "pointer",
                }}
              >
                clear
              </button>
              <span style={{ fontSize: "0.6rem", color: "#aaa" }}>
                {selectedArr.length} selected
              </span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── Label chips (per-row) ─────────────────────────────────────────────────────

function LabelChips({
  metadata,
  search,
  facets,
  onToggleFacet,
}: {
  metadata: Record<string, string> | undefined;
  search: string;
  facets: Record<string, Set<string>>;
  onToggleFacet: (k: string, v: string) => void;
}) {
  if (!metadata) return null;
  const entries = Object.entries(metadata).filter(
    ([k]) => !SYSTEM_LABEL_KEYS.has(k)
  );
  if (entries.length === 0) return null;
  const q = search.trim().toLowerCase();

  return (
    <div style={{ display: "flex", flexWrap: "wrap", gap: 4, marginTop: 5 }}>
      {entries.map(([k, v]) => {
        const c = labelColors(v);
        const isMatch =
          q &&
          (k.toLowerCase().includes(q) ||
            v.toLowerCase().includes(q) ||
            `${k}=${v}`.toLowerCase().includes(q));
        const active = facets[k]?.has(v);
        return (
          <button
            key={k}
            onClick={(e) => {
              e.stopPropagation();
              onToggleFacet(k, v);
            }}
            title={active ? `Remove filter ${k}=${v}` : `Filter by ${k}=${v}`}
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 1,
              border: `1.5px solid ${
                active ? c.text : isMatch ? c.text : c.border
              }`,
              borderRadius: 3,
              padding: "1px 7px",
              fontSize: "0.62rem",
              fontFamily: MONO,
              background: active ? c.text : c.bg,
              color: active ? "white" : c.text,
              lineHeight: 1.6,
              boxShadow:
                isMatch && !active ? `0 0 0 1.5px ${c.border}` : "none",
              cursor: "pointer",
              transition: "all 0.1s",
            }}
          >
            <span style={{ opacity: active ? 0.85 : 0.7 }}>{k}</span>
            <span
              style={{
                opacity: active ? 0.6 : 1,
                color: active ? "white" : c.border,
                margin: "0 2px",
              }}
            >
              =
            </span>
            <span style={{ fontWeight: 700 }}>{v}</span>
          </button>
        );
      })}
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function JobList() {
  const navigate = useNavigate();
  const { jobs, jobCache } = useEvents();
  const [search, setSearch] = useState("");
  const [timePreset, setTimePreset] = useState(0);
  const [facets, setFacets] = useState<Record<string, Set<string>>>({});

  const workerPools = useWorkerPools(jobs);

  // ── Facet helpers ──────────────────────────────────────────────────────────

  const toggleFacet = useCallback((k: string, v: string) => {
    setFacets((prev) => {
      const cur = new Set(prev[k] ?? []);
      if (cur.has(v)) cur.delete(v);
      else cur.add(v);
      const next = { ...prev };
      if (cur.size === 0) delete next[k];
      else next[k] = cur;
      return next;
    });
  }, []);

  const clearFacet = useCallback((k: string) => {
    setFacets((prev) => {
      const next = { ...prev };
      delete next[k];
      return next;
    });
  }, []);

  const clearAllFacets = useCallback(() => setFacets({}), []);

  const activeFacetCount = useMemo(
    () => Object.values(facets).reduce((n, s) => n + s.size, 0),
    [facets]
  );

  // ── Filtering ──────────────────────────────────────────────────────────────

  const jobsWithMeta = useMemo(
    () =>
      jobs.map((j) => ({
        ...j,
        submitDate: new Date(j.created_at),
        metadata: jobCache[j.job_id]?.metadata,
      })),
    [jobs, jobCache]
  );

  // Stage 1: time filter
  const timeFiltered = useMemo(() => {
    const cutoffMs = Date.now() - TIME_PRESETS[timePreset].hours * 3600 * 1000;
    return jobsWithMeta.filter((j) => j.submitDate.getTime() >= cutoffMs);
  }, [jobsWithMeta, timePreset]);

  // Stage 2: search filter
  const searchFiltered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return timeFiltered;
    return timeFiltered.filter((j) => {
      if (j.job_id.toLowerCase().includes(q)) return true;
      if (!j.metadata) return false;
      return Object.entries(j.metadata).some(
        ([k, v]) =>
          k.toLowerCase().includes(q) ||
          v.toLowerCase().includes(q) ||
          `${k}=${v}`.toLowerCase().includes(q)
      );
    });
  }, [timeFiltered, search]);

  // Helper: does a job match all facets except one key?
  function matchesFacetsExcept(
    meta: Record<string, string> | undefined,
    exceptKey: string | null
  ) {
    return Object.entries(facets).every(([k, vs]) => {
      if (k === exceptKey) return true;
      return meta ? vs.has(meta[k] ?? "") : false;
    });
  }

  // Stage 3: facet filter
  const filteredJobs = useMemo(
    () =>
      searchFiltered
        .filter((j) => matchesFacetsExcept(j.metadata, null))
        .sort((a, b) => b.submitDate.getTime() - a.submitDate.getTime()),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [searchFiltered, facets]
  );

  // Build facet index: for each (key, value) compute how many jobs would remain
  // if that value were toggled (OR within key, AND across keys).
  const facetIndex = useMemo(() => {
    const index: Record<string, Record<string, number>> = {};
    for (const j of searchFiltered) {
      if (!j.metadata) continue;
      for (const k of Object.keys(j.metadata)) {
        if (SYSTEM_LABEL_KEYS.has(k)) continue;
        if (!index[k]) index[k] = {};
        const v = j.metadata[k];
        if (!index[k][v]) index[k][v] = 0;
      }
    }
    for (const k of Object.keys(index)) {
      const peers = searchFiltered.filter((j) =>
        matchesFacetsExcept(j.metadata, k)
      );
      for (const v of Object.keys(index[k])) {
        index[k][v] = peers.filter((j) => j.metadata?.[k] === v).length;
      }
    }
    return index;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchFiltered, facets]);

  const facetKeys = useMemo(() => Object.keys(facetIndex).sort(), [facetIndex]);

  return (
    <>
      <style>{styles}</style>
      <div className="jl-root">
        <div className="jl-layout">
          {/* ── Main content ── */}
          <div className="jl-main">
            <h1 className="jl-page-title">sparkles</h1>

            {/* Filter bar */}
            <div className="jl-filter-bar">
              <div className="jl-filter-search">
                <span className="jl-filter-label">Search</span>
                <input
                  type="text"
                  className="jl-search-input"
                  placeholder="id, label key, value, or key=value…"
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                />
                {search && (
                  <button
                    className="jl-search-clear"
                    onClick={() => setSearch("")}
                  >
                    ✕
                  </button>
                )}
              </div>

              <div className="jl-filter-divider" />

              <div className="jl-filter-time">
                <span className="jl-filter-label">Time</span>
                <div className="jl-time-presets">
                  {TIME_PRESETS.map((p, i) => (
                    <button
                      key={i}
                      className={`jl-time-preset${
                        timePreset === i ? " active" : ""
                      }`}
                      onClick={() => setTimePreset(i)}
                    >
                      {p.label}
                    </button>
                  ))}
                </div>
              </div>
            </div>

            {/* Facet bar */}
            {facetKeys.length > 0 && (
              <div className="jl-facet-bar">
                <span className="jl-filter-label" style={{ flexShrink: 0 }}>
                  Filter by
                </span>
                {facetKeys.map((k) => (
                  <FacetKeyButton
                    key={k}
                    k={k}
                    values={Object.entries(
                      facetIndex[k]
                    ).map(([value, count]) => ({ value, count }))}
                    selected={facets[k] ?? new Set()}
                    onToggle={(v) => toggleFacet(k, v)}
                    onClear={() => clearFacet(k)}
                  />
                ))}
                {activeFacetCount > 0 && (
                  <button onClick={clearAllFacets} className="jl-facet-clear">
                    clear {activeFacetCount} filter
                    {activeFacetCount !== 1 ? "s" : ""}
                  </button>
                )}
              </div>
            )}

            {/* Jobs section */}
            <section className="jl-section">
              <h2 className="jl-section-title">Jobs</h2>
              <p className="jl-subtitle">
                {filteredJobs.length} job{filteredJobs.length !== 1 ? "s" : ""}{" "}
                found
              </p>
              <div className="jl-divider" />
              {filteredJobs.length === 0 ? (
                <div className="jl-empty">no jobs found</div>
              ) : (
                <table className="jl-table">
                  <thead>
                    <tr>
                      <th className="jl-th jl-th-index" />
                      <th className="jl-th">Identifier</th>
                      <th className="jl-th jl-th-pool">Worker Pool</th>
                      <th className="jl-th jl-th-stats">tasks / ok / fail</th>
                      <th className="jl-th jl-th-time">Start Time (local)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredJobs.map(
                      ({ job_id, workpool_id, metadata, created_at }, i) => {
                        const submitDate = new Date(created_at);
                        const cc = workpool_id
                          ? workerPoolColor(workpool_id)
                          : null;
                        return (
                          <tr
                            key={job_id}
                            className="jl-tr"
                            onClick={() => navigate(`/jobs/${job_id}`)}
                          >
                            <td className="jl-td jl-td-index">
                              {String(i + 1).padStart(2, "0")}
                            </td>
                            <td className="jl-td">
                              <div className="jl-id">{job_id}</div>
                              <LabelChips
                                metadata={metadata}
                                search={search}
                                facets={facets}
                                onToggleFacet={toggleFacet}
                              />
                            </td>
                            <td className="jl-td jl-td-pool">
                              {workpool_id && cc ? (
                                <span className="jl-pool-cell">
                                  <span
                                    className="jl-pool-swatch"
                                    style={{
                                      background: cc.bg,
                                      border: `1.5px solid ${cc.border}`,
                                    }}
                                  />
                                  <span
                                    style={{
                                      color: cc.text,
                                      overflow: "hidden",
                                      textOverflow: "ellipsis",
                                      whiteSpace: "nowrap",
                                    }}
                                  >
                                    {workpool_id}
                                  </span>
                                </span>
                              ) : (
                                <span className="jl-pool-empty">—</span>
                              )}
                            </td>
                            <td className="jl-td jl-td-stats">
                              <JobStatsChip
                                job={jobs.find((j) => j.job_id === job_id)!}
                              />
                            </td>
                            <td className="jl-td jl-td-time">
                              {formatTimestamp(submitDate)}
                            </td>
                          </tr>
                        );
                      }
                    )}
                  </tbody>
                </table>
              )}
            </section>

            <div className="jl-footer">◆ sparkles dashboard</div>
          </div>

          {/* ── Sidebar ── */}
          <div className="jl-sidebar">
            <WorkerPoolsSidebar workerPools={workerPools} />
          </div>
        </div>
      </div>
    </>
  );
}

function JobStatsChip({ job }: { job: BackendJobSummary }) {
  const { taskCount: total, successCount: ok, failureCount: fail } = job;
  let cls = "jl-chip";
  if (fail > 0) cls += " jl-chip-red";
  else if (total > 0 && total === ok) cls += " jl-chip-green";
  return <span className={cls}>{`${total} / ${ok} / ${fail}`}</span>;
}

// ColorSwatch used in sidebar (kept for completeness)
void ColorSwatch;

// ── Styles ────────────────────────────────────────────────────────────────────

const styles = `
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&display=swap');

  .jl-root {
    min-height: 100vh;
    background: #fff;
    color: #333;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    padding: 2rem;
    box-sizing: border-box;
  }

  .jl-layout {
    display: flex;
    gap: 20px;
    align-items: flex-start;
  }

  .jl-main {
    flex: 1;
    min-width: 0;
  }

  .jl-sidebar {
    width: 300px;
    flex-shrink: 0;
    padding-top: 4.5rem; /* align below page title */
  }

  .jl-page-title {
    font-size: 2rem;
    font-weight: 700;
    color: #111;
    margin: 0 0 1.5rem 0;
    letter-spacing: -0.03em;
  }

  /* ── Filter bar ─────────────────────────────────── */

  .jl-filter-bar {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 0;
    padding: 8px 12px;
    border: 1px solid #e8e8e8;
    border-bottom: none;
    border-radius: 4px 4px 0 0;
    background: #fafafa;
  }

  .jl-filter-search {
    display: flex;
    align-items: center;
    gap: 6px;
    flex: 1;
    min-width: 0;
  }

  .jl-filter-label {
    font-size: 0.6rem;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: #aaa;
    flex-shrink: 0;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
  }

  .jl-search-input {
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-size: 0.75rem;
    border: 1.5px solid #ccc;
    border-radius: 3px;
    padding: 4px 8px;
    outline: none;
    background: white;
    color: #111;
    flex: 1;
    min-width: 0;
    transition: border-color 0.12s;
  }

  .jl-search-input:focus { border-color: #333; }

  .jl-search-clear {
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-size: 0.7rem;
    color: #aaa;
    background: none;
    border: none;
    cursor: pointer;
    padding: 0 2px;
    line-height: 1;
    flex-shrink: 0;
  }

  .jl-filter-divider {
    width: 1px;
    height: 20px;
    background: #e0e0e0;
    flex-shrink: 0;
  }

  .jl-filter-time {
    display: flex;
    align-items: center;
    gap: 6px;
    flex-shrink: 0;
  }

  .jl-time-presets {
    display: flex;
    border: 1.5px solid #ccc;
    border-radius: 3px;
    overflow: hidden;
  }

  .jl-time-preset {
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-size: 0.68rem;
    padding: 3px 9px;
    border: none;
    border-right: 1px solid #ccc;
    background: white;
    color: #555;
    cursor: pointer;
    transition: background 0.1s, color 0.1s;
  }

  .jl-time-preset:last-child { border-right: none; }
  .jl-time-preset.active { background: #111; color: white; }

  /* ── Facet bar ───────────────────────────────────── */

  .jl-facet-bar {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
    padding: 8px 12px;
    border: 1px solid #e8e8e8;
    border-bottom: none;
    background: #f5f5f2;
    margin-bottom: 1.5rem;
    border-radius: 0 0 4px 4px;
    border-top: 1px dashed #e0e0e0;
  }

  .jl-facet-clear {
    margin-left: auto;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-size: 0.68rem;
    color: #888;
    background: none;
    border: 1px dashed #bbb;
    border-radius: 3px;
    padding: 2px 8px;
    cursor: pointer;
  }

  /* ── Section chrome ──────────────────────────────── */

  .jl-section { margin-bottom: 3.5rem; }

  .jl-section-title {
    font-size: 0.65rem;
    font-weight: 700;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: #888;
    margin: 0 0 0.25rem 0;
  }

  .jl-subtitle {
    font-size: 0.72rem;
    color: #bbb;
    margin: 0 0 0.75rem 0;
  }

  .jl-divider {
    height: 1px;
    background: linear-gradient(90deg, #1565c044, #1565c011 60%, transparent);
    margin-bottom: 0;
  }

  /* ── Table ───────────────────────────────────────── */

  .jl-table {
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
  }

  .jl-th {
    font-size: 0.6rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: #aaa;
    font-weight: 400;
    padding: 0.45rem 0.5rem;
    text-align: left;
    border-bottom: 1px solid #f0f0f0;
  }

  .jl-th-stats { text-align: right; width: 8rem; }
  .jl-th-time  { text-align: right; width: 14rem; }
  .jl-th-index { width: 2.2rem; }
  .jl-th-pool  { width: 9rem; }

  .jl-tr { cursor: pointer; }

  .jl-td {
    padding: 0.65rem 0.5rem;
    vertical-align: top;
    border-bottom: 1px solid #f0f0f0;
    transition: background 0.12s;
  }

  .jl-tr:hover .jl-td { background: #f0f5ff; }
  .jl-tr:hover .jl-id  { color: #1565c0; }

  .jl-td-index {
    font-size: 0.65rem;
    color: #ccc;
    font-weight: 400;
    padding-top: 0.68rem;
    border-left: 2px solid transparent;
    transition: background 0.12s, border-color 0.15s;
  }

  .jl-tr:hover .jl-td-index { border-left-color: #1565c0; }

  .jl-id {
    font-size: 0.82rem;
    font-weight: 500;
    color: #222;
    transition: color 0.12s;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .jl-td-stats {
    text-align: right;
    white-space: nowrap;
    padding-top: 0.68rem;
  }

  .jl-td-time {
    text-align: right;
    font-size: 0.72rem;
    color: #999;
    white-space: nowrap;
    padding-top: 0.68rem;
  }

  .jl-td-pool {
    padding-top: 0.68rem;
    overflow: hidden;
  }

  .jl-pool-cell {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-size: 0.68rem;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-weight: 600;
    max-width: 100%;
    overflow: hidden;
  }

  .jl-pool-swatch {
    width: 9px;
    height: 9px;
    border-radius: 2px;
    flex-shrink: 0;
  }

  .jl-pool-empty {
    font-size: 0.72rem;
    color: #ccc;
  }

  /* ── Label chips ─────────────────────────────────── */

  .jl-chip {
    font-size: 0.68rem;
    font-weight: 600;
    border-radius: 4px;
    padding: 1px 8px;
    white-space: nowrap;
    flex-shrink: 0;
    background: #f0f0f0;
    color: #777;
    margin: 0 0.5rem;
  }

  .jl-chip-green { background: #e8f5e9; color: #2e7d32; }
  .jl-chip-red   { background: #ffebee; color: #b71c1c; }

  .jl-empty {
    font-size: 0.75rem;
    color: #ccc;
    padding: 1rem 0.5rem;
    border-top: 1px solid #f0f0f0;
    border-bottom: 1px solid #f0f0f0;
  }

  .jl-footer {
    margin-top: 2rem;
    font-size: 0.62rem;
    letter-spacing: 0.1em;
    color: #ddd;
    text-align: right;
  }
`;
