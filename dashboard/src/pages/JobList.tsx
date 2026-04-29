import { useState, useEffect, useMemo, useRef, useCallback } from "react";
import { Link } from "react-router-dom";
import { getJobs, getClusters, getJobTaskStats } from "../data/events";
import type { JobTaskStats } from "../data/events";
import { useEvents, mergeEvents } from "../data/EventProvider";
import type { AnyEvent } from "../types";

const TIME_PRESETS = [
  { label: "Last day", hours: 24 },
  { label: "Last week", hours: 24 * 7 },
  { label: "All time", hours: 24 * 365 * 10 },
];

const DEFAULT_HIDDEN_LABELS = new Set([
  "UUID",
  "job-env-sha256",
  "job-spec-sha256",
]);

const LABEL_HUES = [145, 250, 60, 320, 200, 30, 280, 170];
const hueCache = new Map<string, number>();
let hueIndex = 0;

function getLabelHue(value: string): number {
  if (!hueCache.has(value)) {
    hueCache.set(value, LABEL_HUES[hueIndex % LABEL_HUES.length]);
    hueIndex++;
  }
  return hueCache.get(value)!;
}

function labelColors(value: string) {
  const h = getLabelHue(value);
  return {
    bg: `oklch(93% 0.06 ${h})`,
    border: `oklch(72% 0.10 ${h})`,
    text: `oklch(35% 0.10 ${h})`,
  };
}

const styles = `
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&display=swap');

  .jl-root {
    min-height: 100vh;
    background: #fff;
    color: #333;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    padding: 3rem 2rem;
    box-sizing: border-box;
  }

  .jl-inner {
    max-width: 860px;
    margin: 0 auto;
  }

  .jl-page-title {
    font-size: 2rem;
    font-weight: 700;
    color: #111;
    margin: 0 0 2.5rem 0;
    letter-spacing: -0.03em;
  }

  .jl-filter-bar {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 2rem;
    padding: 10px 12px;
    border: 1px solid #e8e8e8;
    border-radius: 4px;
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

  .jl-search-input:focus {
    border-color: #333;
  }

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

  .jl-time-preset:last-child {
    border-right: none;
  }

  .jl-time-preset.active {
    background: #111;
    color: white;
  }

  /* Label visibility dropdown */
  .jl-labels-wrap {
    position: relative;
    flex-shrink: 0;
  }

  .jl-labels-btn {
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-size: 0.68rem;
    padding: 3px 9px;
    border: 1.5px solid #ccc;
    border-radius: 3px;
    background: white;
    color: #555;
    cursor: pointer;
    white-space: nowrap;
    transition: border-color 0.1s, color 0.1s;
  }

  .jl-labels-btn:hover {
    border-color: #999;
    color: #111;
  }

  .jl-labels-btn.has-hidden {
    border-color: #bbb;
    background: #f5f5f5;
  }

  .jl-labels-badge {
    display: inline-block;
    margin-left: 4px;
    background: #888;
    color: white;
    font-size: 0.55rem;
    border-radius: 8px;
    padding: 0 5px;
    vertical-align: middle;
    line-height: 1.6;
  }

  .jl-labels-panel {
    position: absolute;
    top: calc(100% + 6px);
    right: 0;
    background: white;
    border: 1.5px solid #ccc;
    border-radius: 4px;
    min-width: 220px;
    z-index: 200;
    box-shadow: 0 4px 16px rgba(0,0,0,0.10);
    overflow: hidden;
  }

  .jl-labels-panel-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 8px 12px 6px;
    border-bottom: 1px solid #f0f0f0;
  }

  .jl-labels-panel-title {
    font-size: 0.58rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: #aaa;
  }

  .jl-labels-reset {
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-size: 0.6rem;
    color: #aaa;
    background: none;
    border: none;
    cursor: pointer;
    padding: 0;
    text-decoration: underline;
    text-underline-offset: 2px;
  }

  .jl-labels-reset:hover {
    color: #333;
  }

  .jl-labels-empty {
    padding: 12px;
    font-size: 0.68rem;
    color: #bbb;
    text-align: center;
  }

  .jl-labels-list {
    padding: 4px 0;
    max-height: 260px;
    overflow-y: auto;
  }

  .jl-labels-row {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 5px 12px;
    cursor: pointer;
    user-select: none;
    transition: background 0.08s;
  }

  .jl-labels-row:hover {
    background: #f7f7f7;
  }

  .jl-labels-check {
    width: 13px;
    height: 13px;
    border: 1.5px solid #ccc;
    border-radius: 2px;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    font-size: 0.6rem;
    color: white;
    transition: background 0.1s, border-color 0.1s;
    background: white;
  }

  .jl-labels-check.checked {
    background: #222;
    border-color: #222;
  }

  .jl-labels-key {
    font-size: 0.72rem;
    color: #333;
    flex: 1;
  }

  .jl-labels-key.hidden-label {
    color: #bbb;
    text-decoration: line-through;
    text-decoration-color: #ccc;
  }

  /* Job list */
  .jl-section {
    margin-bottom: 3.5rem;
  }

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
    margin-bottom: 0.25rem;
  }

  .jl-divider-green {
    height: 1px;
    background: linear-gradient(90deg, #1b5e2044, #1b5e2011 60%, transparent);
    margin-bottom: 0.25rem;
  }

  .jl-col-headers {
    display: flex;
    justify-content: space-between;
    font-size: 0.6rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: #aaa;
    padding: 0.4rem 0.5rem 0.6rem 0.5rem;
  }

  .jl-list {
    list-style: none;
    margin: 0;
    padding: 0;
  }

  .jl-item {
    border-top: 1px solid #f0f0f0;
  }

  .jl-item:last-child {
    border-bottom: 1px solid #f0f0f0;
  }

  .jl-link {
    display: flex;
    align-items: flex-start;
    gap: 0;
    padding: 0.7rem 0.5rem;
    text-decoration: none;
    color: inherit;
    transition: background 0.12s ease;
    position: relative;
  }

  .jl-link::before {
    content: '';
    position: absolute;
    left: 0;
    top: 0;
    bottom: 0;
    width: 2px;
    background: #1565c0;
    transform: scaleY(0);
    transform-origin: center;
    transition: transform 0.15s ease;
  }

  .jl-link:hover {
    background: #f0f5ff;
  }

  .jl-link:hover::before {
    transform: scaleY(1);
  }

  .jl-link:hover .jl-id {
    color: #1565c0;
  }

  .jl-cluster-link::before {
    background: #2e7d32;
  }

  .jl-cluster-link:hover {
    background: #f1f8f1;
  }

  .jl-cluster-link:hover .jl-id {
    color: #2e7d32;
  }

  .jl-index {
    font-size: 0.65rem;
    color: #ccc;
    min-width: 2.5rem;
    font-weight: 400;
    flex-shrink: 0;
    padding-top: 1px;
  }

  .jl-id-col {
    flex: 1;
    min-width: 0;
  }

  .jl-id {
    font-size: 0.82rem;
    font-weight: 500;
    color: #222;
    transition: color 0.12s ease;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .jl-label-chips {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
    margin-top: 5px;
  }

  .jl-label-chip {
    display: inline-flex;
    align-items: center;
    gap: 1px;
    border-radius: 3px;
    padding: 1px 7px;
    font-size: 0.65rem;
    line-height: 1.6;
    white-space: nowrap;
    border-width: 1.5px;
    border-style: solid;
    transition: box-shadow 0.1s;
  }

  .jl-label-chip-key {
    opacity: 0.7;
  }

  .jl-label-chip-eq {
    margin: 0 2px;
  }

  .jl-label-chip-val {
    font-weight: 700;
  }

  .jl-leader {
    flex: 1;
    min-width: 1rem;
    overflow: hidden;
    margin: 0 0.75rem;
    padding-bottom: 2px;
    background-image: radial-gradient(circle, #ccc 1px, transparent 1px);
    background-size: 6px 4px;
    background-repeat: repeat-x;
    background-position: left center;
    opacity: 0.6;
    flex-shrink: 0;
    align-self: flex-start;
    margin-top: 3px;
  }

  .jl-timestamp {
    font-size: 0.72rem;
    color: #999;
    white-space: nowrap;
    flex-shrink: 0;
  }

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

  .jl-chip-green {
    background: #e8f5e9;
    color: #2e7d32;
  }

  .jl-chip-red {
    background: #ffebee;
    color: #b71c1c;
  }

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

function JobChip({ stats }: { stats: JobTaskStats }) {
  const label = `${stats.total} / ${stats.success} / ${stats.failure}`;
  let cls = "jl-chip";
  if (stats.failure > 0) cls += " jl-chip-red";
  else if (stats.total > 0 && stats.total === stats.success)
    cls += " jl-chip-green";
  return <span className={cls}>{label}</span>;
}

function LabelChips({
  metadata,
  search,
  hiddenLabels,
}: {
  metadata: Record<string, string> | undefined;
  search: string;
  hiddenLabels: Set<string>;
}) {
  if (!metadata) return null;
  const entries = Object.entries(metadata).filter(
    ([k]) => !hiddenLabels.has(k)
  );
  if (entries.length === 0) return null;

  const q = search.trim().toLowerCase();

  return (
    <div className="jl-label-chips">
      {entries.map(([k, v]) => {
        const c = labelColors(v);
        const isMatch =
          q &&
          (k.toLowerCase().includes(q) ||
            v.toLowerCase().includes(q) ||
            `${k}=${v}`.toLowerCase().includes(q));
        return (
          <span
            key={k}
            className="jl-label-chip"
            style={{
              background: c.bg,
              borderColor: isMatch ? c.text : c.border,
              boxShadow: isMatch ? `0 0 0 1.5px ${c.border}` : "none",
            }}
          >
            <span className="jl-label-chip-key" style={{ color: c.text }}>
              {k}
            </span>
            <span className="jl-label-chip-eq" style={{ color: c.border }}>
              =
            </span>
            <span className="jl-label-chip-val" style={{ color: c.text }}>
              {v}
            </span>
          </span>
        );
      })}
    </div>
  );
}

function LabelVisibilityPanel({
  knownKeys,
  hiddenLabels,
  onToggle,
  onReset,
}: {
  knownKeys: string[];
  hiddenLabels: Set<string>;
  onToggle: (key: string) => void;
  onReset: () => void;
}) {
  const isDefault =
    hiddenLabels.size === DEFAULT_HIDDEN_LABELS.size &&
    [...DEFAULT_HIDDEN_LABELS].every((k) => hiddenLabels.has(k));

  return (
    <div className="jl-labels-panel">
      <div className="jl-labels-panel-header">
        <span className="jl-labels-panel-title">Label visibility</span>
        {!isDefault && (
          <button className="jl-labels-reset" onClick={onReset}>
            reset
          </button>
        )}
      </div>
      {knownKeys.length === 0 ? (
        <div className="jl-labels-empty">no labels seen yet</div>
      ) : (
        <div className="jl-labels-list">
          {knownKeys.map((k) => {
            const visible = !hiddenLabels.has(k);
            return (
              <div
                key={k}
                className="jl-labels-row"
                onClick={() => onToggle(k)}
              >
                <div className={`jl-labels-check${visible ? " checked" : ""}`}>
                  {visible && "✓"}
                </div>
                <span
                  className={`jl-labels-key${visible ? "" : " hidden-label"}`}
                >
                  {k}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function formatTimestamp(d: Date): string {
  const pad = (n: number, w = 2) => String(n).padStart(w, "0");
  return (
    `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}` +
    ` ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(
      d.getUTCSeconds()
    )} UTC`
  );
}

export default function JobList() {
  const { addEventListener, jobCache } = useEvents();
  const [allEvents, setAllEvents] = useState<AnyEvent[]>([]);
  const [search, setSearch] = useState("");
  const [timePreset, setTimePreset] = useState(0);
  const [hiddenLabels, setHiddenLabels] = useState<Set<string>>(
    new Set(DEFAULT_HIDDEN_LABELS)
  );
  const [showLabelPanel, setShowLabelPanel] = useState(false);
  const labelWrapRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    return addEventListener((newEvents) =>
      setAllEvents((prev) => mergeEvents(prev, newEvents))
    );
  }, [addEventListener]);

  // Close panel on outside click
  useEffect(() => {
    if (!showLabelPanel) return;
    function handleClick(e: MouseEvent) {
      if (
        labelWrapRef.current &&
        !labelWrapRef.current.contains(e.target as Node)
      ) {
        setShowLabelPanel(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [showLabelPanel]);

  const toggleLabel = useCallback((key: string) => {
    setHiddenLabels((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);

  const resetLabels = useCallback(() => {
    setHiddenLabels(new Set(DEFAULT_HIDDEN_LABELS));
  }, []);

  const knownLabelKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const job of Object.values(jobCache)) {
      if (job.metadata) {
        for (const k of Object.keys(job.metadata)) keys.add(k);
      }
    }
    return Array.from(keys).sort();
  }, [jobCache]);

  const hiddenCount = useMemo(
    () => knownLabelKeys.filter((k) => hiddenLabels.has(k)).length,
    [knownLabelKeys, hiddenLabels]
  );

  const jobs = useMemo(() => getJobs(allEvents), [allEvents]);
  const clusters = useMemo(() => getClusters(allEvents), [allEvents]);
  const jobsWithStats = useMemo(
    () =>
      jobs.map((j) => ({ ...j, stats: getJobTaskStats(allEvents, j.jobId) })),
    [jobs, allEvents]
  );

  const filteredJobs = useMemo(() => {
    const now = Date.now();
    const cutoffMs = now - TIME_PRESETS[timePreset].hours * 3600 * 1000;
    const q = search.trim().toLowerCase();

    return jobsWithStats.filter(({ jobId, startTime }) => {
      if (startTime.getTime() < cutoffMs) return false;
      if (!q) return true;
      if (jobId.toLowerCase().includes(q)) return true;
      const meta = jobCache[jobId]?.metadata;
      if (!meta) return false;
      return Object.entries(meta).some(
        ([k, v]) =>
          k.toLowerCase().includes(q) ||
          v.toLowerCase().includes(q) ||
          `${k}=${v}`.toLowerCase().includes(q)
      );
    });
  }, [jobsWithStats, jobCache, timePreset, search]);

  return (
    <>
      <style>{styles}</style>
      <div className="jl-root">
        <div className="jl-inner">
          <h1 className="jl-page-title">sparkles</h1>

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

            <div className="jl-filter-divider" />

            <div className="jl-labels-wrap" ref={labelWrapRef}>
              <button
                className={`jl-labels-btn${
                  hiddenCount > 0 ? " has-hidden" : ""
                }`}
                onClick={() => setShowLabelPanel((v) => !v)}
              >
                Labels
                {hiddenCount > 0 && (
                  <span className="jl-labels-badge">{hiddenCount} hidden</span>
                )}{" "}
                ▾
              </button>
              {showLabelPanel && (
                <LabelVisibilityPanel
                  knownKeys={knownLabelKeys}
                  hiddenLabels={hiddenLabels}
                  onToggle={toggleLabel}
                  onReset={resetLabels}
                />
              )}
            </div>
          </div>

          <section className="jl-section">
            <h2 className="jl-section-title">Jobs</h2>
            <p className="jl-subtitle">
              {filteredJobs.length} job{filteredJobs.length !== 1 ? "s" : ""}{" "}
              found
            </p>
            <div className="jl-divider" />
            <div className="jl-col-headers">
              <span>Identifier</span>
              <span>tasks / ok / fail</span>
              <span>Start Time (UTC)</span>
            </div>
            {filteredJobs.length === 0 ? (
              <div className="jl-empty">no jobs found</div>
            ) : (
              <ul className="jl-list">
                {filteredJobs.map(({ jobId, startTime, stats }, i) => (
                  <li key={jobId} className="jl-item">
                    <Link to={`/jobs/${jobId}`} className="jl-link">
                      <span className="jl-index">
                        {String(i + 1).padStart(2, "0")}
                      </span>
                      <div className="jl-id-col">
                        <div className="jl-id">{jobId}</div>
                        <LabelChips
                          metadata={jobCache[jobId]?.metadata}
                          search={search}
                          hiddenLabels={hiddenLabels}
                        />
                      </div>
                      <JobChip stats={stats} />
                      <span className="jl-leader" aria-hidden="true" />
                      <span className="jl-timestamp">
                        {formatTimestamp(startTime)}
                      </span>
                    </Link>
                  </li>
                ))}
              </ul>
            )}
          </section>

          <section className="jl-section">
            <h2 className="jl-section-title">Clusters</h2>
            <p className="jl-subtitle">
              {clusters.length} cluster{clusters.length !== 1 ? "s" : ""} found
            </p>
            <div className="jl-divider-green" />
            <div className="jl-col-headers">
              <span>Cluster ID</span>
              <span>Start Time (UTC)</span>
            </div>
            {clusters.length === 0 ? (
              <div className="jl-empty">no clusters found</div>
            ) : (
              <ul className="jl-list">
                {clusters.map(({ clusterId, startTime }, i) => (
                  <li key={clusterId} className="jl-item">
                    <Link
                      to={`/clusters/${clusterId}`}
                      className="jl-link jl-cluster-link"
                    >
                      <span className="jl-index">
                        {String(i + 1).padStart(2, "0")}
                      </span>
                      <span className="jl-id">{clusterId}</span>
                      <span className="jl-leader" aria-hidden="true" />
                      <span className="jl-timestamp">
                        {formatTimestamp(startTime)}
                      </span>
                    </Link>
                  </li>
                ))}
              </ul>
            )}
          </section>

          <div className="jl-footer">◆ sparkles dashboard</div>
        </div>
      </div>
    </>
  );
}
