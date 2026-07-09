import { useNavigate } from "react-router-dom";

const MONO = "'IBM Plex Mono', monospace";

const SYSTEM_LABEL_KEYS = new Set([
  "UUID",
  "job-env-sha256",
  "job-spec-sha256",
]);

const ACTIVE_TASK_STATES = new Set([
  "pending",
  "claimed",
  "running",
  "writing",
]);
const FAILURE_TASK_STATES = new Set(["error", "failed", "killed"]);

export interface JobsTableTask {
  state: string;
  count: number;
}

export interface JobsTableRow {
  job_id: string;
  name?: string;
  workpool_id?: string;
  created_at: string;
  tasks: JobsTableTask[];
  metadata?: Record<string, string>;
}

export interface JobsTableProps {
  jobs: JobsTableRow[];
  search?: string;
  facets?: Record<string, Set<string>>;
  onToggleFacet?: (k: string, v: string) => void;
  emptyMessage?: string;
}

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
export function labelColors(value: string) {
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
export function workerPoolColor(id: string) {
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

// ── Helpers ──────────────────────────────────────────────────────────────────

const LOCAL_TZ =
  new Intl.DateTimeFormat("en", { timeZoneName: "short" })
    .formatToParts(new Date())
    .find((p) => p.type === "timeZoneName")?.value ?? "";

export function formatTimestamp(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}` +
    ` ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(
      d.getSeconds()
    )} ${LOCAL_TZ}`
  );
}

function taskSummary(tasks: JobsTableTask[]) {
  let active = 0,
    ok = 0,
    fail = 0;
  for (const t of tasks) {
    if (ACTIVE_TASK_STATES.has(t.state)) active += t.count;
    else if (t.state === "success") ok += t.count;
    else if (FAILURE_TASK_STATES.has(t.state)) fail += t.count;
  }
  return { active, ok, fail };
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
  onToggleFacet?: (k: string, v: string) => void;
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
          !!q &&
          (k.toLowerCase().includes(q) ||
            v.toLowerCase().includes(q) ||
            `${k}=${v}`.toLowerCase().includes(q));
        const active = facets[k]?.has(v);
        const chipStyle = {
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
          boxShadow: isMatch && !active ? `0 0 0 1.5px ${c.border}` : "none",
          transition: "all 0.1s",
        } as const;
        const content = (
          <>
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
          </>
        );
        if (!onToggleFacet) {
          return (
            <span key={k} style={chipStyle}>
              {content}
            </span>
          );
        }
        return (
          <button
            key={k}
            onClick={(e) => {
              e.stopPropagation();
              onToggleFacet(k, v);
            }}
            title={active ? `Remove filter ${k}=${v}` : `Filter by ${k}=${v}`}
            style={{ ...chipStyle, cursor: "pointer" }}
          >
            {content}
          </button>
        );
      })}
    </div>
  );
}

// ── Stats chip ─────────────────────────────────────────────────────────────

function JobStatsChip({ tasks }: { tasks: JobsTableTask[] }) {
  const { active, ok, fail } = taskSummary(tasks);
  let cls = "jt-chip";
  const total = tasks.reduce((sum, t) => sum + t.count, 0);
  if (fail > 0) cls += " jt-chip-red";
  else if (total > 0 && total === ok) cls += " jt-chip-green";
  const tip =
    "active = pending + claimed + running + writing\n" +
    "ok = success\n" +
    "fail = error + failed + killed";
  return (
    <span className={cls} title={tip}>
      {`${active} / ${ok} / ${fail}`}
    </span>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function JobsTable({
  jobs,
  search = "",
  facets = {},
  onToggleFacet,
  emptyMessage = "no jobs found",
}: JobsTableProps) {
  const navigate = useNavigate();

  if (jobs.length === 0) {
    return (
      <>
        <style>{styles}</style>
        <div className="jt-empty">{emptyMessage}</div>
      </>
    );
  }

  return (
    <>
      <style>{styles}</style>
      <table className="jt-table">
        <thead>
          <tr>
            <th className="jt-th jt-th-index" />
            <th className="jt-th">Name</th>
            <th className="jt-th jt-th-pool">Worker Pool</th>
            <th
              className="jt-th jt-th-stats"
              title="active = pending + claimed + running + writing&#10;ok = success&#10;fail = error + failed + killed"
            >
              active / ok / fail
            </th>
            <th className="jt-th jt-th-time">Start Time (local)</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map(
            ({ job_id, name, workpool_id, metadata, created_at, tasks }, i) => {
              const submitDate = new Date(created_at);
              const cc = workpool_id ? workerPoolColor(workpool_id) : null;
              return (
                <tr
                  key={job_id}
                  className="jt-tr"
                  onClick={() => navigate(`/jobs/${job_id}`)}
                >
                  <td className="jt-td jt-td-index">
                    {String(i + 1).padStart(2, "0")}
                  </td>
                  <td className="jt-td">
                    <div className="jt-id">{name || job_id}</div>
                    <LabelChips
                      metadata={metadata}
                      search={search}
                      facets={facets}
                      onToggleFacet={onToggleFacet}
                    />
                  </td>
                  <td className="jt-td jt-td-pool">
                    {workpool_id && cc ? (
                      <span className="jt-pool-cell">
                        <span
                          className="jt-pool-swatch"
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
                      <span className="jt-pool-empty">—</span>
                    )}
                  </td>
                  <td className="jt-td jt-td-stats">
                    <JobStatsChip tasks={tasks} />
                  </td>
                  <td className="jt-td jt-td-time">
                    {formatTimestamp(submitDate)}
                  </td>
                </tr>
              );
            }
          )}
        </tbody>
      </table>
    </>
  );
}

// ── Styles ────────────────────────────────────────────────────────────────────

const styles = `
  .jt-table {
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
    font-family: 'IBM Plex Mono', monospace;
  }

  .jt-th {
    font-size: 0.6rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: #aaa;
    font-weight: 400;
    padding: 0.45rem 0.5rem;
    text-align: left;
    border-bottom: 1px solid #f0f0f0;
  }

  .jt-th-stats { text-align: right; width: 8rem; }
  .jt-th-time  { text-align: right; width: 14rem; }
  .jt-th-index { width: 2.2rem; }
  .jt-th-pool  { width: 9rem; }

  .jt-tr { cursor: pointer; }

  .jt-td {
    padding: 0.65rem 0.5rem;
    vertical-align: top;
    border-bottom: 1px solid #f0f0f0;
    transition: background 0.12s;
  }

  .jt-tr:hover .jt-td { background: #f0f5ff; }
  .jt-tr:hover .jt-id  { color: #1565c0; }

  .jt-td-index {
    font-size: 0.65rem;
    color: #ccc;
    font-weight: 400;
    padding-top: 0.68rem;
    border-left: 2px solid transparent;
    transition: background 0.12s, border-color 0.15s;
  }

  .jt-tr:hover .jt-td-index { border-left-color: #1565c0; }

  .jt-id {
    font-size: 0.82rem;
    font-weight: 500;
    color: #222;
    transition: color 0.12s;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .jt-td-stats {
    text-align: right;
    white-space: nowrap;
    padding-top: 0.68rem;
  }

  .jt-td-time {
    text-align: right;
    font-size: 0.72rem;
    color: #999;
    white-space: nowrap;
    padding-top: 0.68rem;
  }

  .jt-td-pool {
    padding-top: 0.68rem;
    overflow: hidden;
  }

  .jt-pool-cell {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-size: 0.68rem;
    font-family: 'IBM Plex Mono', monospace;
    font-weight: 600;
    max-width: 100%;
    overflow: hidden;
  }

  .jt-pool-swatch {
    width: 9px;
    height: 9px;
    border-radius: 2px;
    flex-shrink: 0;
  }

  .jt-pool-empty {
    font-size: 0.72rem;
    color: #ccc;
  }

  .jt-chip {
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

  .jt-chip-green { background: #e8f5e9; color: #2e7d32; }
  .jt-chip-red   { background: #ffebee; color: #b71c1c; }

  .jt-empty {
    font-size: 0.75rem;
    color: #ccc;
    padding: 1rem 0.5rem;
    border-top: 1px solid #f0f0f0;
    border-bottom: 1px solid #f0f0f0;
    font-family: 'IBM Plex Mono', monospace;
  }
`;
