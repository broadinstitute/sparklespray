import { useState, useEffect, useMemo } from "react";
import { Link } from "react-router-dom";
import { getJobs, getClusters } from "../data/events";
import { useEvents, mergeEvents } from "../data/EventProvider";
import type { AnyEvent } from "../types";

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
    align-items: baseline;
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

  .jl-id {
    font-size: 0.82rem;
    font-weight: 500;
    color: #222;
    transition: color 0.12s ease;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    flex-shrink: 1;
    min-width: 0;
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
  }

  .jl-timestamp {
    font-size: 0.72rem;
    color: #999;
    white-space: nowrap;
    flex-shrink: 0;
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
  const { addEventListener } = useEvents();
  const [allEvents, setAllEvents] = useState<AnyEvent[]>([]);

  useEffect(() => {
    return addEventListener((newEvents) =>
      setAllEvents((prev) => mergeEvents(prev, newEvents))
    );
  }, [addEventListener]);

  const jobs = useMemo(() => getJobs(allEvents), [allEvents]);
  const clusters = useMemo(() => getClusters(allEvents), [allEvents]);

  return (
    <>
      <style>{styles}</style>
      <div className="jl-root">
        <div className="jl-inner">
          <h1 className="jl-page-title">sparkles</h1>

          <section className="jl-section">
            <h2 className="jl-section-title">Jobs</h2>
            <p className="jl-subtitle">
              {jobs.length} job{jobs.length !== 1 ? "s" : ""} found
            </p>
            <div className="jl-divider" />
            <div className="jl-col-headers">
              <span>Identifier</span>
              <span>Start Time (UTC)</span>
            </div>
            {jobs.length === 0 ? (
              <div className="jl-empty">no jobs found</div>
            ) : (
              <ul className="jl-list">
                {jobs.map(({ jobId, startTime }, i) => (
                  <li key={jobId} className="jl-item">
                    <Link to={`/jobs/${jobId}`} className="jl-link">
                      <span className="jl-index">
                        {String(i + 1).padStart(2, "0")}
                      </span>
                      <span className="jl-id">{jobId}</span>
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
