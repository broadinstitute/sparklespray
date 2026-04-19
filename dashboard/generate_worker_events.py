#!/usr/bin/env python3
"""
Generate cluster and worker lifecycle events to augment an existing events.json file.

For each unique cluster_id found in the input events:
  - Creates a cluster_started event before the first event for that cluster
  - Creates 100 worker_started events (spread across cluster startup window)
  - After the last event timestamp, creates worker_stopped events for all workers
  - 5% of workers experience a mid-run restart: worker_stopped + (1-10 min later) worker_started

Output is original events + generated events, sorted by timestamp.
"""

import json
import random
import argparse
import uuid
from datetime import datetime, timedelta
from collections import defaultdict


def parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def fmt_ts(dt: datetime) -> str:
    return dt.isoformat()


def cluster_event(event_type: str, cluster_id: str, ts: datetime) -> dict:
    return {"type": event_type, "cluster_id": cluster_id, "timestamp": fmt_ts(ts)}


def worker_event(
    event_type: str, cluster_id: str, worker_id: str, ts: datetime
) -> dict:
    return {
        "type": event_type,
        "cluster_id": cluster_id,
        "worker_id": worker_id,
        "timestamp": fmt_ts(ts),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Generate cluster/worker lifecycle events from an existing events.json."
    )
    parser.add_argument(
        "--input",
        default="events.json",
        help="Input events JSON file (default: events.json)",
    )
    parser.add_argument(
        "--output",
        default="events-with-workers.json",
        help="Output events JSON file (default: events-with-workers.json)",
    )
    parser.add_argument(
        "--workers-per-cluster",
        type=int,
        default=100,
        help="Number of workers per cluster (default: 100)",
    )
    parser.add_argument(
        "--restart-fraction",
        type=float,
        default=0.05,
        help="Fraction of workers that crash and restart mid-run (default: 0.05)",
    )
    parser.add_argument(
        "--seed", type=int, default=None, help="Random seed for reproducibility"
    )
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    with open(args.input) as f:
        raw = json.load(f)

    if isinstance(raw, list):
        input_events = raw
        wrapper = None
    elif isinstance(raw, dict) and "events" in raw:
        input_events = raw["events"]
        wrapper = {k: v for k, v in raw.items() if k != "events"}
    else:
        raise ValueError(
            "Input JSON must be a list of events or a dict with an 'events' key."
        )

    # Assign a single cluster_id to any events that don't already have one
    default_cluster_id = str(uuid.uuid4())
    for e in input_events:
        if not e.get("cluster_id"):
            e["cluster_id"] = default_cluster_id

    # Group input events by cluster_id
    by_cluster: dict[str, list] = defaultdict(list)
    for e in input_events:
        by_cluster[e["cluster_id"]].append(e)

    generated = []

    for cluster_id, cl_events in by_cluster.items():
        timestamps = [parse_ts(e["timestamp"]) for e in cl_events]
        first_ts = min(timestamps)
        last_ts = max(timestamps)
        cluster_span_secs = max((last_ts - first_ts).total_seconds(), 1.0)

        # cluster_started: 1–5 minutes before the first event for this cluster
        pre_start_secs = random.uniform(60, 300)
        cluster_started_ts = first_ts - timedelta(seconds=pre_start_secs)
        generated.append(
            cluster_event("cluster_started", cluster_id, cluster_started_ts)
        )

        job_id = next((e["job_id"] for e in cl_events if e.get("job_id")), None)
        if job_id:
            generated.append(
                {
                    "type": "job_started",
                    "job_id": job_id,
                    "cluster_id": cluster_id,
                    "timestamp": fmt_ts(cluster_started_ts),
                }
            )

        n_workers = args.workers_per_cluster
        n_restarts = round(n_workers * args.restart_fraction)
        restart_indices = set(
            random.sample(range(n_workers), min(n_restarts, n_workers))
        )

        # Workers start within the pre-start window (between cluster_started and first event)
        worker_ids = [
            f"worker-{cluster_id[:8]}-{i:04d}-{uuid.uuid4().hex[:6]}"
            for i in range(n_workers)
        ]

        for i, worker_id in enumerate(worker_ids):
            # Initial worker_started: somewhere between cluster_started and first_ts
            init_start_ts = cluster_started_ts + timedelta(
                seconds=random.uniform(0, pre_start_secs)
            )
            generated.append(
                worker_event("worker_started", cluster_id, worker_id, init_start_ts)
            )

            if i in restart_indices:
                # Crash at a random point during the cluster run (10%–90% of span)
                crash_offset = random.uniform(
                    cluster_span_secs * 0.1, cluster_span_secs * 0.9
                )
                crash_ts = first_ts + timedelta(seconds=crash_offset)
                generated.append(
                    worker_event("worker_stopped", cluster_id, worker_id, crash_ts)
                )

                # Restart 1–10 minutes after the crash
                restart_delay_secs = random.uniform(60, 600)
                restart_ts = crash_ts + timedelta(seconds=restart_delay_secs)
                generated.append(
                    worker_event("worker_started", cluster_id, worker_id, restart_ts)
                )

        # After the last event, stop all workers (staggered by up to 60s)
        shutdown_base_ts = last_ts + timedelta(seconds=random.uniform(30, 120))
        for worker_id in worker_ids:
            stop_ts = shutdown_base_ts + timedelta(seconds=random.uniform(0, 60))
            generated.append(
                worker_event("worker_stopped", cluster_id, worker_id, stop_ts)
            )

    all_events = input_events + generated
    all_events.sort(key=lambda e: e["timestamp"])

    with open(args.output, "w") as f:
        if wrapper is not None:
            json.dump({**wrapper, "events": all_events}, f, indent=2)
        else:
            json.dump(all_events, f, indent=2)

    type_counts: dict[str, int] = {}
    for e in all_events:
        type_counts[e["type"]] = type_counts.get(e["type"], 0) + 1

    print(f"Input events:    {len(input_events)}")
    print(f"Generated events: {len(generated)}")
    print(f"Total events:    {len(all_events)}")
    print(f"Written to:      {args.output}")
    print("Event type counts:")
    for event_type, count in sorted(type_counts.items()):
        print(f"  {event_type}: {count}")


if __name__ == "__main__":
    main()
