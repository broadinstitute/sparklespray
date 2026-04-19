#!/usr/bin/env python3
"""Convert dumped-tasks.json history into events format matching events.json."""

import json
from datetime import datetime, timezone, timedelta


def ts_to_iso(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.isoformat()


def make_event(event_type: str, task_id: str, job_id: str, ts: float, **extra) -> dict:
    expiry_ts = ts + 86400  # 24 hours later
    event = {
        "type": event_type,
        "task_id": task_id,
        "job_id": job_id,
        "timestamp": ts_to_iso(ts),
        "expiry": ts_to_iso(expiry_ts),
    }
    event.update(extra)
    return event


def task_to_events(task: dict) -> list[dict]:
    task_id = task["_id"]
    job_id = task["job_id"]
    exit_code = 0  # task.get("exit_code", 0)
    events = []

    for entry in task.get("history", []):
        status = entry["status"]
        ts = entry["timestamp"]

        if status == "pending":
            pass  # no event for pending

        elif status == "claimed":
            events.append(make_event("task_claimed", task_id, job_id, ts))
            # Synthetic: task_staged 1 second after claimed
            events.append(make_event("task_staged", task_id, job_id, ts + 1))

        elif status == "complete":
            # Synthetic: task_executed 1 second before complete
            events.append(
                make_event(
                    "task_executed",
                    task_id,
                    job_id,
                    ts - 1,
                    exit_code=exit_code,
                    max_mem_in_gb=0.0,
                )
            )
            events.append(make_event("task_complete", task_id, job_id, ts))

        elif status == "failed":
            events.append(
                make_event(
                    "task_failed",
                    task_id,
                    job_id,
                    ts,
                    failure_reason="unknown",
                )
            )

        elif status == "orphaned":
            events.append(make_event("task_orphaned", task_id, job_id, ts))

    return events


def main():
    with open("dumped-tasks.json") as f:
        tasks = json.load(f)

    all_events = []
    all_task_ids = set()
    for task in tasks:
        all_task_ids.add(task["_id"])
        try:
            all_events.extend(task_to_events(task))
        except Exception:
            print(f"failed converting {task}")
            raise

    all_events.sort(key=lambda e: e["timestamp"])

    with open("events-from-dump.json", "w") as f:
        json.dump({"task_ids": sorted(all_task_ids), "events": all_events}, f, indent=2)

    print(
        f"Wrote {len(all_events)} events from {len(tasks)} tasks to events-from-dump.json"
    )


if __name__ == "__main__":
    main()
