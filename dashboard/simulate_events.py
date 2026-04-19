#!/usr/bin/env python3
"""
Simulate a stream of sparklespray task lifecycle events for testing.
"""

import random
import json
import argparse
from datetime import datetime, timedelta, timezone
import uuid


def sample_normal(mean: float, std: float) -> float:
    """Sample from a normal distribution, clamped to non-negative."""
    return max(0.0, random.gauss(mean, std))


def make_event(
    event_type: str, task_id: str, job_id: str, ts: datetime, **extra
) -> dict:
    expiry = ts + timedelta(hours=24)
    return {
        "type": event_type,
        "task_id": task_id,
        "job_id": job_id,
        "timestamp": ts.isoformat(),
        "expiry": expiry.isoformat(),
        **extra,
    }


FAILURE_REASONS = [
    "out of memory",
    "disk quota exceeded",
    "network timeout",
    "worker node preempted",
    "unhandled exception in task",
]


def generate_task_events(
    task_id: str,
    job_id: str,
    start_time: datetime,
    max_time_pending: float,
    mean_staging: float,
    std_staging: float,
    mean_executing: float,
    std_executing: float,
    mean_uploading: float,
    std_uploading: float,
    prob_orphaned: float,
    prob_failed: float,
) -> list[dict]:
    """
    Generate the full event sequence for a single task, including any
    orphan-and-retry cycles. Returns events in chronological order.
    """
    events = []
    current_time = start_time

    while True:
        # --- pending phase (uniform [0, max_time_pending]) ---
        pending_secs = random.uniform(0, max_time_pending)
        current_time += timedelta(seconds=pending_secs)

        # task_claimed
        events.append(make_event("task_claimed", task_id, job_id, current_time))

        # --- staging phase ---
        # check for failure/orphan immediately after claim (before staging completes)
        if random.random() < prob_failed:
            events.append(
                make_event(
                    "task_failed",
                    task_id,
                    job_id,
                    current_time,
                    failure_reason=random.choice(FAILURE_REASONS),
                )
            )
            return events
        if random.random() < prob_orphaned:
            events.append(make_event("task_orphaned", task_id, job_id, current_time))
            continue  # task goes back to pending

        staging_secs = sample_normal(mean_staging, std_staging)
        current_time += timedelta(seconds=staging_secs)
        events.append(make_event("task_staged", task_id, job_id, current_time))

        # --- executing phase ---
        if random.random() < prob_failed:
            events.append(
                make_event(
                    "task_failed",
                    task_id,
                    job_id,
                    current_time,
                    failure_reason=random.choice(FAILURE_REASONS),
                )
            )
            return events
        if random.random() < prob_orphaned:
            events.append(make_event("task_orphaned", task_id, job_id, current_time))
            continue

        executing_secs = sample_normal(mean_executing, std_executing)
        current_time += timedelta(seconds=executing_secs)
        exit_code = 0
        max_mem_in_gb = round(random.uniform(0.1, 8.0), 3)
        events.append(
            make_event(
                "task_executed",
                task_id,
                job_id,
                current_time,
                exit_code=exit_code,
                max_mem_in_gb=max_mem_in_gb,
            )
        )

        # --- uploading phase ---
        if random.random() < prob_failed:
            events.append(
                make_event(
                    "task_failed",
                    task_id,
                    job_id,
                    current_time,
                    failure_reason=random.choice(FAILURE_REASONS),
                )
            )
            return events
        if random.random() < prob_orphaned:
            events.append(make_event("task_orphaned", task_id, job_id, current_time))
            continue

        uploading_secs = sample_normal(mean_uploading, std_uploading)
        current_time += timedelta(seconds=uploading_secs)
        events.append(make_event("task_complete", task_id, job_id, current_time))
        return events


def main():
    parser = argparse.ArgumentParser(
        description="Generate simulated sparklespray task lifecycle events."
    )

    parser.add_argument(
        "--num-tasks",
        type=int,
        default=10,
        help="Number of tasks to simulate (default: 10)",
    )
    parser.add_argument(
        "--job-id", default=None, help="Job ID to use (default: random UUID)"
    )
    parser.add_argument(
        "--seed", type=int, default=None, help="Random seed for reproducibility"
    )
    parser.add_argument(
        "--output",
        default="events.json",
        help="Output file path (default: events.json)",
    )

    # Pending stage
    parser.add_argument(
        "--max-time-pending",
        type=float,
        default=30.0 * 60,
        help="Max seconds a task spends pending (uniform [0, max]). Default: 30",
    )

    # Staging stage
    parser.add_argument(
        "--mean-staging",
        type=float,
        default=60.0,
        help="Mean seconds for staging phase (default: 10)",
    )
    parser.add_argument(
        "--std-staging",
        type=float,
        default=2.0,
        help="Std dev for staging phase (default: 2)",
    )

    # Executing stage
    parser.add_argument(
        "--mean-executing",
        type=float,
        default=60.0 * 10,
        help="Mean seconds for executing phase (default: 60)",
    )
    parser.add_argument(
        "--std-executing",
        type=float,
        default=15.0,
        help="Std dev for executing phase (default: 15)",
    )

    # Uploading stage
    parser.add_argument(
        "--mean-uploading",
        type=float,
        default=10.0,
        help="Mean seconds for uploading phase (default: 5)",
    )
    parser.add_argument(
        "--std-uploading",
        type=float,
        default=1.0,
        help="Std dev for uploading phase (default: 1)",
    )

    # Failure probabilities (checked at each stage transition)
    parser.add_argument(
        "--prob-orphaned",
        type=float,
        default=0.05,
        help="Probability of orphan at each stage transition (default: 0.05)",
    )
    parser.add_argument(
        "--prob-failed",
        type=float,
        default=0.02,
        help="Probability of failure at each stage transition (default: 0.02)",
    )

    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    job_id = args.job_id or str(uuid.uuid4())
    base_time = datetime.now(tz=timezone.utc)

    all_events = []
    for i in range(args.num_tasks):
        task_id = f"task-{i:04d}-{uuid.uuid4().hex[:8]}"
        # Stagger task start times slightly so they're not all identical
        task_start = base_time + timedelta(
            seconds=random.uniform(0, args.max_time_pending * 0.1)
        )
        task_events = generate_task_events(
            task_id=task_id,
            job_id=job_id,
            start_time=task_start,
            max_time_pending=args.max_time_pending,
            mean_staging=args.mean_staging,
            std_staging=args.std_staging,
            mean_executing=args.mean_executing,
            std_executing=args.std_executing,
            mean_uploading=args.mean_uploading,
            std_uploading=args.std_uploading,
            prob_orphaned=args.prob_orphaned,
            prob_failed=args.prob_failed,
        )
        all_events.extend(task_events)

    # Sort all events by timestamp
    all_events.sort(key=lambda e: e["timestamp"])

    with open(args.output, "w") as f:
        json.dump(all_events, f, indent=2)

    # Print a summary
    type_counts: dict[str, int] = {}
    for e in all_events:
        type_counts[e["type"]] = type_counts.get(e["type"], 0) + 1

    print(
        f"Generated {len(all_events)} events for {args.num_tasks} tasks in job '{job_id}'"
    )
    print(f"Written to: {args.output}")
    print("Event type counts:")
    for event_type, count in sorted(type_counts.items()):
        print(f"  {event_type}: {count}")


if __name__ == "__main__":
    main()
