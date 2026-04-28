#!/usr/bin/env python3
"""
Backfill missing job_started events in SparklesV6Event.

For every unique job_id found in SparklesV6Event that does not already have a
job_started event, insert one using the earliest event timestamp for that job
and metadata from SparklesV6Job (cluster_id, task_count).

Usage:
    python backfill_job_started_events.py --project <gcp-project> [--dryrun]
"""

import argparse
import uuid
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from google.cloud import datastore

EVENT_COLLECTION = "SparklesV6Event"
JOB_COLLECTION = "SparklesV6Job"
EVENT_EXPIRY = timedelta(days=7)


def main():
    parser = argparse.ArgumentParser(
        description="Backfill job_started events into SparklesV6Event."
    )
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--dryrun",
        action="store_true",
        help="Print what would be inserted without writing",
    )
    args = parser.parse_args()

    client = datastore.Client(project=args.project)

    print("Querying SparklesV6Event for all events with a job_id...")
    query = client.query(kind=EVENT_COLLECTION)
    query.add_filter("job_id", ">=", "")

    # Collect per-job info: earliest timestamp, cluster_id, and whether job_started exists
    job_earliest: dict[str, datetime] = {}
    job_cluster: dict[str, str] = {}
    jobs_with_started: set[str] = set()

    total = 0
    for entity in query.fetch():
        total += 1
        event_type = entity.get("type", "")
        job_id = entity.get("job_id", "")
        cluster_id = entity.get("cluster_id", "")
        timestamp = entity.get("timestamp")

        if not job_id:
            continue

        if event_type == "job_started":
            jobs_with_started.add(job_id)
            continue

        if isinstance(timestamp, datetime):
            if job_id not in job_earliest or timestamp < job_earliest[job_id]:
                job_earliest[job_id] = timestamp
            if cluster_id and job_id not in job_cluster:
                job_cluster[job_id] = cluster_id

    print(f"Scanned {total} events.")
    print(f"Found {len(job_earliest)} unique job_id(s) in events.")
    print(
        f"Found {len(jobs_with_started)} job_id(s) that already have a job_started event."
    )

    missing = {jid for jid in job_earliest if jid not in jobs_with_started}
    print(f"Jobs missing a job_started event: {len(missing)}")

    if not missing:
        print("Nothing to do.")
        return

    # Fetch task_count and fill in any missing cluster_id from SparklesV6Job
    print("Fetching job metadata from SparklesV6Job...")
    job_task_count: dict[str, int] = {}
    job_keys = [client.key(JOB_COLLECTION, jid) for jid in missing]
    # Batch get in chunks of 1000 (datastore limit)
    chunk_size = 1000
    for i in range(0, len(job_keys), chunk_size):
        chunk = job_keys[i : i + chunk_size]
        entities = client.get_multi(chunk)
        for entity in entities:
            if entity is None:
                continue
            jid = entity.get("job_id") or entity.key.name
            if not jid:
                continue
            job_task_count[jid] = entity.get("task_count", 0)
            if jid not in job_cluster or not job_cluster.get(jid):
                job_cluster[jid] = entity.get("cluster_id", "")

    # Build and (optionally) insert the events
    to_insert = []
    for job_id in sorted(missing):
        cluster_id = job_cluster.get(job_id, "")
        task_count = job_task_count.get(job_id, 0)
        timestamp = job_earliest[job_id]
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        expiry = timestamp + EVENT_EXPIRY
        event_id = str(uuid.uuid4())

        event = {
            "event_id": event_id,
            "type": "job_started",
            "job_id": job_id,
            "cluster_id": cluster_id,
            "task_count": task_count,
            "timestamp": timestamp,
            "expiry": expiry,
        }
        to_insert.append(event)

        status = "[DRYRUN]" if args.dryrun else "[INSERT]"
        print(
            f"{status} job_started: job_id={job_id!r} cluster_id={cluster_id!r} "
            f"task_count={task_count} timestamp={timestamp.isoformat()} event_id={event_id}"
        )

    if args.dryrun:
        print(
            f"\nDry run complete. Would insert {len(to_insert)} job_started event(s)."
        )
        return

    print(f"\nInserting {len(to_insert)} job_started event(s)...")
    entities_to_put = []
    for event in to_insert:
        key = client.key(EVENT_COLLECTION, event["event_id"])
        entity = datastore.Entity(key=key)
        entity.update(
            {
                "event_id": event["event_id"],
                "type": event["type"],
                "job_id": event["job_id"],
                "cluster_id": event["cluster_id"],
                "task_count": event["task_count"],
                "timestamp": event["timestamp"],
                "expiry": event["expiry"],
            }
        )
        entities_to_put.append(entity)

    # Batch put in chunks of 500 (datastore mutation limit)
    for i in range(0, len(entities_to_put), 500):
        chunk = entities_to_put[i : i + 500]
        client.put_multi(chunk)
        print(f"  Wrote {i + len(chunk)}/{len(entities_to_put)} events.")

    print("Done.")


if __name__ == "__main__":
    main()
