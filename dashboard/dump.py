#!/usr/bin/env python3
"""Dump SparklesV6Task documents with a specific job_id to JSON."""

import json
from google.cloud import datastore


def main():
    project = "depmap-portal-pipeline"
    job_id = "daintree-fit-v1-1acdceb51f5d5c0301de-2"
    output_file = "dumped-tasks.json"

    client = datastore.Client(project=project)

    query = client.query(kind="SparklesV6Task")
    query.add_filter("job_id", "=", job_id)

    tasks = []
    for entity in query.fetch():
        data = dict(entity)
        data["_id"] = entity.key.id or entity.key.name
        tasks.append(data)

    print(f"Found {len(tasks)} tasks for job_id={job_id!r}")

    with open(output_file, "w") as f:
        json.dump(tasks, f, indent=2, default=str)

    print(f"Written to {output_file}")


if __name__ == "__main__":
    main()
