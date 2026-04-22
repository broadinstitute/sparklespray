"""Get/set a JSON dictionary stored as a Datastore entity.

Usage:
    datastore_helper.py get --project <project> --out <json_file>
    datastore_helper.py set --project <project> --in <json_file>
    datastore_helper.py wait-for-indexes --project <project>

The entity is stored with Kind='SparklesV6ProjectConfig', id='SparklesV6ProjectConfig'.
Authentication uses the access token in the GCP_ACCESS_TOKEN environment variable.
"""

import argparse
import json
import os
import sys
import time

from google.cloud import datastore
from google.cloud.datastore_admin_v1 import DatastoreAdminClient
from google.cloud.datastore_admin_v1.types import Index
from google.oauth2.credentials import Credentials

_KIND = "SparklesV6ProjectConfig"
_ENTITY_ID = "SparklesV6ProjectConfig"


def _get_token() -> str:
    token = os.environ.get("GCP_ACCESS_TOKEN")
    if not token:
        print("GCP_ACCESS_TOKEN environment variable is required", file=sys.stderr)
        sys.exit(1)
    return token


def _client(project: str) -> datastore.Client:
    return datastore.Client(
        project=project, credentials=Credentials(token=_get_token())
    )


def cmd_get(project: str, out_path: str):
    client = _client(project)
    key = client.key(_KIND, _ENTITY_ID)
    entity = client.get(key)
    if entity is None:
        print(f"No entity found ({_KIND}/{_ENTITY_ID})", file=sys.stderr)
        sys.exit(1)
    data = dict(entity)
    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Wrote {len(data)} keys to {out_path}")


def cmd_set(project: str, in_path: str):
    with open(in_path) as f:
        data = json.load(f)
    if not isinstance(data, dict):
        print("Input JSON must be a dictionary", file=sys.stderr)
        sys.exit(1)
    client = _client(project)
    key = client.key(_KIND, _ENTITY_ID)
    entity = datastore.Entity(key=key)
    entity.update(data)
    client.put(entity)
    print(f"Stored {len(data)} keys to {_KIND}/{_ENTITY_ID}")


def cmd_wait_for_indexes(project: str):
    credentials = Credentials(token=_get_token())
    admin_client = DatastoreAdminClient(credentials=credentials)
    print("Waiting for all Datastore indexes to be ready...")
    deadline = time.time() + 600
    while True:
        indexes = list(admin_client.list_indexes(request={"project_id": project}))
        not_ready = [idx for idx in indexes if idx.state != Index.State.READY]
        if not not_ready:
            print(f"All {len(indexes)} index(es) are ready.")
            return
        if time.time() >= deadline:
            states = ", ".join(
                f"{idx.kind}({Index.State(idx.state).name})" for idx in not_ready
            )
            print(
                f"Timed out after 10 minutes. Indexes still not ready: {states}",
                file=sys.stderr,
            )
            sys.exit(1)
        states = ", ".join(
            f"{idx.kind}({Index.State(idx.state).name})" for idx in not_ready
        )
        print(f"  {len(not_ready)} index(es) not yet ready: {states} — retrying in 10s")
        time.sleep(10)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)

    get_p = sub.add_parser("get", help="Read entity and write to JSON file")
    get_p.add_argument("--project", required=True, help="GCP project ID")
    get_p.add_argument(
        "--out", required=True, metavar="json_file", help="Output JSON file path"
    )

    set_p = sub.add_parser("set", help="Read JSON file and write to entity")
    set_p.add_argument("--project", required=True, help="GCP project ID")
    set_p.add_argument(
        "--in",
        required=True,
        dest="in_file",
        metavar="json_file",
        help="Input JSON file path",
    )

    wait_p = sub.add_parser(
        "wait-for-indexes", help="Block until all Datastore indexes are ready"
    )
    wait_p.add_argument("--project", required=True, help="GCP project ID")

    args = parser.parse_args()

    if args.command == "get":
        cmd_get(args.project, args.out)
    elif args.command == "set":
        cmd_set(args.project, args.in_file)
    else:
        cmd_wait_for_indexes(args.project)


if __name__ == "__main__":
    main()
