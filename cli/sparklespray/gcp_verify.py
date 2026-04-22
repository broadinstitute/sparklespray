"""Verify that a service account has all permissions needed by sparklespray."""

import argparse
import sys
import time

from google.auth import impersonated_credentials
from google.cloud import datastore, pubsub_v1, storage
from google.cloud.batch_v1alpha.services.batch_service import BatchServiceClient
from google.cloud.batch_v1alpha import types as batch
from google.cloud.batch_v1alpha import types as batch
from google.oauth2 import service_account

from sparklespray.config import SCOPES
from sparklespray.util import random_string


def _retry(label: str, fn, max_seconds: int = 900):
    """Run fn() with exponential backoff up to max_seconds."""
    delay = 2
    start = time.time()
    attempt = 0
    while True:
        try:
            result = fn()
            print(f"  [OK] {label}")
            return result
        except Exception as exc:
            elapsed = time.time() - start
            if elapsed + delay > max_seconds:
                print(f"  [FAIL] {label}: {exc}")
                raise
            attempt += 1
            print(f"  [retry {attempt}] {label}: {exc} — waiting {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 60)


def _parse_gcs_url(url_prefix: str):
    """Return (bucket_name, blob_prefix) from a gs://bucket/prefix/ URL."""
    assert url_prefix.startswith(
        "gs://"
    ), f"url_prefix must start with gs://, got {url_prefix!r}"
    without_scheme = url_prefix[5:]
    parts = without_scheme.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


def verify(
    project: str, keyfile: str, url_prefix: str, dashboard_user_service_account: str
):
    credentials = service_account.Credentials.from_service_account_file(
        keyfile, scopes=SCOPES
    )

    bucket_name, blob_prefix = _parse_gcs_url(url_prefix)
    blob_name = f"{blob_prefix}gcp-verify-{random_string(10)}.tmp"
    sub_id = f"gcp-verify-{random_string(10)}"
    subscription_path = f"projects/{project}/subscriptions/{sub_id}"
    topic_path = f"projects/{project}/topics/sparkles-v6-task-out"
    entity_key_name = f"gcp-verify-{random_string(10)}"

    failures = []

    # 1. GCS create
    gcs_client = storage.Client(project=project, credentials=credentials)
    bucket = gcs_client.bucket(bucket_name)
    blob_obj = None
    try:
        blob_obj = _retry(
            f"GCS: create blob gs://{bucket_name}/{blob_name}",
            lambda: _gcs_create(bucket, blob_name),
        )
    except Exception:
        failures.append("GCS create")

    # 2. GCS delete
    try:
        _retry(
            f"GCS: delete blob gs://{bucket_name}/{blob_name}",
            lambda: _gcs_delete(bucket, blob_name),
        )
    except Exception:
        failures.append("GCS delete")

    # 3. PubSub: create subscription
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    sub_created = False
    try:
        _retry(
            f"PubSub: create subscription {subscription_path}",
            lambda: subscriber.create_subscription(
                request={"name": subscription_path, "topic": topic_path}
            ),
        )
        sub_created = True
    except Exception:
        failures.append("PubSub create subscription")

    # 4. PubSub: pull (as main service account)
    if sub_created:
        try:
            _retry(
                f"PubSub: pull from {subscription_path}",
                lambda: subscriber.pull(
                    request={"subscription": subscription_path, "max_messages": 1}
                ),
            )
        except Exception:
            failures.append("PubSub pull")

    # 5. PubSub: impersonated pull (as dashboard user service account)
    if sub_created:
        try:
            dashboard_creds = impersonated_credentials.Credentials(
                source_credentials=credentials,
                target_principal=dashboard_user_service_account,
                target_scopes=SCOPES,
            )
            dashboard_subscriber = pubsub_v1.SubscriberClient(
                credentials=dashboard_creds
            )
            _retry(
                f"PubSub: impersonated pull as {dashboard_user_service_account}",
                lambda: dashboard_subscriber.pull(
                    request={"subscription": subscription_path, "max_messages": 1}
                ),
            )
        except Exception:
            failures.append("PubSub impersonated pull")

    # 6. PubSub: delete subscription
    if sub_created:
        try:
            _retry(
                f"PubSub: delete subscription {subscription_path}",
                lambda: subscriber.delete_subscription(
                    request={"subscription": subscription_path}
                ),
            )
        except Exception:
            failures.append("PubSub delete subscription")

    # 7. Datastore write + delete
    ds_client = datastore.Client(project=project, credentials=credentials)
    try:
        _retry(
            "Datastore: write and delete entity",
            lambda: _datastore_write_delete(ds_client, entity_key_name),
        )
    except Exception:
        failures.append("Datastore write/delete")

    # 8. Batch API list jobs
    batch_client = BatchServiceClient(credentials=credentials)
    try:
        _retry(
            "Batch API: list jobs",
            lambda: _batch_list_jobs(batch_client, project),
        )
    except Exception:
        failures.append("Batch API list jobs")

    if failures:
        print(f"\nFailed checks: {', '.join(failures)}")
        sys.exit(1)
    else:
        print("\nAll checks passed.")


def _gcs_create(bucket, blob_name: str):
    blob = bucket.blob(blob_name)
    blob.upload_from_string(b"gcp-verify")
    return blob


def _gcs_delete(bucket, blob_name: str):
    blob = bucket.blob(blob_name)
    blob.delete()


def _datastore_write_delete(client: datastore.Client, key_name: str):
    key = client.key("SparklesV6Verify", key_name)
    entity = datastore.Entity(key=key)
    entity["verify"] = True
    client.put(entity)
    client.delete(key)


def _batch_list_jobs(batch_client: BatchServiceClient, project: str):
    pager = batch_client.list_jobs(
        batch.ListJobsRequest(parent=f"projects/{project}/locations/-", page_size=5)
    )
    for _ in zip(pager, range(5)):
        pass


def main():
    parser = argparse.ArgumentParser(
        description="Verify GCP permissions needed by sparklespray"
    )
    parser.add_argument(
        "--keyfile", required=True, help="Path to service account key JSON"
    )
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--url-prefix", required=True, help="GCS URL prefix, e.g. gs://bucket/prefix/"
    )
    parser.add_argument(
        "--dashboard-user-service-account",
        required=True,
        help="Dashboard user service account email",
    )
    args = parser.parse_args()

    verify(
        project=args.project,
        keyfile=args.keyfile,
        url_prefix=args.url_prefix,
        dashboard_user_service_account=args.dashboard_user_service_account,
    )


if __name__ == "__main__":
    main()
