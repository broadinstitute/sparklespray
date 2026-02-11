import subprocess
import os
from importlib import resources

from sparklespray.gcp_permissions import parse_docker_image_name, ArtifactRegistryPath
from sparklespray.util import random_string
from google.cloud import datastore
from google.api_core import exceptions
from google.cloud.storage.client import Client as GSClient
from sparklespray.config import SCOPES
from google.oauth2 import service_account
from typing import List
import time
from collections import namedtuple
from google.api_core.exceptions import PermissionDenied, Forbidden
from google.auth.exceptions import RefreshError
import json
import sparklespray


services_to_add = [  # "storage.googleapis.com",
    "datastore.googleapis.com",
    "storage-component.googleapis.com",
    "pubsub.googleapis.com",
    "storage-api.googleapis.com",
    "compute.googleapis.com",
    "containerregistry.googleapis.com",
    "logging",
    "batch.googleapis.com",
]

roles_to_add = [
    "roles/owner",  # Eventually drop this
    "roles/compute.admin",
    "roles/datastore.user",
    "roles/lifesciences.workflowsRunner",
    "roles/pubsub.editor",
    "roles/storage.admin",
]


def _run_cmd(cmd, args, suppress_warning=False, max_attempts=10):
    attempt = 0

    cmd = [cmd] + args
    cmd_str = " ".join(cmd)

    while attempt < max_attempts:
        print(f"Executing: {cmd_str}")
        try:
            return subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            if not suppress_warning:
                print("Command failed. Output:")
                print(e.output)

            # these are some spurious errors due to it taking some time before the service
            # account is fully ready
            output = e.output.decode("utf-8")
            if "Service account" in output and "does not exist" in output:
                print(
                    f"Got the following output: {repr(e.output)}, but this looks likely to be a spurious error and so we will the operation."
                )
            else:
                raise

        attempt += 1
        print("Sleeping 10 secs and then will try again...")
        time.sleep(10)

    raise Exception("Too many failed attempts. Aborting")


def gcloud_capturing_stdout(args):
    cmd = ["gcloud"] + args
    cmd_str = " ".join(cmd)
    print(f"Executing: {cmd_str}")

    return subprocess.check_output(cmd, text=True)


def gcloud(args, suppress_warning=False):
    _run_cmd("gcloud", args, suppress_warning=suppress_warning)


def gsutil(args):
    _run_cmd("gsutil", args)


def enable_services(project_id):
    for service in services_to_add:
        gcloud(["services", "enable", service, "--project", project_id])


def grant(service_acct, project_id, role):
    gcloud(
        [
            "projects",
            "add-iam-policy-binding",
            project_id,
            "--member",
            f"serviceAccount:{service_acct}",
            "--role",
            role,
        ]
    )


def create_service_account(service_acct, project_id, key_path):
    creator = gcloud_capturing_stdout(["config", "get", "core/account"])

    gcloud(
        [
            "iam",
            "service-accounts",
            "create",
            service_acct,
            "--project",
            project_id,
            "--display-name",
            f"Service account for sparklespray created by {creator}",
        ]
    )

    for role in roles_to_add:
        grant(f"{service_acct}@{project_id}.iam.gserviceaccount.com", project_id, role)

    gcloud(
        [
            "iam",
            "service-accounts",
            "keys",
            "create",
            key_path,
            "--iam-account",
            f"{service_acct}@{project_id}.iam.gserviceaccount.com",
        ]
    )

    # TODO Add check for access google.api_core.exceptions.Forbidden


#    print("Waiting for a minute for permissions to take effect...")
#    time.sleep(60)


def add_firewall_rule(project_id):
    """Add the sparkles firewall rule in VPC Network of Google Cloud"""
    # Create the FirewallRule object for manipulation easiness
    FirewallRule = namedtuple("FirewallRule", ["name", "protocol", "port"])
    firewall_rule_obj = FirewallRule("sparklespray-monitor", "tcp", 6032)
    protocol_and_port = "{}:{}".format(
        firewall_rule_obj.protocol, firewall_rule_obj.port
    )

    try:
        gcloud(
            [
                "compute",
                "firewall-rules",
                "create",
                firewall_rule_obj.name,
                "--allow",
                protocol_and_port,
                "--project",
                project_id,
            ],
            suppress_warning=True,
        )
    except subprocess.CalledProcessError as e:
        output = e.output.decode("utf8")
        # make sure the error says the resource exists
        assert (
            "The resource" in output and "already exists" in output
        ), "Creating firewall failed: {}".format(output)


def can_reach_datastore_api(project_id, key_path):
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=SCOPES
    )
    client = datastore.Client(project_id, credentials=credentials)
    max_attempts = 50
    for attempt in range(max_attempts):
        try:
            client.get(client.key("invalid", "invalid"))
            return True
        except exceptions.NotFound:
            return False
        except PermissionDenied:
            print(
                "Attempt {} out of {}: Got a permissions denied accessing datastore service with service account -- may just be a delay in permissions being applied. (It can take a few minutes for this to take effect) Retrying in 10 seconds...".format(
                    attempt, max_attempts
                )
            )
            time.sleep(10)
    client.close()
    raise Exception("Failed to confirm access to datastore")


def indices_are_good_enough(existing_indices, new_indices):
    # the format of these two is not exactly the same so canonicalize and compare
    # example format of existing:
    #  gcloud datastore indexes list --project depmap-portal-pipeline       (base)
    # ---
    # ancestor: NONE
    # indexId: CICAgOjXh4EK
    # kind: SparklesV5Task
    # projectId: depmap-portal-pipeline
    # properties:
    # - direction: ASCENDING
    #   name: job_id
    # - direction: ASCENDING
    #   name: last_updated
    # state: READY
    #
    # example format of new_indices:
    # indexes:
    #   # Required for IncrementalTaskFetcher.get_tasks_updated_since()
    #   # Query: job_id = X AND last_updated > Y
    #   - kind: SparklesV5Task
    #     properties:
    #       - name: job_id
    #       - name: last_updated
    return False
    canonicalized_existing = {}
    for record in existing_indices:
        canonicalized_existing[record["kind"]] = [
            x["name"] for x in record["properties"]
        ]

    canonicalized_new = {}
    for record in new_indices["indexes"]:
        canonicalized_new[record["kind"]] = [x["name"] for x in record["properties"]]

    print("existing: ", canonicalized_existing)
    print("new: ", canonicalized_new)

    # don't worry about extra indices, but make sure we have the ones we need
    for kind, columns in canonicalized_new.items():
        if kind not in canonicalized_existing:
            return False
        if canonicalized_existing[kind] != canonicalized_new[kind]:
            return False

    return True


def deploy_datastore_indexes(project_id):
    """Deploy Datastore composite indexes required for sparklespray queries.

    The indexes are defined in index.yaml and are required for queries that
    combine equality and inequality filters on different properties.

    Raises an exception if deployment fails, since the indexes are required
    for sparklespray to function correctly.
    """
    index_yaml_resource = resources.files(sparklespray).joinpath("index.yaml")

    if not index_yaml_resource.is_file():
        raise Exception(
            "index.yaml not found in sparklespray package. "
            "This file is required to deploy Datastore indexes. "
            "Please ensure sparklespray is installed correctly."
        )

    print("Deploying Datastore indexes...")
    # Use as_file() to get a real filesystem path for gcloud command
    with resources.as_file(index_yaml_resource) as index_yaml_path:
        try:
            gcloud(
                [
                    "datastore",
                    "indexes",
                    "create",
                    str(index_yaml_path),
                    "--project",
                    project_id,
                    "--quiet",  # Don't prompt for confirmation
                ]
            )
            print(
                "Datastore indexes deployment initiated. Note: indexes may take several "
                "minutes to build. You can check status at: "
                f"https://console.cloud.google.com/datastore/indexes?project={project_id}"
            )
        except subprocess.CalledProcessError as e:
            output = e.output.decode("utf8") if e.output else ""
            # If indexes already exist, that's fine
            if "already exists" in output or "has already been created" in output:
                print("Datastore indexes already exist.")
            else:
                raise Exception(
                    f"Failed to deploy Datastore indexes: {output}\n"
                    "You can try manually running:\n"
                    f"  gcloud datastore indexes create <path-to-index.yaml> --project {project_id}"
                )

    # # Use as_file() to get a real filesystem path for gcloud command
    # with resources.as_file(index_yaml_resource) as index_yaml_path:
    #     print("Checking to see if we need to recreate indices")
    #     # indexes create is kind of slow, so make sure we really need to do it
    #     existing_indices_yaml = gcloud_capturing_stdout([
    #                 "datastore",
    #                 "indexes",
    #                 "list",
    #                 "--project",
    #                 project_id,
    #             ])
    #     new_indices = yaml.safe_load(index_yaml_path.open("rt"))
    #     existing_indices = yaml.safe_load(io.StringIO(existing_indices_yaml))
    #
    #     if not indices_are_good_enough(existing_indices, new_indices):
    #         print("Deploying Datastore new indexes...")
    #         try:
    #             gcloud(
    #                 [
    #                     "datastore",
    #                     "indexes",
    #                     "create",
    #                     str(index_yaml_path),
    #                     "--project",
    #                     project_id,
    #                     "--quiet",  # Don't prompt for confirmation
    #                 ]
    #             )
    #             print(
    #                 "Datastore indexes deployment initiated. Note: indexes may take several "
    #                 "minutes to build. You can check status at: "
    #                 f"https://console.cloud.google.com/datastore/indexes?project={project_id}"
    #             )
    #         except subprocess.CalledProcessError as e:
    #             output = e.output.decode("utf8") if e.output else ""
    #             # If indexes already exist, that's fine
    #             if "already exists" in output or "has already been created" in output:
    #                 print("Datastore indexes already exist.")
    #             else:
    #                 raise Exception(
    #                     f"Failed to deploy Datastore indexes: {output}\n"
    #                     "You can try manually running:\n"
    #                     f"  gcloud datastore indexes create <path-to-index.yaml> --project {project_id}"
    #                 )


def setup_project(
    project_id: str, key_path: str, bucket_name: str, image_names: List[str]
):
    print("Enabling services for project {}...".format(project_id))
    enable_services(project_id)
    if not os.path.exists(key_path):
        service_acct = "sparkles-" + random_string(10).lower()
        parent = os.path.dirname(key_path)
        if not os.path.exists(parent):
            os.makedirs(parent)
        print(f"Creating service account and writing key to {key_path} ...")
        create_service_account(service_acct, project_id, key_path)
    else:
        print(
            f"Not creating service account because key already exists at {key_path} Delete this and rerun if you wish to create a new service account."
        )

    with open(key_path, "rt") as fd:
        key = json.load(fd)
        service_account_name = key["client_email"]
    print(f"Sparkles jobs will run using the service account {service_account_name}")

    setup_bucket(project_id, key_path, bucket_name)

    # Setup firewall using gcloud function
    print("Adding firewall rule...")
    add_firewall_rule(project_id)

    if not can_reach_datastore_api(project_id, key_path):
        print(
            f"Go to https://console.cloud.google.com/datastore/setup?project={project_id} and "
            f'click "Create a Firestore database". You\'ll need to choose your region and under '
            f'configuration options choose "Firestore with Datastore compatibility". (Not creating the '
            f"database with 'Datastore compatibility' will appear like everything is working, but sparkles will fail "
            f"with cryptic errors when it tries to use the database)"
        )
        input("Hit enter once you've completed the above: ")
        print("checking datastore again..")
        if can_reach_datastore_api(project_id, key_path):
            print("Success!")

    # Deploy Datastore indexes after database is confirmed accessible
    deploy_datastore_indexes(project_id)

    grant_access_to_images(service_account_name, image_names)


def grant_access_to_images(service_account_name, image_names):
    repositories = set()
    for image_name in image_names:
        parsed = parse_docker_image_name(image_name)

        if isinstance(parsed, ArtifactRegistryPath):
            repository = parsed.repository
            if repository in repositories:
                continue

            repositories.add(repository)

            grant_access(service_account_name, parsed)
        else:
            print(
                f"{image_name} does not look like the name of a Google Artifact Registry docker repository. If this is not a public repo, you will get an error when sparkles tries to use this repo."
            )


def grant_access(service_account_name, parsed: ArtifactRegistryPath):
    project_id = parsed.project

    print(
        f"Granting Artifact Registry Reader access on GCP project {project_id} to {service_account_name}"
    )
    gcloud(
        [
            "projects",
            "add-iam-policy-binding",
            project_id,
            f"--member=serviceAccount:{service_account_name}",
            f"--role=roles/artifactregistry.reader",
        ]
    )


def setup_bucket(project_id, service_account_key, bucket_name):
    credentials = service_account.Credentials.from_service_account_file(
        service_account_key, scopes=SCOPES
    )

    client = GSClient(project_id, credentials)
    bucket = client.bucket(bucket_name)

    attempt = 1
    max_attempts = 100
    while True:
        try:
            needs_create = not bucket.exists()
            break
        except (Forbidden, RefreshError):
            attempt += 1
            if attempt > max_attempts:
                raise Exception(
                    "Too many attempts. There's probably something else wrong with permissions"
                )
            print(
                "Attempt {} out of {}: Got a Forbidden except accessing bucket {} with service account -- may just be a delay in permissions being applied. (It can take a few minutes for this to take effect) Retrying in 15 seconds...".format(
                    attempt, max_attempts, bucket_name
                )
            )
            time.sleep(15)

    if needs_create:
        print(f"Bucket {bucket_name} does not exist. Creating...")
        bucket.create()
    else:
        print(f"Found existing bucket named {bucket_name}. Skipping creation")
