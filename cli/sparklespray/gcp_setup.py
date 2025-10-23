import subprocess
import os
from sparklespray.util import random_string
from google.cloud import datastore
from google.api_core import exceptions
from google.cloud.storage.client import Client as GSClient
from sparklespray.config import SCOPES
from google.oauth2 import service_account
import time
from collections import namedtuple
from google.api_core.exceptions import PermissionDenied, Forbidden
from google.auth.exceptions import RefreshError
import json
import re

REPO_NAME_PATTERN = "(?P<repo_name>[^/]+/[^/]+)/.*$"

ARTIFACT_REGISTRY_REPO_PATTERN = (
    "^[a-z0-9-]+-docker.pkg.dev/(?P<project_id>[a-z0-9-]+)$"
)

services_to_add = [  # "storage.googleapis.com",
    "datastore.googleapis.com",
    "storage-component.googleapis.com",
    "pubsub.googleapis.com",
    "storage-api.googleapis.com",
    "compute.googleapis.com",
    "containerregistry.googleapis.com",
    "logging",
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
    raise Exception("Failed to confirm access to datastore")


def setup_project(project_id: str, key_path: str, bucket_name: str, image_names: str):
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
            'Go to https://console.cloud.google.com/datastore/setup?project={} to choose where to store your data will reside in and then set up will be complete. Select "Cloud Datastore" and then select a region close to you, and then "Create database".'.format(
                project_id
            )
        )
        input("Hit enter once you've completed the above: ")
        print("checking datastore again..")
        if can_reach_datastore_api(project_id, key_path):
            print("Success!")

    grant_access_to_images(service_account_name, image_names)


def grant_access_to_images(service_account_name, image_names):
    repositories = set()
    for image_name in image_names:

        repository = get_repository_name(image_name)
        if repository in repositories:
            continue

        repositories.add(repository)

        if looks_like_artifactory_repository(repository):
            grant_access(service_account_name, repository)
        else:
            print(
                f"{repository} does not look like the name of a Google Artifact Registry docker repository. If this is not a public repo, you will get an error when sparkles tries to use this repo."
            )


def get_repository_name(image_name):
    m = re.match(REPO_NAME_PATTERN, image_name)
    assert m is not None, f"Could not parse image name {image_name}"
    return m.group("repo_name")


def looks_like_artifactory_repository(repository):
    return re.match(ARTIFACT_REGISTRY_REPO_PATTERN, repository) is not None


def grant_access(service_account_name, repository):
    m = re.match(ARTIFACT_REGISTRY_REPO_PATTERN, repository)
    assert m is not None
    project_id = m.group("project_id")

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
