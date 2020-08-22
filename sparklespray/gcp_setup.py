import subprocess
import os
from sparklespray.util import random_string
import re
import sys
from google.cloud import datastore
from google.api_core import exceptions
from google.cloud.storage.client import Client as GSClient
from sparklespray.config import SCOPES
from google.oauth2 import service_account
import google.api_core.exceptions
import time
from collections import namedtuple
from google.api_core.exceptions import PermissionDenied


services_to_add = [  # "storage.googleapis.com",
    "datastore.googleapis.com",
    "storage-component.googleapis.com",
    "genomics.googleapis.com",
    "pubsub.googleapis.com",
    "storage-api.googleapis.com",
    "compute.googleapis.com",
    "containerregistry.googleapis.com",
]

roles_to_add = [
    "roles/owner",  # Eventually drop this
    "roles/compute.admin",
    "roles/datastore.user",
    "roles/genomics.pipelinesRunner",
    "roles/pubsub.editor",
    "roles/storage.admin",
]


def _run_cmd(cmd, args, suppress_warning=False):
    cmd = [cmd] + args
    cmd_str = " ".join(cmd)
    print(f"Executing: {cmd_str}")

    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        if not suppress_warning:
            print("Command failed. Output:")
            print(e.output.decode("utf8"))
        raise


def gcloud(args, suppress_warning=False):
    _run_cmd("gcloud", args, suppress_warning=suppress_warning)


def gsutil(args):
    _run_cmd("gsutil", args)


def enable_services(project_id):
    for service in services_to_add:
        gcloud(["services", "enable", service, "--project", project_id])


def create_service_account(service_acct, project_id, key_path):
    gcloud(
        [
            "iam",
            "service-accounts",
            "create",
            service_acct,
            "--project",
            project_id,
            "--display-name",
            "Service account for sparklespray",
        ]
    )

    for role in roles_to_add:
        gcloud(
            [
                "projects",
                "add-iam-policy-binding",
                project_id,
                "--member",
                f"serviceAccount:{service_acct}@{project_id}.iam.gserviceaccount.com",
                "--role",
                role,
            ]
        )

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

    def error_callback(error):
        # If we have an error, we should try to check if rule already exists. If yes, we are all set.
        # Assuming if the rule 'sparkles-monitor' exists, we are ok.
        # TODO: Check the rule is on port 6032 with protocol tcp
        # If no, we should stop here and let the user debug
        gcloud_command = [
            "compute",
            "firewall-rules",
            "describe",
            firewall_rule_obj.name,
            "--project",
            project_id,
        ]
        gcloud(gcloud_command)
        print("Firewall rule seems already set. Ignoring.")

    #
    # try:
    #    gcloud(['compute', 'firewall-rules', 'describe', firewall_rule_obj.name])
    # except subprocess.CalledProcessError as e:

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
            time.sleep(20)
    raise Exception("Failed to confirm access to datastore")


def setup_project(
    project_id,
    key_path,
    bucket_name,
    helper_image_name,
    helper_exe,
    gcsfuse_exe,
    setup_account,
    setup_firewall,
    setup_docker_image,
):
    if setup_account:
        print("Enabling services for project {}...".format(project_id))
        enable_services(project_id)
        service_acct = "sparkles-" + random_string(10).lower()
        if not os.path.exists(key_path):
            parent = os.path.dirname(key_path)
            if not os.path.exists(parent):
                os.makedirs(parent)
            print(f"Creating service account and writing key to {key_path} ...")
            create_service_account(service_acct, project_id, key_path)
        else:
            print(
                f"Not creating service account because key already exists at {key_path} Delete this and rerun if you wish to create a new service account."
            )

        setup_bucket(project_id, key_path, bucket_name)

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

        print(
            "Using gcloud to setup google authentication with Google Container Registry"
        )
        gcloud(["auth", "configure-docker"])

    if setup_firewall:
        # Setup firewall using gcloud function
        print("Adding firewall rule...")
        add_firewall_rule(project_id)

    if setup_docker_image:
        print("Building docker image used for setting up VMs")
        prepare_helper_docker_image(helper_image_name, helper_exe, gcsfuse_exe)


def prepare_helper_docker_image(helper_image_name, helper_exe, gcsfuse_exe):
    import tempfile
    import shutil

    with tempfile.TemporaryDirectory("sparkles-helper-build") as tmpdir:
        shutil.copy(helper_exe, os.path.join(tmpdir, "sparkles-helper"))
        shutil.copy(gcsfuse_exe, os.path.join(tmpdir, "gcsfuse_0.30.0_amd64.deb"))
        with open(os.path.join(tmpdir, "Dockerfile"), "wt") as fd:
            fd.write(
                """
FROM ubuntu:18.04
RUN apt-get update && apt-get install -y ca-certificates fuse libfuse2
RUN mkdir /sparkles
COPY sparkles-helper /sparkles
COPY gcsfuse_0.30.0_amd64.deb /sparkles
RUN apt -y install /sparkles/gcsfuse_0.30.0_amd64.deb
            """
            )
        build_output = _run_cmd(
            "docker", ["build", "-t", helper_image_name, str(tmpdir)]
        )
        print(build_output)
        _run_cmd("docker", ["push", helper_image_name])


def setup_bucket(project_id, service_account_key, bucket_name):
    credentials = service_account.Credentials.from_service_account_file(
        service_account_key, scopes=SCOPES
    )

    client = GSClient(project_id, credentials)
    bucket = client.bucket(bucket_name)
    needs_create = not bucket.exists()

    if needs_create:
        bucket.create()
