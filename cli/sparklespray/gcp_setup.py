import subprocess
import os
import tempfile
from contextlib import contextmanager
from importlib import resources
import sys
import certifi
import shutil
from .config import get_default_key_path

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
from dataclasses import dataclass
from typing import Optional
from sparklespray import __version__


@dataclass
class SetupOptions:
    project: str
    region: str
    worker_dockerfile_path: str
    dry_run: bool
    url_prefix: Optional[str]
    service_account: Optional[str]
    worker_docker_image: Optional[str]
    images_for_jobs: list[str]
    write_config: Optional[str] = None


services_to_add = [
    "datastore.googleapis.com",
    "firestore.googleapis.com",
    "storage-component.googleapis.com",
    "storage-api.googleapis.com",
    "pubsub.googleapis.com",
    "compute.googleapis.com",
    "artifactregistry.googleapis.com",
    "logging",
    "batch.googleapis.com",
    "iamcredentials.googleapis.com",
]

roles_to_add = [
    "roles/datastore.user",
    "roles/storage.objectAdmin",
    "roles/pubsub.editor",
    "roles/compute.admin",
    "roles/artifactregistry.createOnPushWriter",
    "roles/batch.admin",
    "roles/editor",
]


def _run_cmd(
    cmd,
    args,
    suppress_warning=False,
    max_attempts=10,
    success_if_output_contains=None,
    dry_run=False,
    env=None,
):
    attempt = 0

    cmd = [cmd] + args
    cmd_str = " ".join(cmd)

    if dry_run:
        print(f"[dry run] {cmd_str}")
        return

    if env is None:
        env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    while attempt < max_attempts:
        print(f"Executing: {cmd_str}")
        try:
            return subprocess.check_output(
                cmd,
                stderr=subprocess.STDOUT,
                text=True,
                stdin=subprocess.DEVNULL,
                env=env,
            )
        except subprocess.CalledProcessError as e:
            if success_if_output_contains is not None:
                if success_if_output_contains in e.output:
                    return
            if not suppress_warning:
                print("Command failed. Output:")
                print(e.output)

            # these are some spurious errors due to it taking some time before the service
            # account is fully ready
            output = e.output
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


def gcloud(
    args: list[str],
    dry_run: bool,
    *,
    suppress_warning=False,
    success_if_output_contains=None,
):
    if dry_run:
        print(f"[dry run] gcloud {' '.join(args)}")
    else:
        _run_cmd(
            "gcloud",
            args,
            suppress_warning=suppress_warning,
            success_if_output_contains=success_if_output_contains,
        )


def enable_services(project_id: str, dry_run: bool):
    for service in services_to_add:
        gcloud(["services", "enable", service, "--project", project_id], dry_run)


def grant(service_acct, project_id, role, dry_run):
    gcloud(
        [
            "projects",
            "add-iam-policy-binding",
            project_id,
            "--member",
            f"serviceAccount:{service_acct}",
            "--role",
            role,
        ],
        dry_run,
    )


def create_service_account(project_id, service_acct, dry_run):
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
        ],
        dry_run,
        success_if_output_contains="already exists within project",
    )

    return f"{service_acct}@{project_id}.iam.gserviceaccount.com"


@contextmanager
def temp_access_key_file(project_id: str, service_acct: str, dry_run):
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        key_path = f.name
    key_id = None
    try:
        create_service_account_key(service_acct, key_path, dry_run)
        if dry_run:
            key_id = "fake_id"
        else:
            with open(key_path) as f:
                key_id = json.load(f)["private_key_id"]
        yield key_path
    finally:
        os.unlink(key_path)
        if key_id is not None:
            gcloud(
                [
                    "iam",
                    "service-accounts",
                    "keys",
                    "delete",
                    key_id,
                    "--iam-account",
                    service_acct,
                    "--project",
                    project_id,
                    "--quiet",
                ],
                dry_run,
            )


def create_service_account_key(service_account, key_path, dry_run):
    print(f"Creating access key for {service_account} and storing at {key_path}")
    gcloud(
        [
            "iam",
            "service-accounts",
            "keys",
            "create",
            key_path,
            "--iam-account",
            service_account,
        ],
        dry_run,
    )


#    print("Waiting for a minute for permissions to take effect...")
#    time.sleep(60)


def setup_firewall(project_id, dry_run):
    """Add the sparkles firewall rule in VPC Network of Google Cloud"""
    print("Adding firewall rule used to get live log stream...")

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
            dry_run,
            suppress_warning=True,
        )
    except subprocess.CalledProcessError as e:
        output = e.output
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


def setup_datastore(project_id: str, region: str, dry_run: bool):
    gcloud(
        [
            "firestore",
            "databases",
            "create",
            "--type=datastore-mode",
            f"--location={region}",
            f"--project={project_id}",
        ],
        dry_run,
        success_if_output_contains="Database already exists",
    )
    gcloud(
        [
            "datastore",
            "indexes",
            "create",
            os.path.join(os.path.dirname(__file__), "index.yaml"),
            f"--project={project_id}",
        ],
        dry_run,
    )


def setup_project(options: SetupOptions):
    # setup only runs commands but does not directly use google cloud APIs. This is largely
    # because we want the operations to occur using the user's gcloud credentials. Also, it
    # has been observed that making datastore API requests results in weird hangs in subsequent
    # calls to subprocess.run. Not fully understood why, but to work around, put any cloud API
    # calls into a separate script and run via subprocess to keep processes insulated from one
    # another.
    project_id = options.project
    dry_run = options.dry_run
    region = options.region

    print(f"Enabling services for project {project_id}...")
    enable_services(project_id, dry_run)

    project_settings = get_sparkles_project_settings(project_id)

    # ensure service account in place
    key_path = None
    if options.service_account is None:
        # check to see if we have this set from a previous setup
        service_account = project_settings.get("service_account")
        if service_account is None:
            service_account_name = "sparkles-" + random_string(10).lower()
            print(f"No service_account specified, creating {service_account_name}...")
            service_account = create_service_account(
                project_id, service_account_name, dry_run
            )
            key_path = get_default_key_path(project_id)
            create_service_account_key(service_account, key_path, dry_run)
    else:
        service_account = options.service_account

    # ensure default url is valid
    if options.url_prefix is None:
        url_prefix = project_settings.get("url_prefix")
        if url_prefix is None:
            bucket = "sparkles-" + random_string(10).lower()
            url_prefix = f"gs://{bucket}/"
            print(f"No url prefix specified, creating bucket {bucket}")
            create_bucket(project_id, bucket, region, dry_run)
    else:
        url_prefix = options.url_prefix

    if options.worker_docker_image is None:
        worker_docker_image = project_settings.get("worker_docker_image")
        if worker_docker_image is None:
            repo_name = f"{region}-docker.pkg.dev/{project_id}/sparkles"
            create_artifact_registry_docker_repo(
                project_id, region, "sparkles", dry_run
            )
            worker_docker_image = f"{repo_name}/sparklesworker:{__version__}"
            build_and_push_image(
                worker_docker_image, options.worker_dockerfile_path, dry_run
            )
    else:
        worker_docker_image = options.worker_docker_image

    dashboard_user_service_account = project_settings.get(
        "dashboard_user_service_account"
    )
    if dashboard_user_service_account is None:
        dashboard_user_service_account = setup_dashboard_user_service_account(
            project_id, dry_run
        )

    setup_pubsub_topics(project_id, dry_run)
    setup_datastore(project_id, region, dry_run)
    _run_cmd(
        sys.executable,
        [
            "-m",
            "sparklespray.datastore_helper",
            "wait-for-indexes",
            "--project",
            project_id,
        ],
        dry_run=dry_run,
        env=_datastore_helper_env(),
    )
    setup_firewall(project_id, dry_run)

    store_sparkles_project_settings(
        project_id,
        dict(
            url_prefix=url_prefix,
            default_region=region,
            worker_docker_image=worker_docker_image,
            dashboard_user_service_account=dashboard_user_service_account,
            service_account=service_account,
        ),
        dry_run,
    )

    # perform necessary grants
    grant_access_to_images(service_account, options.images_for_jobs, dry_run)
    for role in roles_to_add:
        grant(service_account, project_id, role, dry_run)
    grant(
        dashboard_user_service_account, project_id, "roles/pubsub.subscriber", dry_run
    )
    # allow `service_account` to impersonate `dashboard_user_service_account`
    gcloud(
        [
            "iam",
            "service-accounts",
            "add-iam-policy-binding",
            dashboard_user_service_account,
            f"--member=serviceAccount:{service_account}",
            "--role=roles/iam.serviceAccountTokenCreator",
            f"--project={project_id}",
        ],
        dry_run,
    )
    # allow `service_account` to attach itself as the identity for Batch job VMs
    gcloud(
        [
            "iam",
            "service-accounts",
            "add-iam-policy-binding",
            service_account,
            f"--member=serviceAccount:{service_account}",
            "--role=roles/iam.serviceAccountUser",
            f"--project={project_id}",
        ],
        dry_run,
    )

    def run_verify(key_file):
        print("Verifying permissions and services are set up correctly")
        _run_cmd(
            sys.executable,
            [
                "-m",
                "sparklespray.gcp_verify",
                "--keyfile",
                key_file,
                "--project",
                project_id,
                "--url-prefix",
                url_prefix,
                "--dashboard-user-service-account",
                dashboard_user_service_account,
            ],
            dry_run,
        )

    if key_path is None:
        with temp_access_key_file(
            project_id, service_account, dry_run
        ) as temp_key_file:
            run_verify(temp_key_file)
    else:
        run_verify(key_path)

    if options.write_config:
        _write_sparkles_config(
            options.write_config,
            project_id,
            region,
            url_prefix,
            worker_docker_image,
            dry_run,
        )


def _write_sparkles_config(
    path: str,
    project_id: str,
    region: str,
    url_prefix: str,
    worker_docker_image: str,
    dry_run: bool,
):
    content = (
        "[config]\n"
        f"default_url_prefix={url_prefix}\n"
        f"project={project_id}\n"
        f"region={region}\n"
        "machine_type=n2-standard-2\n"
        f"sparklesworker_image={worker_docker_image}\n"
    )
    if dry_run:
        print(f"[dry run] Would write config to {path}:\n{content}")
    else:
        with open(path, "w") as f:
            f.write(content)
        print(f"Wrote sparkles config to {path}")


def build_and_push_image(worker_docker_image, worker_dockerfile_path, dry_run):
    if dry_run:
        print(
            f"[dry run] Skipping build in {worker_dockerfile_path} and push of {worker_docker_image}"
        )
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            # make a copy of the go code and docker file
            go_code_dir = os.path.join(tmpdir, "staged")
            shutil.copytree(worker_dockerfile_path, go_code_dir)
            create_embedded_cert_bundle(worker_dockerfile_path)
            subprocess.run(
                ["docker", "build", ".", "-t", worker_docker_image],
                cwd=go_code_dir,
                check=True,
            )
        subprocess.run(["docker", "push", worker_docker_image], check=True)


def create_embedded_cert_bundle(go_root):
    cert_go_path = os.path.join(
        go_root, "src/github.com/broadinstitute/sparklesworker/certs.go"
    )
    assert os.path.exists(cert_go_path), f"Expected path to exist: {cert_go_path}"
    with open(cert_go_path, "wt") as o:
        # write out the
        with open(certifi.where()) as fd:
            o.write(
                f"// autogenerated from create_cert_bundle() in gcp_setup.py using {certifi.__version__}\n"
            )
            o.write(
                """
package sparklesworker

const pemCerts = `"""
            )
            o.write(fd.read())
            o.write("`\n")


def create_artifact_registry_docker_repo(project_id, region, repo_name, dry_run):
    gcloud(
        [
            "artifacts",
            "repositories",
            "create",
            repo_name,
            "--repository-format=docker",
            f"--location={region}",
            f"--project={project_id}",
        ],
        dry_run,
        success_if_output_contains="ALREADY_EXISTS",
    )


def _get_gcloud_access_token() -> str:
    return subprocess.check_output(
        ["gcloud", "auth", "print-access-token"], text=True
    ).strip()


def _datastore_helper_env() -> dict:
    env = os.environ.copy()
    env["GCP_ACCESS_TOKEN"] = _get_gcloud_access_token()
    return env


def get_sparkles_project_settings(project_id):
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        out_path = f.name
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "sparklespray.datastore_helper",
                "get",
                "--project",
                project_id,
                "--out",
                out_path,
            ],
            capture_output=True,
            text=True,
            env=_datastore_helper_env(),
        )
        if result.returncode != 0:
            # Entity may not exist yet (first run)
            return {}
        with open(out_path) as f:
            return json.load(f)
    finally:
        os.unlink(out_path)


def store_sparkles_project_settings(project_id, settings, dry_run):
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
        json.dump(settings, f)
        in_path = f.name
    try:
        _run_cmd(
            sys.executable,
            [
                "-m",
                "sparklespray.datastore_helper",
                "set",
                "--project",
                project_id,
                "--in",
                in_path,
            ],
            dry_run=dry_run,
            env=_datastore_helper_env(),
        )
    finally:
        os.unlink(in_path)


def create_bucket(project_id, bucket, region, dry_run):
    print(f"Creating {region} regional bucket {bucket}...")
    gcloud(
        [
            "storage",
            "buckets",
            "create",
            f"gs://{bucket}",
            f"--location={region}",
            f"--project={project_id}",
        ],
        dry_run,
    )


def setup_dashboard_user_service_account(project_id: str, dry_run: bool):
    return create_service_account(project_id, "sparkles-dashboard-user", dry_run)


def setup_pubsub_topics(project_id: str, dry_run: bool):
    for topic in ["sparkles-task-in", "sparkles-task-out", "sparkles-events"]:
        gcloud(
            ["pubsub", "topics", "create", topic, f"--project={project_id}"],
            dry_run,
            success_if_output_contains="Resource already exists in the project",
        )


def grant_access_to_images(service_account_name, image_names, dry_run):
    repositories = set()
    for image_name in image_names:
        parsed = parse_docker_image_name(image_name)

        if isinstance(parsed, ArtifactRegistryPath):
            repository = parsed.repository
            if repository in repositories:
                continue

            repositories.add(repository)

            grant_access(service_account_name, parsed, dry_run)
        else:
            print(
                f"{image_name} does not look like the name of a Google Artifact Registry docker repository. If this is not a public repo, you will get an error when sparkles tries to use this repo."
            )


def grant_access(service_account_name, parsed: ArtifactRegistryPath, dry_run):
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
        ],
        dry_run,
    )
