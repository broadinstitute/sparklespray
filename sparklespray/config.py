import logging
import os
import sys


from .io import IO
from configparser import ConfigParser
from .cluster_service import Cluster
from .node_req_store import AddNodeReqStore
from .task_store import TaskStore
from .job_store import JobStore
from .job_queue import JobQueue
from google.cloud import datastore
from .util import url_join
from google.oauth2 import service_account
from .txtui import log

SCOPES = [
    "https://www.googleapis.com/auth/genomics",
    "https://www.googleapis.com/auth/cloud-platform",
]


def load_only_config_dict(
    config_file,
    gcloud_config_file="~/.config/gcloud/configurations/config_default",
    verbose=False,
):

    # first load defaults from gcloud config
    gcloud_config_file = os.path.expanduser(gcloud_config_file)
    defaults = {}
    if os.path.exists(gcloud_config_file):
        gcloud_config = ConfigParser()
        gcloud_config.read(gcloud_config_file)
        defaults = dict(
            account=gcloud_config.get("core", "account"),
            project=gcloud_config.get("core", "project"),
            zones=[gcloud_config.get("compute", "zone")],
            region=gcloud_config.get("compute", "region"),
        )
        if verbose:
            print("Using defaults from {}: {}".format(gcloud_config_file, defaults))

    config_file = get_config_path(config_file)
    config_file = os.path.expanduser(config_file)
    log.info("Using config: %s", config_file)
    if verbose:
        print("Using config: {}".format(config_file))

    config = ConfigParser()
    config.read(config_file)
    config_from_file = dict(config.items("config"))
    if "zones" in config_from_file:
        config_from_file["zones"] = [
            x.strip() for x in config_from_file["zones"].split(",")
        ]

    merged_config = dict(defaults)
    merged_config.update(config_from_file)
    merged_config["sparkles_config_path"] = config_file

    for unused_property in ["default_resource_cpu", "default_resource_memory"]:
        if unused_property in merged_config:
            log.warning(
                "'%s' in config file but no longer used. Use 'machine_type' instead",
                unused_property,
            )

    missing_values = []
    required_properties = [
        "default_url_prefix",
        "project",
        "default_image",
        "machine_type",
        "zones",
        "region",
        "account",
    ]
    for property in required_properties:
        if (
            property not in merged_config
            or merged_config[property] == ""
            or merged_config[property] is None
        ):
            missing_values.append(property)

    if len(missing_values) > 0:
        print(
            "Missing the following parameters in {}: {}".format(
                config_file, ", ".join(missing_values)
            )
        )
        sys.exit(1)

    if "kubequeconsume_exe_path" not in merged_config:
        merged_config["kubequeconsume_exe_path"] = os.path.join(
            os.path.dirname(__file__), "bin/kubequeconsume"
        )
        assert os.path.exists(merged_config["kubequeconsume_exe_path"])

    if "cas_url_prefix" not in merged_config:
        merged_config["cas_url_prefix"] = merged_config["default_url_prefix"] + "/CAS/"

    assert isinstance(merged_config["zones"], list)

    project_id = merged_config["project"]
    service_account_key = os.path.expanduser(
        merged_config.get(
            "service_account_key", f"~/.sparkles-cache/service-keys/{project_id}.json"
        )
    )
    merged_config["service_account_key"] = service_account_key

    return merged_config


def load_config(config_file):
    merged_config = load_only_config_dict(config_file)
    service_account_key = merged_config["service_account_key"]
    if not os.path.exists(service_account_key):
        raise Exception("Could not find service account key at %s", service_account_key)

    merged_config[
        "credentials"
    ] = service_account.Credentials.from_service_account_file(
        service_account_key, scopes=SCOPES
    )

    jq, io, cluster = load_config_from_dict(merged_config)
    return merged_config, jq, io, cluster


def load_config_from_dict(config):
    allowed_parameters = set(
        [
            "default_image",
            "machine_type",
            "cas_url_prefix",
            "default_url_prefix",
            "kubequeconsume_exe_path",
            "project",
            "zones",
            "region",
            "account",
            "service_account_key",
            "credentials",
            "bootdisksizegb",  # deprecated name, replaced with boot_volume_in_gb
            "boot_volume_in_gb",
            "preemptible",
            "local_work_dir",
            "gpu_count",
            "mount",
            "sparkles_config_path",
        ]
    )
    unknown_parameters = set(config.keys()).difference(allowed_parameters)
    assert (
        len(unknown_parameters) == 0
    ), "The following parameters in config are unrecognized: {}".format(
        ", ".join(unknown_parameters)
    )

    credentials = config["credentials"]
    project_id = config["project"]
    io = IO(project_id, config["cas_url_prefix"], credentials)

    client = datastore.Client(project_id, credentials=credentials)
    job_store = JobStore(client)
    task_store = TaskStore(client)
    jq = JobQueue(client, job_store, task_store)

    node_req_store = AddNodeReqStore(client)
    debug_log_prefix = config.get(
        "debug_log_prefix", url_join(config["default_url_prefix"], "node-logs")
    )
    cluster = Cluster(
        config["project"],
        config["zones"],
        node_req_store=node_req_store,
        job_store=job_store,
        task_store=task_store,
        client=client,
        debug_log_prefix=debug_log_prefix,
        credentials=credentials,
    )

    return jq, io, cluster


def get_config_path(config_path):
    if config_path is not None:
        if not os.path.exists(config_path):
            raise Exception("Could not find config at {}".format(config_path))
        return config_path
    else:

        def possible_config_names():
            for config_name in [".sparkles", ".kubeque"]:
                dirname = os.path.abspath(".")
                while True:
                    yield os.path.join(dirname, config_name)
                    next_dirname = os.path.dirname(dirname)
                    if dirname == next_dirname:
                        break
                    dirname = next_dirname
            return

        checked_paths = []
        for config_path in possible_config_names():
            if os.path.exists(config_path):
                return config_path
            checked_paths.append(config_path)

        raise Exception(
            "Could not find config file at any of locations: {}".format(checked_paths)
        )
