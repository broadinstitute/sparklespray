import logging
import os
import sys

from .model import PersistentDiskMount, LOCAL_SSD
from .io_helper import IO
from configparser import RawConfigParser, NoSectionError, NoOptionError
from .cluster_service import Cluster
from .node_req_store import AddNodeReqStore
from .task_store import TaskStore
from .job_store import JobStore
from .job_queue import JobQueue
from google.cloud import datastore
from .util import url_join
from google.oauth2 import service_account
from .txtui import log
from typing import List, Optional, Tuple
import dataclasses
from google.auth.credentials import Credentials


class BadConfig(Exception):
    pass

class MissingRequired(BadConfig):
    pass

class UnknownParameters(BadConfig):
    pass

SCOPES = [
    "https://www.googleapis.com/auth/genomics",
    "https://www.googleapis.com/auth/cloud-platform",
]


from dataclasses import dataclass


@dataclass
class PrepConfig:
    sparkles_config_path: Optional[str] = None
    default_image: Optional[str] = None
    machine_type: Optional[str] = None
    cas_url_prefix: Optional[str] = None
    default_url_prefix: Optional[str] = None
    kubequeconsume_exe_path: Optional[str] = None
    project: Optional[str] = None
    zones: Optional[List[str]] = None
    region: Optional[str] = None
    account: Optional[str] = None
    service_account_key: Optional[str] = None
    credentials: Optional[str] = None
    boot_volume_in_gb: Optional[int] = None
    local_work_dir: Optional[str] = None
    max_preemptable_attempts_scale: Optional[int] = None
    mounts: Optional[List[PersistentDiskMount]] = None
    debug_log_prefix: Optional[str] = None
    monitor_port: Optional[int] = None
    work_root_dir: Optional[str] = None
    cache_db_path: Optional[str] = None


@dataclass
class Config:
    sparkles_config_path: str
    default_image: str
    machine_type: str
    cas_url_prefix: str
    default_url_prefix: str
    kubequeconsume_exe_path: str
    project: str
    zones: List[str]
    region: str
    account: str
    service_account_key: str
    boot_volume_in_gb: int
    local_work_dir: str
    max_preemptable_attempts_scale: int
    mounts: List[PersistentDiskMount]
    debug_log_prefix: str
    work_root_dir : str
    monitor_port: int
    cache_db_path : str
    credentials: Credentials = dataclasses.field(repr=False)

class ConfigAdapter:
    def __init__(self):
        pass

    #        self.config

    def _safe_get(config, section, key, default=None):
        try:
            return config.get(section, key)
        except NoSectionError:
            return default
        except NoOptionError:
            return default


NO_DEFAULT = object()


@dataclass
class GCloudConfig:
    zones: List[str] = dataclasses.field(default_factory=list)
    account: Optional[str] = None
    region: Optional[str] = None
    project: Optional[str] = None


def _safe_get(config, section, key, default=None):
    try:
        return config.get(section, key)
    except NoSectionError:
        return default
    except NoOptionError:
        return default


def _parse_gcloud_config(gcloud_config_file: str, verbose: bool) -> GCloudConfig:
    gcloud_config = RawConfigParser()
    gcloud_config.read(gcloud_config_file)
    zone = _safe_get(gcloud_config, "compute", "zone")
    zones = []
    if zone:
        zones.append(zone)
    account = _safe_get(gcloud_config, "core", "account")
    project = _safe_get(gcloud_config, "core", "project")
    region = _safe_get(gcloud_config, "compute", "region")

    config = GCloudConfig(zones=zones, account=account, region=region, project=project)
    if verbose:
        print("Using defaults from {}: {}".format(gcloud_config_file, config))
    return config


def load_config(
    config_file: str,
    gcloud_config_file: str = "~/.config/gcloud/configurations/config_default",
    verbose: bool = False,
) -> Config:

    # first load defaults from gcloud config
    gcloud_config_file = os.path.expanduser(gcloud_config_file)
    if os.path.exists(gcloud_config_file):
        gcloud_config = _parse_gcloud_config(gcloud_config_file, verbose)
    else:
        gcloud_config = GCloudConfig()

    config_file = get_config_path(config_file)
    config_file = os.path.expanduser(config_file)
    log.info("Using config: %s", config_file)
    if verbose:
        print("Using config: {}".format(config_file))

    config_parser = RawConfigParser()
    config_parser.read(config_file)
    config_dict = dict(config_parser.items("config"))
    config_used = set()

    def consume(name, default=NO_DEFAULT, parser=str):
        assert name not in config_used, f"Consumed {name} twice"
        config_used.add(name)

        if name in config_dict:
            value = parser(config_dict[name])
        else:
            if default is NO_DEFAULT:
                raise BadConfig(f"Missing {name} in config")
            else:
                value = default
        return value

    config = PrepConfig()
    config.zones = consume(
        "zones",
        default=gcloud_config.zones,
        parser=lambda value: [x.strip() for x in value.split(",")],
    )
    config.sparkles_config_path = config_file
    config.monitor_port = consume("monitor_port", 6032, int)
    config.work_root_dir=consume("work_root_dir", "/mnt/")
    for unused_property in ["default_resource_cpu", "default_resource_memory"]:
        if unused_property in config_dict:
            raise BadConfig(
                "'{unused_property}' in config file but no longer used. Use 'machine_type' instead"
            )

    missing_values = []
    required_properties = [
        "default_url_prefix",
        "project",
        "default_image",
        "machine_type",
        "region",
        "account",
    ]
    for property in required_properties:
        value = None
        if property in config_dict:
            value = consume(property)
        else:
            if hasattr(gcloud_config, property):
                value = getattr(gcloud_config, property)

        if value == "" or value is None:
            missing_values.append(property)
        
        setattr(config, property, value)

    if len(missing_values) > 0:
        raise MissingRequired(
            f"Missing the following required parameters in {config_file}: {', '.join(missing_values)}"
        )

    config.kubequeconsume_exe_path = consume(
        "kubequeconsume_exe_path",
        os.path.join(os.path.dirname(__file__), "bin/kubequeconsume"),
    )
    assert config.default_url_prefix is not None
    config.cas_url_prefix = consume(
        "cas_url_prefix", config.default_url_prefix + "/CAS/"
    )

    config.boot_volume_in_gb = consume("boot_volume_in_gb", 20, int)

    assert isinstance(config.zones, list)

    config.service_account_key = consume(
        "service_account_key",
        default=os.path.expanduser(
            f"~/.sparkles-cache/service-keys/{config.project}.json",
        ),
    )

    config.max_preemptable_attempts_scale = consume(
        "max_preemptable_attempts_scale", 2, int
    )

    #    config.
    #    allowed_parameters = set(
    #        [
    #            "default_image",
    #            "machine_type",
    #            "cas_url_prefix",
    #            "default_url_prefix",
    #            "kubequeconsume_exe_path",
    #            "project",
    #            "zones",
    #            "region",
    #            "account",
    #            "service_account_key",
    #            "credentials",
    #            "bootdisksizegb",  # deprecated name, replaced with boot_volume_in_gb
    #            "boot_volume_in_gb",
    #            "preemptible",
    #            "local_work_dir",
    #            "mount",
    #            "sparkles_config_path",
    #            "mount_count",
    #            "pd_volume_in_gb",
    #            "max_preemptable_attempts_scale"
    #        ]
    #    )

    mount_count = consume("mount_count", 1, int)
    mounts = []
    for i in range(mount_count):
        if i == 0:
            path = consume(f"mount_{i+1}_path", "/mnt")
            type = consume(f"mount_{i+1}_type", LOCAL_SSD)
        else:
            path = consume(f"mount_{i+1}_path")
            type = consume(f"mount_{i+1}_type")
        name = consume(f"mount_{i+1}_name", None)
        size_in_gb = consume(f"mount_{i+1}_size_in_gb", 100, int)
        mounts.append(
            PersistentDiskMount(path=path, type=type, name=name, size_in_gb=size_in_gb)
        )

    # TODO: Add validation that no two mounts have the same path and that workdir lines up with at least one mount

    config.mounts = mounts
    config.cache_db_path = consume("cache_db_path", ".kubeque-cached-file-hashes")
    config.debug_log_prefix = consume(
        "debug_log_prefix", url_join(config.default_url_prefix, "node-logs")
    )

    unknown_parameters = set(config_dict.keys()).difference(config_used)
    if len(unknown_parameters) != 0:
        raise UnknownParameters(
            "The following parameters in config are unrecognized: {}".format(
                ", ".join(unknown_parameters)
            )
        )

    return Config(**dataclasses.asdict(config))


def create_services(config_file: str) -> Tuple[Config, JobQueue, IO, Cluster]:
    config = load_config(config_file)
    service_account_key = config.service_account_key
    if not os.path.exists(service_account_key):
        raise Exception("Could not find service account key at %s", service_account_key)

    config.credentials = service_account.Credentials.from_service_account_file(
        service_account_key, scopes=SCOPES
    )

    jq, io, cluster = create_services_from_config(config)
    return config, jq, io, cluster


def create_services_from_config(config: Config):
    credentials = config.credentials
    project_id = config.project
    io = IO(project_id, config.cas_url_prefix, credentials)

    client = datastore.Client(project_id, credentials=credentials)
    job_store = JobStore(client)
    task_store = TaskStore(client)
    jq = JobQueue(client, job_store, task_store)

    node_req_store = AddNodeReqStore(client)
    debug_log_prefix = config.debug_log_prefix

    cluster = Cluster(
        config.project,
        config.zones,
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
