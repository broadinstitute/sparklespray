import os

from .model import (
    PersistentDiskMount,
    LOCAL_SSD,
    ExistingDiskMount,
    DiskMountT,
    MachineSpec,
)
from .io_helper import IO
from configparser import RawConfigParser, NoSectionError, NoOptionError
from .cluster_service import Cluster
from .task_store import TaskStore
from .job_store import JobStore
from .job_queue import JobQueue
from google.cloud import datastore
from .util import url_join
from google.oauth2 import service_account
from .txtui import log
from typing import List, Optional, Tuple, Dict
import dataclasses
from google.auth.credentials import Credentials
from typing import Callable


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
    mounts: Optional[List[DiskMountT]] = None
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
    mounts: List[DiskMountT]
    debug_log_prefix: str
    work_root_dir: str
    monitor_port: int
    cache_db_path: str
    credentials: Credentials = dataclasses.field(repr=False)

    def create_machine_specs(self):
        return MachineSpec(
            service_account_email=self.credentials.service_account_email,  # type: ignore
            boot_volume_in_gb=self.boot_volume_in_gb,
            mounts=self.mounts,
            work_root_dir=self.work_root_dir,
            machine_type=self.machine_type,
        )
    
    @property
    def location(self):
        print("Warning: hardcoded location")
        return "us-central1"


class NoDefault:
    pass


NO_DEFAULT = NoDefault()


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


from typing import TypeVar, Union

T = TypeVar("T")


def load_config(
    config_file: str,
    overrides: Dict[str, str],
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

    config_dict.update(overrides)

    # check for deprecated options
    if "bootdisksizegb" in config_dict:
        print(
            'Warning: The option "bootdisksizegb" was seen in the config file but this name is deprecated. This parameter has been replaced with boot_volume_in_gb.'
        )
        config_dict["boot_volume_in_gb"] = config_dict["bootdisksizegb"]
        del config_dict["bootdisksizegb"]

    config_used = set()

    def consume(
        name: str,
        default: Union[NoDefault, T] = NO_DEFAULT,
        parser: Callable[[str], T] = str,
    ) -> T:
        assert name not in config_used, f"Consumed {name} twice"
        config_used.add(name)

        if name in config_dict:
            value = parser(config_dict[name])
        else:
            if isinstance(default, NoDefault):
                raise BadConfig(f"Missing {name} in config")
            else:
                value = default
        return value

    config = PrepConfig()
    zones = consume(
        "zones",
        default=gcloud_config.zones,
        parser=lambda value: [x.strip() for x in value.split(",")],
    )
    assert isinstance(zones, list)
    config.zones = zones
    config.sparkles_config_path = config_file
    config.monitor_port = consume("monitor_port", 6032, int)
    config.work_root_dir = consume("work_root_dir", "/mnt/")
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

    preemptible_yn = consume("preemptible", "y")
    assert preemptible_yn.lower() in [
        "y",
        "n",
    ], f"expected preemptible should be either y or n but value was: {preemptible_yn}"
    if preemptible_yn.lower() == "n":
        assert (
            "max_preemptable_attempts_scale" not in config_dict
        ), f"Cannot specify both preemptible=n and max_preemptable_attempts_scale"

        config.max_preemptable_attempts_scale = 0
    else:
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
        size_in_gb = consume(f"mount_{i+1}_size_in_gb", 100, int)
        name = consume(f"mount_{i+1}_name", None)
        if name is not None:
            mounts.append(ExistingDiskMount(name=name, path=path))
        else:
            mounts.append(
                PersistentDiskMount(path=path, type=type, size_in_gb=size_in_gb)
            )

    # TODO: Add validation that no two mounts have the same path and that workdir lines up with at least one mount

    config.mounts = mounts
    config.cache_db_path = consume("cache_db_path", ".kubeque-cached-file-hashes")
    config.debug_log_prefix = consume(
        "debug_log_prefix", url_join(config.default_url_prefix, "node-logs")
    )

    # make sure that the directory that is used for the working directory is one
    # of the

    unknown_parameters = set(config_dict.keys()).difference(config_used)
    if len(unknown_parameters) != 0:
        raise UnknownParameters(
            "The following parameters in config are unrecognized: {}".format(
                ", ".join(unknown_parameters)
            )
        )

    return Config(**dataclasses.asdict(config))


def create_func_params(
    config_file: str, overrides: Dict[str, str], extras: Dict, requested: List
) -> Dict:
    extras = dict(extras)

    config = load_config(config_file, overrides)
    extras["config"] = config

    service_account_key = config.service_account_key
    if not os.path.exists(service_account_key):
        raise Exception("Could not find service account key at %s", service_account_key)

    config.credentials = service_account.Credentials.from_service_account_file(
        service_account_key, scopes=SCOPES
    )

    params = dict(create_services_from_config(config, set(requested).difference(extras.keys())))
    for name, value in extras.items():
        if name in requested:
            params[name] = value
    return params

class LazyInit:
    def __init__(self, **constructors):
        self.constructors = constructors
        self.initialized = {}
    
    def get(self, name):
        if name not in self.initialized:
            value = self.constructors[name](self)
            self.initialized[name] =value
        return self.initialized[name]

from .batch_api import ClusterAPI
from google.cloud.batch_v1alpha.services.batch_service import BatchServiceClient

def create_services_from_config(config: Config, requested : List[str]):
    credentials = config.credentials
    project_id = config.project

    services = LazyInit(jq=lambda services: JobQueue(services.get("datastore_client"), services.get("job_store"), services.get("task_store")),
                        job_store=lambda services: JobStore(services.get("datastore_client")),
                        task_store=lambda services: TaskStore(services.get("datastore_client")),
                        datastore_client=lambda services: datastore.Client(project_id, credentials=credentials),
                        io=lambda services: IO(project_id, config.cas_url_prefix, credentials),
                        batch_service_client=lambda services: BatchServiceClient(credentials=credentials),
                        cluster_api= lambda services:  ClusterAPI(services.get("batch_service_client"))
                        )

    return dict([(name, services.get(name)) for name in requested])

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
