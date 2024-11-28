import re
from ..task_store import (
    STATUS_FAILED,
    STATUS_CLAIMED,
    STATUS_PENDING,
    STATUS_KILLED,
    STATUS_COMPLETE,
)
from ..job_queue import JobQueue, Job
from ..cluster_service import Cluster
from ..io_helper import IO
from ..resize_cluster import GetPreempted
from ..config import get_config_path, load_config, create_services, Config, BadConfig
from ..log import log
from .shared import _get_jobids_from_pattern
from ..gcp_setup import setup_project

def setup_cmd(args, config: Config):
    default_url_prefix = config.default_url_prefix
    m = re.match("^gs://([^/]+)(?:/.*)?$", default_url_prefix)
    assert m is not None, "invalid remote path: {}".format(default_url_prefix)
    bucket_name = m.group(1)

    setup_project(config.project, config.service_account_key, bucket_name)

def add_setup_cmd(subparser):
    parser = subparser.add_parser(
        "setup",
        help="Configures the google project chosen in the config to be compatible with sparklespray. (requires gcloud installed in path)",
    )
    parser.set_defaults(func=setup_cmd)
