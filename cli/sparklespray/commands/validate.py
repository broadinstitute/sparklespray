import sparklespray

import pprint
from ..util import random_string
from ..job_queue import JobQueue
from ..io_helper import IO
from ..cluster_service import Cluster
from ..job_store import JobStore, Job, JOB_STATUS_KILLED
import os
import time
from ..config import Config
from ..batch_api import ClusterAPI, _machine_type_has_gpu
from ..errors import UserError
from ..sprinkles_client import SprinklesClient, wait_for_sprinkles_job

SPRINKLES_VALIDATE_TIMEOUT_SEC = 15 * 60


def _test_datastore_api(job_store: JobStore, job_id: str):
    """Test we the datastore api is enabled by writing a value and deleting a value."""
    job = Job(
        job_id=job_id,
        tasks=[],
        kube_job_spec="invalid",
        metadata={},
        cluster=job_id,
        status=JOB_STATUS_KILLED,
        submit_time=time.time(),
        max_preemptable_attempts=2,
    )

    job_store.insert(job)
    fetched_job = job_store.get_job_must(job_id)
    assert isinstance(fetched_job, Job)
    assert fetched_job.job_id == job_id
    job_store.delete(job_id)


def _test_gcs(io: IO, config: Config):
    service_acct = config.credentials.service_account_email  # pyright: ignore
    print(f"Verifying we can access google cloud storage.")
    print(
        f"This should work as long as the buckets used are owned by the project '{config.project}'. If not you will need to explictly grant access to {service_acct}"
    )

    sample_value = random_string(20)
    sample_url = io.write_str_to_cas(sample_value)
    fetched_value = io.get_as_str(sample_url)
    assert sample_value == fetched_value
    return sample_value


def _validate_sprinkles(io: IO, config: Config) -> int:
    print(f"Validating config, using sparklespray {sparklespray.__version__}")
    print("sprinkles_url is set, so validating using sprinkles")

    api_key = os.environ.get("SPRINKLES_KEY")
    if api_key is None:
        raise UserError(
            "If using sprinkles url, you must set environment variable SPRINKLES_KEY"
        )

    print("Printing config:")
    pprint.pprint(config)

    # same restriction as "sub": flex only works with GPUs, and validate does not request any
    if config.provision_mode == "flex" and not _machine_type_has_gpu(config.machine_type):
        raise UserError(
            "provision_mode=flex only works when requesting GPUs, but we are not "
            "provisioning a machine with GPUs. Aborting."
        )

    _test_gcs(io, config)

    print("Verifying we can run a job via sprinkles by submitting a test job")
    client = SprinklesClient(
        config.sprinkles_url,
        api_key,
        io,
        config.cache_db_path,
        config.cas_url_prefix,
        1,
        config.default_url_prefix,
    )
    # name must be unique because submit() reuses any existing job with the same name
    job = client.submit(
        f"validate-{random_string(8).lower()}",
        ["echo", "test"],
        [{}],
        config.default_image,
        [],
        config.machine_type,
        config.project,
        config.region,
        config.boot_volume,
        config.max_preemptable_attempts_scale,
        config.mounts,
        config.provision_mode,
        config.worker_linger,
        [],
        labels={"sparkles-validate": "true"},
    )

    print(f"Waiting for job {job.id} to complete...")
    try:
        wait_for_sprinkles_job(client, job.id, SPRINKLES_VALIDATE_TIMEOUT_SEC)
    except UserError as ex:
        print(f"Validation failed: {ex.message}")
        return 1
    print(" success!")
    print("Verification successful!")
    return 0


def validate_cmd(jq: JobQueue, io: IO, config: Config, cluster_api: ClusterAPI):
    if config.sprinkles_url is not None:
        return _validate_sprinkles(io, config)

    print(f"Validating config, using sparklespray {sparklespray.__version__}")

    service_acct = config.credentials.service_account_email  # pyright: ignore
    print("Printing config:")
    pprint.pprint(config)
    project_id = config.project

    sample_value = _test_gcs(io, config)

    print("Verifying we can read/write from the google datastore service")
    _test_datastore_api(jq.job_storage, sample_value)

    print("Verifying we can access google's Batch apis by creating test job")
    from ..worker_job import create_test_job
    from ..batch_api import is_job_complete, is_job_successful, print_job_failure_debug
    from ..gcp_utils import make_unique_label

    job = create_test_job(
        "validate-test",
        make_unique_label("cluster"),
        config.default_image,
        service_acct,
        config,
    )
    operation_id = cluster_api.create_job(config.project, config.location, job, 1, 1)

    print(f"Waiting for job {operation_id} to complete ", end="", flush=True)
    while True:
        status = cluster_api.get_job_status(operation_id)
        if is_job_complete(status):
            if not is_job_successful(status):
                print_job_failure_debug(status)
                assert False, f"Job did not complete successfully"
            break
        else:
            print(".", end="", flush=True)
            time.sleep(5)
    print(" success!")
    print("Verification successful!")


def add_validate_cmd(subparser):
    parser = subparser.add_parser(
        "validate", help="Run a series of tests to confirm the configuration is valid"
    )
    parser.set_defaults(func=validate_cmd)
