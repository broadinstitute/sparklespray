import sparklespray

import pprint
from ..util import random_string
from ..job_queue import JobQueue
from ..io_helper import IO
from ..cluster_service import Cluster
from ..job_store import JobStore, Job, JOB_STATUS_KILLED
import time
from ..config import Config
from ..batch_api import ClusterAPI


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

def validate_cmd(jq: JobQueue, io: IO, config: Config, cluster_api: ClusterAPI):
    print(f"Validating config, using sparklespray {sparklespray.__version__}")

    service_acct = config.credentials.service_account_email  # pyright: ignore
    print("Printing config:")
    pprint.pprint(config)
    project_id = config.project

    print(f"Verifying we can access google cloud storage.")
    print(
        f"This should work as long as the buckets used are owned by the project '{project_id}'. If not you will need to explictly grant access to {service_acct}"
    )

    sample_value = random_string(20)
    sample_url = io.write_str_to_cas(sample_value)
    fetched_value = io.get_as_str(sample_url)
    assert sample_value == fetched_value

    print(
        "Verifying we can read/write from the google datastore service"
    )
    _test_datastore_api(jq.job_storage, sample_value)

    print("Verifying we can access google's Batch apis by creating test job")
    from ..worker_job import create_test_job
    from ..batch_api import is_job_complete, is_job_successful
    from ..gcp_utils import make_unique_label
    job = create_test_job("validate-test", make_unique_label("cluster"), config.default_image, service_acct, config)
    operation_id = cluster_api.create_job(config.project, config.location, job, 1)

    print(f"Waiting for job {operation_id} to complete ", end="", flush=True)
    while True:
        status = cluster_api.get_job_status(operation_id)
        if is_job_complete(status):
            assert is_job_successful(status), f"Job did not complete successfully: {status}"
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
