import sparklespray

import pprint
from ..util import random_string
from ..job_queue import JobQueue
from ..io_helper import IO
from ..cluster_service import Cluster
from ..job_store import JobStore, Job, JOB_STATUS_KILLED
import time
from ..config import Config


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
    fetched_job = job_store.get_job(job_id)
    assert isinstance(fetched_job, Job)
    assert fetched_job.job_id == job_id
    job_store.delete(job_id)


def validate_cmd(jq: JobQueue, io: IO, cluster: Cluster, config: Config):
    print(f"Validating config, using sparklespray {sparklespray.__version__}")

    service_acct = config.credentials.service_account_email  # pyright: ignore
    print("Printing config:")
    pprint.pprint(config)
    project_id = config.project

    print(f"Verifying we can access google cloud storage.")
    print(
        f"This should work as long as the buckets used are owned by the project '{project_id}'. If not you will need to explictly grant access to the buckets to the account {service_acct}. This can be done via 'sparkles grant'"
    )

    sample_value = random_string(20)
    sample_url = io.write_str_to_cas(sample_value)
    fetched_value = io.get_as_str(sample_url)
    assert sample_value == fetched_value

    print(
        "Verifying we can read/write from the google datastore service and google pubsub"
    )
    _test_datastore_api(jq.job_storage, sample_value)

    raise NotImplementedError()
    # print("Verifying we can access google lifesciences apis")
    # cluster.test_pipeline_api()

    # default_image = config.default_image

    # print(f'Verifying google lifesciences can launch image "{default_image}"')
    # print(
    #     f'If this fails due to "permission denied", make sure that {service_acct} has been granted access to pull the docker image "{default_image}". You may need to explictly grant access via "sparkles grant" if the docker repo is owned by a different project than {project_id}.'
    # )
    # logging_url = config.default_url_prefix + "/node-logs"

    # cluster.test_image(
    #     project=project_id,
    #     zones=config.zones,
    #     docker_image=config.default_image,
    #     debug_log_url=logging_url,
    #     machine_specs=config.create_machine_specs(),
    #     monitor_port=config.monitor_port,
    # )

    print("Verification successful!")


def add_validate_cmd(subparser):
    parser = subparser.add_parser(
        "validate", help="Run a series of tests to confirm the configuration is valid"
    )
    parser.set_defaults(func=validate_cmd)
