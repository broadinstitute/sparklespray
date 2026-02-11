import pytest
import time
from unittest.mock import MagicMock, patch
from typing import List

from sparklespray.commands.watch import watch
from sparklespray.job_queue import JobQueue
from sparklespray.task_store import (
    Task,
    STATUS_PENDING,
    STATUS_COMPLETE,
    STATUS_CLAIMED,
)
from sparklespray.batch_api import ClusterAPI, JobSpec, Disk, Runnable
from sparklespray.cluster_service import Cluster
from sparklespray.node_req_store import NodeReq, NODE_REQ_SUBMITTED
from sparklespray.task_store import TaskStore
from sparklespray.job_store import JobStore, Job, JOB_STATUS_SUBMITTED
from sparklespray.task_store import TaskStore, Task

from .factories import DatastoreClientSimulator, MockIO


@pytest.fixture
def mock_io():
    return MockIO()


@pytest.fixture
def datastore_client():
    return DatastoreClientSimulator()


def job_spec_factory(sparkles_job="x", sparkles_cluster="x"):
    return JobSpec(
        task_count="1",
        runnables=[Runnable(image="mock-image", command=["consume"])],
        machine_type="",
        preemptible=True,
        locations=[],
        monitor_port=1203,
        network_tags=[],
        sparkles_job=sparkles_job,
        sparkles_cluster=sparkles_cluster,
        sparkles_timestamp="",
        boot_disk=Disk(
            name="disk1",
            size_gb=50,
            type="pd-standard",
            # The mount path for the volume, e.g. /mnt/disks/share.
            mount_path="/mnt/disk1",
        ),
        disks=[],
        service_account_email="invalid@sample.com",
        scopes=[],
    )


@pytest.fixture
def task_storage(datastore_client):
    task_storage = TaskStore(datastore_client)
    return task_storage


@pytest.fixture
def job_storage(datastore_client):
    job_storage = JobStore(datastore_client)
    return job_storage


@pytest.fixture
def job_queue(datastore_client, task_storage, job_storage):
    # Create a real JobQueue instance
    job_queue = JobQueue(datastore_client, job_storage, task_storage)

    # Create a mock job
    job = MagicMock()
    job.job_id = "test-job-id"
    job.cluster = "test-cluster"
    job.target_node_count = 2
    job.max_preemptable_attempts = 1
    job_spec = job_spec_factory(sparkles_job=job.job_id, sparkles_cluster=job.cluster)
    job.kube_job_spec = job_spec.model_dump_json()

    # We still need to mock some methods for testing
    job_queue.get_job_must = MagicMock(return_value=job)

    return job_queue


@pytest.fixture
def cluster_api():
    api = MagicMock(spec=ClusterAPI)
    return api


# @pytest.fixture
# def mock_tasks():
#     """Create a list of mock tasks with different statuses"""
#     tasks = []

#     # Create 5 pending tasks
#     for i in range(5):
#         task = MagicMock(spec=Task)
#         task.task_id = f"test-job-id.{i}"
#         task.job_id = "test-job-id"
#         task.status = STATUS_PENDING
#         task.command_result_url = f"gs://results/test-job-id/{i}/result.json"
#         task.history = []
#         tasks.append(task)

#     # Create 3 claimed tasks
#     for i in range(5, 8):
#         task = MagicMock(spec=Task)
#         task.task_id = f"test-job-id.{i}"
#         task.job_id = "test-job-id"
#         task.status = STATUS_CLAIMED
#         task.command_result_url = f"gs://results/test-job-id/{i}/result.json"
#         task.history = []
#         tasks.append(task)

#     # Create 2 completed tasks
#     for i in range(8, 10):
#         task = MagicMock(spec=Task)
#         task.task_id = f"test-job-id.{i}"
#         task.job_id = "test-job-id"
#         task.status = STATUS_COMPLETE
#         task.command_result_url = f"gs://results/test-job-id/{i}/result.json"
#         task.history = []
#         tasks.append(task)

#     return tasks


@pytest.fixture
def mock_node_reqs():
    """Create a list of mock node requirements"""
    node_reqs = []

    # Create 2 submitted node reqs
    for i in range(2):
        node_req = MagicMock(spec=NodeReq)
        node_req.node_req_id = f"node-req-{i}"
        node_req.status = NODE_REQ_SUBMITTED
        node_reqs.append(node_req)

    return node_reqs


@pytest.fixture
def job(job_storage):
    job_storage.insert(
        Job(
            job_id="test-job-id",
            tasks=[],
            kube_job_spec="",
            metadata={},
            cluster="mock-cluster-id",
            status=JOB_STATUS_SUBMITTED,
            submit_time=0.0,
            max_preemptable_attempts=100,
            target_node_count=1,
        )
    )


@pytest.fixture
def cluster(datastore_client, mock_node_reqs, task_storage, job_storage, job):
    """Create a mock cluster with tasks and node reqs"""
    cluster_api = MagicMock(ClusterAPI)
    job_storage.insert(
        Job(
            job_id="test-job-id",
            tasks=[],
            kube_job_spec="",
            metadata={},
            cluster="mock-cluster-id",
            status=JOB_STATUS_SUBMITTED,
            submit_time=0.0,
            max_preemptable_attempts=100,
            target_node_count=1,
        )
    )
    cluster = Cluster(
        project="mock-project",
        location="mock-location",
        cluster_id="mock-cluster-id",
        job_id="test-job-id",
        job_store=job_storage,
        task_store=task_storage,
        client=datastore_client,
        cluster_api=cluster_api,
        debug_log_prefix="https://sample.com",
    )
    cluster.job_id = "test-job-id"

    cluster.task_store = task_storage

    # Setup get_node_reqs to return mock node reqs
    cluster.get_node_reqs = MagicMock(return_value=mock_node_reqs)

    return cluster


@patch("sparklespray.commands.watch._wait_until_tasks_exist")
@patch("sparklespray.commands.watch.run_tasks")
def test_watch_basic(mock_run_tasks, mock_wait, job_queue, mock_io, cluster):
    """Test basic watch functionality"""
    # Call the watch function
    result = watch(
        mock_io,
        job_queue,
        cluster,
        target_nodes=2,
        max_preemptable_attempts_scale=1,
        initial_poll_delay=0.1,
        max_poll_delay=1.0,
        loglive=False,
    )

    # Verify _wait_until_tasks_exist was called
    mock_wait.assert_called_once_with(cluster, "test-job-id")

    # Verify run_tasks was called with the correct arguments
    mock_run_tasks.assert_called_once()

    # Check the tasks passed to run_tasks
    args = mock_run_tasks.call_args[0]
    assert args[0] == "test-job-id"  # job_id
    assert args[1] == "test-cluster"  # cluster_id
    assert (
        len(args[2]) == 6
    )  # 6 tasks: CompletionMonitor, StreamLogs, PrintStatus, Heartbeat, ResizeCluster, ResetOrphans
    assert args[3] == cluster  # cluster

    # Verify result is True (normal completion)
    assert result is True


@patch("sparklespray.commands.watch._wait_until_tasks_exist")
@patch("sparklespray.commands.watch.run_tasks")
def test_watch_no_nodes(mock_run_tasks, mock_wait, job_queue, mock_io, cluster):
    """Test watch with target_nodes=0"""
    # Call the watch function with target_nodes=0
    result = watch(
        mock_io,
        job_queue,
        cluster,
        target_nodes=0,
        max_preemptable_attempts_scale=1,
        initial_poll_delay=0.1,
        max_poll_delay=1.0,
        loglive=False,
    )

    # Verify _wait_until_tasks_exist was called
    mock_wait.assert_called_once_with(cluster, "test-job-id")

    # Verify run_tasks was called with the correct arguments
    mock_run_tasks.assert_called_once()

    # Check the tasks passed to run_tasks
    args = mock_run_tasks.call_args[0]
    assert args[0] == "test-job-id"  # job_id
    assert args[1] == "test-cluster"  # cluster_id
    assert (
        len(args[2]) == 4
    )  # 4 tasks: CompletionMonitor, StreamLogs, PrintStatus, Heartbeat (no ResizeCluster/ResetOrphans)
    assert args[3] == cluster  # cluster

    # Verify result is True (normal completion)
    assert result is True


@patch("sparklespray.commands.watch._wait_until_tasks_exist")
@patch("sparklespray.commands.watch.run_tasks", side_effect=KeyboardInterrupt)
def test_watch_keyboard_interrupt(
    mock_run_tasks, mock_wait, job_queue, mock_io, cluster
):
    """Test watch with KeyboardInterrupt"""
    # Call the watch function, which should catch the KeyboardInterrupt
    result = watch(
        mock_io,
        job_queue,
        cluster,
        target_nodes=2,
        max_preemptable_attempts_scale=1,
        initial_poll_delay=0.1,
        max_poll_delay=1.0,
        loglive=False,
    )

    # Verify _wait_until_tasks_exist was called
    mock_wait.assert_called_once_with(cluster, "test-job-id")

    # Verify run_tasks was called
    mock_run_tasks.assert_called_once()

    # Verify result is False when did not complete successfully
    assert result is False


# @patch("sparklespray.commands.watch.time.sleep")
# def test_wait_until_tasks_exist(mock_sleep, cluster : Cluster, task_storage : TaskStore, job :Job):
#     """Test _wait_until_tasks_exist function"""
#     from sparklespray.commands.watch import _wait_until_tasks_exist

#     task_storage.insert(Task(
#     task_id= "test-job-id.1",
#     task_index=1,
#     owner=None,
#     monitor_address=None,
#     job_id="test-job-id",
#     status=STATUS_PENDING,
#     args="",
#     history=[],
#     command_result_url= "",
#     cluster=cluster.cluster_id,
#     log_url="",
#     ))

#     # Call the function
#     _wait_until_tasks_exist(cluster, "test-job-id")


@patch("sparklespray.commands.watch.time.sleep")
def test_wait_until_tasks_exist_timeout(mock_sleep, cluster):
    """Test _wait_until_tasks_exist function with timeout"""
    from sparklespray.commands.watch import _wait_until_tasks_exist, TimeoutException

    # Call the function, should raise TimeoutException after a while
    with pytest.raises(TimeoutException, match="no tasks ever appeared"):
        _wait_until_tasks_exist(cluster, "test-job-id")


def test_check_completion(job_queue, mock_io):
    """Test check_completion function"""
    from sparklespray.commands.watch import check_completion

    # Create mock tasks
    tasks = []
    for i in range(5):
        task = MagicMock(spec=Task)
        task.task_id = f"test-job-id.{i}"
        task.status = STATUS_COMPLETE
        task.command_result_url = f"gs://results/test-job-id/{i}/result.json"
        tasks.append(task)

    # Setup task_storage to return mock tasks
    job_queue.task_storage.get_tasks = MagicMock(return_value=tasks)

    # Setup IO.exists to return True for even indices, False for odd indices
    mock_io.exists = lambda url: int(url.split("/")[-2]) % 2 == 0

    # Setup reset_task mock
    job_queue.reset_task = MagicMock()

    # Call the function
    check_completion(job_queue, mock_io, "test-job-id")

    # Verify reset_task was called for tasks with odd indices
    assert job_queue.reset_task.call_count == 2
    job_queue.reset_task.assert_any_call("test-job-id.1")
    job_queue.reset_task.assert_any_call("test-job-id.3")
