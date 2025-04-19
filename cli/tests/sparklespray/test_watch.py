import pytest
import time
from unittest.mock import MagicMock, patch
from typing import List

from sparklespray.commands.watch import watch
from sparklespray.job_queue import JobQueue
from sparklespray.task_store import Task, STATUS_PENDING, STATUS_COMPLETE, STATUS_CLAIMED
from sparklespray.batch_api import ClusterAPI, JobSpec
from sparklespray.cluster_service import Cluster
from sparklespray.node_req_store import NodeReq, NODE_REQ_SUBMITTED

from .factories import DatastoreClientSimulator, MockIO


@pytest.fixture
def mock_io():
    return MockIO()


@pytest.fixture
def datastore_client():
    return DatastoreClientSimulator()


@pytest.fixture
def job_queue(datastore_client):
    job_storage = MagicMock()
    task_storage = MagicMock()
    
    # Setup job_queue with a mock job
    job_queue = JobQueue(datastore_client, job_storage, task_storage)
    
    # Create a mock job
    job = MagicMock()
    job.job_id = "test-job-id"
    job.cluster = "test-cluster"
    job.target_node_count = 2
    job.max_preemptable_attempts = 1
    job.kube_job_spec = JobSpec(
        job_id="test-job-id",
        image="test-image",
        service_account_email="test-sa@example.com",
        machine_type="n1-standard-1",
        region="us-central1",
        boot_volume_in_gb=10,
        boot_volume_type="pd-standard",
        mounts=[],
        work_dir="/tmp",
        monitor_port=8080
    ).model_dump_json()
    
    job_queue.get_job_must = MagicMock(return_value=job)
    
    return job_queue


@pytest.fixture
def cluster_api():
    api = MagicMock(spec=ClusterAPI)
    return api


@pytest.fixture
def mock_tasks():
    """Create a list of mock tasks with different statuses"""
    tasks = []
    
    # Create 5 pending tasks
    for i in range(5):
        task = MagicMock(spec=Task)
        task.task_id = f"test-job-id.{i}"
        task.job_id = "test-job-id"
        task.status = STATUS_PENDING
        task.command_result_url = f"gs://results/test-job-id/{i}/result.json"
        task.history = []
        tasks.append(task)
    
    # Create 3 claimed tasks
    for i in range(5, 8):
        task = MagicMock(spec=Task)
        task.task_id = f"test-job-id.{i}"
        task.job_id = "test-job-id"
        task.status = STATUS_CLAIMED
        task.command_result_url = f"gs://results/test-job-id/{i}/result.json"
        task.history = []
        tasks.append(task)
    
    # Create 2 completed tasks
    for i in range(8, 10):
        task = MagicMock(spec=Task)
        task.task_id = f"test-job-id.{i}"
        task.job_id = "test-job-id"
        task.status = STATUS_COMPLETE
        task.command_result_url = f"gs://results/test-job-id/{i}/result.json"
        task.history = []
        tasks.append(task)
    
    return tasks


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
def cluster(datastore_client, mock_tasks, mock_node_reqs):
    """Create a mock cluster with tasks and node reqs"""
    cluster = MagicMock(spec=Cluster)
    cluster.job_id = "test-job-id"
    
    # Setup task_store to return mock tasks
    task_store = MagicMock()
    task_store.get_tasks = MagicMock(return_value=mock_tasks)
    cluster.task_store = task_store
    
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
        loglive=False
    )
    
    # Verify _wait_until_tasks_exist was called
    mock_wait.assert_called_once_with(cluster, "test-job-id")
    
    # Verify run_tasks was called with the correct arguments
    mock_run_tasks.assert_called_once()
    
    # Check the tasks passed to run_tasks
    args = mock_run_tasks.call_args[0]
    assert args[0] == "test-job-id"  # job_id
    assert args[1] == "test-cluster"  # cluster_id
    assert len(args[2]) == 4  # 4 tasks: CompletionMonitor, StreamLogs, PrintStatus, ResizeCluster
    assert args[3] == cluster  # cluster
    
    # Verify result is None (normal completion)
    assert result is None


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
        loglive=False
    )
    
    # Verify _wait_until_tasks_exist was called
    mock_wait.assert_called_once_with(cluster, "test-job-id")
    
    # Verify run_tasks was called with the correct arguments
    mock_run_tasks.assert_called_once()
    
    # Check the tasks passed to run_tasks
    args = mock_run_tasks.call_args[0]
    assert args[0] == "test-job-id"  # job_id
    assert args[1] == "test-cluster"  # cluster_id
    assert len(args[2]) == 3  # 3 tasks: CompletionMonitor, StreamLogs, PrintStatus (no ResizeCluster)
    assert args[3] == cluster  # cluster
    
    # Verify result is None (normal completion)
    assert result is None


@patch("sparklespray.commands.watch._wait_until_tasks_exist")
@patch("sparklespray.commands.watch.run_tasks", side_effect=KeyboardInterrupt)
def test_watch_keyboard_interrupt(mock_run_tasks, mock_wait, job_queue, mock_io, cluster):
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
        loglive=False
    )
    
    # Verify _wait_until_tasks_exist was called
    mock_wait.assert_called_once_with(cluster, "test-job-id")
    
    # Verify run_tasks was called
    mock_run_tasks.assert_called_once()
    
    # Verify result is 20 (keyboard interrupt exit code)
    assert result == 20


@patch("sparklespray.commands.watch.time.sleep")
def test_wait_until_tasks_exist(mock_sleep, cluster):
    """Test _wait_until_tasks_exist function"""
    from sparklespray.commands.watch import _wait_until_tasks_exist
    
    # First call returns empty list, second call returns tasks
    cluster.task_store.get_tasks.side_effect = [[], ["task1", "task2"]]
    
    # Call the function
    _wait_until_tasks_exist(cluster, "test-job-id")
    
    # Verify get_tasks was called twice
    assert cluster.task_store.get_tasks.call_count == 2
    
    # Verify sleep was called once
    mock_sleep.assert_called_once_with(5)


@patch("sparklespray.commands.watch.time.sleep")
def test_wait_until_tasks_exist_timeout(mock_sleep, cluster):
    """Test _wait_until_tasks_exist function with timeout"""
    from sparklespray.commands.watch import _wait_until_tasks_exist
    
    # Always return empty list of tasks
    cluster.task_store.get_tasks.return_value = []
    
    # Call the function, should raise exception after 21 attempts
    with pytest.raises(Exception, match="no tasks ever appeared"):
        _wait_until_tasks_exist(cluster, "test-job-id")
    
    # Verify get_tasks was called 21 times (initial + 20 retries)
    assert cluster.task_store.get_tasks.call_count == 21
    
    # Verify sleep was called 20 times
    assert mock_sleep.call_count == 20


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
    mock_io.exists = lambda url: int(url.split('/')[-2]) % 2 == 0
    
    # Setup reset_task mock
    job_queue.reset_task = MagicMock()
    
    # Call the function
    check_completion(job_queue, mock_io, "test-job-id")
    
    # Verify reset_task was called for tasks with odd indices
    assert job_queue.reset_task.call_count == 2
    job_queue.reset_task.assert_any_call("test-job-id.1")
    job_queue.reset_task.assert_any_call("test-job-id.3")
