import pytest
from unittest.mock import MagicMock, patch

from sparklespray.commands.watch import watch_cmd
from sparklespray.job_queue import JobQueue
from sparklespray.io_helper import IO
from sparklespray.config import Config
from sparklespray.batch_api import ClusterAPI
from sparklespray.cluster_service import Cluster

from .factories import DatastoreClientSimulator, MockIO


@pytest.fixture
def mock_io():
    return MockIO()


@pytest.fixture
def datastore_client():
    return DatastoreClientSimulator()


@pytest.fixture
def job_queue(datastore_client):
    from sparklespray.job_store import JobStore
    from sparklespray.task_store import TaskStore
    
    # Create actual JobStore and TaskStore instances
    job_storage = JobStore(datastore_client)
    task_storage = TaskStore(datastore_client)
    
    # Create a real JobQueue instance
    job_queue = JobQueue(datastore_client, job_storage, task_storage)
    
    # Mock get_job_ids to return a test job
    job_queue.get_job_ids = MagicMock(return_value=["test-job-id"])
    
    # Create a mock job
    job = MagicMock()
    job.job_id = "test-job-id"
    job.cluster = "test-cluster"
    job.target_node_count = 2
    job.max_preemptable_attempts = 1
    
    # Mock get_job_must to return our test job
    job_queue.get_job_must = MagicMock(return_value=job)
    
    return job_queue


@pytest.fixture
def cluster_api():
    api = MagicMock(spec=ClusterAPI)
    return api


@pytest.fixture
def config():
    config = MagicMock(spec=Config)
    config.project = "test-project"
    config.location = "us-central1"
    config.region = "us-central1"
    config.zones = ["us-central1-a"]
    config.max_preemptable_attempts_scale = 2
    config.debug_log_prefix = "gs://mock-logs"
    return config


@pytest.fixture
def args():
    args = MagicMock()
    args.jobid = "test-job-id"
    args.nodes = None
    args.verify = False
    args.loglive = True
    return args


@patch("sparklespray.commands.watch.create_cluster")
@patch("sparklespray.commands.watch.watch")
def test_watch_cmd_basic(mock_watch, mock_create_cluster, job_queue, mock_io, config, args, datastore_client, cluster_api):
    """Test basic watch_cmd functionality"""
    # Setup mock cluster
    mock_cluster = MagicMock(spec=Cluster)
    mock_cluster.job_id = "test-job-id"
    mock_create_cluster.return_value = mock_cluster
    
    # Call the watch_cmd function
    watch_cmd(job_queue, mock_io, config, args, cluster_api, datastore_client)
    
    # Verify create_cluster was called with correct arguments
    mock_create_cluster.assert_called_once_with(
        config, job_queue, datastore_client, cluster_api, "test-job-id"
    )
    
    # Verify watch was called with correct arguments
    mock_watch.assert_called_once_with(
        mock_io,
        job_queue,
        mock_cluster,
        target_nodes=None,
        loglive=True,
        max_preemptable_attempts_scale=2,
    )


@patch("sparklespray.commands.watch.create_cluster")
@patch("sparklespray.commands.watch.watch")
@patch("sparklespray.commands.watch.check_completion")
def test_watch_cmd_with_verify(mock_check_completion, mock_watch, mock_create_cluster, job_queue, mock_io, config, args, datastore_client, cluster_api):
    """Test watch_cmd with verify flag"""
    # Setup mock cluster
    mock_cluster = MagicMock(spec=Cluster)
    mock_cluster.job_id = "test-job-id"
    mock_create_cluster.return_value = mock_cluster
    
    # Set verify flag
    args.verify = True
    
    # Call the watch_cmd function
    watch_cmd(job_queue, mock_io, config, args, cluster_api, datastore_client)
    
    # Verify check_completion was called
    mock_check_completion.assert_called_once_with(job_queue, mock_io, "test-job-id")
    
    # Verify create_cluster was called
    mock_create_cluster.assert_called_once()
    
    # Verify watch was called
    mock_watch.assert_called_once()


@patch("sparklespray.commands.watch.create_cluster")
@patch("sparklespray.commands.watch.watch")
def test_watch_cmd_with_nodes(mock_watch, mock_create_cluster, job_queue, mock_io, config, args, datastore_client, cluster_api):
    """Test watch_cmd with nodes parameter"""
    # Setup mock cluster
    mock_cluster = MagicMock(spec=Cluster)
    mock_cluster.job_id = "test-job-id"
    mock_create_cluster.return_value = mock_cluster
    
    # Set nodes parameter
    args.nodes = 5
    
    # Call the watch_cmd function
    watch_cmd(job_queue, mock_io, config, args, cluster_api, datastore_client)
    
    # Verify create_cluster was called
    mock_create_cluster.assert_called_once()
    
    # Verify watch was called with correct target_nodes
    mock_watch.assert_called_once_with(
        mock_io,
        job_queue,
        mock_cluster,
        target_nodes=5,
        loglive=True,
        max_preemptable_attempts_scale=2,
    )


@patch("sparklespray.commands.watch._resolve_jobid")
@patch("sparklespray.commands.watch.create_cluster")
@patch("sparklespray.commands.watch.watch")
def test_watch_cmd_with_jobid_pattern(mock_watch, mock_create_cluster, mock_resolve_jobid, job_queue, mock_io, config, args, datastore_client, cluster_api):
    """Test watch_cmd with jobid pattern"""
    # Setup mock cluster
    mock_cluster = MagicMock(spec=Cluster)
    mock_cluster.job_id = "test-job-id"
    mock_create_cluster.return_value = mock_cluster
    
    # Setup _resolve_jobid to return a specific job ID
    mock_resolve_jobid.return_value = "resolved-job-id"
    
    # Call the watch_cmd function
    watch_cmd(job_queue, mock_io, config, args, cluster_api, datastore_client)
    
    # Verify _resolve_jobid was called with correct arguments
    mock_resolve_jobid.assert_called_once_with(job_queue, "test-job-id")
    
    # Verify create_cluster was called with resolved job ID
    mock_create_cluster.assert_called_once_with(
        config, job_queue, datastore_client, cluster_api, "resolved-job-id"
    )
