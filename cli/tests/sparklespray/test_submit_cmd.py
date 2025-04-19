import pytest
import os
import json
from unittest.mock import MagicMock, patch, mock_open
import argparse

from sparklespray.commands.submit import submit_cmd
from sparklespray.job_queue import JobQueue
from sparklespray.batch_api import ClusterAPI
from sparklespray.config import Config

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
    
    # Setup get_job_optional to return None (no existing job)
    job_queue = JobQueue(datastore_client, job_storage, task_storage)
    job_queue.get_job_optional = MagicMock(return_value=None)
    job_queue.submit = MagicMock()
    
    return job_queue


@pytest.fixture
def cluster_api():
    api = MagicMock(spec=ClusterAPI)
    api.create_job = MagicMock(return_value="mock-job-id")
    return api


@pytest.fixture
def config(tmp_path):
    from sparklespray.config import load_config
    
    # Create a temporary config file
    config_file = tmp_path / ".sparkles"
    config_content = """
project = test-project
location = us-central1
region = us-central1
zones = us-central1-a
default_image = ubuntu:latest
machine_type = n1-standard-1
cas_url_prefix = gs://mock-cas
default_url_prefix = gs://mock-results
debug_log_prefix = gs://mock-logs
work_root_dir = /tmp
monitor_port = 8080
sparklesworker_image = sparklesworker:latest
sparklesworker_exe_path = /tmp/sparklesworker
cache_db_path = /tmp/cache.db
max_preemptable_attempts_scale = 2
service_account_email = test-sa@test-project.iam.gserviceaccount.com
boot_volume_path = /
boot_volume_size_in_gb = 10
boot_volume_type = pd-standard
"""
    config_file.write_text(config_content)
    
    # Mock the gcloud config file reading
    with patch("sparklespray.config.os.path.exists", return_value=True), \
         patch("sparklespray.config.open", mock_open(read_data="[core]\nproject = test-project\n")):
        # Load the config using the actual load_config function
        config = load_config(str(config_file), verbose=False)
    
    # Add credentials attribute which is normally added by create_services_from_config
    credentials = MagicMock()
    credentials.service_account_email = "test-sa@test-project.iam.gserviceaccount.com"
    config.credentials = credentials
    
    return config


def parse_args_for_test(cmd_line):
    """Parse command line arguments using the actual parser from main.py"""
    from sparklespray.commands.submit import add_submit_cmd
    import argparse
    
    # Create a parser with just the submit command
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers()
    add_submit_cmd(subparser)
    
    # Parse the command line
    args = parser.parse_args(cmd_line)
    return args


# Create a temporary file for testing
@pytest.fixture
def temp_file(tmp_path):
    file_path = tmp_path / "test_file.txt"
    with open(file_path, "w") as f:
        f.write("test content")
    return str(file_path)


@patch("sparklespray.commands.submit.watch")
@patch("os.path.exists")
def test_submit_cmd_basic(mock_exists, mock_watch, job_queue, mock_io, datastore_client, cluster_api, config, temp_file):
    # Setup mocks
    mock_exists.return_value = True
    mock_watch.return_value = 0
    
    # Set up IO mock to handle file existence checks
    mock_io.bulk_exists_results = {}  # All files need upload
    
    # Use args with push parameter
    args = parse_args_for_test(["sub", "--name", "test-job", "--push", temp_file, "echo", "hello", "world"])
    
    # Run the function under test
    result = submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config)
    
    # Verify the result
    assert result == 0
    
    # Verify job was submitted
    assert job_queue.submit.called
    
    # Get the arguments passed to submit
    call_args = job_queue.submit.call_args[0]
    
    # Verify job_id
    assert call_args[0] == "test-job"
    
    # Verify task specs were created
    assert len(call_args[1]) > 0


@patch("sparklespray.commands.submit.watch")
@patch("os.path.exists")
def test_submit_cmd_with_existing_job(mock_exists, mock_watch, job_queue, mock_io, datastore_client, cluster_api, config):
    # Setup mocks
    mock_exists.return_value = True
    mock_watch.return_value = 0
    
    # Set up job_queue to return an existing job
    existing_job = MagicMock()
    existing_job.cluster = "existing-cluster"
    job_queue.get_job_optional = MagicMock(return_value=existing_job)
    
    # Use args with skipifexists flag
    args = parse_args_for_test(["sub", "--name", "test-job", "--skipifexists", "echo", "hello", "world"])
    
    # Run the function under test
    result = submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config)
    
    # Verify the result - should exit early
    assert result == 0
    
    # Verify job was not submitted
    assert not job_queue.submit.called


@patch("sparklespray.commands.submit.watch")
@patch("os.path.exists")
def test_submit_cmd_with_seq_parameter(mock_exists, mock_watch, job_queue, mock_io, datastore_client, cluster_api, config):
    # Setup mocks
    mock_exists.return_value = True
    mock_watch.return_value = 0
    
    # Use args with seq parameter
    args = parse_args_for_test(["sub", "--name", "test-job", "--seq", "3", "echo", "hello", "world"])
    
    # Run the function under test
    result = submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config)
    
    # Verify the result
    assert result == 0
    
    # Verify job was submitted
    assert job_queue.submit.called
    
    # Get the arguments passed to submit
    call_args = job_queue.submit.call_args[0]
    
    # Verify job_id
    assert call_args[0] == "test-job"
    
    # Verify task specs were created - should be 3 for seq=3
    assert len(call_args[1]) == 3

@patch("sparklespray.commands.submit.watch")
@patch("os.path.exists")
def test_submit_cmd_complex_args(mock_exists, mock_watch, job_queue, mock_io, datastore_client, cluster_api, config, temp_file):
    # Setup mocks
    mock_exists.return_value = True
    mock_watch.return_value = 0
    
    # Set up IO mock to handle file existence checks
    mock_io.bulk_exists_results = {}  # All files need upload
    
    # Use args with multiple parameters
    args = parse_args_for_test([
        "sub", 
        "--name", "complex-job",
        "--machine-type", "n1-standard-4",
        "--image", "custom-image:latest",
        "--nodes", "5",
        "--cd", "/tmp/workdir",
        "--results", "*.txt", 
        "--results", "*.csv",
        "--ignore", "*.tmp",
        "--no-wait",
        "--symlinks",
        "echo", "complex", "command"
    ])
    
    # Run the function under test
    result = submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config)
    
    # Verify the result
    assert result == 0
    
    # Verify job was submitted
    assert job_queue.submit.called
    
    # Get the arguments passed to submit
    call_args = job_queue.submit.call_args[0]
    
    # Verify job_id
    assert call_args[0] == "complex-job"
    
    # Verify task specs were created
    assert len(call_args[1]) > 0
