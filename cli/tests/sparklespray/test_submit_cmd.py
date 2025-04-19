import pytest
import os
import json
from unittest.mock import MagicMock, patch, mock_open
import argparse
from typing import Dict, List, Optional, Any

from sparklespray.commands.submit import submit_cmd
from sparklespray.job_queue import JobQueue
from sparklespray.io_helper import IO
from sparklespray.batch_api import ClusterAPI
from sparklespray.config import Config


class DatastoreClientSimulator:
    """
    A simulator for the Google Cloud Datastore client that stores entities in memory.
    """
    def __init__(self):
        self.entities: Dict[str, Dict[str, Any]] = {}
        self.keys = []
        self.next_id = 1

    def key(self, kind, id=None):
        """Create a key for the given kind and ID."""
        if id is None:
            id = self.next_id
            self.next_id += 1
        key = MagicMock()
        key.kind = kind
        key.id = id
        key.name = str(id)
        key.__str__ = lambda self: f"{kind}({id})"
        self.keys.append(key)
        return key

    def get(self, key):
        """Get an entity by key."""
        key_str = f"{key.kind}:{key.name}"
        if key_str in self.entities:
            entity = MagicMock()
            for k, v in self.entities[key_str].items():
                setattr(entity, k, v)
            entity.key = key
            return entity
        return None

    def put(self, entity):
        """Store an entity."""
        key = entity.key
        key_str = f"{key.kind}:{key.name}"
        
        # Convert entity to dict for storage
        entity_dict = {}
        for k in dir(entity):
            if not k.startswith('_') and k != 'key':
                entity_dict[k] = getattr(entity, k)
        
        self.entities[key_str] = entity_dict
        return key

    def delete(self, key):
        """Delete an entity by key."""
        key_str = f"{key.kind}:{key.name}"
        if key_str in self.entities:
            del self.entities[key_str]

    def query(self, kind=None):
        """Create a query for the given kind."""
        query = MagicMock()
        query.kind = kind
        
        def fetch():
            results = []
            for key_str, entity_dict in self.entities.items():
                if key_str.startswith(f"{kind}:"):
                    entity = MagicMock()
                    for k, v in entity_dict.items():
                        setattr(entity, k, v)
                    key_name = key_str.split(':')[1]
                    entity.key = self.key(kind, key_name)
                    results.append(entity)
            return results
        
        query.fetch = fetch
        return query


class MockIO(IO):
    """Mock IO helper for testing."""
    def __init__(self):
        self.files = {}
        self.exists_results = {}
        self.bulk_exists_results = {}
        
    def exists(self, path):
        return self.exists_results.get(path, False)
        
    def bulk_exists_check(self, paths):
        if self.bulk_exists_results:
            return self.bulk_exists_results
        return {path: False for path in paths}
        
    def put(self, src_filename, dst_url, must=True, skip_if_exists=False):
        if os.path.exists(src_filename):
            with open(src_filename, 'rb') as f:
                self.files[dst_url] = f.read()
        else:
            self.files[dst_url] = b"mock content"
        return dst_url
        
    def write_json_to_cas(self, data):
        url = f"gs://mock-cas/{hash(json.dumps(data, sort_keys=True))}"
        self.files[url] = json.dumps(data).encode('utf-8')
        return url
        
    def write_file_to_cas(self, filename):
        url = f"gs://mock-cas/{hash(filename)}"
        self.files[url] = b"mock content"
        return url
        
    def get_child_keys(self, prefix):
        return [k for k in self.files.keys() if k.startswith(prefix)]


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

@pytest.fixture
def args():
    """Create args object from a sample command line"""
    return parse_args_for_test(["sub", "--name", "test-job", "echo", "hello", "world"])


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
