import pytest
from unittest.mock import MagicMock, patch, mock_open

from sparklespray.commands.submit import submit_cmd
from sparklespray.job_queue import JobQueue
from sparklespray.batch_api import ClusterAPI
from sparklespray.config import Config
from sparklespray.errors import UserError

from .factories import DatastoreClientSimulator, MockIO


@pytest.fixture
def mock_io():
    return MockIO()


@pytest.fixture
def datastore_client():
    return DatastoreClientSimulator()

@pytest.fixture
def task_storage(datastore_client):
    from sparklespray.task_store import TaskStore
    return TaskStore(datastore_client)

@pytest.fixture
def job_queue(datastore_client, task_storage):
    from sparklespray.job_store import JobStore
    
    # Create actual JobStore and TaskStore instances
    job_storage = JobStore(datastore_client)
    
    # Create a real JobQueue instance
    job_queue = JobQueue(datastore_client, job_storage, task_storage)
    
    return job_queue


@pytest.fixture
def cluster_api():
    api = MagicMock(spec=ClusterAPI)
    api.create_job = MagicMock(return_value="mock-job-id")
    return api


@pytest.fixture
def config(tmpdir):
    from sparklespray.config import load_config
    
    # Create a temporary config file
    config_file = tmpdir.join( ".sparkles")

    config_content = f"""
[config]
project = test-project
account = mock@sample.com
region = us-central1
default_image = ubuntu:latest
machine_type = n1-standard-1
cas_url_prefix = gs://mock-cas
default_url_prefix = gs://mock-results
debug_log_prefix = gs://mock-logs
monitor_port = 8080
sparklesworker_image = sparklesworker:latest
"""
    config_file.write_text(config_content, "utf8")
    
    config = load_config(str(config_file), verbose=False, gcloud_config_file=None, overrides={})
    
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
def test_submit_cmd_basic(mock_watch, job_queue, mock_io, datastore_client, cluster_api, config, temp_file, task_storage):
    # Setup mocks
    mock_watch.return_value = True

    # Set up IO mock to handle file existence checks
    mock_io.bulk_exists_results = {}  # All files need upload
    
    # Use args with push parameter
    args = parse_args_for_test(["sub", "--name", "test-job", "--push", temp_file, "echo", "hello", "world"])
    
    # Run the function under test
    result = submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config)
    
    # Verify the result
    assert result == 0

    # verify that a job with the right name exists, with one task    
    job = job_queue.get_job_must("test-job")
    original_job_uuid = job.metadata["UUID"]
    tasks = task_storage.get_tasks(job.job_id)
    assert len(tasks) == 1

    # Now verify submitting a job with the same name results cleaning the old job and creating a new one
    args = parse_args_for_test(["sub", "--name", "test-job", "echo", "hello", "world"])
    result = submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config)
    
    assert result == 0
    new_job = job_queue.get_job_must("test-job")
    assert original_job_uuid != new_job.metadata["UUID"]

    original_job_uuid = new_job.metadata["UUID"]
    # Now verify resubmitting with --skipifexists produces no error
    args = parse_args_for_test(["sub", "--name", "test-job", "--skipifexists", "echo", "hello", "world"])
    result = submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config)
    
    # Verify the result - should exit early
    assert result == 0
    new_job = job_queue.get_job_must("test-job")
    assert original_job_uuid == new_job.metadata["UUID"]

    


@patch("sparklespray.commands.submit.watch")
def test_submit_cmd_when_sub_job_exists_abort(mock_watch, job_queue, mock_io, datastore_client, cluster_api, config, task_storage):
    mock_watch.return_value = True

    args = parse_args_for_test(["sub", "--name", "test-job", "echo", "hello", "world"])
    result = submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config)
    assert result == 0
    original_job_uuid = job_queue.get_job_must("test-job").metadata["UUID"]

    config.when_sub_job_exists = "abort"
    args = parse_args_for_test(["sub", "--name", "test-job", "echo", "hello", "world"])
    with pytest.raises(UserError):
        submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config)

    # the existing job must be untouched
    assert job_queue.get_job_must("test-job").metadata["UUID"] == original_job_uuid


@patch("sparklespray.commands.submit.watch")
def test_submit_cmd_when_sub_job_exists_confirm(mock_watch, job_queue, mock_io, datastore_client, cluster_api, config, task_storage):
    mock_watch.return_value = True

    args = parse_args_for_test(["sub", "--name", "test-job", "echo", "hello", "world"])
    result = submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config)
    assert result == 0
    original_job_uuid = job_queue.get_job_must("test-job").metadata["UUID"]

    config.when_sub_job_exists = "confirm"

    with patch("sparklespray.commands.submit._confirm_overwrite", return_value=False):
        args = parse_args_for_test(["sub", "--name", "test-job", "echo", "hello", "world"])
        with pytest.raises(UserError):
            submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config)
    assert job_queue.get_job_must("test-job").metadata["UUID"] == original_job_uuid

    with patch("sparklespray.commands.submit._confirm_overwrite", return_value=True):
        args = parse_args_for_test(["sub", "--name", "test-job", "echo", "hello", "world"])
        result = submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config)
    assert result == 0
    assert job_queue.get_job_must("test-job").metadata["UUID"] != original_job_uuid


@patch("sparklespray.commands.submit.watch")
def test_submit_cmd_when_sub_job_exists_ignored_for_new_job(mock_watch, job_queue, mock_io, datastore_client, cluster_api, config, task_storage):
    mock_watch.return_value = True

    config.when_sub_job_exists = "abort"

    args = parse_args_for_test(["sub", "--name", "brand-new-job", "echo", "hello", "world"])
    result = submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config)
    assert result == 0


@patch("sparklespray.commands.submit.watch")
def test_submit_cmd_reuses_cluster_when_env_unchanged(mock_watch, job_queue, mock_io, datastore_client, cluster_api, config, task_storage):
    mock_watch.return_value = True

    args = parse_args_for_test(["sub", "--name", "reuse-job", "--no-wait", "echo", "hi"])
    assert submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config) == 0

    # resubmit with identical runtime config: the lingering cluster must be kept
    with patch("sparklespray.commands.submit.delete") as mock_delete, patch(
        "sparklespray.commands.submit.kill"
    ) as mock_kill, patch("sparklespray.commands.submit.submit") as mock_submit:
        args = parse_args_for_test(["sub", "--name", "reuse-job", "--no-wait", "echo", "hi"])
        assert submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config) == 0

    mock_kill.assert_not_called()
    mock_delete.assert_called_once()
    assert mock_delete.call_args.kwargs.get("stop_cluster") is False
    mock_submit.assert_called_once()


@patch("sparklespray.commands.submit.watch")
def test_submit_cmd_rebuilds_cluster_when_env_changes(mock_watch, job_queue, mock_io, datastore_client, cluster_api, config, task_storage):
    mock_watch.return_value = True

    args = parse_args_for_test(["sub", "--name", "rebuild-job", "--machine-type", "n1-standard-1", "--no-wait", "echo", "hi"])
    assert submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config) == 0

    # resubmit with a different machine type (changes the cluster name): the old
    # cluster must be torn down and rebuilt
    with patch("sparklespray.commands.submit.delete") as mock_delete, patch(
        "sparklespray.commands.submit.kill"
    ) as mock_kill, patch("sparklespray.commands.submit.submit") as mock_submit:
        args = parse_args_for_test(["sub", "--name", "rebuild-job", "--machine-type", "n1-standard-2", "--no-wait", "echo", "hi"])
        assert submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config) == 0

    mock_kill.assert_called_once()
    mock_delete.assert_called_once()
    assert mock_delete.call_args.kwargs.get("stop_cluster") is True
    mock_submit.assert_called_once()


@patch("sparklespray.commands.submit.watch")
def test_submit_cmd_rebuilds_cluster_when_non_env_cluster_input_changes(mock_watch, job_queue, mock_io, datastore_client, cluster_api, config, task_storage):
    # work_root_dir feeds into the cluster name but was NOT part of the old
    # image+machine_type env-hash, so this case only triggers a rebuild once the
    # decision is based on the full cluster identity.
    mock_watch.return_value = True

    args = parse_args_for_test(["sub", "--name", "workdir-job", "--no-wait", "echo", "hi"])
    assert submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config) == 0

    config.work_root_dir = "/mnt/disks/somewhere_else"

    with patch("sparklespray.commands.submit.delete") as mock_delete, patch(
        "sparklespray.commands.submit.kill"
    ) as mock_kill, patch("sparklespray.commands.submit.submit") as mock_submit:
        args = parse_args_for_test(["sub", "--name", "workdir-job", "--no-wait", "echo", "hi"])
        assert submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config) == 0

    mock_kill.assert_called_once()
    mock_delete.assert_called_once()
    assert mock_delete.call_args.kwargs.get("stop_cluster") is True
    mock_submit.assert_called_once()


@patch("sparklespray.commands.submit.watch")
def test_submit_cmd_with_seq_parameter(mock_watch, job_queue, mock_io, datastore_client, cluster_api, config, task_storage):
    # Setup mocks
    mock_watch.return_value = True
    
    # Use args with seq parameter
    args = parse_args_for_test(["sub", "--name", "test-job", "--seq", "3", "echo", "hello", "world"])
    
    # Run the function under test
    result = submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config)
    
    # Verify the result
    assert result == 0
    
    # verify three tasks created
    job = job_queue.get_job_must("test-job")
    tasks = task_storage.get_tasks(job.job_id)
    assert len(tasks) == 3

@patch("sparklespray.commands.submit.watch")
def test_submit_cmd_flex_without_gpu_rejected(mock_watch, job_queue, mock_io, datastore_client, cluster_api, config, task_storage):
    mock_watch.return_value = True

    config.provision_mode = "flex"

    args = parse_args_for_test(["sub", "--name", "flex-job", "echo", "hello", "world"])

    with pytest.raises(UserError):
        submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config)


@patch("sparklespray.commands.submit.watch")
def test_submit_cmd_flex_with_gpu_allowed(mock_watch, job_queue, mock_io, datastore_client, cluster_api, config, task_storage):
    mock_watch.return_value = True

    config.provision_mode = "flex"

    args = parse_args_for_test([
        "sub", "--name", "flex-gpu-job",
        "--machine-type", "n1-standard-4",
        "--add-gpu", "nvidia-tesla-t4",
        "echo", "hello", "world",
    ])

    result = submit_cmd(job_queue, mock_io, datastore_client, cluster_api, args, config)

    assert result == 0


@patch("sparklespray.commands.submit.watch")
def test_submit_cmd_complex_args(mock_watch, job_queue, mock_io, datastore_client, cluster_api, config, temp_file, task_storage):
    # Setup mocks
    mock_watch.return_value = True
    
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
    
    # verify that a job with the right name exists, with one task    
    job = job_queue.get_job_must("complex-job")
    tasks = task_storage.get_tasks(job.job_id)
    assert len(tasks) ==1 