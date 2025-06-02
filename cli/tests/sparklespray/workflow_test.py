import pytest
from unittest.mock import MagicMock, patch
import os
import tempfile
import json
from sparklespray.workflow import (
    run_workflow,
    SparklesInterface,
    WorkflowDefinition,
    WorkflowRunArgs,
    WriteOnCompletion,
    handle_write_on_completion,
)

from pathlib import Path

class MockSparkles(SparklesInterface):
    def __init__(self):
        self.jobs = {}
        self.job_exists_calls = []
        self.clear_failed_calls = []
        self.wait_for_completion_calls = []
        self.start_calls = []
        self.blobs = {}

    def job_exists(self, name: str) -> bool:
        self.job_exists_calls.append(name)
        return name in self.jobs

    def clear_failed(self, name: str):
        self.clear_failed_calls.append(name)

    def wait_for_completion(self, name: str):
        self.wait_for_completion_calls.append(name)

    def start(self, name: str, command, params, image, uploads, machine_type):
        self.start_calls.append((name, command, params, image, uploads, machine_type))
        self.jobs[name] = True

    def get_job_path_prefix(self) -> str:
        return "/path/to/jobs"

    def read_as_bytes(self, path):
        if path.startswith("gs://"):
            return self.blobs[path]
        else:
            with open(path, "rb") as fd:
                return fd.read()


def create_workflow_file(filename, content):
    with open(filename, "w") as f:
        f.write(json.dumps(content))


def test_run_workflow_basic(tmpdir):
    # Create a simple workflow definition
    workflow_def = {
        "steps": [
            {
                "command": ["echo", "Hello World"],
            }
        ]
    }

    workflow_path = str(tmpdir.join("workflow.json"))
    create_workflow_file(workflow_path, workflow_def)

    sparkles = MockSparkles()
    job_name = "test-job"

    # Run the workflow
    run_workflow(sparkles, job_name, workflow_path, WorkflowRunArgs())

    # Verify the expected calls were made
    assert sparkles.job_exists_calls == ["test-job-1"]
    assert sparkles.start_calls == [
        ("test-job-1", ["echo", "Hello World"], [{}], None, [], None),
    ]
    assert sparkles.wait_for_completion_calls == ["test-job-1"]
    assert len(sparkles.clear_failed_calls) == 0


def test_run_workflow_with_retry(tmpdir):
    # Create a workflow definition
    workflow_def = {
        "steps": [
            {"command": ["echo", "Step 1"], "run_local": False},
            {"command": ["echo", "Step 2"], "run_local": False, "image": "python:3.9"},
        ]
    }

    workflow_path = str(tmpdir.join("workflow.json"))
    create_workflow_file(workflow_path, workflow_def)

    sparkles = MockSparkles()
    # Pre-populate a job to simulate an existing job
    sparkles.jobs["test-job-1"] = True
    job_name = "test-job"

    # Run the workflow with retry flag¬129
    run_workflow(sparkles, job_name, workflow_path, WorkflowRunArgs(retry=True))

    # Verify the expected calls were made
    assert sparkles.job_exists_calls == ["test-job-1", "test-job-2"]
    assert sparkles.clear_failed_calls == ["test-job-1"]
    assert sparkles.start_calls == [
        ("test-job-1", ["echo", "Step 1"], [{}], None, [], None),
        ("test-job-2", ["echo", "Step 2"], [{}], "python:3.9", [], None),
    ]
    assert sparkles.wait_for_completion_calls == ["test-job-1", "test-job-2"]


def test_run_workflow_with_parameters(tmpdir):
    # Create a CSV file with parameters
    params_path = str(tmpdir.join("params.csv"))
    with open(params_path, "w") as f:
        f.write("name,value\nitem1,100\nitem2,200\n")

    # Create a workflow definition that uses the parameters and automatic variables
    workflow_def = {
        "steps": [
            {
                "command": ["process", "{job_name}", "{job_path}"],
                "run_local": False,
                "parameters_csv": params_path,
            }
        ]
    }

    workflow_path = str(tmpdir.join("workflow.json"))
    create_workflow_file(workflow_path, workflow_def)

    sparkles = MockSparkles()
    sparkles.get_job_path_prefix = lambda: "gs://path/to/jobs"
    job_name = "test-job"

    # Run the workflow
    run_workflow(sparkles, job_name, workflow_path, WorkflowRunArgs())

    # Verify the expected calls were made
    assert sparkles.job_exists_calls == ["test-job-1"]
    assert len(sparkles.start_calls) == 1
    name, command, params, image, uploads, machine_type = sparkles.start_calls[0]
    assert name == "test-job-1"
    assert command == ["process", "test-job-1", "gs://path/to/jobs/test-job-1"]
    assert len(params) == 2
    assert params[0] == {"name": "item1", "value": "100"}
    assert params[1] == {"name": "item2", "value": "200"}
    assert image is None
    assert uploads == []
    assert machine_type is None




def test_run_workflow_with_file_localization(tmpdir):
    # Create a workflow definition with files_to_localize and paths_to_localize
    workflow_def = {
        "files_to_localize": ["global_file.txt"],
        "paths_to_localize": [
            {"src": "gs://bucket/global_path.txt", "dst": "global_dest.txt"}
        ],
        "steps": [
            {
                "command": ["process", "data"],
                "files_to_localize": ["step_file.txt"],
                "paths_to_localize": [
                    {"src": "gs://bucket/{job_name}/step_path.txt", "dst": "step_dest.txt"}
                ],
            }
        ]
    }

    workflow_path = str(tmpdir.join("workflow.json"))
    create_workflow_file(workflow_path, workflow_def)

    sparkles = MockSparkles()
    job_name = "test-job"

    # Set up workflow args with uploads that match the files_to_localize
    workflow_args = WorkflowRunArgs(
        uploads=[
            ("local_global_file.txt", "global_file.txt"),
            ("local_step_file.txt", "step_file.txt"),
        ]
    )

    # Run the workflow
    run_workflow(sparkles, job_name, workflow_path, workflow_args)

    # Verify the expected calls were made
    assert sparkles.job_exists_calls == ["test-job-1"]
    assert len(sparkles.start_calls) == 1
    name, command, params, image, uploads, machine_type = sparkles.start_calls[0]
    assert name == "test-job-1"
    assert command == ["process", "data"]
    
    # Check that uploads include both files_to_localize and paths_to_localize
    expected_uploads = {
        ("local_global_file.txt", "global_file.txt"),  # from workflow files_to_localize
        ("local_step_file.txt", "step_file.txt"),      # from step files_to_localize
        ("gs://bucket/global_path.txt", "global_dest.txt"),  # from workflow paths_to_localize
        ("gs://bucket/test-job-1/step_path.txt", "step_dest.txt"),  # from step paths_to_localize (with variable expansion)
    }
    assert set(uploads) == expected_uploads
    assert sparkles.wait_for_completion_calls == ["test-job-1"]


def test_write_on_completion(tmp_path: Path):
    dest = tmp_path / "out"
    handle_write_on_completion(
        WriteOnCompletion(filename=str(dest), expression="sample{version}"),
        {"version": 1},
    )
    assert dest.read_text() == "sample1"

    handle_write_on_completion(
        WriteOnCompletion(filename=str(dest), expression={"version": "{version}"}),
        {"version": 1},
    )
    assert json.loads(dest.read_text()) == {"version": "1"}
