import pytest
from unittest.mock import MagicMock, patch
import os
import tempfile
import json
from sparklespray.workflow import run_workflow, SparklesInterface, WorkflowDefinition

class MockSparkles(SparklesInterface):
    def __init__(self):
        self.jobs = {}
        self.job_exists_calls = []
        self.clear_failed_calls = []
        self.wait_for_completion_calls = []
        self.start_calls = []
        
    def job_exists(self, name: str) -> bool:
        self.job_exists_calls.append(name)
        return name in self.jobs
    
    def clear_failed(self, name: str):
        self.clear_failed_calls.append(name)
    
    def wait_for_completion(self, name: str):
        self.wait_for_completion_calls.append(name)
    
    def start(self, name: str, command, params, image, uploads=None):
        self.start_calls.append((name, command, params, image, uploads))
        self.jobs[name] = True
        
    def get_job_path_prefix(self) -> str:
        return "/path/to/jobs"

def create_workflow_file(filename, content):
    with open(filename, 'w') as f:
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
    run_workflow(sparkles, job_name, workflow_path, False, {})
    
    # Verify the expected calls were made
    assert sparkles.job_exists_calls == ["test-job-1"]
    assert sparkles.start_calls == [("test-job-1", ["echo", "Hello World"], [{}], None)]
    assert sparkles.wait_for_completion_calls == ["test-job-1"]
    assert len(sparkles.clear_failed_calls) == 0

def test_run_workflow_with_retry(tmpdir):
    # Create a workflow definition
    workflow_def = {
        "steps": [
            {
                "command": ["echo", "Step 1"],
                "run_local": False
            },
            {
                "command": ["echo", "Step 2"],
                "run_local": False,
                "image": "python:3.9"
            }
        ]
    }
    
    workflow_path = str(tmpdir.join("workflow.json"))
    create_workflow_file(workflow_path, workflow_def)

    sparkles = MockSparkles()
    # Pre-populate a job to simulate an existing job
    sparkles.jobs["test-job-1"] = True
    job_name = "test-job"
    
    # Run the workflow with retry flag
    run_workflow(sparkles, job_name, workflow_path, True, {})
    
    # Verify the expected calls were made
    assert sparkles.job_exists_calls == ["test-job-1", "test-job-2"]
    assert sparkles.clear_failed_calls == ["test-job-1"]
    assert sparkles.start_calls == [("test-job-2", ["echo", "Step 2"], [{}], "python:3.9")]
    assert sparkles.wait_for_completion_calls == ["test-job-1", "test-job-2"]

def test_run_workflow_with_parameters(tmpdir):
    # Create a CSV file with parameters
    params_path = str(tmpdir.join("params.csv"))
    with open(params_path, 'w') as f:
        f.write("name,value\nitem1,100\nitem2,200\n")
    
    # Create a workflow definition that uses the parameters and automatic variables
    workflow_def = {
        "steps": [
            {
                "command": ["process", "{job_name}", "{job_path}"],
                "run_local": False,
                "parameters_csv": params_path
            }
        ]
    }
    
    workflow_path = str(tmpdir.join("workflow.json"))
    create_workflow_file(workflow_path, workflow_def)

    sparkles = MockSparkles()
    sparkles.get_job_path_prefix = lambda: "/path/to/jobs"
    job_name = "test-job"
    
    # Run the workflow
    run_workflow(sparkles, job_name, workflow_path, False, {})
    
    # Verify the expected calls were made
    assert sparkles.job_exists_calls == ["test-job-1"]
    assert len(sparkles.start_calls) == 1
    name, command, params, image = sparkles.start_calls[0]
    assert name == "test-job-1"
    assert command == ["process", "test-job", "/path/to/jobs/test-job"]
    assert len(params) == 2
    assert params[0] == {"name": "item1", "value": "100"}
    assert params[1] == {"name": "item2", "value": "200"}
    assert image is None
