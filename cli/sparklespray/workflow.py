import json
import os
import csv
import subprocess
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from .job_queue import JobQueue
from .io_helper import IO
from .cluster_service import Cluster
from .log import log
from . import txtui

@dataclass
class WorkflowStep:
    """Represents a single step in a workflow."""
    command: List[str]
    run_local: bool = False
    image: Optional[str] = None
    parameters_csv: Optional[str] = None

class WorkflowDefinition:
    """Represents a workflow definition loaded from a JSON file."""
    
    def __init__(self, workflow_data: Dict[str, Any]):
        self.workflow_data = workflow_data
        self.steps = []
        self.validate()
        self._parse_steps()
        
    @classmethod
    def from_file(cls, file_path: str) -> 'WorkflowDefinition':
        """Load a workflow definition from a JSON file."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Workflow definition file not found: {file_path}")
        
        with open(file_path, 'r') as f:
            try:
                workflow_data = json.load(f)
                return cls(workflow_data)
            except json.JSONDecodeError:
                raise ValueError(f"Invalid JSON in workflow definition file: {file_path}")
    
    def validate(self) -> None:
        """Validate the workflow definition."""
        if not isinstance(self.workflow_data, dict):
            raise ValueError("Workflow definition must be a JSON object")
            
        if 'steps' not in self.workflow_data:
            raise ValueError("Workflow definition must contain a 'steps' array")
            
        if not isinstance(self.workflow_data['steps'], list):
            raise ValueError("'steps' must be an array")
            
        for i, step in enumerate(self.workflow_data['steps']):
            if not isinstance(step, dict):
                raise ValueError(f"Step {i} must be an object")
                
            if 'command' not in step:
                raise ValueError(f"Step {i} must contain a 'command' array")
                
            if not isinstance(step['command'], list):
                raise ValueError(f"'command' in step {i} must be an array of strings")
                
            for cmd_part in step['command']:
                if not isinstance(cmd_part, str):
                    raise ValueError(f"Command parts in step {i} must be strings")
                    
            if 'run_local' in step and not isinstance(step['run_local'], bool):
                raise ValueError(f"'run_local' in step {i} must be a boolean")
                
            if 'image' in step and step['image'] is not None and not isinstance(step['image'], str):
                raise ValueError(f"'image' in step {i} must be a string or null")
                
            if 'parameters_csv' in step and step['parameters_csv'] is not None:
                if not isinstance(step['parameters_csv'], str):
                    raise ValueError(f"'parameters_csv' in step {i} must be a string or null")
                if not os.path.exists(step['parameters_csv']):
                    raise ValueError(f"Parameters CSV file not found: {step['parameters_csv']}")
    
    def _parse_steps(self) -> None:
        """Parse the steps from the workflow definition."""
        self.steps = []
        for step_data in self.workflow_data.get('steps', []):
            step = WorkflowStep(
                command=step_data['command'],
                run_local=step_data.get('run_local', False),
                image=step_data.get('image'),
                parameters_csv=step_data.get('parameters_csv')
            )
            self.steps.append(step)
    
    def get_steps(self) -> List[WorkflowStep]:
        """Get the list of workflow steps."""
        return self.steps


def _run_local_command(command: List[str]) -> int:
    """Run a command locally and return the exit code."""
    log.info(f"Running local command: {' '.join(command)}")
    txtui.user_print(f"Running local command: {' '.join(command)}")
    
    try:
        result = subprocess.run(command, check=False)
        return result.returncode
    except Exception as e:
        log.error(f"Error running local command: {str(e)}")
        return 1

def _load_parameters_from_csv(csv_path: str) -> List[Dict[str, str]]:
    """Load parameters from a CSV file."""
    if not csv_path:
        return [{}]  # Return a single empty parameter set if no CSV
        
    parameters = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            parameters.append(dict(row))
    
    if not parameters:
        return [{}]  # Return a single empty parameter set if CSV is empty
        
    return parameters

def run_workflow(jq: JobQueue, io: IO, cluster: Cluster, job_name: str, workflow_def_path: str) -> None:
    """
    Run a workflow defined in a JSON file.
    
    Args:
        jq: JobQueue instance
        io: IO helper instance
        cluster: Cluster instance
        job_name: Name to use for the job
        workflow_def_path: Path to the JSON file containing the workflow definition
    """
    try:
        # Load and validate the workflow definition
        workflow = WorkflowDefinition.from_file(workflow_def_path)
        
        # Log the start of workflow execution
        log.info(f"Starting workflow execution for job: {job_name}")
        txtui.user_print(f"Starting workflow: {job_name}")
        
        # Process each step in the workflow
        for i, step in enumerate(workflow.get_steps()):
            step_num = i + 1
            txtui.user_print(f"Executing step {step_num}/{len(workflow.get_steps())}")
            
            if step.run_local:
                # Run the command locally
                exit_code = _run_local_command(step.command)
                if exit_code != 0:
                    raise RuntimeError(f"Local command in step {step_num} failed with exit code {exit_code}")
            else:
                # This is a distributed job
                parameters = _load_parameters_from_csv(step.parameters_csv)
                
                # TODO: Submit the job to the cluster
                # This would involve:
                # 1. Creating a job with the specified command
                # 2. Setting the docker image if specified
                # 3. Submitting the job with the parameters
                
                txtui.user_print(f"Would submit distributed job for step {step_num} with {len(parameters)} parameter sets")
                txtui.user_print(f"  Command: {' '.join(step.command)}")
                if step.image:
                    txtui.user_print(f"  Image: {step.image}")
                else:
                    txtui.user_print("  Using default image")
                
                # For now, just log that we would submit a job
                log.info(f"Would submit job for step {step_num} with command: {step.command}")
        
        txtui.user_print(f"Workflow execution completed successfully")
        
    except Exception as e:
        log.error(f"Error running workflow: {str(e)}")
        txtui.user_print(f"Error: {str(e)}")
        raise

def add_workflow_cmd(subparser):
    """Add the workflow command to the CLI parser."""
    parser = subparser.add_parser("workflow", help="Manage and run workflows")
    workflow_subparser = parser.add_subparsers(dest="workflow_cmd")
    
    # Add the 'run' subcommand
    run_parser = workflow_subparser.add_parser("run", help="Run a workflow")
    run_parser.add_argument("job_name", help="Name to use for the job")
    run_parser.add_argument("workflow_def", help="Path to a JSON file containing the workflow definition")
    run_parser.set_defaults(func=workflow_run_cmd)

def workflow_run_cmd(jq: JobQueue, io: IO, cluster: Cluster, args):
    """Command handler for 'workflow run'."""
    return run_workflow(jq, io, cluster, args.job_name, args.workflow_def)
