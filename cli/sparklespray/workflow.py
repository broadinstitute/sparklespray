import json
import os
import csv
import subprocess
from typing import Dict, Any, List, Optional, Tuple
from pydantic import BaseModel, Field, validator, root_validator
from .job_queue import JobQueue
from .io_helper import IO
from .cluster_service import Cluster
from .log import log
from . import txtui
from .task_store import STATUS_FAILED
from .submit import submit_cmd, construct_submit_cmd_args
from .config import Config

class WorkflowStep(BaseModel):
    """Represents a single step in a workflow."""
    command: List[str]
    run_local: bool = False
    image: Optional[str] = None
    parameters_csv: Optional[str] = None
    files_to_localize: Optional[List[str]] = None
    
class WorkflowDefinition(BaseModel):
    """Represents a workflow definition loaded from a JSON file."""
    steps: List[WorkflowStep]
    
    @classmethod
    def from_file(cls, file_path: str) -> 'WorkflowDefinition':
        """Load a workflow definition from a JSON file."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Workflow definition file not found: {file_path}")
        
        with open(file_path, 'r') as f:
            try:
                workflow_data = json.load(f)
                return cls.parse_obj(workflow_data)
            except json.JSONDecodeError:
                raise ValueError(f"Invalid JSON in workflow definition file: {file_path}")

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

class SparklesInterface:
    def job_exists(self, name: str) -> bool:
        raise NotImplementedError()
    def clear_failed(self, name: str):
        raise NotImplementedError()
    def wait_for_completion(self, name: str):
        raise NotImplementedError()
    def start(self, name: str, command: List[str], params: List[Dict[str,str]], image: Optional[str], uploads: List[Tuple[str,str]]):
        raise NotImplementedError()
    def get_job_path_prefix(self) -> str:
        raise NotImplementedError()


def _expand_template(value: str, _get_var):
    if value is None:
        return None
        
    result = ""
    i = 0
    length = len(value)
    
    while i < length:
        if i + 1 < length and value[i] == '{' and value[i+1] != '{':
            # Found an opening brace, look for the closing one
            start = i + 1
            i += 1
            while i < length and value[i] != '}':
                i += 1
            
            if i < length:  # Found closing brace
                var_name = value[start:i].strip()
                replacement = _get_var(var_name)
                result += str(replacement)
            else:  # No closing brace found
                result += '{' + value[start:]
        else:
            result += value[i]
        i += 1
        
    return result

from typing import Tuple

def run_workflow(sparkles: SparklesInterface, job_name: str, workflow_def_path: str, retry: bool, command_line_parameters: Dict[str, str], uploads: List[Tuple[str, str]]=[]) -> None:
    """
    Run a workflow defined in a JSON file.
    
    Args:
        sparkles: SparklesInterface instance for job management
        job_name: Name to use for the job
        workflow_def_path: Path to the JSON file containing the workflow definition
        retry: Whether to retry failed tasks
    """
    job_path_prefix = sparkles.get_job_path_prefix()

    src_path_by_dest = {}
    for src, dst in uploads:
        src_path_by_dest[dst] = src

    try:
        # Load and validate the workflow definition
        workflow = WorkflowDefinition.from_file(workflow_def_path)
        
        # Log the start of workflow execution
        log.info(f"Starting workflow execution for job: {job_name}")
        txtui.user_print(f"Starting workflow: {job_name}")

        variables = dict(command_line_parameters)
        def _get_var(name):
            if name.startswith("parameter."):
                return "{"+name[len("parameter."):]+"}"
            return variables[name]

        # Process each step in the workflow
        for i, step in enumerate(workflow.steps):
            step_num = i + 1
            sub_job_name = f"{job_name}-{step_num}"
            variables["job_name"] = job_name
            variables["job_path"] = f"{job_path_prefix}/{job_name}"

            assert not step.run_local, "Currently not supported because we don't have a way to tell if local jobs are complete yet"
            # if step.run_local:
            #     # Run the command locally
            #     exit_code = _run_local_command(step.command)
            #     if exit_code != 0:
            #         raise RuntimeError(f"Local command in step {step_num} failed with exit code {exit_code}")
            # else:

            if sparkles.job_exists(sub_job_name):
                if retry:
                    sparkles.clear_failed(sub_job_name)
            else:
                txtui.user_print(f"Executing step {step_num}/{len(workflow.steps)}")

                try:
                    parameters_csv = step.parameters_csv
                    if parameters_csv is not None:
                        parameters_csv = _expand_template(parameters_csv, lambda name: variables[name])
                except KeyError:
                    raise Exception(f"Could not expand variable in step {step_num}'s parameter_csv: {repr(step.parameters_csv)}")

                # If this is a fan-out we'll have a list of parameters. If not, we'll get a single record for a single job
                parameters = _load_parameters_from_csv(parameters_csv) if parameters_csv else [{}]
                
                try:
                    command = [_expand_template(x, _get_var) if isinstance(x, str) else x for x in step.command]
                except KeyError as ex:
                    raise Exception(f"Could not expand variable in step {step_num}'s command: {repr(step.command)}: {ex}")

                uploads_for_step = []
                if step.files_to_localize:
                    for dst in step.files_to_localize:
                        src = src_path_by_dest[dst]
                        uploads_for_step.append((src, dst))
                sparkles.start(sub_job_name, command, parameters, step.image, uploads_for_step)

            sparkles.wait_for_completion(sub_job_name)
            txtui.user_print(f"Executing step {step_num}/{len(workflow.steps)} completed")
            variables["prev_job_name"] = job_name
            variables["prev_job_path"] = f"{job_path_prefix}/{job_name}"
        
        txtui.user_print(f"Workflow execution completed successfully")
        
    except Exception as e:
        log.error(f"Error running workflow: {str(e)}", exc_info=True)
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
    run_parser.add_argument("--retry", help="if set, will retry running any failed tasks", action="store_true")
    def key_value_pair(value: str):
        key, value = value.split("=", 1)
        return (key, value)
    def upload_file(path: str):
        if ":" in path:
            src, dst = path.split(":", 1)
        else:
            src = path
            dst = os.path.basename(src)
        assert os.path.exists(src), f"Requested upload of {repr(src)} but file does not exist"
        return (src, dst)
    run_parser.add_argument("--nodes", help="max number of nodes to power on at one time", type=int)
    run_parser.add_argument("--parameter,-p", help="argument should be of the form var=value. The values will be used in expanding variables listed in the step's commands", action="append", type=key_value_pair)
    run_parser.add_argument("--upload,-u", help="file to upload. Filenames should be specified as either \"src\" or \"src:dst\" where src is the local path and dst is the name that will be used when it is stored on the remote machine. If dst is not specified, it will default to the basename of src", action="append", type=upload_file)
    run_parser.set_defaults(func=workflow_run_cmd)

def workflow_run_cmd(jq: JobQueue, io: IO, cluster: Cluster, config: Config, args):
    """Command handler for 'workflow run'."""
    # Create a SparklesInterface implementation that uses the provided services
    class SparklesImpl(SparklesInterface):
        def __init__(self, target_nodes):
            self.target_nodes = target_nodes

        def job_exists(self, name: str) -> bool:
            # Check if job exists by trying to get it
            try:
                jq.get_job(name)
                return True
            except:
                return False
        
        def clear_failed(self, name: str):
            # Reset failed tasks to pending
            jq.reset(name, None, statuses_to_clear=[STATUS_FAILED])
        
        def wait_for_completion(self, name: str):
            # Use the existing watch functionality to wait for completion
            from .watch import watch
            watch(jq, io, cluster, name, target_nodes=self.target_nodes)
        
        def start(self, name: str, command, params, image: Optional[str], uploads: List[Tuple[str, str]]):
            # Submit a new job with the given parameters
            from tempfile import NamedTemporaryFile
            import csv
            submit_cmd=["-n", name, "--no-wait"]
            if image:
                submit_cmd.append("-i", image)

            for src, dst in uploads:
                submit_cmd.extend(["-u", f"{src}:{dst}"])

            with NamedTemporaryFile(suffix=".csv") as tmpcsv:
                w = csv.DictWriter(tmpcsv)
                w.writeheader(params[0].keys())
                for param in params:
                    w.writerow(param)

                if params != [{}]:
                    submit_cmd.extend(["--params", tmpcsv.name])

                submit_cmd.extend(command)

                txtui.user_print(f"Executing: sub {' '.join(submit_cmd)}")
                args = construct_submit_cmd_args(submit_cmd)
                submit_cmd(jq, io, cluster, name, args, config)
                   
        def get_job_path_prefix(self) -> str:
            # Return the base path for jobs
            return io._get_url_prefix()
    
    parameters = {}
    if args.parameter:
        parameters.update(dict(args.parameter))
    uploads = []
    if args.upload:
        parameters.extend(uploads)

    return run_workflow(SparklesImpl(args.nodes), args.job_name, args.workflow_def, args.retry, parameters, uploads)
