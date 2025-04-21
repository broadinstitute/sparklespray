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
from tempfile import NamedTemporaryFile
import csv
import io
from dataclasses import dataclass, field
from .hasher import CachingHashFunction, compute_dict_hash
from typing import Tuple

@dataclass
class WorkflowRunArgs:
    retry: bool = False
    parameters: Dict[str, str] = field(default_factory=dict)
    uploads: List[Tuple[str, str]] = field(default_factory=list)
    machine_type: Optional[str] = None
    image: Optional[str] = None

class SparklesInterface:
    """An abstract interface for decoupling this workflow code
    much of the internals of sparkles"""
    def job_exists(self, name: str) -> bool:
        raise NotImplementedError()
    def clear_failed(self, name: str):
        raise NotImplementedError()
    def wait_for_completion(self, name: str):
        raise NotImplementedError()
    def start(self, name: str, command: List[str], params: List[Dict[str,str]], image: Optional[str], uploads: List[Tuple[str,str]], machine_type: Optional[str]):
        raise NotImplementedError()
    def get_job_path_prefix(self) -> str:
        raise NotImplementedError()
    def read_as_bytes(self, path) -> bytes:
        raise NotImplementedError()

class WorkflowStep(BaseModel):
    """Represents a single step in a workflow."""
    command: List[str]
    run_local: bool = False
    image: Optional[str] = None
    parameters_csv: Optional[str] = None
    files_to_localize: Optional[List[str]] = None
    machine_type: Optional[str] = None

class WriteOnCompletion(BaseModel):
    filename: str
    expression: str

class WorkflowDefinition(BaseModel):
    """Represents a workflow definition loaded from a JSON file."""
    steps: List[WorkflowStep]
    files_to_localize: Optional[List[str]] = None
    write_on_completion: Optional[List[WriteOnCompletion]] = None
    
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

# def _run_local_command(command: List[str]) -> int:
#     """Run a command locally and return the exit code."""
#     log.info(f"Running local command: {' '.join(command)}")
#     txtui.user_print(f"Running local command: {' '.join(command)}")
    
#     try:
#         result = subprocess.run(command, check=False)
#         return result.returncode
#     except Exception as e:
#         log.error(f"Error running local command: {str(e)}")
#         return 1

def _load_parameters_from_csv(sparkles: SparklesInterface, csv_path: str) -> List[Dict[str, str]]:
    """Load parameters from a CSV file."""
    if not csv_path:
        return [{}]  # Return a single empty parameter set if no CSV
        
    parameters = []
    csv_content = sparkles.read_as_bytes(csv_path)
    reader = csv.DictReader(io.StringIO(csv_content.decode("utf8")))
    for row in reader:
        parameters.append(dict(row))
    
    if not parameters:
        return [{}]  # Return a single empty parameter set if CSV is empty
        
    return parameters



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


def run_workflow(sparkles: SparklesInterface, job_name: str, workflow_def_path: str, workflow_args: WorkflowRunArgs) -> None:
    """
    Run a workflow defined in a JSON file.
    
    Args:
        sparkles: SparklesInterface instance for job management
        job_name: Name to use for the job
        workflow_def_path: Path to the JSON file containing the workflow definition
        retry: Whether to retry failed tasks
    """
    retry = workflow_args.retry
    command_line_parameters = workflow_args.parameters
    uploads=workflow_args.uploads
    default_image = workflow_args.image
    default_machine_type = workflow_args.machine_type

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

        # create variables for each step
        for i, step in enumerate(workflow.steps):
            step_num = i + 1
            sub_job_name = f"{job_name}-{step_num}"
            variables[f"step.{step_num}.job_name"] = sub_job_name
            variables[f"step.{step_num}.job_path"] = f"{job_path_prefix}/{sub_job_name}"

        # Process each step in the workflow
        for i, step in enumerate(workflow.steps):
            step_num = i + 1
            sub_job_name = variables["job_name"] = variables[f"step.{step_num}.job_name"]
            sub_job_path = variables["job_path"] = variables[f"step.{step_num}.job_path"]

            assert not step.run_local, "Currently not supported because we don't have a way to tell if local jobs are complete yet"
            # if step.run_local:
            #     # Run the command locally
            #     exit_code = _run_local_command(step.command)
            #     if exit_code != 0:
            #         raise RuntimeError(f"Local command in step {step_num} failed with exit code {exit_code}")
            # else:

            if sparkles.job_exists(sub_job_name):
                txtui.user_print(f"Found job {sub_job_name} for step {step_num}/{len(workflow.steps)}")
                if retry:
                    sparkles.clear_failed(sub_job_name)

            txtui.user_print(f"Executing step {step_num}/{len(workflow.steps)}")

            try:
                parameters_csv = step.parameters_csv
                if parameters_csv is not None:
                    parameters_csv = _expand_template(parameters_csv, lambda name: variables[name])
            except KeyError:
                raise Exception(f"Could not expand variable in step {step_num}'s parameter_csv: {repr(step.parameters_csv)}")

            # If this is a fan-out we'll have a list of parameters. If not, we'll get a single record for a single job
            parameters = _load_parameters_from_csv(sparkles, parameters_csv) if parameters_csv else [{}]
            
            try:
                command = [_expand_template(x, _get_var) if isinstance(x, str) else x for x in step.command]
            except KeyError as ex:
                raise Exception(f"Could not expand variable in step {step_num}'s command: {repr(step.command)}: {ex}")

            uploads_for_step = set()

            all_files_to_localize = []
            if workflow.files_to_localize:
                all_files_to_localize.extend(workflow.files_to_localize)                    

            if step.files_to_localize:
                all_files_to_localize.extend(step.files_to_localize)

            for dst in all_files_to_localize:
                src = src_path_by_dest[dst]
                uploads_for_step.add((src, dst))
            
            image = default_image if step.image is None else step.image
            machine_type = default_machine_type if step.machine_type is None else step.machine_type

            sparkles.start(sub_job_name, command, parameters, image, list(uploads_for_step), machine_type)

            sparkles.wait_for_completion(sub_job_name)
            txtui.user_print(f"Executing step {step_num}/{len(workflow.steps)} completed")
            variables["prev_job_name"] = sub_job_name
            variables["prev_job_path"] = sub_job_path
        
        txtui.user_print(f"Workflow execution completed successfully")
        if workflow.write_on_completion:
            for write_on_completion in workflow.write_on_completion:
                txtui.user_print(f"Writing {write_on_completion.filename} as defined in {workflow_def_path}")
                with open(write_on_completion.filename, "wt") as fd:
                    fd.write(_expand_template(write_on_completion.expression, lambda name: variables[name]))

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
    # run_parser.add_argument("--write-var", help="expects parameter of the format VAR:FILENAME. Will write the variable VAR to FILENAME. ")
    run_parser.add_argument("--add-hash-to-job-id", help="if set, will append a hash of the uploaded files and the command to run onto the job id provided. This is to allow sparkles to generate a unique ID for a set of inputs which avoid an identical job from running", action="store_true")
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
    run_parser.add_argument("-i", "--image", help="The docker image to use for steps that don't explictly set one")
    run_parser.add_argument("-m", "--machine-type", help="The machine type to use for steps that don't explictly set one")
    run_parser.add_argument("-p", "--parameter", help="argument should be of the form var=value. The values will be used in expanding variables listed in the step's commands", action="append", type=key_value_pair)
    run_parser.add_argument("-u", "--upload", help="file to upload. Filenames should be specified as either \"src\" or \"src:dst\" where src is the local path and dst is the name that will be used when it is stored on the remote machine. If dst is not specified, it will default to the basename of src", action="append", type=upload_file)
    run_parser.set_defaults(func=workflow_run_cmd)


def workflow_run_cmd(jq: JobQueue, io: IO, cluster: Cluster, config: Config, args):
    """Command handler for 'workflow run'."""
    # Create a SparklesInterface implementation that uses the provided services
    class SparklesImpl(SparklesInterface):
        def __init__(self, target_nodes):
            self.target_nodes = target_nodes

        def read_as_bytes(self, path):
            # this isn't technically right -- clean this up later
            return io.get_as_str_must(path).encode("utf8")

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
            completed_successfully =  watch(io, jq, name, cluster, target_nodes=self.target_nodes)
            if not completed_successfully:
                raise Exception("Job did not complete successfully")
        
        def start(self, name: str, command, params, image: Optional[str], uploads: List[Tuple[str, str]], machine_type: Optional[str]):
            # Submit a new job with the given parameters
            submit_cmd_args=["-n", name, "--no-wait", "--skipifexists"]

            if machine_type:
                submit_cmd_args.extend(["-m", machine_type])

            if image:
                submit_cmd_args.extend(["-i", image])

            for src, dst in uploads:
                submit_cmd_args.extend(["-u", f"{src}:{dst}"])

            with NamedTemporaryFile(suffix=".csv", mode="wt") as tmpcsv:
                w = csv.DictWriter(tmpcsv, params[0].keys())
                w.writeheader()
                for param in params:
                    w.writerow(param)
                tmpcsv.flush()

                if params != [{}]:
                    submit_cmd_args.extend(["--params", tmpcsv.name])

                submit_cmd_args.extend(command)

                print(f"Executing: sub {' '.join(submit_cmd_args)}")
                args = construct_submit_cmd_args(submit_cmd_args)
                exit_code = submit_cmd(jq, io, cluster, args, config)
                if exit_code != 0:
                    raise Exception("Sparkles job failed with exit code {exit_code}")
                   
        def get_job_path_prefix(self) -> str:
            # Return the base path for jobs
            return config.default_url_prefix
    
    parameters = {}
    if args.parameter:
        parameters.update(dict(args.parameter))
    uploads = []
    if args.upload:
        uploads.extend(args.upload)

    workflow_args = WorkflowRunArgs(
        retry=args.retry,
        parameters=parameters,
        uploads=uploads,
        machine_type=args.machine_type,
        image= args.image)

    job_name = args.job_name
    if args.add_hash_to_job_id:
        job_hash = _calc_workflow_hash(config.cache_db_path, workflow_args)[:20]
        job_name = f"{job_name}-{job_hash}"

    return run_workflow(SparklesImpl(args.nodes), job_name, args.workflow_def, workflow_args)


def _calc_workflow_hash(cache_db_path: str, workflow_args :WorkflowRunArgs):
    hash_db = CachingHashFunction(cache_db_path)

    def _hash_upload(src, dst):
        return {"src": src, "dst": dst, "sha256": hash_db.get_sha256(src)}
    
    workflow_dict = {
        "parameters": workflow_args.parameters,
        "uploads": [_hash_upload(src, dst) for src,dst in workflow_args.uploads],
        "machine_type": workflow_args.machine_type,
        "image": workflow_args.image,
    }

    return compute_dict_hash(workflow_dict)
