import argparse
import copy
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple
from ..errors import UserError
from google.cloud import datastore

from pydantic import BaseModel
from ..batch_api import ClusterAPI

import sparklespray
from ..cluster_service import MinConfig, Cluster, create_cluster
from ..key_store import KeyStore
import hashlib
import uuid
from .kill import kill
from .delete import delete
import sparklespray
from .watch import watch

from .. import txtui
from .delete import delete
from .watch import watch
from ..config import Config
from ..csv_utils import read_csv_as_dicts
from ..hasher import CachingHashFunction
from ..io_helper import IO
from ..job_queue import JobQueue
from ..log import log
from ..model import LOCAL_SSD, MachineSpec, PersistentDiskMount, SubmitConfig
from ..spec import SrcDstPair, make_spec_from_command
from ..util import get_timestamp, random_string, url_join
from datetime import datetime
from ..certgen import create_self_signed_cert
from ..hasher import compute_dict_hash
from ..worker_job import create_job_spec


class ExistingJobException(Exception):
    pass


# spec should have three rough components:
#   common: keys shared by everything
#   tasks: list of dicts which are per-task
#   resources: resource requirements, used to specify container needs
#
#   a task spec should be defined as:
#   log_path: string ( merged helper, stdout, stderr)
#   command: string
#   command_result_path: string ( file containing the retcode info )
#   command_result_url: string ( file containing the retcode info )
#   uploads: list of {src, dst_url}
#   downloads: list of {src_url, dst}  if src_url is a local path, rewrite to be CAS url


def expand_task_spec(common, task):
    "returns a list of task specs"
    # merge the common attrs and the per task attrs
    task_spec = copy.deepcopy(common)
    for attr in ["helper_log", "command", "uploads"]:
        if attr in task:
            task_spec[attr] = task[attr]
    task_spec["downloads"].extend(task.get("downloads", []))
    return task_spec


def rewrite_url_with_prefix(url, default_url_prefix):
    # look to see if we have a rooted url, or a relative path
    a = [url, default_url_prefix]
    if not (":" in url):
        if not default_url_prefix.endswith("/"):
            default_url_prefix += "/"
        if url.startswith("/"):
            url = url[1:]
        url = default_url_prefix + url
        if url.endswith("/"):
            url = url[:-1]
    assert not ("//" in url[4:]), "url=%s, default_url_prefix=%s" % (url, a)
    return url


def rewrite_url_in_dict(d, prop_name, default_url_prefix):
    if not (prop_name in d):
        return d

    d = dict(d)
    url = d[prop_name]
    d[prop_name] = rewrite_url_with_prefix(url, default_url_prefix)
    return d


def rewrite_downloads(io, downloads, default_url_prefix):
    def rewrite_download(url):
        if "src" in url:
            # upload to CAS if the source isn't a url
            src_url = io.write_file_to_cas(url["src"])
        else:
            src_url = url["src_url"]

        dst = os.path.normpath(url["dst"])
        # only allow paths to be relative to working directory
        assert not (dst.startswith("../"))
        assert not (dst.startswith("/"))

        return dict(
            src_url=src_url,
            dst=dst,
            executable=url.get("executable", False),
            is_cas_key=url.get("is_cas_key", False),
            symlink_safe=url.get("symlink_safe", False),
        )

    src_expanded = [rewrite_download(x) for x in downloads]

    return [rewrite_url_in_dict(x, "src_url", default_url_prefix) for x in src_expanded]


def expand_tasks(spec, io, default_url_prefix, default_job_url_prefix):
    common = spec["common"]
    common["downloads"] = rewrite_downloads(
        io, common.get("downloads", []), default_url_prefix
    )
    # common['uploads'] = rewrite_uploads(common.get('uploads', []), default_job_url_prefix)

    tasks = []
    for task_i, spec_task in enumerate(spec["tasks"]):
        task_url_prefix = "{}/{}".format(default_job_url_prefix, task_i + 1)
        task = expand_task_spec(common, spec_task)
        task["downloads"] = rewrite_downloads(io, task["downloads"], default_url_prefix)
        # task['uploads'] = rewrite_uploads(task['uploads'], task_url_prefix)
        task["stdout_url"] = rewrite_url_with_prefix(
            task["stdout_url"], task_url_prefix
        )
        task["command_result_url"] = rewrite_url_with_prefix(
            task["command_result_url"], task_url_prefix
        )
        task["parameters"] = spec_task["parameters"]

        assert set(spec_task.keys()).issubset(
            task.keys()
        ), "task before expand: {}, after expand: {}".format(
            spec_task.keys(), task.keys()
        )

        tasks.append(task)
    return tasks


def _make_cluster_name(
    job_name: str, image: str, machine_spec: MachineSpec, unique_name: bool
):

    if unique_name:
        return "l-" + random_string(20)
    else:
        machine_json = json.dumps(machine_spec.model_dump(), sort_keys=True)
        # print(f"machine_json: {machine_json}")
        hash = hashlib.md5()
        hash.update(f"{job_name}-{image}-{sparklespray.__version__}".encode("utf8"))
        hash.update(machine_json.encode("utf8"))
        return f"c-{hash.hexdigest()[:20]}"


from ..gcp_permissions import has_access_to_docker_image


def update_running_status(job_id: str, cluster: Cluster, jq: JobQueue):
    # todo: refactor into a method which updates job status
    possibly_running = jq.get_possibily_running_tasks(job_id)
    running = []
    orphaned = []
    for task in possibly_running:
        if cluster.is_live_owner(task.owner):
            running.append(task)
        else:
            orphaned.append(task)

    for task in orphaned:
        jq.reset_task(task.task_id)

    return running


def submit(
    jq: JobQueue,
    io: IO,
    job_id: str,
    spec: dict,
    config: SubmitConfig,
    datastore_client,
    cluster: Cluster,
    metadata: Dict[str, str] = {},
    clean_if_exists: bool = False,
):
    """
    Submit a job to the Sparklespray execution system.

    This function handles the core job submission process, including:
    - Generating or retrieving SSL certificates for secure communication
    - Expanding task specifications
    - Creating the cluster configuration
    - Submitting the job to the job queue

    Args:
        jq: JobQueue instance for storing job and task information
        io: IO helper for interacting with cloud storage
        job_id: Unique identifier for the job
        spec: Job specification dictionary containing tasks and common configuration
        config: SubmitConfig with machine and environment configuration
        datastore_client: Google Cloud Datastore client
        cluster: Cluster instance for managing compute resources
        metadata: Optional dictionary of metadata to attach to the job
        clean_if_exists: If True, remove any existing job with the same ID before submission

    Raises:
        ExistingJobException: If a job with the same ID exists and clean_if_exists is False
    """

    key_store = KeyStore(datastore_client)
    cert, key = key_store.get_cert_and_key()
    if cert is None:
        log.info("No cert and key for cluster found -- generating now")

        cert, key = create_self_signed_cert()
        key_store.set_cert_and_key(cert, key)

    log.info("Submitting job with id: %s", job_id)

    boot_volume = config.boot_volume
    default_url_prefix = config.default_url_prefix

    default_job_url_prefix = url_join(default_url_prefix, job_id)
    tasks = expand_tasks(spec, io, default_url_prefix, default_job_url_prefix)
    task_spec_urls = []
    command_result_urls = []
    log_urls = []

    # TODO: When len(tasks) is a fair size (>100) this starts taking a noticable amount of time.
    # Perhaps store tasks in a single blob?  Or do write with multiple requests in parallel?
    for task in tasks:
        url = io.write_json_to_cas(task)
        task_spec_urls.append(url)
        command_result_urls.append(task["command_result_url"])
        log_urls.append(task["stdout_url"])

    machine_specs = MachineSpec(
        service_account_email=config.service_account_email,
        boot_volume=boot_volume,
        mounts=config.mounts,
        work_root_dir=config.work_root_dir,
        machine_type=config.machine_type,
    )

    image = config.image
    cluster_name = _make_cluster_name(job_id, image, machine_specs, False)

    existing_job = jq.get_job_optional(job_id)
    if existing_job is not None:
        if clean_if_exists:
            log.info(
                f'Found existing job with id "{job_id}". Cleaning it up before resubmitting'
            )

            running = update_running_status(job_id, cluster, jq)

            if len(running) > 0:
                raise ExistingJobException(
                    'Could not remove running job "{job_id}", aborting! (Run "sparkles kill {job_id}" if you want it to stop and resubmit)'
                )

            # delete the old job so we can create a new one
            jq.delete_job(job_id)

            # do a little housekeeping on the cluster as well, otherwise these old requests will accumulate
            cluster.delete_complete_requests()

        else:
            raise ExistingJobException(
                'Existing job with id "{}", aborting!'.format(job_id)
            )

    # if len(config.mounts) > 1:
    # assert (
    #     len(config.zones) == 1
    # ), "Cannot create jobs in multiple zones if you are mounting PD volumes"
    #        cluster.ensure_named_volumes_exist(config.zones[0], config.mounts)

    job = create_job_spec(
        job_id,
        config.sparklesworker_image,
        config.work_root_dir,
        config.image,
        cluster_name,
        config.project,
        config.monitor_port,
        config.service_account_email,
        config.machine_type,
        config.region,
        config.boot_volume,
        config.mounts,
    )

    pipeline_spec = job.model_dump_json()

    max_preemptable_attempts = (
        config.target_node_count * config.max_preemptable_attempts_scale
    )

    jq.submit(
        job_id,
        list(zip(task_spec_urls, command_result_urls, log_urls)),
        pipeline_spec,
        metadata,
        cluster_name,
        config.target_node_count,
        max_preemptable_attempts,
    )


def new_job_id():
    return get_timestamp() + "-" + uuid.uuid4().hex[:4]


def _split_source_dest(file):
    if file.startswith("gs://"):
        index = file.find(":", 5)
    else:
        index = file.find(":")

    if index >= 0:
        source, dest = file[:index], file[index + 1 :]
    else:
        source = dest = file

    if dest.startswith("/") or dest.startswith("gs://"):
        dest = os.path.basename(dest)

    return source, dest


def _add_name_pair_to_list(file):
    if file.startswith("@"):
        # if filename starts with @, read this file for the actual files to include
        included_files = []
        with open(file[1:], "rt") as fd:
            for line in fd:
                line = line.strip()
                if len(line) == 0:
                    continue
                included_files.extend(_add_name_pair_to_list(line))
        return included_files
    else:
        return [_split_source_dest(file)]


def _parse_push(files):
    filenames = []
    for file in files:
        filenames.extend(_add_name_pair_to_list(file))
    return filenames


def expand_files_to_upload(io, filenames):
    pairs = []
    for src, dst in _parse_push(filenames):
        if src.startswith("gs://"):
            if io.exists(src):
                pairs.append(SrcDstPair(src, dst))
            else:
                child_keys = io.get_child_keys(src)
                assert len(child_keys) > 0, "The object {} does not exist".format(src)
                for child_key in child_keys:
                    pairs.append(SrcDstPair(child_key, dst + child_key[len(src) :]))
        else:
            pairs.append(SrcDstPair(src, dst))
    return pairs


def _setup_parser_for_sub_command(parser):
    parser.add_argument(
        "--machine-type",
        "-m",
        help="The machine type that should be used when starting up instances at GCP (overrides the 'machine_type' parameter in the .sparkles config file)",
        dest="machine_type",
        default=None,
    )
    parser.add_argument(
        "--file",
        "-f",
        help="Job specification file (in JSON).  Only needed if command is not specified.",
    )
    parser.add_argument(
        "--push",
        "-u",
        action="append",
        default=[],
        help="Path to a local file which should be uploaded to working directory of command before execution starts.  If filename starts with a '@' the file is interpreted as a list of files which need to be uploaded.",
    )
    parser.add_argument(
        "--image",
        "-i",
        help="Name of docker image to run job within.  Defaults to value from sparkles config file.",
    )
    parser.add_argument("--name", "-n", help="The name to assign to the job")
    parser.add_argument(
        "--seq",
        type=int,
        help="Parameterize the command by 'index'.  Submitting with --seq=10 will submit 10 commands with a parameter 'index' varied from 1 to 10",
    )
    parser.add_argument(
        "--params",
        "-p",
        help="Parameterize the command by the rows in the specified CSV file.  If the CSV file has 5 rows, then 5 commands will be submitted.",
    )
    # parser.add_argument("--fetch", help="After run is complete, automatically download the results")
    parser.add_argument(
        "--skipkube",
        action="store_true",
        dest="skip_kube_submit",
        help="Do all steps except submitting the job to kubernetes",
    )
    parser.add_argument(
        "--no-wait",
        action="store_false",
        dest="wait_for_completion",
        help="Exit immediately after submission instead of waiting for job to complete",
    )
    parser.add_argument(
        "--results",
        action="append",
        help="Wildcard to use to find results which will be uploaded.  (defaults to '*')  Can be specified multiple times",
        default=None,
        dest="results_wildcards",
    )

    parser.add_argument(
        "--ignore",
        action="append",
        help="Wildcard to used for identifying which files should be excluding files from upload at end of job. Can be specified multiple times",
        default=None,
        dest="exclude_wildcards",
    )

    parser.add_argument(
        "--nodes",
        help="Max number of VMs to start up to run these tasks",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--cd",
        help="The directory to change to before executing the command",
        default=".",
        dest="working_dir",
    )
    parser.add_argument(
        "--skipifexists",
        help="If the job with this name already exists, do not submit a new one",
        action="store_true",
    )
    parser.add_argument(
        "--symlinks",
        help="When localizing files, use symlinks instead of copying files into location. This should only be used when the uploaded files will not be modified by the job.",
        action="store_true",
    )
    # parser.add_argument(
    #     "--use-vm",
    #     help="Instead of powering on a new VM, use an existing GCP VM by providing it's ssh connect string (mostly for testing)",
    # )
    parser.add_argument(
        "--rerun",
        help="If set, will download all of the files from previous execution of this job to worker before running",
        action="store_true",
    )

    def key_value_pair(text):
        key, value = text.split("=", 1)
        return (key, value)

    parser.add_argument(
        "--metadata",
        help="adds metdata to job. parameter should be of the form key=value",
        action="append",
        type=key_value_pair,
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)


def add_submit_cmd(subparser):
    parser = subparser.add_parser(
        "sub", help="Submit a command (or batch of commands) for execution"
    )
    parser.set_defaults(func=submit_cmd)
    _setup_parser_for_sub_command(parser)


def construct_submit_cmd_args(unparsed_args: List[str]):
    # create a temp parser for converting the unparsed_args to an instance of args that `submit_cmd` will
    # accept
    parser = argparse.ArgumentParser()
    _setup_parser_for_sub_command(parser)
    args = parser.parse_args(unparsed_args)
    return args


def submit_cmd(
    jq: JobQueue,
    io: IO,
    datastore_client: datastore.Client,
    cluster_api: ClusterAPI,
    args: argparse.Namespace,
    config: Config,
):
    metadata: Dict[str, str] = {
        "UUID": str(uuid.uuid4())
    }  # assign it a unique ID so we can recognize when a job has been resubmitted with the same name

    if args.image:
        image = args.image
    else:
        image = config.default_image

    boot_volume = config.boot_volume
    default_url_prefix = config.default_url_prefix

    job_id = args.name
    if job_id is None:
        job_id = new_job_id()

    target_node_count = args.nodes
    machine_type = config.machine_type
    if args.machine_type:
        machine_type = args.machine_type

    cas_url_prefix = config.cas_url_prefix
    default_url_prefix = config.default_url_prefix

    if args.seq is not None:
        parameters = [{"index": str(i)} for i in range(args.seq)]
    elif args.params is not None:
        parameters = read_csv_as_dicts(args.params)
        assert len(parameters) > 0
    else:
        parameters = [{}]

    assert len(args.command) != 0

    dest_url = url_join(default_url_prefix, job_id)
    files_to_push = list(args.push)
    if args.rerun:
        assert args.name is not None, "Cannot re-run a job if the name isn't specified"
        assert len(parameters) == 1, "Cannot re-run a job with more than one task"
        # Add the existing job directory to the list of files to download to the worker

        stdout_log = url_join(dest_url, "1/stdout.txt")
        if io.exists(stdout_log):
            print(
                "Since this job was submitted with --rerun, deleting {} before job starts".format(
                    stdout_log
                )
            )
            io.delete(stdout_log)
        files_to_push.append(url_join(dest_url, "1") + ":.")

    hash_db = CachingHashFunction(config.cache_db_path)
    upload_map, spec = make_spec_from_command(
        args.command,
        image,
        dest_url=dest_url,
        cas_url=cas_url_prefix,
        parameters=parameters,
        hash_function=hash_db.get_sha256,
        src_wildcards=args.results_wildcards,
        extra_files=expand_files_to_upload(io, files_to_push),
        working_dir=args.working_dir,
        allow_symlinks=args.symlinks,
        exclude_patterns=args.exclude_wildcards,
    )

    if not os.path.exists(config.sparklesworker_exe_path):
        raise UserError(
            f"Could not find {config.sparklesworker_exe_path}. This most commonly happens when one doesn't "
            "install from the packaged releases at https://github.com/broadinstitute/sparklespray/releases"
        )

    log.debug("upload_map = %s", upload_map)

    # First check existance of files, so we can print out a single summary statement
    needs_upload = []
    needs_upload_bytes = 0
    pending_uploads = upload_map.uploads()

    key_exists = io.bulk_exists_check([dest for _, dest, _ in pending_uploads])

    for filename, dest, is_public in pending_uploads:
        if not key_exists[dest]:
            needs_upload.append((filename, dest, is_public))
            needs_upload_bytes += os.path.getsize(filename)

    # now upload those which did not exist
    txtui.user_print(
        f"{len(needs_upload)} files ({needs_upload_bytes} bytes) out of {len(upload_map.uploads())} files will be uploaded"
    )
    for filename, dest, is_public in needs_upload:
        log.debug(f"Uploading {filename}-> to {dest} (is_public={is_public}")
        io.put(filename, dest, skip_if_exists=False)

    log.debug("spec: %s", json.dumps(spec, indent=2))

    max_preemptable_attempts_scale = config.max_preemptable_attempts_scale

    for image_ in [image, config.sparklesworker_image]:
        ok, err = has_access_to_docker_image(
            config.service_account_email, config.credentials, image_
        )
        if not ok:
            raise UserError(
                f"{config.service_account_email} does not appear able to read docker image {image_}. This could be due to missing permission or the image not existing: {err}"
            )

    mount_ = config.mounts
    submit_config = SubmitConfig(
        service_account_email=config.service_account_email,  # pyright: ignore
        boot_volume=boot_volume,
        default_url_prefix=default_url_prefix,
        machine_type=machine_type,
        image=image,
        project=config.project,
        monitor_port=config.monitor_port,
        region=config.region,
        work_root_dir=config.work_root_dir,
        mounts=mount_,
        sparklesworker_image=config.sparklesworker_image,
        target_node_count=target_node_count,
        max_preemptable_attempts_scale=max_preemptable_attempts_scale,
    )
    assert mount_ == submit_config.mounts

    cluster = Cluster(
        config.project,
        config.location,
        "none",
        job_id,
        jq.job_storage,
        jq.task_storage,
        datastore_client,
        cluster_api,
        config.debug_log_prefix,
    )

    # test to see if we already have such a job submitted, in which case, we don't want to do anything
    already_submitted = False
    needs_kill_before_submit = False
    spec_hash = compute_dict_hash(spec)
    job_env_hash = compute_dict_hash(
        dict(
            # boot_volume_in_gb=boot_volume_in_gb,
            image=spec["image"],
            # kubequeconsume_exe_md5=kubequeconsume_exe_md5,
            machine_type=machine_type,
        )
    )
    metadata["job-env-sha256"] = job_env_hash
    metadata["job-spec-sha256"] = spec_hash
    existing_job = jq.get_job_optional(job_id=job_id)
    if existing_job:
        previous_spec_hash = existing_job.metadata.get("job-spec-sha256")
        if previous_spec_hash == spec_hash:
            already_submitted = True
        if existing_job.metadata.get("job-env-sha256") != job_env_hash:
            needs_kill_before_submit = True

    if args.skipifexists and already_submitted and (not needs_kill_before_submit):
        txtui.user_print(
            f"Found existing job {job_id} with identical specification. Skipping submission."
        )
    else:
        if needs_kill_before_submit:
            txtui.user_print(
                f"Found existing job {job_id} with different runtime environment. Stopping any running instances before proceeding"
            )

            kill(jq, cluster, datastore_client, cluster_api, job_id, keepcluster=False)
        if existing_job:
            txtui.user_print(
                f"Found existing job {job_id} with different specification. Removing before submitting new job."
            )
            delete(cluster, jq, job_id)
        txtui.user_print(f"Submitting job: {job_id}")

        submit(
            jq,
            io,
            job_id,
            spec,
            submit_config,
            datastore_client,
            cluster,
            metadata=metadata,
            clean_if_exists=True,
        )

    finished = False
    successful_execution = True
    if args.wait_for_completion:
        log.info("Waiting for job to terminate")
        successful_execution = watch(
            io, jq, cluster, target_nodes=target_node_count, loglive=True
        )
        finished = True

    if finished:
        txtui.user_print(
            "Done waiting for job. You can download results via 'gsutil rsync -r {} DEST_DIR'".format(
                url_join(default_url_prefix, job_id)
            )
        )

    if successful_execution:
        return 0
    else:
        return 1
