import os
import json
import copy
import argparse
import re

from typing import List
from pydantic import BaseModel

import sparklespray

from .csv_utils import read_csv_as_dicts
from .util import random_string, url_join
from .node_service import MachineSpec
from .hasher import CachingHashFunction
from .spec import make_spec_from_command, SrcDstPair
from .main import clean
from .util import get_timestamp
from .job_queue import JobQueue
from .cluster_service import Cluster
from .io import IO
from .watch import watch, local_watch
from . import txtui
from .watch import DockerFailedException
from typing import Optional

from .log import log

MEMORY_REQUEST = "memory"
CPU_REQUEST = "cpu"


class SubmitConfig(BaseModel):
    preemptible: bool
    boot_volume_in_gb: float
    default_url_prefix: str
    machine_type: str
    image: str
    project: str
    monitor_port: int
    zones: List[str]
    mount_point: str
    kubequeconsume_url: str
    kubequeconsume_md5: str
    gpu_count: int
    gpu_type: Optional[str]
    target_node_count: int


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


# include_patterns"`
# 	ExcludePatterns []string `json:"exclude_patterns"`
# 	UploadDstURL     string   `json:"dst_url"`


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


def _parse_cpu_request(txt):
    import math

    return int(math.ceil(float(txt)))


def _parse_mem_limit(txt):
    if txt[-1:] == "M":
        return float(txt[:-1]) / 1000.0
    else:
        assert txt[-1:] == "G"
        return float(txt[:-1])


def _make_cluster_name(job_name, image, machine_type, unique_name):
    import hashlib

    if unique_name:
        return "l-" + random_string(20)
    else:
        return (
            "c-"
            + hashlib.md5(
                f"{job_name}-{image}-{machine_type}-{sparklespray.__version__}".encode(
                    "utf8"
                )
            ).hexdigest()[:20]
        )


def submit(
    jq: JobQueue,
    io: IO,
    cluster: Cluster,
    job_id: str,
    spec: dict,
    config: SubmitConfig,
    metadata: dict = {},
    clean_if_exists: bool = False,
    dry_run: bool = False,
    cluster_name=None,
):
    from .key_store import KeyStore

    key_store = KeyStore(cluster.client)
    cert, key = key_store.get_cert_and_key()
    if cert is None:
        log.info("No cert and key for cluster found -- generating now")
        from .certgen import create_self_signed_cert

        cert, key = create_self_signed_cert()
        key_store.set_cert_and_key(cert, key)

    log.info("Submitting job with id: %s", job_id)

    # where to take this from? arg with a default of 1?
    if dry_run:
        skip_kube_submit = True

    preemptible = config.preemptible
    boot_volume_in_gb = config.boot_volume_in_gb
    default_url_prefix = config.default_url_prefix

    default_job_url_prefix = url_join(default_url_prefix, job_id)
    tasks = expand_tasks(spec, io, default_url_prefix, default_job_url_prefix)
    task_spec_urls = []
    command_result_urls = []
    log_urls = []

    # TODO: When len(tasks) is a fair size (>100) this starts taking a noticable amount of time.
    # Perhaps store tasks in a single blob?  Or do write with multiple requests in parallel?
    for task in tasks:
        if not dry_run:
            url = io.write_json_to_cas(task)
            task_spec_urls.append(url)
            command_result_urls.append(task["command_result_url"])
            log_urls.append(task["stdout_url"])
        else:
            log.debug("task post expand: %s", json.dumps(task, indent=2))

    if not dry_run:
        image = config.image
        if cluster_name is None:
            cluster_name = _make_cluster_name(job_id, image, config.machine_type, False)

        existing_job = jq.get_job(job_id, must=False)
        if existing_job is not None:
            if clean_if_exists:
                log.info('Cleaning existing job with id "{}"'.format(job_id))
                success = clean(cluster, jq, job_id)
                if not success:
                    raise ExistingJobException(
                        'Could not remove running job "{}", aborting!'.format(job_id)
                    )
            else:
                raise ExistingJobException(
                    'Existing job with id "{}", aborting!'.format(job_id)
                )

        project = config.project
        monitor_port = config.monitor_port
        consume_exe_args = [
            "--cluster",
            cluster_name,
            "--projectId",
            project,
            "--zones",
            ",".join(config.zones),
            "--port",
            str(monitor_port),
        ]

        machine_specs = MachineSpec(
            boot_volume_in_gb=boot_volume_in_gb,
            mount_point=config.mount_point,
            machine_type=config.machine_type,
            gpu_count=config.gpu_count,
            gpu_type=config.gpu_type,
        )

        pipeline_spec = cluster.create_pipeline_spec(
            jobid=job_id,
            cluster_name=cluster_name,
            consume_exe_url=config.kubequeconsume_url,
            consume_exe_md5=config.kubequeconsume_md5,
            docker_image=image,
            consume_exe_args=consume_exe_args,
            machine_specs=machine_specs,
            monitor_port=monitor_port,
        )

        max_preemptable_attempts = 0
        if preemptible:
            max_preemptable_attempts = config.target_node_count * 2

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
    import uuid

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


def _parse_resources(resources_str):
    # not robust parsing at all
    spec = {}
    if resources_str is None:
        return spec
    pairs = resources_str.split(",")
    for pair in pairs:
        m = re.match("([^=]+)=(.*)", pair)
        if m is None:
            raise Exception("resource constraint malformed: {}".format(pair))
        name, value = m.groups()
        assert name in [
            MEMORY_REQUEST,
            CPU_REQUEST,
        ], "Unknown resource requested: {}. Must be one of {} {}".format(
            name, MEMORY_REQUEST, CPU_REQUEST
        )
        spec[name] = value
    return spec


def add_submit_cmd(subparser):
    parser = subparser.add_parser(
        "sub", help="Submit a command (or batch of commands) for execution"
    )
    parser.set_defaults(func=submit_cmd)
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
        "--dryrun",
        action="store_true",
        help="Don't actually submit the job but just print what would have been done",
    )
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
    parser.add_argument(
        "--local",
        help="Run the tasks inside of docker on the local machine",
        action="store_true",
    )
    parser.add_argument(
        "--rerun",
        help="If set, will download all of the files from previous execution of this job to worker before running",
        action="store_true",
    )
    parser.add_argument(
        "--preemptible",
        action="store_true",
        help="If set, will try to turn on nodes initally as preemptible nodes",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    parser.add_argument(
        "--gpu_count", type=int, help="Number of gpus on your VM", default=0
    )


def _get_boot_volume_in_gb(config):
    bootDiskSizeGb_flag = config.get("bootDiskSizeGb")
    if bootDiskSizeGb_flag is None:
        bootDiskSizeGb_flag = config.get("bootdisksizegb")
    if bootDiskSizeGb_flag is None:
        bootDiskSizeGb_flag = config.get("boot_volume_in_gb")
    if bootDiskSizeGb_flag is None:
        bootDiskSizeGb_flag = "20"
    # check for G as the suffix, because we used to allow this
    if bootDiskSizeGb_flag.lower().endswith("g"):
        bootDiskSizeGb_flag = bootDiskSizeGb_flag[:-1]
    bootDiskSizeGb = int(bootDiskSizeGb_flag)
    assert bootDiskSizeGb >= 10
    return bootDiskSizeGb


def submit_cmd(jq, io, cluster, args, config):
    metadata = {}

    if args.image:
        image = args.image
    else:
        image = config["default_image"]

    if args.preemptible:
        preemptible = True
    else:
        preemptible_flag = config.get("preemptible", "n").lower()
        if preemptible_flag not in ["y", "n"]:
            raise Exception(
                "setting 'preemptible' in config must either by 'y' or 'n' but was: {}".format(
                    preemptible_flag
                )
            )
        preemptible = preemptible_flag == "y"

    boot_volume_in_gb = _get_boot_volume_in_gb(config)
    default_url_prefix = config.get("default_url_prefix", "")
    work_dir = config.get(
        "local_work_dir", os.path.expanduser("~/.sparkles-cache/local_work_dir")
    )

    job_id = args.name
    if job_id is None:
        job_id = new_job_id()
    elif args.skipifexists:
        job = jq.get_job(job_id, must=False)
        if job is not None:
            txtui.user_print(
                f"Found existing job {job_id} and submitted job with --skipifexists so aborting"
            )
            return 0

    target_node_count = args.nodes
    machine_type = config["machine_type"]
    if args.machine_type:
        machine_type = args.machine_type

    gpu_count = config.get("gpu_count", 0)
    gpu_type = config.get("gpu_type", None)
    if args.gpu_count:
        gpu_count = args.gpu_count
    if gpu_count:
        if gpu_type is None:
            raise Exception(
                f"Requesting {gpu_count} GPUs but gpu_type is missing from config"
            )

    cas_url_prefix = config["cas_url_prefix"]
    default_url_prefix = config["default_url_prefix"]

    if args.file:
        assert len(args.command) == 0
        spec = json.load(open(args.file, "rt"))
    else:
        if args.seq is not None:
            parameters = [{"index": str(i)} for i in range(args.seq)]
        elif args.params is not None:
            parameters = read_csv_as_dicts(args.params)
        else:
            parameters = [{}]

        assert len(args.command) != 0

        dest_url = url_join(default_url_prefix, job_id)
        files_to_push = list(args.push)
        if args.rerun:
            assert (
                args.name is not None
            ), "Cannot re-run a job if the name isn't specified"
            assert len(parameters) == 1, "Cannot re-run a job with more than one task"
            # Add the existing job directory to the list of files to download to the worker

            files_to_push.append(url_join(dest_url, "1") + ":.")

        hash_db = CachingHashFunction(
            config.get("cache_db_path", ".kubeque-cached-file-hashes")
        )
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

        kubequeconsume_exe_path = config["kubequeconsume_exe_path"]
        kubequeconsume_exe_obj_path = upload_map.add(
            hash_db.get_sha256, cas_url_prefix, kubequeconsume_exe_path, is_public=True,
        )
        kubequeconsume_exe_md5 = hash_db.get_md5(kubequeconsume_exe_path)
        hash_db.persist()

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

    # now that the executable is uploaded, we should be able to get a signed url for it
    kubequeconsume_exe_url = io.generate_signed_url(kubequeconsume_exe_obj_path)
    log.info("kubeconsume at %s", kubequeconsume_exe_url)

    submit_config = SubmitConfig(
        preemptible=preemptible,
        boot_volume_in_gb=boot_volume_in_gb,
        default_url_prefix=default_url_prefix,
        machine_type=machine_type,
        image=spec["image"],
        project=config["project"],
        monitor_port=int(config.get("monitor_port", "6032")),
        zones=config["zones"],
        mount_point=config.get("mount", "/mnt/"),
        kubequeconsume_url=kubequeconsume_exe_url,
        kubequeconsume_md5=kubequeconsume_exe_md5,
        gpu_count=gpu_count,
        gpu_type=gpu_type,
        target_node_count=target_node_count,
    )

    cluster_name = None
    if args.local:
        # if doing a local submission, generate a unique cluster name each time
        # to ensure the local process is the one which picks up the job.
        cluster_name = "local-" + random_string(8)

    txtui.user_print("Submitting job: {}".format(job_id))
    submit(
        jq,
        io,
        cluster,
        job_id,
        spec,
        submit_config,
        metadata=metadata,
        clean_if_exists=True,
        dry_run=args.dryrun,
        cluster_name=cluster_name,
    )

    finished = False
    successful_execution = True

    if args.local:
        try:
            successful_execution = local_watch(
                job_id, kubequeconsume_exe_path, work_dir, cluster
            )
            finished = True
        except DockerFailedException:
            log.error(
                "Docker process prematurely died -- reseting job %s to release any claimed tasks",
                job_id,
            )
            jq.reset(job_id, None)
            finished = False
    else:
        if not (args.dryrun or args.skip_kube_submit) and args.wait_for_completion:
            log.info("Waiting for job to terminate")
            successful_execution = watch(
                io, jq, job_id, cluster, target_nodes=target_node_count, loglive=True
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
