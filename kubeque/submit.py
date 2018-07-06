import time
import logging
import os
import json
import sys
from .util import random_string, url_join

import kubeque
from .node_service import MachineSpec
from .hasher import CachingHashFunction

from kubeque.spec import make_spec_from_command, SrcDstPair
import csv
import copy
import argparse
from kubeque.logclient import LogMonitor
from configparser import ConfigParser
from .main import clean

log = logging.getLogger(__name__)


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
    for attr in ['helper_log', 'command', "uploads"]:
        if attr in task:
            task_spec[attr] = task[attr]
    task_spec['downloads'].extend(task.get('downloads', []))
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
            src_url = url['src_url']

        dst = os.path.normpath(url['dst'])
        # only allow paths to be relative to working directory
        assert not (dst.startswith("../"))
        assert not (dst.startswith("/"))

        return dict(src_url=src_url, dst=dst, executable=url.get("executable", False))

    src_expanded = [rewrite_download(x) for x in downloads]

    return [rewrite_url_in_dict(x, "src_url", default_url_prefix) for x in src_expanded]


# include_patterns"`
# 	ExcludePatterns []string `json:"exclude_patterns"`
# 	UploadDstURL     string   `json:"dst_url"`

def expand_tasks(spec, io, default_url_prefix, default_job_url_prefix):
    common = spec['common']
    common['downloads'] = rewrite_downloads(io, common.get('downloads', []), default_url_prefix)
    #common['uploads'] = rewrite_uploads(common.get('uploads', []), default_job_url_prefix)

    tasks = []
    for task_i, spec_task in enumerate(spec['tasks']):
        task_url_prefix = "{}/{}".format(default_job_url_prefix, task_i + 1)
        task = expand_task_spec(common, spec_task)
        task['downloads'] = rewrite_downloads(io, task['downloads'], default_url_prefix)
        #task['uploads'] = rewrite_uploads(task['uploads'], task_url_prefix)
        task['stdout_url'] = rewrite_url_with_prefix(task['stdout_url'], task_url_prefix)
        task['command_result_url'] = rewrite_url_with_prefix(task['command_result_url'], task_url_prefix)
        task['parameters'] = spec_task['parameters']

        assert set(spec_task.keys()).issubset(task.keys()), "task before expand: {}, after expand: {}".format(
            spec_task.keys(), task.keys())

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



def _make_cluster_name(job_name, image, cpu_request, mem_limit, unique_name):
    import hashlib
    import os
    if unique_name:
        return 'l-' + random_string(20)
    return "c-" + hashlib.md5("{}-{}-{}-{}-{}-{}".format(job_name, image, cpu_request, mem_limit, kubeque.__version__, os.getlogin()).encode("utf8")).hexdigest()[:20]

def submit(jq, io, cluster, job_id, spec, dry_run, config, skip_kube_submit, metadata, kubequeconsume_url,
           exec_local=False, loglive=False, ):
    from .key_store import KeyStore

    key_store = KeyStore(cluster.client)
    cert, key = key_store.get_cert_and_key()
    if cert is None:
        log.info("No cert and key for cluster found -- generating now")
        import kubeque.certgen
        cert, key = kubeque.certgen.create_self_signed_cert()
        key_store.set_cert_and_key(cert, key)

    log.info("Submitting job with id: %s", job_id)

    # where to take this from? arg with a default of 1?
    if dry_run:
        skip_kube_submit = True

    preemptible_flag = config.get("preemptible", "n").lower()
    if preemptible_flag not in ['y', 'n']:
        raise Exception("setting 'preemptable' in config must either by 'y' or 'n' but was: {}".format(preemptible_flag))

    preemptible = preemptible_flag == 'y'

    bootDiskSizeGb_flag = config.get("bootDiskSizeGb", "20")
    bootDiskSizeGb = int(bootDiskSizeGb_flag)
    assert bootDiskSizeGb >= 10

    default_url_prefix = config.get("default_url_prefix", "")
    if default_url_prefix.endswith("/"):
        default_url_prefix = default_url_prefix[:-1]
    default_job_url_prefix = default_url_prefix + "/" + job_id

    tasks = expand_tasks(spec, io, default_url_prefix, default_job_url_prefix)
    task_spec_urls = []
    command_result_urls = []

    # TODO: When len(tasks) is a fair size (>100) this starts taking a noticable amount of time.
    # Perhaps store tasks in a single blob?  Or do write with multiple requests in parallel?
    for task in tasks:
        if not dry_run:
            url = io.write_json_to_cas(task)
            task_spec_urls.append(url)
            command_result_urls.append(task['command_result_url'])
        else:
            log.debug("task post expand: %s", json.dumps(task, indent=2))

    if not dry_run:
        image = spec['image']
        resources = spec["resources"]
        cpu_request = _parse_cpu_request(resources.get(CPU_REQUEST, config['default_resource_cpu']))
        mem_limit = _parse_mem_limit(resources.get(MEMORY_REQUEST, config["default_resource_memory"]))
        cluster_name = _make_cluster_name(job_id, image, cpu_request, mem_limit, unique_name=exec_local)

        assert mem_limit <= 5
        assert cpu_request < 2

        project = config['project']
        port = config.get('monitor_port', '6032')
        consume_exe_args = ["--cluster", cluster_name, "--projectId", project, "--zones", ",".join(config['zones']), "--port", port]

        machine_specs = MachineSpec(boot_volume_in_gb = bootDiskSizeGb,
            mount_point = config.get("mount", "/mnt/"),
            machine_type = "n1-standard-1")

        pipeline_spec = cluster.create_pipeline_spec(
            jobid=job_id,
            cluster_name=cluster_name,
            consume_exe_url=kubequeconsume_url,
            docker_image=image,
            consume_exe_args=consume_exe_args,
            machine_specs=machine_specs,
            monitor_port=int(port))

        jq.submit(job_id, list(zip(task_spec_urls, command_result_urls)), pipeline_spec, metadata, cluster_name)
        if not skip_kube_submit and not exec_local:
            existing_nodes = cluster.get_cluster_status(cluster_name)
            if not existing_nodes.is_running():
                operation_ids = _addnodes(job_id, jq, cluster, 1, preemptible, config['default_url_prefix'])
                log.info("Adding initial node for cluster (operation %s)", operation_ids[0])
            else:
                log.info("Cluster already exists, not adding node. Cluster status: %s", existing_nodes.as_string())
            existing_tasks = jq.get_tasks_for_cluster(cluster_name, STATUS_PENDING)
            if len(existing_tasks) > 0:
                log.warning("%d tasks already exist queued up to run on this cluster. If this is not intentional, delete the jobs via 'kubeque clean' and resubmit this job.", len(existing_tasks))
        elif exec_local:
            raise Exception("unimplemented -- broke when migrated to new version of pipeline API")
            # cmd = _write_local_script(job_id, spec, kubeque_command, config['kubequeconsume_exe_path'],
            #                           kubeque_exe_in_container)
            # log.info("Running job locally via executing: ./%s", cmd)
            # log.warning("CPU and memory requirements are not honored when running locally. Will use whatever the docker host is configured for by default")
            # os.system(os.path.abspath(cmd))
        else:
            raise Exception("unimplemented -- broke when migrated to new version of pipeline API")
            # cmd = _write_local_script(job_id, spec, kubeque_command, config['kubequeconsume_exe_path'],
            #                           kubeque_exe_in_container)
            # log.info("Skipping submission.  You can execute tasks locally via: ./%s", cmd)


def _write_local_script(job_id, spec, kubeque_command, kubequeconsume_exe_path, kubeque_exe_in_container):
    from kubeque.gcp import _gcloud_cmd
    import stat

    image = spec['image']
    cmd = _gcloud_cmd(
        ["docker", "--", "run",
         "-v", os.path.expanduser("~/.config/gcloud") + ":/google-creds",
         "-e", "GOOGLE_APPLICATION_CREDENTIALS=/google-creds/application_default_credentials.json",
         "-v", kubequeconsume_exe_path + ":" + kubeque_exe_in_container,
         image, 'bash -c "' + kubeque_command + ' --owner localhost"', ])
    script_name = "run-{}-locally.sh".format(job_id)
    with open(script_name, "wt") as fd:
        fd.write("#!/usr/bin/env bash\n")
        fd.write(" ".join(cmd) + "\n")

    # make script executable
    os.chmod(script_name, os.stat(script_name).st_mode | stat.S_IXUSR)
    return script_name

def new_job_id():
    import uuid
    return get_timestamp() + "-" + uuid.uuid4().hex[:4]

def read_parameters_from_csv(filename):
    with open(filename, "rt") as fd:
        return list(csv.DictReader(fd))


def _split_source_dest(file):
    if file.startswith("gs://"):
        index = file.find(":", 5)
    else:
        index = file.find(":")

    if index >= 0:
        source, dest = file[:index], file[index+1:]
    else:
        source = dest = file

    if dest.startswith("/") or dest.startswith("gs://"):
        dest = os.path.basename(dest)

    return source, dest

def _add_name_pair_to_list(file):
    if file.startswith('@'):
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
                    pairs.append(SrcDstPair(child_key, dst + child_key[len(src):]))
        else:
            pairs.append(SrcDstPair(src, dst))
    return pairs

MEMORY_REQUEST = "memory"
CPU_REQUEST = "cpu"

import re

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
        assert name in [MEMORY_REQUEST, CPU_REQUEST], "Unknown resource requested: {}. Must be one of {} {}".format(
            name, MEMORY_REQUEST, CPU_REQUEST)
        spec[name] = value
    return spec

def _obj_path_to_url(path):
    m = re.match("gs://([^/]+)/(.+)$", path)
    assert m is not None
    bucket, key = m.groups()
    return "https://{}.storage.googleapis.com/{}".format(bucket, key)


def submit_cmd(jq, io, cluster, args, config):
    metadata = {}

    if args.image:
        image = args.image
    else:
        image = config['default_image']

    job_id = args.name
    if job_id is None:
        job_id = new_job_id()

    existing_job = jq.get_job(job_id, must = False)
    if existing_job is not None:
        if args.clean or args.rerun:
            log.info("Cleaning existing job with id \"{}\"".format(job_id))
            success = clean(cluster, jq, job_id)
            if not success:
                log.error("Could not remove \"{}\", aborting!".format(job_id))
                return
        else:
            log.error("Existing job with id \"{}\", aborting!".format(job_id))
            return

    cas_url_prefix = config['cas_url_prefix']
    default_url_prefix = config['default_url_prefix']

    if args.file:
        assert len(args.command) == 0
        spec = json.load(open(args.file, "rt"))
    else:
        if args.seq is not None:
            parameters = [{"index": str(i)} for i in range(args.seq)]
        elif args.params is not None:
            parameters = read_parameters_from_csv(args.params)
        else:
            parameters = [{}]

        assert len(args.command) != 0

        resource_spec = _parse_resources(args.resources)

        dest_url = url_join(default_url_prefix, job_id)
        files_to_push = list(args.push)
        if args.rerun:
            assert args.name is not None, "Cannot re-run a job if the name isn't specified"
            assert len(parameters) == 1, "Cannot re-run a job with more than one task"
            # Add the existing job directory to the list of files to download to the worker
            files_to_push.append(url_join(dest_url, "1")+":.")

        hash_db = CachingHashFunction(config.get("cache_db_path", ".kubeque-cached-file-hashes"))
        upload_map, spec = make_spec_from_command(args.command,
                                                  image,
                                                  dest_url=dest_url,
                                                  cas_url=cas_url_prefix,
                                                  parameters=parameters,
                                                  resource_spec=resource_spec,
                                                  hash_function=hash_db.hash_filename,
                                                  src_wildcards=args.results_wildcards,
                                                  extra_files=expand_files_to_upload(io, files_to_push),
                                                  working_dir=args.working_dir)

        kubequeconsume_exe_path = config['kubequeconsume_exe_path']
        kubequeconsume_exe_obj_path = upload_map.add(hash_db.hash_filename, cas_url_prefix,
                                                        kubequeconsume_exe_path, is_public=True)
        kubequeconsume_exe_url = _obj_path_to_url(kubequeconsume_exe_obj_path)
        hash_db.persist()

        log.debug("upload_map = %s", upload_map)
        log.info("kubeconsume at %s", kubequeconsume_exe_url)
        for filename, dest, is_public in upload_map.uploads():
            print("filename={}, dest={}, is_public={}".format(filename, dest, is_public))
            io.put(filename, dest, skip_if_exists=True, is_public=is_public)

    log.debug("spec: %s", json.dumps(spec, indent=2))
    submit(jq, io, cluster, job_id, spec, args.dryrun, config, args.skip_kube_submit, metadata, kubequeconsume_exe_url,
           args.local, args.loglive)

    finished = False
    successful_execution = True
    if args.local:
        # if we ran it within docker, and the docker command completed, then the job is done
        finished = True
    else:
        if not (args.dryrun or args.skip_kube_submit) and args.wait_for_completion:
            log.info("Waiting for job to terminate")
            successful_execution = watch(io, jq, job_id, cluster, loglive=args.loglive)
            finished = True

    if finished:
        if args.fetch:
            log.info("Done waiting for job to complete, downloading results to %s", args.fetch)
            fetch_cmd_(jq, io, job_id, args.fetch)
        else:
            log.info("Done waiting for job to complete, results written to %s", default_url_prefix + "/" + job_id)
            log.info("You can download results via 'gsutil rsync -r %s DEST_DIR'", default_url_prefix + "/" + job_id)

    if successful_execution:
        sys.exit(0)
    else:
        sys.exit(1)

def retry_cmd(jq, cluster, io, args):
    resource_spec = _parse_resources(args.resources)

    jobids = _get_jobids_from_pattern(jq, args.jobid_pattern)
    if len(jobids) == 0:
        print("No jobs found with name matching {}".format(args.jobid_pattern))
        return

    for jobid in jobids:
        log.info("retrying %s", jobid)
        jq.reset(jobid, args.owner)
        _resubmit(jq, jobid, resource_spec)

    if args.wait_for_completion:
        log.info("Waiting for job to terminate")
        for job_id in jobids:
            watch(io, jq, job_id, cluster)
