import time
import logging
import os
import json
import sys

import kubeque
from kubeque.gcp import create_gcs_job_queue, IO, STATUS_PENDING, STATUS_FAILED, STATUS_COMPLETE, STATUS_CLAIMED, \
    STATUS_KILLED, JOB_STATUS_KILLED
from kubeque.gcs_pipeline import MachineSpec
from kubeque.hasher import CachingHashFunction

from kubeque.spec import make_spec_from_command, SrcDstPair
import csv
import copy
import kubeque.gcs_pipeline as pipeline
import argparse
from kubeque.logclient import LogMonitor

log = logging.getLogger(__name__)

try:
    from configparser import ConfigParser
except:
    from ConfigParser import ConfigParser



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


def _random_string(length):
    import random
    import string
    alphabet = string.ascii_uppercase + string.digits
    return (''.join(random.choice(alphabet) for _ in range(length)))


def _make_cluster_name(job_name, image, cpu_request, mem_limit, unique_name):
    import hashlib
    import os
    if unique_name:
        return 'l-' + _random_string(20)
    return "c-" + hashlib.md5("{}-{}-{}-{}-{}-{}".format(job_name, image, cpu_request, mem_limit, kubeque.__version__, os.getlogin()).encode("utf8")).hexdigest()[:20]


def validate_cmd(jq, io, cluster, config):
    log.info("Validating config, using kubeque %s", kubeque.__version__)
    log.info("Printing config:")
    import pprint
    pprint.pprint(config)

    log.info("Verifying we can access google cloud storage")
    sample_value = new_job_id()
    sample_url = io.write_str_to_cas(sample_value)
    fetched_value = io.get_as_str(sample_url)
    assert sample_value == fetched_value

    log.info("Verifying we can read/write from the google datastore service and google pubsub")
    jq.test_datastore_api(sample_value)

    log.info("Verifying we can access google genomics apis")
    cluster.test_api()

    log.info("Verifying google genomics can launch image \"%s\"", config['default_image'])
    logging_url = config["default_url_prefix"] + "/node-logs"
    cluster.test_image(config['default_image'], sample_url, logging_url)

    log.info("Verification successful!")

def submit(jq, io, cluster, job_id, spec, dry_run, config, skip_kube_submit, metadata, kubequeconsume_url,
           exec_local=False, loglive=False, ):
    cert, key = jq.storage.get_cert_and_key()
    if cert is None:
        log.info("No cert and key for cluster found -- generating now")
        import kubeque.certgen
        cert, key = kubeque.certgen.create_self_signed_cert()
        jq.storage.set_cert_and_key(cert, key)

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


def load_config(config_file, gcloud_config_file="~/.config/gcloud/configurations/config_default"):
    # first load defaults from gcloud config
    gcloud_config_file = os.path.expanduser(gcloud_config_file)
    defaults = {}
    if os.path.exists(gcloud_config_file):
        gcloud_config = ConfigParser()
        gcloud_config.read(gcloud_config_file)
        defaults = dict(account=gcloud_config.get("core", "account"),
                        project=gcloud_config.get("core", "project"),
                        zones=[gcloud_config.get("compute", "zone")],
                        region=gcloud_config.get("compute", "region"))
                        
    config_file = os.path.expanduser(config_file)

    config = ConfigParser()
    config.read(config_file)
    config_from_file = dict(config.items('config'))
    if 'zones' in config_from_file:
        config_from_file['zones'] = [x.strip() for x in config_from_file['zones'].split(",")]

    merged_config = dict(defaults)
    merged_config.update(config_from_file)

    missing_values = []
    for property in ["default_url_prefix", "project",
                     "default_image", "default_resource_cpu", "default_resource_memory", "zones", "region", "account"]:
        if property not in merged_config or merged_config[property] == "" or merged_config[property] is None:
            missing_values.append(property)

    if len(missing_values) > 0:
        print("Missing the following parameters in {}: {}".format(config_file, ", ".join(missing_values)))
        sys.exit(1)

    if "kubequeconsume_exe_path" not in merged_config:
        merged_config["kubequeconsume_exe_path"] = os.path.join(os.path.dirname(__file__), "bin/kubequeconsume")
        assert os.path.exists(merged_config["kubequeconsume_exe_path"])

    if "cas_url_prefix" not in merged_config:
        merged_config["cas_url_prefix"] = merged_config["default_url_prefix"] + "/CAS/"

    assert isinstance(merged_config['zones'], list)

    jq, io, cluster = load_config_from_dict(merged_config)
    return merged_config, jq, io, cluster


def load_config_from_dict(config):
    credentials = None
    io = IO(config['project'], config['cas_url_prefix'], credentials)
    jq = create_gcs_job_queue(config['project'], credentials, use_pubsub=False)
    cluster = pipeline.Cluster(config['project'], config['zones'], credentials=credentials)

    return jq, io, cluster


def get_timestamp():
    import datetime
    d = datetime.datetime.now()
    return d.strftime("%Y%m%d-%H%M%S")

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
from kubeque.gcp import _join

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
            success = _clean(cluster, jq, job_id)
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

        dest_url = _join(default_url_prefix, job_id)
        files_to_push = list(args.push)
        if args.rerun:
            assert args.name is not None, "Cannot re-run a job if the name isn't specified"
            assert len(parameters) == 1, "Cannot re-run a job with more than one task"
            # Add the existing job directory to the list of files to download to the worker
            files_to_push.append(_join(dest_url, "1")+":.")

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

def _resubmit(jq, jobid, resource_spec={}):
    raise Exception("unimp")
    # pending_count = jq.get_status_counts(jobid).get(STATUS_PENDING, 0)
    # if pending_count == 0:
    #     log.warning("No tasks are pending for jobid %s.  Skipping resubmit.", jobid)
    #     return False
    #
    # kube_job_spec_json = jq.get_kube_job_spec(jobid)
    # kube_job_spec = json.loads(kube_job_spec_json)
    # # correct parallelism to reflect the remaining jobs
    # kube_job_spec["spec"]["parallelism"] = pending_count
    # import random, string
    # name = kube_job_spec["metadata"]["name"]
    # name += "-" + (''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(5)))
    # kube_job_spec["metadata"]["name"] = name
    # prev_cpu, prev_mem = get_resource_limits(kube_job_spec)
    # set_resource_limits(kube_job_spec, resource_spec.get(CPU_REQUEST, prev_cpu), resource_spec.get(MEMORY_REQUEST, prev_mem))
    # submit_job_spec(json.dumps(kube_job_spec))
    # return True


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


def list_params_cmd(jq, io, args):
    jobid = _resolve_jobid(jq, args.jobid)
    retcode = args.exitcode
    include_extra = args.extra

    if args.incomplete:
        tasks = []
        for status in [STATUS_FAILED, STATUS_CLAIMED, STATUS_PENDING, STATUS_KILLED]:
            tasks.extend(jq.get_tasks(jobid, status=status))
    else:
        tasks = jq.get_tasks(jobid)

    if retcode is not None:
        def retcode_matches(exit_code):
            return exit_code is not None and int(exit_code) == retcode

        before_count = len(tasks)
        tasks = [task for task in tasks if retcode_matches(task.exit_code)]
        print("Filtered {} tasks to {} tasks with exit code {}".format(before_count, len(tasks), retcode))

    if len(tasks) == 0:
        print("No tasks found")
    else:
        print("Getting parameters from %d tasks" % len(tasks))
        parameters = []
        for task in tasks:
            task_spec = json.loads(io.get_as_str(task.args))
            task_parameters = task_spec.get('parameters', {})
            if include_extra:
                task_parameters['task_id'] = task.task_id
                task_parameters['exit_code'] = task.exit_code
            parameters.append(task_parameters)

        # find the union of all keys
        keys = set()
        for p in parameters:
            keys.update(p.keys())

        columns = list(keys)
        columns.sort()

        with open(args.filename, "wt") as fd:
            w = csv.writer(fd)
            w.writerow(columns)
            for p in parameters:
                row = [str(p.get(column, "")) for column in columns]
                w.writerow(row)


def reset_cmd(jq, args):
    for jobid in _get_jobids_from_pattern(jq, args.jobid_pattern):
        if args.all:
            statuses_to_clear = [STATUS_CLAIMED, STATUS_FAILED, STATUS_COMPLETE, STATUS_KILLED]
        else:
            statuses_to_clear = [STATUS_CLAIMED, STATUS_FAILED, STATUS_KILLED]
        log.info("reseting %s by changing tasks with statuses (%s) -> %s", jobid, ",".join(statuses_to_clear),
                 STATUS_PENDING)
        updated = jq.reset(jobid, args.owner, statuses_to_clear=statuses_to_clear)
        log.info("updated %d tasks", updated)
        if args.resubmit:
            _resubmit(jq, jobid)


def _summarize_task_statuses(tasks):
    import collections
    complete = True
    counts = collections.defaultdict(lambda: 0)
    for task in tasks:
        if task.status == STATUS_COMPLETE:
            label = "{}(code={})".format(task.status, task.exit_code)
        elif task.status == STATUS_FAILED:
            label = "{}({})".format(task.status, task.failure_reason)
        else:
            label = task.status
        counts[label] += 1

        if not _is_terminal_status(task.status):
            complete = False

    labels = list(counts.keys())
    labels.sort()
    status_str = ", ".join(["{}: {}".format(l, counts[l]) for l in labels])
    return status_str, complete


def _was_oom_killed(task):
    log.warning("_was_oom_killed is stubbed. Returning false")
    # if task.status == STATUS_CLAIMED:
    #     oom_killed = kube.was_oom_killed(task.owner)
    #     return oom_killed
    return False


def _get_jobids_from_pattern(jq, jobid_pattern):
    if not jobid_pattern:
        jobid_pattern = "*"

    if jobid_pattern == "LAST":
        job = jq.get_last_job()
        return [job.job_id]
    else:
        return jq.get_jobids(jobid_pattern)


def _resolve_jobid(jq, jobid):
    if jobid == "LAST":
        job = jq.get_last_job()
        return job.job_id
    else:
        return jobid


def saturate_cmd(jq, io, cluster, args):
    jobid = _resolve_jobid(jq, args.jobid)
    watch(io, jq, jobid, cluster, saturate=True, saturate_nodes=args.nodes)

def status_cmd(jq, io, cluster, args):
    jobids = _get_jobids_from_pattern(jq, args.jobid_pattern)

    if args.wait or args.loglive:
        assert len(jobids) == 1, "When watching, only one jobid allowed, but the following matched wildcard: {}".format(
            jobids)
        jobid = jobids[0]
        watch(io, jq, jobid, cluster, loglive=args.loglive)
    else:
        for jobid in jobids:
            if args.detailed or args.failures:
                for task in jq.get_tasks(jobid):
                    if args.failures and task.status != STATUS_FAILED:
                        continue

                    command_result_json = None
                    if task.command_result_url is not None:
                        command_result_json = io.get_as_str(task.command_result_url, must=False)
                    if command_result_json is not None:
                        command_result = json.loads(command_result_json)
                        command_result_block = "\n  command result: {}".format(json.dumps(command_result, indent=4))
                    else:
                        command_result_block = ""

                    log.info("task_id: %s\n"
                             "  status: %s, exit_code: %s, failure_reason: %s\n"
                             "  started on pod: %s\n"
                             "  args: %s, history: %s%s\n"
                             "  cluster: %s", task.task_id,
                             task.status, task.exit_code, task.failure_reason, task.owner, task.args, task.history,
                             command_result_block, task.cluster)

                    if _was_oom_killed(task):
                        print("Was OOM killed")
            else:
                tasks = jq.get_tasks(jobid)
                status, complete = _summarize_task_statuses(tasks)
                log.info("%s: %s", jobid, status)

def _commonprefix(paths):
    "Given a list of paths, returns the longest common prefix"
    if not paths:
        return ()

    # def split(path):
    #     return [x for x in path.split("/") if x != ""]

    paths = [x.split("/") for x in paths]

    min_path = min(paths)
    max_path = max(paths)
    common_path = min_path
    for i in range(len(min_path)):
        if min_path[i] != max_path[i]:
            common_path = common_path[:i]
            break

    return "/".join(common_path)

def fetch_cmd(jq, io, args):
    jobid = _resolve_jobid(jq, args.jobid)
    if args.dest is None:
        dest = jobid
    else:
        dest = args.dest
    fetch_cmd_(jq, io, jobid, dest, flat=args.flat)


def fetch_cmd_(jq, io, jobid, dest_root, force=False, flat=False):
    def get(src, dst, **kwargs):
        if os.path.exists(dst) and not force:
            log.warning("%s exists, skipping download", dst)
        return io.get(src, dst, **kwargs)

    tasks = jq.get_tasks(jobid)

    if not os.path.exists(dest_root):
        os.mkdir(dest_root)

    include_index = not flat

    for task in tasks:
        spec = json.loads(io.get_as_str(task.args))
        log.debug("task %d spec: %s", task.task_index + 1, spec)

        if include_index:
            dest = os.path.join(dest_root, str(task.task_index + 1))
            if not os.path.exists(dest):
                os.mkdir(dest)
        else:
            dest = dest_root

        # save parameters taken from spec
        # with open(os.path.join(dest, "parameters.json"), "wt") as fd:
        #     fd.write(json.dumps(spec['parameters']))
        command_result_json = io.get_as_str(spec['command_result_url'], must=False)
        to_download = []
        if command_result_json is None:
            log.warning("Results did not appear to be written yet at %s", spec['command_result_url'])
        else:
            get(spec['stdout_url'], os.path.join(dest, "stdout.txt"))
            command_result = json.loads(command_result_json)
            log.debug("command_result: %s", json.dumps(command_result))
            for ul in command_result['files']:
                to_download.append((ul['src'], ul['dst_url']))

        for src, dst_url in to_download:
            if include_index:
                localpath = os.path.join(dest_root, str(task.task_index + 1), src)
            else:
                localpath = os.path.join(dest_root, src)
            pdir = os.path.dirname(localpath)
            if not os.path.exists(pdir):
                os.makedirs(pdir)
            get(dst_url, localpath)


def _is_terminal_status(status):
    return status in [STATUS_FAILED, STATUS_COMPLETE]


def _is_complete(status_counts):
    all_terminal = True
    for status in status_counts.keys():
        if not _is_terminal_status(status):
            all_terminal = True
    return all_terminal


class NodeRespawn:
    def __init__(self, cluster_status_fn, tasks_status_fn, get_pending_fn, max_nodes):
        self.max_restarts = tasks_status_fn().active_tasks
        self.cluster_status_fn = cluster_status_fn
        self.tasks_status_fn = tasks_status_fn
        self.last_cluster_status = None
        self.nodes_added = 0
        self.max_nodes = max_nodes
        self.get_pending_fn = get_pending_fn

    def reset_added_count(self):
        self.nodes_added = 0

    def reconcile_node_count(self, add_node_callback):
        # get latest status
        cluster_status = self.tasks_status_fn()
        if cluster_status == self.last_cluster_status:
            # don't try to reconcile if we see the identical as last time we polled. We might not
            # be able to see newly spawned nodes yet, so wait for the next poll
            return

        needed_nodes = cluster_status.active_tasks
        if self.max_nodes is not None:
            needed_nodes = min(self.max_nodes, needed_nodes)
        running_count = self.cluster_status_fn().running_count
        # for now, count pending requests as "running" because they eventually will
        #print("calling get_pending_fn")
        running_count += self.get_pending_fn()
        self.last_cluster_status = cluster_status

        # see if we're short and add nodes of the appropriate type
        if needed_nodes > running_count:
            nodes_to_add = needed_nodes - running_count
            capped_nodes_to_add = min(nodes_to_add, self.max_restarts - self.nodes_added)
            if capped_nodes_to_add == 0:
                raise Exception("Wanted to add {} nodes, but we have reached our limit on how many nodes can be restarted ({})".format(nodes_to_add, self.max_restarts))
            else:
                add_node_callback(capped_nodes_to_add)
                self.nodes_added += capped_nodes_to_add
                log.info("Added {} nodes (total: {}/{})".format(capped_nodes_to_add, self.nodes_added, self.max_restarts))

class TasksStatus:
    def __init__(self, tasks):
        self.tasks = tasks

    @property
    def active_tasks(self):
        # compute how many nodes are needed to run everything in parallel
        last_needed_nodes = 0
        for task in self.tasks:
            if task.status in [STATUS_CLAIMED, STATUS_PENDING]:
                last_needed_nodes += 1
        return last_needed_nodes

    @property
    def failed_tasks(self):
        failures = 0
        for task in self.tasks:
            if task.status in [STATUS_FAILED]:
                failures += 1
            elif task.status in [STATUS_COMPLETE]:
                if str(task.exit_code) != "0":
                    failures += 1
        return failures

    @property
    def summary(self):
        return _summarize_task_statuses(self.tasks)



def addnodes_cmd(jq, cluster, args, config):
    job_id = _resolve_jobid(jq, args.job_id)
    return _addnodes(job_id, jq, cluster, args.count, None, config['default_url_prefix'])

def _addnodes(job_id, jq, cluster, count, preemptible, default_url_prefix):
    job = jq.get_job(job_id)
    log.info("Adding %d nodes to cluster %s", count, job.cluster)
    operation_ids = []
    timestamp = get_timestamp()
    for i in range(count):
        debug_log_url = _join(default_url_prefix, "node-logs", job_id, timestamp, "output-{}.log".format(i))
        operation_id = jq.add_node(job_id, cluster, preemptible, debug_log_url, job=job)
        log.info("adding node via operation %s, logs will be written to %s", operation_id, debug_log_url)
        operation_ids.append(operation_id)
    return operation_ids

def _resub_preempted(cluster, jq, jobid):
    tasks = jq.get_tasks(jobid, STATUS_CLAIMED)
    for task in tasks:
        _update_if_owner_missing(cluster, jq, task)

def _clean(cluster, jq, jobid, force=False):
    if not force:
        status_counts = jq.get_status_counts(jobid)
        log.debug("job %s has status %s", jobid, status_counts)
        if STATUS_CLAIMED in status_counts:
            # if some tasks are still marked 'claimed' verify that the owner is still running
            tasks = jq.get_tasks(jobid, STATUS_CLAIMED)
            for task in tasks:
                _update_if_owner_missing(cluster, jq, task)

            # now that we may have changed some tasks from claimed -> pending, check again
            status_counts = jq.get_status_counts(jobid)
            if STATUS_CLAIMED in status_counts:
                log.warning("job %s is still running (%s), cannot remove", jobid, status_counts)
                return False

    log.info("deleting %s", jobid)
    jq.delete_job(jobid)
    return True

def clean_cmd(cluster, jq, args):
    log.info("jobid_pattern: %s", args.jobid_pattern)
    jobids = _get_jobids_from_pattern(jq, args.jobid_pattern)
    for jobid in jobids:
        _clean(cluster, jq, jobid, args.force)

def _update_if_owner_missing(cluster, jq, task):
    if task.status != STATUS_CLAIMED:
        return
    if not cluster.is_owner_running(task.owner):
        job = jq.get_job(task.job_id)
        if job.status == JOB_STATUS_KILLED:
            new_status = STATUS_KILLED
        else:
            new_status = STATUS_PENDING
        log.info("Task %s is owned by %s which does not appear to be running, resetting status from 'claimed' to '%s'", task.task_id, task.owner, new_status)
        jq.reset_task(task.task_id, status= new_status)

def kill_cmd(jq, cluster, args):
    jobids = _get_jobids_from_pattern(jq, args.jobid_pattern)
    if len(jobids) == 0:
        log.warning("No jobs found matching pattern")
    for jobid in jobids:
        # TODO: stop just marks the job as it shouldn't run any more.  tasks will still be claimed.
        log.info("Marking %s as killed", jobid)
        ok, job = jq.kill_job(jobid)
        assert ok
        if not args.keepcluster:
            cluster.stop_cluster(job.cluster)
            tasks = jq.get_tasks_for_cluster(job.cluster, STATUS_CLAIMED)
            for task in tasks:
                _update_if_owner_missing(cluster, jq, task)

        # if there are any sit sitting at pending, mark them as killed
        tasks = jq.get_tasks(jobid, status=STATUS_PENDING)
        for task in tasks:
            jq.reset_task(task.task_id, status=STATUS_KILLED)


def dumpjob_cmd(jq, io, args):
    import attr
    tasks_as_dicts = []
    jobid = _resolve_jobid(jq, args.jobid)
    job = jq.get_job(jobid)
    job = attr.asdict(job)
    tasks = jq.get_tasks(jobid)
    for task in tasks:
        t = attr.asdict(task)

        task_args = io.get_as_str(task.args)
        t['args_url'] = t['args']
        t['args'] = json.loads(task_args)
        tasks_as_dicts.append(t)
    print(json.dumps(dict(job=job, tasks=tasks_as_dicts), indent=2, sort_keys=True))


def version_cmd():
    print(kubeque.__version__)


def get_func_parameters(func):
    import inspect
    return inspect.getargspec(func)[0]


def main(argv=None):
    parse = argparse.ArgumentParser()
    parse.add_argument("--config", default=None)
    parse.add_argument("--debug", action="store_true", help="If set, debug messages will be output")
    subparser = parse.add_subparsers()

    parser = subparser.add_parser("validate", help="Run a series of tests to confirm the configuration is valid")
    parser.set_defaults(func=validate_cmd)

    parser = subparser.add_parser("sub", help="Submit a command (or batch of commands) for execution")
    parser.set_defaults(func=submit_cmd)
    parser.add_argument("--resources", "-r",
                        help="Specify the resources that are needed for running job. (ie: -r memory=5G,cpu=0.9) ")
    parser.add_argument("--file", "-f",
                        help="Job specification file (in JSON).  Only needed if command is not specified.")
    parser.add_argument("--push", "-u", action="append", default=[],
                        help="Path to a local file which should be uploaded to working directory of command before execution starts.  If filename starts with a '@' the file is interpreted as a list of files which need to be uploaded.")
    parser.add_argument("--image", "-i",
                        help="Name of docker image to run job within.  Defaults to value from kubeque config file.")
    parser.add_argument("--name", "-n", help="The name to assign to the job")
    parser.add_argument("--seq", type=int,
                        help="Parameterize the command by 'index'.  Submitting with --seq=10 will submit 10 commands with a parameter 'index' varied from 1 to 10")
    parser.add_argument("--loglive", action="store_true", help="If set, will write stdout from tasks to StackDriver logging")
    parser.add_argument("--params", "-p",
                        help="Parameterize the command by the rows in the specified CSV file.  If the CSV file has 5 rows, then 5 commands will be submitted.")
    parser.add_argument("--fetch", help="After run is complete, automatically download the results")
    parser.add_argument("--dryrun", action="store_true",
                        help="Don't actually submit the job but just print what would have been done")
    parser.add_argument("--skipkube", action="store_true", dest="skip_kube_submit",
                        help="Do all steps except submitting the job to kubernetes")
    parser.add_argument("--no-wait", action="store_false", dest="wait_for_completion",
                        help="Exit immediately after submission instead of waiting for job to complete")
    parser.add_argument("--results", action="append",
                        help="Wildcard to use to find results which will be uploaded.  (defaults to '*')  Can be specified multiple times",
                        default=None, dest="results_wildcards")
    parser.add_argument("--cd", help="The directory to change to before executing the command", default=".",
                        dest="working_dir")
    parser.add_argument("--local", help="Run the tasks inside of docker on the local machine", action="store_true")
    parser.add_argument("--clean", help="If the job id already exists, 'clean' it first to avoid an error about the job already existing", action="store_true")
    parser.add_argument("--rerun", help="If set, will download all of the files from previous execution of this job to worker before running", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)

    parser = subparser.add_parser("addnodes", help="Add nodes to be used for executing a specific job")
    parser.set_defaults(func=addnodes_cmd)
    parser.add_argument("job_id", help="the job id used to determine which cluster node should be added to.")
    parser.add_argument("count", help="the number of worker nodes to add to the cluster", type=int)

    parser = subparser.add_parser("reset",
                                  help="Mark any 'claimed', 'killed' or 'failed' jobs as ready for execution again.  Useful largely only during debugging issues with job submission.")
    parser.set_defaults(func=reset_cmd)
    parser.add_argument("jobid_pattern")
    parser.add_argument("--owner")
    parser.add_argument("--resubmit", action="store_true")
    parser.add_argument("--all", action="store_true")

    parser = subparser.add_parser("listparams", help="Write to a csv file the parameters for each task")
    parser.set_defaults(func=list_params_cmd)
    parser.add_argument("jobid")
    parser.add_argument("filename", help="The filename to write the csv file containing the parameters")
    parser.add_argument("--incomplete", "-i",
                        help="By default, will list all parameters. If this flag is present, only those tasks which are not complete will be written to the csv",
                        action="store_true")
    parser.add_argument("--exitcode", "-e", help="Only include those tasks with this return code", type=int)
    parser.add_argument("--extra",
                        help="Add columns 'task_id' and 'exit_code' for each task",
                        action="store_true")

    #    parser = subparser.add_parser("retry", help="Resubmit any 'failed' jobs for execution again. (often after increasing memory required)")
    #    parser.set_defaults(func=retry_cmd)
    #    parser.add_argument("jobid_pattern")
    #    parser.add_argument("--resources", "-r", help="Update the resource requirements that should be used when re-running job. (ie: -r memory=5G,cpu=2) ")
    #    parser.add_argument("--owner", help="if specified, only tasks with this owner will be retried")
    #    parser.add_argument("--no-wait", action="store_false", dest="wait_for_completion", help="Exit immediately after submission instead of waiting for job to complete")

    parser = subparser.add_parser("dumpjob", help="Extract a json description of a submitted job")
    parser.set_defaults(func=dumpjob_cmd)
    parser.add_argument("jobid")

    parser = subparser.add_parser("status", help="Print the status for the tasks which make up the specified job")
    parser.set_defaults(func=status_cmd)
    parser.add_argument("--detailed", action="store_true", help="List attributes of each task")
    parser.add_argument("--failures", action="store_true", help="List attributes of each task (only for failures)")
    parser.add_argument("--wait", action="store_true",
                        help="If set, will periodically poll and print the status until all tasks terminate")
    parser.add_argument("--loglive", action="store_true", help="If set, will read stdout from tasks from StackDriver logging")
    parser.add_argument("jobid_pattern", nargs="?")

    parser = subparser.add_parser("saturate", help="Monitor the job, automatically adding nodes equal to the number of tasks, and re-add nodes when one is preempted")
    parser.set_defaults(func=saturate_cmd)
    parser.add_argument("jobid")
    parser.add_argument("--nodes", "-n", type=int, help="By default, saturate will try to create nodes equal to the number of tasks. This will allow you to override the number of nodes we will want to create")

    parser = subparser.add_parser("clean", help="Remove jobs which are not currently running from the database of jobs")
    parser.set_defaults(func=clean_cmd)
    parser.add_argument("jobid_pattern", nargs="?",
                        help="If specified will only attempt to remove jobs that match this pattern")
    parser.add_argument("--force", "-f", help="If set, will delete job regardless of whether it is running or not")

    parser = subparser.add_parser("kill", help="Terminate the specified job")
    parser.set_defaults(func=kill_cmd)
    parser.add_argument("--keepcluster", action="store_true",
                        help="If set will also terminate the nodes that the job is using to run. (This could impact other running jobs that use the same docker image)")
    parser.add_argument("jobid_pattern")

    parser = subparser.add_parser("fetch", help="Download results from a completed job")
    parser.set_defaults(func=fetch_cmd)
    parser.add_argument("jobid")
    parser.add_argument("--flat", action="store_true", help="Instead of writing each task into a seperate directory, write all files into the destination directory")
    parser.add_argument("--dest", help="The path to the directory where the results will be downloaded. If omitted a directory will be created with the job id")

    parser = subparser.add_parser("version", help="print the version and exit")
    parser.set_defaults(func=version_cmd)

    args = parse.parse_args(argv)

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
    else:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
        logging.getLogger("googleapiclient.discovery").setLevel(logging.WARN)

    if not hasattr(args, 'func'):
        parse.print_help()
        sys.exit(1)

    func_param_names = get_func_parameters(args.func)
    if len(set(["config", "jq", "io"]).intersection(func_param_names)) > 0:
        config_path = get_config_path(args.config)
        log.info("Using config: %s", config_path)
        config, jq, io, cluster = load_config(config_path)
    func_params = {}
    if "args" in func_param_names:
        func_params["args"] = args
    if "config" in func_param_names:
        func_params["config"] = config
    if "io" in func_param_names:
        func_params["io"] = io
    if "jq" in func_param_names:
        func_params["jq"] = jq
    if 'cluster' in func_param_names:
        func_params['cluster'] = cluster

    args.func(**func_params)


def get_config_path(config_path):
    if config_path is not None:
        if not os.path.exists(config_path):
            raise Exception("Could not find config at {}".format(config_path))
        return config_path
    else:
        def possible_config_names():
            for config_name in [".sparkles", ".kubeque"]:
                dirname = os.path.abspath(".")
                while True:
                    yield os.path.join(dirname, config_name)
                    next_dirname = os.path.dirname(dirname)
                    if dirname == next_dirname:
                        break
                    dirname = next_dirname
            return

        checked_paths = []
        for config_path in possible_config_names():
            if os.path.exists(config_path):
                return config_path
            checked_paths.append(config_path)

        raise Exception("Could not find config file at any of locations: {}".format(checked_paths))


if __name__ == "__main__":
    main(sys.argv[1:])
