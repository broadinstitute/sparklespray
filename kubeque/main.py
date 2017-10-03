import time
import logging
import os
import json
import sys

import kubeque
from kubeque.gcp import create_gcs_job_queue, IO, STATUS_PENDING, STATUS_FAILED, STATUS_COMPLETE, STATUS_CLAIMED, \
    STATUS_KILLED
from kubeque.hasher import CachingHashFunction

from kubeque.spec import make_spec_from_command, SrcDstPair, add_file_to_upload_map
import csv
import copy
import contextlib
import kubeque.gcs_pipeline as pipeline
import argparse

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
    for attr in ['helper_log', 'command']:
        if attr in task:
            task_spec[attr] = task[attr]
    task_spec['uploads'].extend(task.get('uploads', []))
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


def rewrite_uploads(uploads, default_url_prefix):
    return [rewrite_url_in_dict(x, 'dst_url', default_url_prefix) for x in uploads]


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


def expand_tasks(spec, io, default_url_prefix, default_job_url_prefix):
    common = spec['common']
    common['downloads'] = rewrite_downloads(io, common.get('downloads', []), default_url_prefix)
    common['uploads'] = rewrite_uploads(common.get('uploads', []), default_job_url_prefix)

    tasks = []
    for task_i, spec_task in enumerate(spec['tasks']):
        task_url_prefix = "{}/{}".format(default_job_url_prefix, task_i + 1)
        task = expand_task_spec(common, spec_task)
        task['downloads'] = rewrite_downloads(io, task['downloads'], default_url_prefix)
        task['uploads'] = rewrite_uploads(task['uploads'], task_url_prefix)
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


def _make_cluster_name(image, cpu_request, mem_limit, unique_name):
    import hashlib
    if unique_name:
        return 'l-' + _random_string(10)
    return "c-" + hashlib.md5("{}-{}-{}-{}".format(image, cpu_request, mem_limit, kubeque.__version__).encode("utf8")).hexdigest()[:10]


def submit(jq, io, cluster, job_id, spec, dry_run, config, skip_kube_submit, metadata, kubequeconsume_url,
           exec_local=False):
    log.info("Submitting job with id: %s", job_id)

    # where to take this from? arg with a default of 1?
    if dry_run:
        skip_kube_submit = True

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
        cluster_name = _make_cluster_name(image, cpu_request, mem_limit, unique_name=skip_kube_submit)

        stage_dir = config.get("mount", "/mnt/kubeque-data")
        project = config['project']
        kubeque_exe_in_container = stage_dir + "/kubequeconsume"
        kubeque_command = [kubeque_exe_in_container, "consume", "--cluster", cluster_name, "--projectId", project,
                           "--cacheDir", stage_dir + "/cache",
                           "--tasksDir", stage_dir + "/tasks", "--zones", ",".join(config['zones'])]
        kubeque_command = "chmod +x {} && {}".format(kubeque_exe_in_container, " ".join(kubeque_command))

        logging_url = config["default_url_prefix"] + "/node-logs"
        pipeline_spec = cluster.create_pipeline_spec(
            image,
            kubeque_command,
            stage_dir,
            logging_url,
            kubequeconsume_url,
            cpu_request,
            mem_limit,
            cluster_name)

        jq.submit(job_id, list(zip(task_spec_urls, command_result_urls)), pipeline_spec, metadata, cluster_name)
        if not skip_kube_submit and not exec_local:
            existing_nodes = cluster.get_cluster_status(cluster_name)
            if not existing_nodes.is_running():
                log.info("Adding initial node for cluster")
                cluster.add_node(pipeline_spec)
            else:
                log.info("Cluster already exists, not adding node. Cluster status: %s", existing_nodes.as_string())
        elif exec_local:
            cmd = _write_local_script(job_id, spec, kubeque_command, config['kubequeconsume_exe_path'],
                                      kubeque_exe_in_container)
            log.info("Running job locally via executing: ./%s", cmd)
            os.system(os.path.abspath(cmd))
        else:
            cmd = _write_local_script(job_id, spec, kubeque_command, config['kubequeconsume_exe_path'],
                                      kubeque_exe_in_container)
            log.info("Skipping submission.  You can execute tasks locally via: ./%s", cmd)


def _write_local_script(job_id, spec, kubeque_command, kubequeconsume_exe_path, kubeque_exe_in_container):
    from kubeque.gcp import _gcloud_cmd
    import stat

    image = spec['image']
    cmd = _gcloud_cmd(
        ["docker", "--", "run",
         "-v", os.path.expanduser("~/.config/gcloud") + ":/google-creds",
         "-e", "GOOGLE_APPLICATION_CREDENTIALS=/google-creds/application_default_credentials.json",
         "-v", kubequeconsume_exe_path + ":" + kubeque_exe_in_container,
         image, 'bash -c "' + kubeque_command + ' --owner local"', ])
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


def new_job_id():
    import uuid
    import datetime
    d = datetime.datetime.now()
    return d.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4]


def read_parameters_from_csv(filename):
    with open(filename, "rt") as fd:
        return list(csv.DictReader(fd))


def expand_files_to_upload(filenames):
    def split_into_src_dst_pairs(filename):
        if ":" in filename:
            src_dst = filename.split(":")
            assert len(src_dst) == 2, "Could not split {} into a source and destination path".format(repr(filename))
            src, dst = src_dst
            # assert os.path.exists(src)

            return make_src_dst_pairs(src, dst)
        else:
            return make_src_dst_pairs(filename, ".")

    def make_src_dst_pairs(src, dst):
        if os.path.isdir(src):
            files_in_dir = [os.path.join(src, x) for x in os.listdir(src)]
            files_in_dir = [x for x in files_in_dir if not os.path.isdir(x)]
            return [SrcDstPair(fn, os.path.normpath(os.path.join(dst, os.path.basename(fn)))) for fn in files_in_dir]
        else:
            if dst == ".":
                return [SrcDstPair(src, os.path.basename(src))]
            else:
                return [SrcDstPair(src, dst)]

    # preprocess list of files to handle those that are actual a file containing list of more files
    expanded = []
    for filename in filenames:
        if filename.startswith("@"):
            with open(filename[1:], "rt") as fd:
                file_list = [x.strip() for x in fd.readlines() if x.strip() != ""]
                expanded.extend(file_list)
        else:
            expanded.append(filename)

    fully_expanded = []
    for x in expanded:
        fully_expanded.extend(split_into_src_dst_pairs(x))

    return fully_expanded


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


def submit_cmd(jq, io, cluster, args, config):
    metadata = {}

    if args.image:
        image = args.image
    else:
        image = config['default_image']

    job_id = args.name
    if job_id is None:
        job_id = new_job_id()

    cas_url_prefix = config['cas_url_prefix']
    default_url_prefix = config['default_url_prefix']

    if args.file:
        assert len(args.command) == 0
        spec = json.load(open(args.file, "rt"))
    else:
        if args.seq is not None:
            parameters = [{"i": str(i)} for i in range(args.seq)]
        elif args.params is not None:
            parameters = read_parameters_from_csv(args.params)
        else:
            parameters = [{}]

        assert len(args.command) != 0

        resource_spec = _parse_resources(args.resources)

        hash_db = CachingHashFunction(config.get("cache_db_path", ".kubeque-cached-file-hashes"))
        upload_map, spec = make_spec_from_command(args.command,
                                                  image,
                                                  dest_url=default_url_prefix + job_id,
                                                  cas_url=cas_url_prefix,
                                                  parameters=parameters,
                                                  resource_spec=resource_spec,
                                                  hash_function=hash_db.hash_filename,
                                                  src_wildcards=args.results_wildcards,
                                                  extra_files=expand_files_to_upload(args.push),
                                                  working_dir=args.working_dir)

        kubequeconsume_exe_path = config['kubequeconsume_exe_path']
        kubequeconsume_exe_url = add_file_to_upload_map(upload_map, hash_db.hash_filename, cas_url_prefix,
                                                        kubequeconsume_exe_path, "!KUBEQUECONSUME")

        hash_db.persist()

        log.debug("upload_map = %s", upload_map)
        for filename, dest in upload_map.items():
            io.put(filename, dest, skip_if_exists=True)

    log.debug("spec: %s", json.dumps(spec, indent=2))
    submit(jq, io, cluster, job_id, spec, args.dryrun, config, args.skip_kube_submit, metadata, kubequeconsume_exe_url,
           args.local)

    finished = False
    if args.local:
        # if we ran it within docker, and the docker command completed, then the job is done
        finished = True
    else:
        if not (args.dryrun or args.skip_kube_submit) and args.wait_for_completion:
            log.info("Waiting for job to terminate")
            watch(jq, job_id, cluster)
            finished = True

    if finished:
        if args.fetch:
            log.info("Done waiting for job to complete, downloading results to %s", args.fetch)
            fetch_cmd_(jq, io, job_id, args.fetch)
        else:
            log.info("Done waiting for job to complete, results written to %s", default_url_prefix + "/" + job_id)
            log.info("You can download results via 'gsutil rsync -r %s DEST_DIR'", default_url_prefix + "/" + job_id)


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
            watch(jq, job_id, cluster)


def list_params_cmd(jq, io, args):
    jobid = _resolve_jobid(jq, args.jobid)

    if args.incomplete:
        tasks = []
        for status in [STATUS_FAILED, STATUS_CLAIMED, STATUS_PENDING, STATUS_KILLED]:
            tasks.extend(jq.get_tasks(jobid, status=status))
    else:
        tasks = jq.get_tasks(jobid)

    if len(tasks) == 0:
        print("No tasks found")
    else:
        print("Getting parameters from %d tasks" % len(tasks))
        parameters = []
        for task in tasks:
            task_spec = json.loads(io.get_as_str(task.args))
            parameters.append(task_spec.get('parameters', {}))

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
                row = [p.get(column, "") for column in columns]
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


def status_cmd(jq, io, cluster, args):
    jobids = _get_jobids_from_pattern(jq, args.jobid_pattern)

    if args.wait:
        assert len(jobids) == 1, "When watching, only one jobid allowed, but the following matched wildcard: {}".format(
            jobids)
        jobid = jobids[0]
        watch(jq, jobid, cluster)
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
                             "  args: %s, history: %s%s", task.task_id,
                             task.status, task.exit_code, task.failure_reason, task.owner, task.args, task.history,
                             command_result_block)

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
    fetch_cmd_(jq, io, jobid, dest)

def fetch_cmd_(jq, io, jobid, dest_root, force=False):
    def get(src, dst, **kwargs):
        if os.path.exists(dst) and not force:
            log.warning("%s exists, skipping download", dst)
        return io.get(src, dst, **kwargs)

    tasks = jq.get_tasks(jobid)

    if not os.path.exists(dest_root):
        os.mkdir(dest_root)

    include_index = len(tasks) > 1

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

        # figure out the common path
        common_prefix = _commonprefix([src for src, _ in to_download])
        for src, dst_url in to_download:
            localpath = os.path.join(dest, os.path.relpath(src, common_prefix))
                # assert not (ul['src'].startswith("/")), "Source must be a relative path: {}".format(repr(ul))
                # assert not (ul['src'].startswith("../")), "Source must not refer to parent dir"
                # localpath = os.path.join(dest, ul['src'])
            get(dst_url, localpath)


def _is_terminal_status(status):
    return status in [STATUS_FAILED, STATUS_COMPLETE]


def _is_complete(status_counts):
    all_terminal = True
    for status in status_counts.keys():
        if not _is_terminal_status(status):
            all_terminal = True
    return all_terminal


@contextlib.contextmanager
def _exception_guard(deferred_msg):
    try:
        yield
    except OSError as ex:
        # consider these as non-fatal
        msg = deferred_msg()
        log.exception(msg)
        log.warning("Ignoring exception and continuing...")


def watch(jq, jobid, cluster, refresh_delay=5, min_check_time=10):
    job = jq.get_job(jobid)
    cluster_name = job.cluster
    prev_status = None
    last_cluster_update = None
    last_cluster_status = None
    last_good_state_time = time.time()
    while True:
        with _exception_guard(lambda: "summarizing status of job {} threw exception".format(jobid)):
            status, complete = _summarize_task_statuses(jq.get_tasks(jobid))
            if status != prev_status:
                log.info("Tasks: %s", status)
            if complete:
                break
            prev_status = status

        if last_cluster_update is None or time.time() - last_cluster_update > 10:
            with _exception_guard(lambda: "summarizing cluster threw exception".format(jobid)):
                cluster_status = cluster.get_cluster_status(cluster_name)
                if last_cluster_status is None or cluster_status != last_cluster_status:
                    log.info("Nodes: %s", cluster_status.as_string())
                if cluster_status.is_running():
                    last_good_state_time = time.time()
                else:
                    if time.time() - last_good_state_time > min_check_time:
                        log.error("Tasks haven't completed, but cluster is now offline. Aborting!")
                        raise Exception("Cluster prematurely stopped")
                last_cluster_status = cluster_status
                last_cluster_update = time.time()

        time.sleep(refresh_delay)


def addnodes_cmd(jq, cluster, args):
    job_id = _resolve_jobid(jq, args.job_id)
    job = jq.get_job(job_id)

    spec = json.loads(job.kube_job_spec)
    for i in range(args.count):
        cluster.add_node(spec)


def clean_cmd(jq, args):
    jobids = _get_jobids_from_pattern(jq, args.jobid_pattern)
    for jobid in jobids:
        status_counts = jq.get_status_counts(jobid)
        if not _is_complete(status_counts) and not ("pending" in status_counts and len(status_counts) == 1):
            log.warning("job %s is still running (%s), cannot remove", jobid, status_counts)
        else:
            log.info("deleting %s", jobid)
            jq.delete_job(jobid)


def kill_cmd(jq, cluster, args):
    jobids = _get_jobids_from_pattern(jq, args.jobid_pattern)
    if len(jobids) == 0:
        log.warning("No jobs found matching pattern")
    for jobid in jobids:
        # TODO: stop just marks the job as it shouldn't run any more.  tasks will still be claimed.
        log.info("Marking %s as killed", jobid)
        ok, job = jq.kill_job(jobid)
        assert ok
        if args.killcluster:
            cluster.stop_cluster(job.cluster)


def dumpjob_cmd(jq, io, args):
    import attr
    tasks_as_dicts = []
    tasks = jq.get_tasks(args.jobid)
    for task in tasks:
        t = attr.asdict(task)

        task_args = io.get_as_str(task.args)
        t['args_url'] = t['args']
        t['args'] = json.loads(task_args)
        tasks_as_dicts.append(t)
    print(json.dumps(dict(tasks=tasks_as_dicts), indent=2, sort_keys=True))


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
                        help="Parameterize the command by index.  Submitting with --seq=10 will submit 10 commands with a parameter index varied from 1 to 10")
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
    parser.add_argument("jobid_pattern", nargs="?")

    parser = subparser.add_parser("clean", help="Remove completed jobs from the database of jobs")
    parser.set_defaults(func=clean_cmd)
    parser.add_argument("jobid_pattern", nargs="?",
                        help="If specified will only attempt to remove jobs that match this pattern")

    parser = subparser.add_parser("kill", help="Terminate the specified job")
    parser.set_defaults(func=kill_cmd)
    parser.add_argument("--killcluster", "-k", action="store_true",
                        help="If set will also terminate the nodes that the job is using to run. (This could impact other running jobs that use the same docker image)")
    parser.add_argument("jobid_pattern")

    parser = subparser.add_parser("fetch", help="Download results from a completed job")
    parser.set_defaults(func=fetch_cmd)
    parser.add_argument("jobid")
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
    else:
        config_path = ".kubeque"
        if not os.path.exists(config_path):
            config_path = os.path.expanduser("~/.kubeque")
            if not os.path.exists(config_path):
                raise Exception("Could not find config file at neither ./.kubeque nor ~/.kubeque")
    return config_path


if __name__ == "__main__":
    main(sys.argv[1:])
