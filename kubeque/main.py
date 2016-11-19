import time
import random
import logging
import os
import json
import re
import sys
import hashlib
import tempfile
import subprocess
from glob import glob

from kubeque import kubesub as kube
from kubeque.kubesub import submit_job
from kubeque.gcp import create_gcs_job_queue, IO

from contextlib import contextmanager

from kubeque.spec import make_spec_from_command
import csv


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
    task_spec = dict(common)
    for attr in ['helper_log', 'command']:
        if attr in task:
            task_spec[attr] = task[attr]
    task_spec['uploads'].extend(task.get('uploads', []))
    task_spec['downloads'].extend(task.get('downloads', []))
    return task_spec


def rewrite_url_in_dict(d, prop_name, default_url_prefix):
    if not (prop_name in d):
        return d

    d = dict(d)
    url = d[prop_name]
    # look to see if we have a rooted url, or a relative path
    if not (":" in url):
        d[prop_name] = default_url_prefix + url
    return d

def rewrite_uploads(uploads, default_url_prefix):
    return [ rewrite_url_in_dict(x, 'dst_url', default_url_prefix) for x in uploads ]

def rewrite_downloads(io, downloads, default_url_prefix):
    def rewrite_download(url):
        if "src" in url:
            # upload to CAS if the source isn't a url
            src_url = io.write_file_to_cas(url["src"])
        else:
            src_url = url['src_url']

        return dict(src_url=src_url, dst=url['dst'], executable=url.get("executable", False))

    src_expanded = [ rewrite_download(x) for x in downloads ]

    return [rewrite_url_in_dict(x, "src_url", default_url_prefix) for x in src_expanded]

def upload_config_for_consume(io, config):
    consume_config = {}
    for key in ['cas_url_prefix', 'project']:
        consume_config[key] = config[key]

    log.debug("consume_config: %s", consume_config)
    config_url = io.write_str_to_cas(json.dumps(consume_config))
    return config_url


def expand_tasks(spec, io, default_url_prefix, default_job_url_prefix):
    common = spec['common']
    common['downloads'] = rewrite_downloads(io, common.get('downloads', []), default_url_prefix)
    common['uploads'] = rewrite_uploads(common.get('uploads', []), default_job_url_prefix)

    tasks = []
    for task in spec['tasks']:
        task = expand_task_spec(common, task)
        task = rewrite_url_in_dict(task, "command_result_url", default_job_url_prefix)
        task['downloads'] = rewrite_downloads(io, task['downloads'], default_url_prefix)
        task['uploads'] = rewrite_uploads(task['uploads'], default_url_prefix)
        tasks.append(task)
    return tasks

def submit(jq, io, job_id, spec, dry_run, config, skip_kube_submit):
    # where to take this from? arg with a default of 1?
    if dry_run:
        skip_kube_submit = True

    default_url_prefix = config.get("default_url_prefix", "")
    default_job_url_prefix = default_url_prefix+job_id+"/"

    tasks = expand_tasks(spec, io, default_url_prefix, default_job_url_prefix)
    task_spec_urls = []
    for task in tasks:    
        if not dry_run:
            url = io.write_json_to_cas(task)
            task_spec_urls.append(url)
        else:
            print("task:", json.dumps(task, indent=2))

    log.info("job_id: %s", job_id)
    if not dry_run:
        jq.submit(job_id, task_spec_urls)
        config_url = upload_config_for_consume(io, config)
        cas_url_prefix = config['cas_url_prefix']
        project = config['project']
        kubeque_command = ["kubeque-consume", config_url, job_id, "--project", project, "--cas_url_prefix", cas_url_prefix]
        if not skip_kube_submit:
            image = spec['image']
            #[("GOOGLE_APPLICATION_CREDENTIALS", "/google_creds/cred")], [("kube")]
            parallelism = len(tasks)
            resources = spec["resources"]
            submit_job(job_id, parallelism, image, kubeque_command, cpu_request=resources.get("cpu",config['default_resource_cpu']), mem_limit=resources.get("memory",config["default_resource_memory"]))
        else:
            log.info("Skipping submission: %s", " ".join(kubeque_command))
        return config_url

def load_config(config_file):
    config_file = os.path.expanduser(config_file)

    config = ConfigParser()
    config.read(config_file)
    config = dict(config.items('config'))

    return [config] + list(load_config_from_dict(config))

def load_config_from_dict(config):
    io = IO(config['project'], config['cas_url_prefix'])
    jq = create_gcs_job_queue(config['project'])

    return jq, io

def new_job_id():
    import uuid
    import datetime
    d = datetime.datetime.now()
    return d.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4]

def read_parameters_from_csv(filename):
    with open(filename, "rt") as fd:
        return list(csv.DictReader(fd))

def submit_cmd(jq, io, args, config):
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
        upload_map, spec = make_spec_from_command(args.command, 
            image,
            dest_url=default_url_prefix+job_id, 
            cas_url=cas_url_prefix,
            parameters=parameters, 
            resources=args.resources)
        log.debug("upload_map = %s", upload_map)
        for filename, dest in upload_map.items():
            io.put(filename, dest)

    log.debug("spec: %s", json.dumps(spec, indent=2))
    submit(jq, io, job_id, spec, args.dryrun, config, args.skip_kube_submit)

    if args.wait_for_completion:
        log.info("Waiting for job to terminate")
        watch(jq, job_id)

def reset_cmd(jq, io, args):
    jq.reset(args.jobid)

def status_cmd(jq, io, args):
    jobid_pattern = args.jobid_pattern
    if not jobid_pattern:
        jobid_pattern = "*"
    for jobid in jq.get_jobids(jobid_pattern):
        counts = jq.get_status_counts(jobid)
        status_str = ", ".join([ "{}: {}".format(status, count) for status, count in counts.items()])
        log.info("%s: %s", jobid, status_str)

def fetch_cmd(jq, io, args):
    tasks = jq.get_tasks(args.jobid)

    if not os.path.exists(args.dest):
        os.mkdir(args.dest)

    include_index = len(tasks) > 1

    for i, task in enumerate(tasks):
        spec = json.loads(io.get_as_str(task.args))
        log.debug("task %d spec: %s", i, spec)

        if include_index:
            dest = os.path.join(args.dest, str(i))
            if not os.path.exists(dest):
                os.mkdir(dest)
        else:
            dest = args.dest

        io.get(spec['stdout_url'], os.path.join(dest, "stdout.txt"))

        command_result = json.loads(io.get_as_str(spec['command_result_url']))
        log.debug("command_result: %s", json.dumps(command_result))
        for ul in command_result['files']:
            assert not (ul['src'].startswith("/")), "Source must be a relative path"
            assert not (ul['src'].startswith("../")), "Source must not refer to parent dir"
            localpath = os.path.join(dest, ul['src'])
            log.info("Downloading to %s", localpath)
            io.get(ul['dst_url'], localpath)


def is_terminal_status(status):
    return status in ["failed", "success"]

def is_complete(counts):
    complete = True
    for status in counts.keys():
        if not is_terminal_status(status):
            complete = False
    return complete

def watch(jq, jobid, refresh_delay=5):
    prev_counts = None
    while True:
        counts = jq.get_status_counts(jobid)
        complete = is_complete(counts)
        if counts != prev_counts:
            log.info("status: %s", counts)
        if complete:
            break
        prev_counts = counts
        time.sleep(refresh_delay)

def remove_cmd(jq, args):
    jobids = jq.get_jobids(args.jobid_pattern)
    for jobid in jobids:
        status_counts = jq.get_status_counts(jobid)
        if not is_complete(status_counts) and not ("pending" in status_counts and len(status_counts) == 1):
            log.warn("job %s is still running (%s), cannot remove", jobid, status_counts)
        else:
            log.info("deleting %s", jobid)
            kube.delete_job(jobid)
            jq.delete_job(jobid)

def start_cmd(config):
    kube.start_cluster(config['cluster_name'], config['machine_type'], 1)

def stop_cmd():
    kube.stop_job(cluster_name)

def kill_cmd(jq, args):
    jobids = jq.get_jobids(args.jobid_pattern)
    for jobid in jobids:
        kube.stop_job(jobid)
        # TODO: stop just marks the job as it shouldn't run any more.  tasks will still be claimed.
        # do we let the re-claimer transition these to pending?  Or do we cancel pending and mark claimed as failed?
        # probably we should

import argparse

def get_func_parameters(func):
    import inspect
    return inspect.getargspec(func)[0]

def main(argv=None):
    logging.basicConfig(level=logging.INFO)

    parse = argparse.ArgumentParser()
    parse.add_argument("--config", default="~/.kubeque")
    subparser = parse.add_subparsers()

    parser = subparser.add_parser("sub")
    parser.set_defaults(func=submit_cmd)
    parser.add_argument("--resources", "-r")
    parser.add_argument("--file", "-f")
    parser.add_argument("--image", "-i")
    parser.add_argument("--name", "-n")
    parser.add_argument("--seq", type=int)
    parser.add_argument("--params")
    parser.add_argument("--dryrun", action="store_true")
    parser.add_argument("--skipkube", action="store_true", dest="skip_kube_submit")
    parser.add_argument("--no-wait", action="store_false", dest="wait_for_completion")
    parser.add_argument("command", nargs=argparse.REMAINDER)

    parser = subparser.add_parser("reset")
    parser.set_defaults(func=reset_cmd)
    parser.add_argument("jobid_pattern")

    parser = subparser.add_parser("status")
    parser.set_defaults(func=status_cmd)
    parser.add_argument("jobid_pattern", nargs="?")

    parser = subparser.add_parser("remove")
    parser.set_defaults(func=remove_cmd)
    parser.add_argument("jobid_pattern")

    parser = subparser.add_parser("kill")
    parser.set_defaults(func=kill_cmd)
    parser.add_argument("jobid_pattern")

    parser = subparser.add_parser("start")
    parser.set_defaults(func=start_cmd)

    parser = subparser.add_parser("stop")
    parser.set_defaults(func=stop_cmd)

    parser = subparser.add_parser("fetch")
    parser.set_defaults(func=fetch_cmd)
    parser.add_argument("jobid")
    parser.add_argument("dest")

    args = parse.parse_args(argv)
    
    if not hasattr(args, 'func'):
        parse.print_help()
        sys.exit(1)
    
    func_param_names = get_func_parameters(args.func)
    if len(set(["config", "jq", "io"]).intersection(func_param_names)) > 0:
        config, jq, io = load_config(args.config)
    func_params = {}
    if "args" in func_param_names:
        func_params["args"] = args
    if "config" in func_param_names:
        func_params["config"] = config
    if "io" in func_param_names:
        func_params["io"] = io
    if "jq" in func_param_names:
        func_params["jq"] = jq

    # if args.func != consume_cmd:
    #     args.config_obj = config
    #     args.func(jq, io, args)
    # else:
    args.func(**func_params)

NOTES = """
Assume process has all necessary with tokens in environment.

Job def consists of
Download via mapping.  Mapping defined as list of (URL, name).   Will download to relative path.
(1) Download file SRC destination
Download folder s3prefix destination 
(1) Execute command stdoutpath stdoutpath, cmdoutpath, command string
     Cmdoutpath will include the return code as well as any useful stats
(1) Upload file SRC destination 
Upload dir destination
Upload to casaddr, path for mapping file to write, list of src filenames

Downloading missing file is hard error 
Uploading missing file is a warning 
Logdest where to write/upload helper output.

Cmd: helper cmdfileins3

Need with for dynamo read/write, s3 read/write/list

Stack driver for logging?

Helper main: 
  Loop forever
  Claim task
  If none stop
  Use args as s3cmdfile
  Download file
  Execute downloads
  Execute command 
  Execute uploads
  Mark task done

One entry point to make jobs.  
"""


if __name__ == "__main__":
    main(sys.argv[1:])

