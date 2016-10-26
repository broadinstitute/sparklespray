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

from kubeque.kubesub import submit_job
from kubeque.aws import JobQueue, IO

from contextlib import contextmanager


log = logging.getLogger(__name__)

#try:
from configparser import ConfigParser
#except:
#    from ConfigParser import ConfigParser

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
    for key in ['cas_url_prefix', 'fake_s3_port', 'dynamodb_prefix', 'dynamodb_region', 'dynamodb_host']:
        consume_config[key] = config.get(key)

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

def submit(jq, io, job_id, spec, dry_run, config, skip_kube_submit=False):
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

    if not dry_run:
        jq.submit(job_id, task_spec_urls)
        config_url = upload_config_for_consume(io, config)
        # owner might need to be changed to generate UUID at startup.  Kubernettes isn't really going to have a way
        # of finding which owners are stale.  May need to use heartbeats after all?
        if not skip_kube_submit:
            image = spec['image']
            submit_job(job_id, 1, image, ["kubeque", "consume", config_url, job_id, "owner"])
        return config_url

@contextmanager
def redirect_output_to_file(filename):
    yield
    # sys.stdout.flush()
    # sys.stderr.flush()

    # saved_stdout = os.dup(sys.stdout.fileno())
    # saved_stderr = os.dup(sys.stderr.fileno())

    # log_file = open(filename, "w")
    # os.dup2(log_file.fileno(), sys.stdout.fileno())
    # os.dup2(log_file.fileno(), sys.stderr.fileno())
    # yield None
    # sys.stdout.flush()
    # sys.stderr.flush()
    
    # os.dup2(saved_stdout, sys.stdout.fileno())
    # os.dup2(saved_stderr, sys.stderr.fileno())

############################################

# Commands

def load_config(config_file):
    config_file = os.path.expanduser(config_file)

    config = ConfigParser()
    config.read(config_file)
    config = dict(config.items('config'))

    print("config", config)

    if 'aws_access_key_id' in config:
        os.environ['AWS_ACCESS_KEY_ID'] = config['aws_access_key_id']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['aws_secret_access_key']

    return [config] + list(load_config_from_dict(config))

def load_config_from_dict(config):

    fake_s3_port = None
    if "fake_s3_port" in config and config["fake_s3_port"] is not None:
        fake_s3_port = int(config["fake_s3_port"])
        aws_access_key_id = "fake"
        aws_secret_access_key = "fake"
    else:
        aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
        aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

    io = IO(aws_access_key_id, aws_secret_access_key, config['cas_url_prefix'], fake_s3_port)
    
    jq = JobQueue(config['dynamodb_prefix'], config['dynamodb_region'], config.get('dynamodb_host'))

    return jq, io

def new_id():
    import uuid
    return uuid.uuid4().hex

from kubeque.spec import make_spec_from_command

def submit_cmd(jq, io, args):
    config = args.config_obj
    cas_url_prefix = config['cas_url_prefix']
    default_url_prefix = config['default_url_prefix']
    if args.file:
        assert len(args.command) == 0
        spec = json.load(open(args.file, "rt"))
    else:
        assert len(args.command) != 0
        upload_map, spec = make_spec_from_command(args.command, args.image,
            dest_url=default_url_prefix, cas_url=cas_url_prefix)

    job_id = args.name
    if job_id is None:
        job_id = new_id()

    print("spec", json.dumps(spec, indent=2))
    submit(jq, io, job_id, spec, args.dryrun, args.config_obj)

def delete_cmd(jq, io, args):
    jq.delete(args.jobid)

def reset_cmd(jq, io, args):
    jq.reset(args.jobid)

def status_cmd(jq, io, args):
    counts = jq.get_status_counts(args.jobid)
    print(counts)

def fetch_cmd(jq, io, args):
    tasks = jq.get_tasks(args.jobid)
    for task in tasks:
        spec = json.loads(io.get_as_str(task.args))
        command_result = json.loads(io.get_as_str(spec['command_result_url']))
        for ul in command_result['files']:
            localpath = os.path.join(args.dest, ul['src'])
            log.info("Downloading to %s", localpath)
            io.get(ul['dst_url'], localpath)

def exec_command_(command, workdir, stdout):
    log.info("(workingdir: %s) Executing: %s", workdir, command)
    stdoutfd = os.open(stdout, os.O_WRONLY | os.O_APPEND | os.O_CREAT)
    try:
        retcode = subprocess.call(command, stderr=subprocess.STDOUT, stdout=stdoutfd, shell=True, cwd=workdir)
    finally:
        os.close(stdoutfd)
    return retcode

def write_result_file(command_result_path, retcode, local_to_url_mapping):
    with open(command_result_path, "wt") as fd:
        fd.write(json.dumps({"return_code": retcode, "files": local_to_url_mapping}))

def resolve_uploads(dir, uploads):
    resolved = []
    for ul in uploads:
        src = os.path.join(dir, ul['src'])
        if os.path.exists(src):
            resolved.append(dict(src=ul['src'], dst_url=ul['dst_url']))
    return resolved

def get_fake_s3_port_from_url(url):
    m = re.match("fakes3://(\\d+)/.*", url)
    if m is None:
        return None
    return int(m.group(1))


def consume_cmd(args):
    "This is what is executed by a worker"
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

    # create an incomplete IO object that at least can do a fetch to get the full config
    # maybe just make the config public in the CAS and then there's no problem.   In theory the hash 
    # should not be guessable, so just as private as anything else.  (Although, use sha256 instead of md5)
    fake_s3_port = get_fake_s3_port_from_url(args.config_url)
    io = IO(aws_access_key_id, aws_secret_access_key, None, fake_s3_port)
    config = json.loads(io.get_as_str(args.config_url))
    jq, io = load_config_from_dict(config)

    def exec_task(task_id, json_url):
        # make working directory.  A directory for the task with two subdirs ("log" where stdout/stderr is written and return code, "work" the working directory the task will be run in)
        taskdir = tempfile.mkdtemp(prefix="task-")
        logdir = os.path.join(taskdir, "log")
        workdir = os.path.join(taskdir, "work")
        os.mkdir(logdir)
        os.mkdir(workdir)

        stdout_path = os.path.join(logdir, "stdout.txt")
        result_path = os.path.join(logdir, "result.json")

        spec = json.loads(io.get_as_str(json_url))
        with redirect_output_to_file(stdout_path):
            for dl in spec['downloads']:
                io.get(dl['src_url'], os.path.join(workdir, dl['dst']))

            retcode = exec_command_(spec['command'], workdir, stdout_path)

            local_to_url_mapping = resolve_uploads(workdir, spec['uploads'])
            for ul in local_to_url_mapping:
                io.put(os.path.join(workdir, ul['src']), ul['dst_url'])

            write_result_file(result_path, retcode, local_to_url_mapping)
            io.put(result_path, spec['command_result_url'])

    consumer_run_loop(jq, args.jobid, args.name, exec_task)

def consumer_run_loop(jq, job_id, owner_name, execute_callback):
    while True:
        claimed = jq.claim_task(job_id, owner_name)
        log.info("claimed: %s", claimed)
        if claimed is None:
            break
        task_id, args = claimed
        log.info("task_id: %s, args: %s", task_id, args)
        execute_callback(task_id, args)
        jq.task_completed(task_id, True)

import argparse

def main(argv=None):
    logging.basicConfig(level=logging.INFO)

    parse = argparse.ArgumentParser()
    parse.add_argument("--config", default="~/.kubeque")
    subparser = parse.add_subparsers()

    parser = subparser.add_parser("sub")
    parser.set_defaults(func=submit_cmd)
    parser.add_argument("--file", "-f")
    parser.add_argument("--image", "-i")
    parser.add_argument("--name", "-n")
    parser.add_argument("--dryrun", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)

    parser = subparser.add_parser("del")
    parser.set_defaults(func=delete_cmd)
    parser.add_argument("jobid")

    parser = subparser.add_parser("reset")
    parser.set_defaults(func=reset_cmd)
    parser.add_argument("jobid")

    parser = subparser.add_parser("status")
    parser.set_defaults(func=status_cmd)
    parser.add_argument("jobid")

    parser = subparser.add_parser("consume")
    parser.set_defaults(func=consume_cmd)
    parser.add_argument("config_url")
    parser.add_argument("jobid")
    parser.add_argument("name")

    parser = subparser.add_parser("fetch")
    parser.set_defaults(func=fetch_cmd)
    parser.add_argument("jobid")
    parser.add_argument("dest")

    args = parse.parse_args(argv)
    
    if not hasattr(args, 'func'):
        parse.print_help()
        sys.exit(1)
    if args.func != consume_cmd:
        config , jq, io = load_config(args.config)
        args.config_obj = config
        args.func(jq, io, args)
    else:
        args.func(args)


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

