import socket
import os
import tempfile
import json
from glob import glob
from kubeque.gcp import create_gcs_job_queue, IO
import kubeque.main 
import logging
import argparse
import subprocess
log = logging.getLogger(__name__)

def consume_cmd(args):
    "This is what is executed by a worker running within a container"

    if args.nodename:
        node_name = args.nodename
    else:
        node_name = os.environ["KUBE_POD_NAME"]

    # create an incomplete IO object that at least can do a fetch to get the full config
    # maybe just make the config public in the CAS and then there's no problem.   In theory the hash 
    # should not be guessable, so just as private as anything else.  (Although, use sha256 instead of md5)
    io = IO(args.project, args.cas_url_prefix)
    config = json.loads(io.get_as_str(args.config_url))
    jq, io = kubeque.main.load_config_from_dict(config)

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
        log.info("Job spec (%s) of claimed task: %s", json_url, json.dumps(spec, indent=2))
        for dl in spec['downloads']:
            io.get(dl['src_url'], os.path.join(workdir, dl['dst']))

        retcode = exec_command_(spec['command'], workdir, stdout_path)

        local_to_url_mapping = resolve_uploads(workdir, spec['uploads'])
        for ul in local_to_url_mapping:
            io.put(os.path.join(workdir, ul['src']), ul['dst_url'])

        write_result_file(result_path, retcode, workdir, local_to_url_mapping)
        io.put(result_path, spec['command_result_url'])
        io.put(stdout_path, spec['stdout_url'])

    consumer_run_loop(jq, args.jobid, node_name, exec_task)

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

def write_result_file(command_result_path, retcode, workdir, local_to_url_mapping):
    relative_local_to_url_mapping = [dict(src=os.path.relpath(x['src'], workdir), dst_url=x['dst_url']) for x in local_to_url_mapping]
    with open(command_result_path, "wt") as fd:
        fd.write(json.dumps({"return_code": retcode, "files": relative_local_to_url_mapping}))

def exec_command_(command, workdir, stdout):
    log.info("(workingdir: %s) Executing: %s", workdir, command)
    stdoutfd = os.open(stdout, os.O_WRONLY | os.O_APPEND | os.O_CREAT)
    try:
        retcode = subprocess.call(command, stderr=subprocess.STDOUT, stdout=stdoutfd, shell=True, cwd=workdir)
    finally:
        os.close(stdoutfd)
    return retcode

def resolve_uploads(dir, uploads):
    resolved = []
    for ul in uploads:
        if "src_wildcard" in ul:
            src_filenames = glob(os.path.join(dir, ul['src_wildcard']))
            for src_filename in src_filenames:
                resolved.append(dict(src=src_filename, dst_url=ul['dst_url']))
        elif "src" in ul:
            src = os.path.join(dir, ul['src'])
            if os.path.exists(src):
                resolved.append(dict(src=ul['src'], dst_url=ul['dst_url']))
        else:
            raise Exception("Malformed {}".format(ul))

    return resolved


def main(argv=None):
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("config_url")
    parser.add_argument("jobid")
    parser.add_argument("--project")
    parser.add_argument("--cas_url_prefix")
    parser.add_argument("--nodename")

    args = parser.parse_args(argv)
    consume_cmd(args)
