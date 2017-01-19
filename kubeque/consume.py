import socket
import stat
import os
import tempfile
import json
from glob import glob
from kubeque.gcp import create_gcs_job_queue, IO
import kubeque.main 
import logging
import argparse
import subprocess
import shutil
import sys
log = logging.getLogger(__name__)

KILLED_RET_CODE = 137

def exec_lifecycle_script(workdir, spec, stage_name):
    if stage_name not in spec:
        return

    command = spec[stage_name]
    log.info("(workingdir: %s) Executing %s: %s", workdir, stage_name, command)
    retcode = subprocess.call(command, shell=True, cwd=workdir)
    log.info("%s script completed with retcode=%s", stage_name, retcode)

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

    cache_dir = args.cache_dir
    if not os.path.exists(cache_dir):
        log.info("cache dir %s does not exist, creating.", cache_dir)
        os.makedirs(cache_dir)

    def exec_task(task_id, json_url):

        # make working directory.  A directory for the task with two subdirs ("log" where stdout/stderr is written and return code, "work" the working directory the task will be run in)
        # Returns True if child exited normally, or False if child was forcibly killed 
        taskdir = tempfile.mkdtemp(prefix="task-")
        logdir = os.path.join(taskdir, "log")
        workdir = os.path.join(taskdir, "work")
        os.mkdir(logdir)
        os.mkdir(workdir)

        stdout_path = os.path.join(logdir, "stdout.txt")
        result_path = os.path.join(logdir, "result.json")

        spec = json.loads(io.get_as_str(json_url))

        exec_lifecycle_script(workdir, spec, 'pre-download-script')

        log.info("Job spec (%s) of claimed task: %s", json_url, json.dumps(spec, indent=2))
        downloaded = set()
        for dl in spec['downloads']:
            src_url = dl['src_url']
            destination = os.path.abspath(os.path.join(workdir, dl['dst']))

            # assuming all src_urls are CAS paths
            parent_dir = os.path.basename(os.path.dirname(src_url)).lower()
            if parent_dir == 'cas':
                cas_name = os.path.basename(src_url)
                cache_dest_name = os.path.join(cache_dir, cas_name)
                if not os.path.exists(cache_dest_name):
                    tfd = tempfile.NamedTemporaryFile(dir=cache_dir, delete=False)
                    tfd.close()
                    temp_dest = tfd.name
                    log.info("Downloading (%s) %s to %s", destination, src_url, temp_dest)
                    io.get(src_url, temp_dest)
                    log.info("Renaming %s to %s", temp_dest, cache_dest_name)
                    os.rename(temp_dest, cache_dest_name)
                else:
                    log.info("No download, %s already exists", cache_dest_name)
                log.info("Copying %s to %s", cache_dest_name, destination)
                if not os.path.exists(os.path.dirname(destination)):
                    os.makedirs(os.path.dirname(destination))
                shutil.copy(cache_dest_name, destination)
            else:
                log.info("%s does not look at a CAS url. Skipping caching", src_url)
                io.get(src_url, destination)

            downloaded.add(destination)

            if dl.get('executable', False):
                os.chmod(destination, os.stat(destination).st_mode | stat.S_IEXEC)

        exec_lifecycle_script(workdir, spec, 'post-download-script')

        retcode = exec_command_(spec['command'], workdir, stdout_path)

        local_to_url_mapping = resolve_uploads(workdir, spec['uploads'], downloaded)

        exec_lifecycle_script(workdir, spec, 'post-exec-script')

        def log_and_put(src, dst):
            log.info("Uploading %s to %s", src, dst)
            io.put(src, dst)

        for ul in local_to_url_mapping:
            log_and_put(os.path.join(workdir, ul['src']), ul['dst_url'])

        write_result_file(result_path, retcode, workdir, local_to_url_mapping)
        log_and_put(result_path, spec['command_result_url'])
        log_and_put(stdout_path, spec['stdout_url'])

        log.info("retcode = %s", repr(retcode))
        return retcode != KILLED_RET_CODE

    normal_termination = consumer_run_loop(jq, args.jobid, node_name, exec_task)
    if not normal_termination:
        log.warn("Terminating due to forcibly killed child process (OOM?)")
        sys.exit(1)
    else:
        log.info("Normal termination")
        sys.exit(0)

def consumer_run_loop(jq, job_id, owner_name, execute_callback):
    # returns True if normal termination, or False we're stopping because a child was forcibly killed 
    while True:
        claimed = jq.claim_task(job_id, owner_name)
        log.info("claimed: %s", claimed)
        if claimed is None:
            break
        task_id, args = claimed
        log.info("task_id: %s, args: %s", task_id, args)
        was_normal_termination = execute_callback(task_id, args)
        if was_normal_termination:
            jq.task_completed(task_id, True)
        else:
            jq.task_completed(task_id, False, "Killed")
            return False
    return True

def write_result_file(command_result_path, retcode, workdir, local_to_url_mapping):
    relative_local_to_url_mapping = [dict(src=os.path.relpath(x['src'], workdir), dst_url=x['dst_url']) for x in local_to_url_mapping]
    with open(command_result_path, "wt") as fd:
        fd.write(json.dumps({"return_code": retcode, "files": relative_local_to_url_mapping}))

def exec_command_(command, workdir, stdout):
    log.info("(workingdir: %s) Executing: %s", workdir, command)
    stdoutfd = os.open(stdout, os.O_WRONLY | os.O_APPEND | os.O_CREAT)

    handle_for_polling = open(stdout, "rt")
    def poll_stdout_file():
        while True:
            b = handle_for_polling.read(8000)
            if b == "":
                break
            sys.stdout.write(b)
            sys.stdout.flush()

    try:
        # TODO: Change this to async execution and poll stdout file, dumping the contents to our stdout?
        # that way the in-progress stdout would be visible in the POD log.
        p = subprocess.Popen(command, stderr=subprocess.STDOUT, stdout=stdoutfd, shell=True, cwd=workdir)
        while True:
            try:
                poll_stdout_file()
                retcode = p.wait(timeout=1)
                break
            except subprocess.TimeoutExpired:
                pass

        # do one final read now that the process has terminated
        sys.stdout.write(handle_for_polling.read())

    finally:
        os.close(stdoutfd)
        handle_for_polling.close()
    return retcode

def resolve_uploads(dir, uploads, paths_to_exclude):
    resolved = []
    for ul in uploads:
        if "src_wildcard" in ul:
            src_filenames = glob(os.path.join(dir, ul['src_wildcard']))
            for src_filename in src_filenames:
                src_filename = os.path.abspath(src_filename)
                if src_filename in paths_to_exclude:
                    continue
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
    parser.add_argument("--cache_dir", default="/var/kubeque-obj-cache")

    args = parser.parse_args(argv)
    consume_cmd(args)

