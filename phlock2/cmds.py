import os
import sys
import re
import subprocess
import json
import datetime

import argparse

import logging

log = logging.getLogger(__name__)

#phlock2_path="/Users/pmontgom/miniconda3/envs/phlock2/bin/phlock2"
phlock2_path="/usr/local/bin/phlock2"

r_exec_script = os.path.join(os.path.dirname(__file__), "execute-r-fn.R")

FUNC_DEFS = "func_defs.R"

def execute_cmd(args):
    subprocess.check_call(args)



def get_output_files(output_dir):
    l = []
    for fn in os.listdir(output_dir):
        m = re.match("([0-9]+)\\.rds", fn)
        if m == None:
            continue
        l.append( (int(m.group(1)), fn ) )
    l.sort()
    return [fn for _, fn in l]

def run_scatter(scatter_func_name, remote):
    completion_path = "task-completions/scatter"

    remote.download_file(FUNC_DEFS, FUNC_DEFS)

    cmd = ["Rscript", r_exec_script, "scatter", scatter_func_name, FUNC_DEFS]
    retcode = subprocess.call(cmd)
    if retcode != 0:
        remote.upload_str(completion_path, json.dumps(dict(state="failed", retcode=retcode)))
    else:
        remote.upload_dir("shared", "shared")
        remote.upload_dir("map-inputs", "map-inputs")
        remote.upload_dir("results", "results")
        remote.upload_str(completion_path, json.dumps(dict(state="success")))

def run_mapper(mapper_func_name, task_index, remote):
    completion_path = "task-completions/map-{}".format(task_index)

    input_file = "map-inputs/"+str(task_index)+".rds"
    #output_file = "map-outputs/"+str(task_index)+".rds"

    remote.download_file(FUNC_DEFS, FUNC_DEFS)
    remote.download_dir("shared", "shared")
    remote.download_file(input_file, input_file)

    cmd = ["Rscript", r_exec_script, "map", str(task_index), mapper_func_name, FUNC_DEFS]
    retcode = subprocess.call(cmd)
    if retcode != 0:
        remote.upload_str(completion_path, json.dumps(dict(state="failed", retcode=retcode)))
    else:
        remote.upload_dir("map-outputs", "map-outputs")
        remote.upload_dir("results", "results")
        remote.upload_str(completion_path, json.dumps(dict(state="success")))

def run_gather(gather_func_name, remote):
    completion_path = "task-completions/gather"

    remote.download_file(FUNC_DEFS, FUNC_DEFS)
    remote.download_dir("shared", "shared")
    remote.download_dir("map-outputs", "map-outputs")

    output_list = get_output_files("map-outputs")
    with open("mapper-outputs.txt", "w") as fd:
        fd.write("".join(["map-outputs/"+x+"\n" for x in output_list]))

    cmd = ["Rscript", r_exec_script, "gather", "mapper-outputs.txt", gather_func_name, FUNC_DEFS]
    retcode = subprocess.call(cmd)
    if retcode != 0:
        remote.upload_str(completion_path, json.dumps(dict(state="failed", retcode=retcode)))
    else:
        remote.upload_dir("results", "results")
        remote.upload_str("task-completions/gather", json.dumps(dict(state="success")))

#########################




def submit_scatter(scatter_fn, remote):
    job_id=remote.job_id+"-scatter"

    job = make_job_submission(job_id, [ {
        "name":"scatter",
        "args":["scatter", remote.remote_url, scatter_fn]
        }
        ] )

    run_job(job)

def is_task_complete(remote, name):
    text = remote.download_as_str("task-completions/"+name)
    if text != None:
        state = json.loads(text)
        if state["state"] == "success":
            return True

    return False

def do_scatter(scatter_fn, remote):
    if is_task_complete(remote, "scatter"):
        return

    submit_scatter(scatter_fn, remote)

    assert is_task_complete(remote, "scatter")

def submit_map(map_fn, indices, remote):
    job_id=remote.job_id+"-map"

    job = make_job_submission(job_id, [
        {
            "name":"map-{}".format(i),
            "args":["map", remote.remote_url, map_fn, str(i)]
        } for i in indices ] )

    run_job(job)

def find_map_indices_not_run(remote):
    indices = set()

    input_prefix = remote.remote_path+"/map-inputs"
    for key in remote.bucket.list(prefix=input_prefix):
        fn = drop_prefix(input_prefix+"/", key.key)
        m = re.match("(\\d+)\\.rds", fn)
        if m != None:
            indices.add(m.group(1))

    output_prefix = remote.remote_path+"/map-outputs"
    for key in remote.bucket.list(prefix=output_prefix):
        fn = drop_prefix(output_prefix+"/", key.key)
        m = re.match("(\\d+)\\.rds", fn)
        if m != None:
            indices.remove(m.group(1))

    return indices

def do_map(map_fn, remote):
    indices = find_map_indices_not_run(remote)
    if len(indices) == 0:
        return

    submit_map(map_fn, indices, remote)

    indices = find_map_indices_not_run(remote)
    assert len(indices) == 0

def submit_gather(gather_fn, remote):
    job_id=remote.job_id+"-gather"

    job = make_job_submission(job_id, [
        {
            "name":"gather",
            "args":["gather", remote.remote_url, gather_fn]
        } ] )

    run_job(job)

def do_gather(gather_fn, remote):
    if is_task_complete(remote, "gather"):
        return

    submit_gather(gather_fn, remote)

    assert is_task_complete(remote, "gather")

def submit(scatter_fn, map_fn, gather_fn, remote, filename):
    remote.upload_file(filename, FUNC_DEFS)

    do_scatter(scatter_fn, remote)
    do_map(map_fn, remote)
    do_gather(gather_fn, remote)

def submit_main():
    scatter_fn, map_fn, gather_fn, remote_url, filename = sys.argv[1:]
    remote = Remote(remote_url)
    submit(scatter_fn, map_fn, gather_fn, remote, filename)
    

def do_execute(args):
    if args.remote == None:
        remote = None
        print args.download
        assert args.download == None
        assert args.upload == None
    else:
        remote = Remote(args.remote)
        if args.download:
            for download in args.download:
                remote.download(download, download)

    start_time = timestamp()
    log.info("executing %s", args.args)
    retcode = subprocess.call(args.args)
    end_time = timestamp()

    with open(args.output, "wt") as fd:
        json.dump(dict(retcode=retcode, start=start_time, end=end_time), fd)

    if remote != None:
        if args.upload:
            for upload in args.upload:
                remote.upload(upload, upload)

def make_phlock_exec_cmd(download, upload, remote_url, output_path, args):
    remote_cmd = ["phlock2", "execute"]
    if download:
        for p in download:
            remote_cmd.extend(["-d", p])
    if upload:
        for p in upload:
            remote_cmd.extend(["-u", p])
    remote_cmd.extend(["-r", remote_url])
    remote_cmd.append(output_path)
    remote_cmd.extend(args)

    return remote_cmd

from phlock2 import job_state
from phlock2 import nomad_job


class JobQueue:
    def __init__(self, db, nomad):
        self.db = db
        self.nomad = nomad

    def do_nomad_submit(self, image, job_name, download, upload, remote_url, output_path, args, config):
        job_id = self.db.add_job(job_name, dict(image=image, command=args, download=download, upload=upload, remote_url=remote_url))
        print("Submitted job: {}".format(job_id))

        command = make_phlock_exec_cmd(download, upload, remote_url, output_path, args)

        if image is None:
            task = nomad_job.create_raw_exec_task("job", command[0], command[1:])
        else:
            task = nomad_job.create_raw_exec_task(image, "job", command[0], command[1:])

        j_job_id = alloc_job_id()
        job = nomad_job.Job(j_job_id)
        job.add_task(task)

        nomad = nomad_job.Nomad()
        nomad_id = nomad.run_job(job)

        self.db.add_nomad_job_id(job_id, nomad_id)

    def get_job_status(self, job_id):
        nomad_job_ids = self.db.get_nomad_job_ids(job_id)
        print("job_id {} -> {}".format(job_id, nomad_job_ids))
        allocations = []
        for nomad_job_id in nomad_job_ids:
            allocations.extend(self.nomad.get_allocations(nomad_job_id))
        return allocations

    def kill(self, job_id):
        nomad_job_ids = self.db.get_nomad_job_ids(job_id)
        for nomad_job_id in nomad_job_ids:
            self.nomad.kill(nomad_job_id)

def do_local_submit(image, working_dir, download, upload, remote_url, output_path, args, config):
    cmd = []
    if image:
        cmd = ['docker', 'run', image, '-v', '/work:{}'.format(working_dir), '-w', '/work']

    phlock_cmd = make_phlock_exec_cmd(download, upload, remote_url, output_path, args)
    cmd.extend(phlock_cmd)

    env = dict(os.environ)
    env['AWS_ACCESS_KEY_ID'] =config['accesskeyid']
    env['AWS_SECRET_ACCESS_KEY'] = config['secretaccesskey']

    #log.info("executing %s with env %s", cmd, env)
    ret = subprocess.call(cmd, cwd=working_dir, env=env)
    if ret != 0:
        log.warn("Return code from {} was {}".format(cmd, ret))

from ConfigParser import ConfigParser

def load_config(path):
    full_path = os.path.expanduser(path)

    config = ConfigParser()
    config.read(full_path)
    return dict(config.items('config'))

def alloc_job_id():
    import uuid
    return uuid.uuid4().hex

from phlock2.remote import Remote

def open_job_queue(config):
    db = job_state.open_db(config.get('jobs_db_filename', 'jobs.sqlite3'))
    nomad = nomad_job.Nomad(config.get('nomad_url', nomad_job.DEFAULT_NOMAD_URL))
    return JobQueue(db, nomad)


from tabulate import tabulate

def do_stat(args):
    config = load_config(args.config)
    job_queue = open_job_queue(config)

    status = job_queue.get_job_status(args.job_id)
    print(tabulate([ (row.id, row.status, row.name, row.node_id) for row in status], headers=['alloc_id', 'status', 'name', 'node']))

def do_submit(args):
    config = load_config(args.config)
    job_queue = open_job_queue(config)

    job_id = alloc_job_id()

    remote_url = "{}/{}".format(config['remoteurl'], job_id)
    remote = Remote(remote_url, config['accesskeyid'], config['secretaccesskey'])

    if args.upload:
        for p in args.upload:
            remote.upload(p, p)

    if args.local:
        working_dir = "work/{}".format(job_id)
        os.makedirs(working_dir)
        do_local_submit(args.image, working_dir, args.upload, args.download, remote_url, "phlock2_output", args.args, config)
    else:
        job_queue.do_nomad_submit(args.image, job_id, args.upload, args.download, remote_url, "phlock2_output", args.args, config)

    if args.download:
        for p in args.download:
            remote.download(p, p)

def add_execute(subparsers):
    parser = subparsers.add_parser("execute")
    parser.set_defaults(func=do_execute)
    parser.add_argument('-u', '--upload', action='append')
    parser.add_argument('-d', '--download', action='append')
    parser.add_argument('-r', '--remote', help="address of cloud storage service to upload/download from")
    parser.add_argument('output', help="the path where the result file should be written.  Should be relative path")
    parser.add_argument('args', help="The command to execute within the container", nargs=argparse.REMAINDER)

def add_submit(subparsers):
    # Require that upload/download use relative paths.   Maybe support alt syntax which looks like
    # (non-local-path):(path-rel-to-working-path) to support cases where inputs are on s3, or not under current working directory
    parser = subparsers.add_parser("sub")
    parser.set_defaults(func=do_submit)
    parser.add_argument('args', help="The command to execute within the container", nargs=argparse.REMAINDER)
    parser.add_argument('--download', '-d', help="files to download to local machine after job finishes", action='append')
    parser.add_argument('--upload', '-u', help="files to upload to remote before job is starts", action='append')
    parser.add_argument('--config', '-c', help="path to settings to use", default="~/.phlock2")
    parser.add_argument('--image', help="name of image to use")
    parser.add_argument('--local', action="store_true")

def add_stat(subparsers):
    parser = subparsers.add_parser("stat")
    parser.set_defaults(func=do_stat)
    parser.add_argument('job_id', type=int)
    parser.add_argument('--config', '-c', help="path to settings to use", default="~/.phlock2")

def add_scatter(subparsers):
    parser = subparsers.add_parser("scatter")
    parser.set_defaults(func=do_scatter)
    parser.add_argument('fn_name')
    parser.add_argument('remote_url')

def add_map(subparsers):
    parser = subparsers.add_parser("map")
    parser.set_defaults(func=do_map)
    parser.add_argument('fn_name')
    parser.add_argument('task_index')
    parser.add_argument('remote_url')

def add_gather(subparsers):
    parser = subparsers.add_parser("gather")
    parser.set_defaults(func=do_gather)
    parser.add_argument('fn_name')
    parser.add_argument('remote_url')

def main(args=None):
    logging.basicConfig(level=logging.INFO)

    parse = argparse.ArgumentParser()
    subparsers = parse.add_subparsers()
    add_execute(subparsers)
    add_submit(subparsers)
    add_scatter(subparsers)
    add_map(subparsers)
    add_gather(subparsers)
    add_stat(subparsers)

    args = parse.parse_args()
    args.func(args)

# keep a db of job id -> external name
# need commands for:
#   list running jobs: job id, alloc ids, status
#   ls alloc file
#   cat alloc file
#
# problem: Need path to phlock2 inside of docker container
#
if __name__ == "__main__":
    main(sys.argv[1:])
