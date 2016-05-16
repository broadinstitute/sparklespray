import os
import sys
import re
import subprocess
import json
import datetime
from phlock2 import job_state
from phlock2 import nomad_job


import argparse

import logging

try:
    from configparser import ConfigParser
except:
    from ConfigParser import ConfigParser

log = logging.getLogger(__name__)

phlock2_path=["python", "/Users/pmontgom/dev/conseq/conseq/helper.py"]

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


#########################




def is_task_complete(remote, name):
    text = remote.download_as_str("completed/"+name)
    if text != None:
        state = json.loads(text)
        if state["state"] == "success":
            return True

    return False

def do_scatter(scatter_fn, remote, rcode, args={}):
    if is_task_complete(remote, "scatter"):
        return

    submit_scatter(scatter_fn, remote, rcode, args)

    assert is_task_complete(remote, "scatter")

def do_map(map_fn, remote, rcode):
    indices = find_map_indices_not_run(remote)
    if len(indices) == 0:
        return

    submit_map(map_fn, indices, remote, rcode)

    indices = find_map_indices_not_run(remote)
    assert len(indices) == 0

def do_gather(gather_fn, remote, rcode):
    if is_task_complete(remote, "gather"):
        return

    submit_gather(gather_fn, remote, rcode)

    assert is_task_complete(remote, "gather")

def generate_r_script(rcode, rest):
    execute_fn_r = open(os.path.join(os.path.dirname(__file__), "execute-r-fn.R"), "rt").read()
    return "{}\n{}\n{}\n".format(rcode, execute_fn_r, rest)


########################################

def drop_prefix(prefix, value):
    assert value[:len(prefix)] == prefix, "Expected {} to be prefixed with {}".format(repr(value), repr(prefix))
    return value[len(prefix):]


def calc_hash(filename):
    h = hashlib.sha256()
    with open(filename, "rb") as fd:
        for chunk in iter(lambda: fd.read(10000), b''):
            h.update(chunk)
    return h.hexdigest()

def push_str_to_cas(remote, content, filename="<unknown>"):
    h = hashlib.sha256(content.encode("utf-8"))
    hash = h.hexdigest()

    remote_name = "CAS/{}".format(hash)
    if remote.exists(remote_name):
        log.info("Skipping upload of %s because %s already exists", filename, remote_name)
    else:
        remote.upload_str(remote_name, content)
    return remote_name

def push_to_cas(remote, filenames):
    name_mapping = {}

    for filename in filenames:
        local_filename = os.path.normpath(os.path.join(remote.local_dir, filename))
        hash = calc_hash(local_filename)
        remote_name = "CAS/{}".format(hash)
        if remote.exists(remote_name):
            log.info("Skipping upload of %s because %s already exists", filename, remote_name)
        else:
            remote.upload(filename, remote_name)
        name_mapping[filename] = remote_name

    return name_mapping

###########################################
import hashlib

def find_map_indices_not_run(remote):
    indices = set()

    input_prefix = remote.remote_path+"/map-in"
    for key in remote.bucket.list(prefix=input_prefix):
        fn = drop_prefix(input_prefix+"/", key.key)
        m = re.match("(\\d+)", fn)
        if m != None:
            indices.add(m.group(1))

    output_prefix = remote.remote_path+"/map-out"
    for key in remote.bucket.list(prefix=output_prefix):
        fn = drop_prefix(output_prefix+"/", key.key)
        m = re.match("(\\d+)", fn)
        if m != None:
            indices.remove(m.group(1))

    return indices

#def submit_config(url):
#    import tempfile
#    t = tempfile.mkdtemp()
#    print("Exec in ", t)
#    subprocess.check_call(phlock2_path + ["exec-config", url], cwd=t)

def submit_config(url):
    subprocess.check_call(["docker",
                           "run",
                           "-e", 'AWS_ACCESS_KEY_ID='+ os.getenv('AWS_ACCESS_KEY_ID'),
                           "-e", 'AWS_SECRET_ACCESS_KEY=' + os.getenv('AWS_SECRET_ACCESS_KEY'),
                           "-w", "/work", "-t", "-i",
                           "enrichment", "python", "/helper.py", "exec-config", url])

def submit_scatter(scatter_fn, remote, rcode, args):
    args_url = push_str_to_cas(remote, json.dumps(args))
    r_script = generate_r_script(rcode, "phlock.exec.scatter(\"{}\")".format(scatter_fn))
    script_url = push_str_to_cas(remote, r_script)

    config = dict(
        remote_url = remote.remote_url,
         pull=[{"src": args_url, "dest": "scatter-in/params.json", "isDir": False},
               {"src": script_url, "dest": "script.R", "isDir": False},],
         mkdir=["shared", "map-in", "results"],
         command=["Rscript", "script.R"],
         exec_summary="retcode.json",
         stdout="stdout.txt",
         stderr="stderr.txt",
         push=[{"src": "shared", "dest": "shared", "isDir": True},
               {"src": "map-in", "dest": "map-in", "isDir": True},
               {"src": "results", "dest": "results", "isDir": True},
               {"src": "retcode.json", "dest": "completed/scatter", "isDir": False},
               {"src": "stdout.txt", "dest": "logs/scatter/stdout.txt", "isDir": False},
               {"src": "stderr.txt", "dest": "logs/scatter/stderr.txt", "isDir": False},])

    config_url = push_str_to_cas(remote, json.dumps(config))
    submit_config(remote.remote_url+"/"+config_url)

def submit_map(map_fn, indices, remote, rcode):
    r_script = generate_r_script(rcode, "phlock.exec.map(\"{}\")".format(map_fn))
    script_url = push_str_to_cas(remote, r_script)

    for index in indices:
        config = dict(
            remote_url = remote.remote_url,
             pull=[{"src": "shared", "dest": "shared", "isDir": True},
                   {"src": "map-in/"+index, "dest": "map-in/"+index, "isDir": False},
                   {"src": script_url, "dest": "script.R", "isDir": False},],
             mkdir=["map-out", "results"],
             command=["Rscript", "script.R"],
             exec_summary="retcode.json",
             stdout="stdout.txt",
             stderr="stderr.txt",
             push=[{"src": "map-out", "dest": "map-out", "isDir": True},
                   {"src": "results", "dest": "results", "isDir": True},
                   {"src": "stdout.txt", "dest": "logs/"+index+"/stdout.txt", "isDir": False},
                   {"src": "stderr.txt", "dest": "logs/"+index+"/stderr.txt", "isDir": False},])

        config_url = push_str_to_cas(remote, json.dumps(config))
        submit_config(remote.remote_url+"/"+config_url)


def submit_gather(gather_fn, remote, rcode):
    r_script = generate_r_script(rcode, "phlock.exec.gather(\"{}\")".format(gather_fn))
    script_url = push_str_to_cas(remote, r_script)

    config = dict(
        remote_url = remote.remote_url,
         pull=[{"src": "shared", "dest": "shared", "isDir": True},
               {"src": "map-out", "dest": "map-out", "isDir": True},
               {"src": script_url, "dest": "script.R", "isDir": False},],
         mkdir=["results"],
         command=["Rscript", "script.R"],
         exec_summary="retcode.json",
         stdout="stdout.txt",
         stderr="stderr.txt",
         push=[{"src": "results", "dest": "results", "isDir": True},
               {"src": "retcode.json", "dest": "completed/gather", "isDir": False},
               {"src": "stdout.txt", "dest": "logs/gather/stdout.txt", "isDir": False},
               {"src": "stderr.txt", "dest": "logs/gather/stderr.txt", "isDir": False},])

    config_url = push_str_to_cas(remote, json.dumps(config))
    submit_config(remote.remote_url+"/"+config_url)

def submit_scatter_gather(args):
    fn_prefix = args.func_prefix
    remote_url = args.remote_url
    filename = args.filename

    scatter_fn = fn_prefix + ".scatter"
    map_fn = fn_prefix +".map"
    gather_fn = fn_prefix +".gather"

    remote = Remote(remote_url)
    rcode = open(filename, "rt").read()

    do_scatter(scatter_fn, remote, rcode)
    do_map(map_fn, remote, rcode)
    do_gather(gather_fn, remote, rcode)

def do_execute(args):
    if args.remote == None:
        remote = None
        print(args.download)
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

def add_sub_sg(subparser):
    parser = subparser.add_parser("subsg")
    parser.set_defaults(func=submit_scatter_gather)
    parser.add_argument("func_prefix")
    parser.add_argument("remote_url")
    parser.add_argument("filename")

def main(args=None):
    logging.basicConfig(level=logging.INFO)

    parse = argparse.ArgumentParser()
    subparsers = parse.add_subparsers()
    add_execute(subparsers)
    add_submit(subparsers)
    add_stat(subparsers)
    add_sub_sg(subparsers)

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
