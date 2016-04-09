import os
import sys
import re
import subprocess
import requests
import json
import datetime

from boto.s3.connection import S3Connection
from boto.s3.key import Key

nomad_url = "http://127.0.0.1:4646"
datacenter="dc1"
phlock2_path="/Users/pmontgom/miniconda3/envs/phlock2/bin/phlock2"

r_exec_script = os.path.join(os.path.dirname(__file__), "execute-r-fn.R")

FUNC_DEFS = "func_defs.R"

def execute_cmd(args):
    subprocess.check_call(args)

def parse_remote(path):
    m = re.match("^s3://([^/]+)/(.*)$", path)
    assert m != None, "invalid remote path: {}".format(path)
    bucket_name = m.group(1)
    path = m.group(2)
    
    c = S3Connection()
    bucket = c.get_bucket(bucket_name)

    return bucket, path

def timestamp():
    return datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

class Remote:
    def __init__(self, remote_url):
        self.remote_url = remote_url
        self.bucket, self.remote_path = parse_remote(remote_url)
        self.job_id = remote_url.split("/")[-1]+"-"+timestamp()

    def download(self, remote, local):
        remote_path = self.remote_path + "/" + remote

        key = self.bucket.get_key(remote_path)
        if key != None:
            # if it's a file, download it
            key.get_contents_to_filename(os.path.join(local, remote))
        else:
            # download everything with the prefix
            for key in self.bucket.list(prefix=remote_path):
                rest = drop_prefix(remote_path+"/", key.key)
                if not os.path.exists(local):
                    os.makedirs(local)
                key.get_contents_to_filename(os.path.join(local, rest))

    def upload(self, local, remote):
        remote_path = self.remote_path + "/" + remote
        local_path = os.path.join(local, remote)

        if os.path.exist(local_path):
            # if it's a file, upload it
            key = Key(self.bucket)
            key.name = remote_path
            key.set_contents_from_filename(local_path)
        else:
            # upload everything in the dir
            for fn in os.listdir(local):
                full_fn = os.path.join(local, fn)
                if os.path.isfile(full_fn):
                    k = Key(self.bucket)
                    k.key = os.path.join(remote_path, fn)
                    k.set_contents_from_filename(full_fn)

    def download_dir(self, remote, local):
        remote_path = self.remote_path + "/" + remote

        for key in self.bucket.list(prefix=remote_path):
            rest = drop_prefix(remote_path+"/", key.key)
            #print("local={}, rest={}, filename={}".format(local, rest, os.path.join(local, rest)))
            if not os.path.exists(local):
                os.makedirs(local)
            key.get_contents_to_filename(os.path.join(local, rest))

    def download_file(self, remote, local):
        remote_path = self.remote_path + "/" + remote

        local_dir = os.path.dirname(local)
        if local_dir != "" and not os.path.exists(local_dir):
            os.makedirs(local_dir)

        k = Key(self.bucket)
        k.key = remote_path
        k.get_contents_to_filename(local)

    def download_as_str(self, remote):
        remote_path = self.remote_path+"/"+remote
        key = self.bucket.get_key(remote_path)
        if key == None:
            return None
        return key.get_contents_as_string()

    def upload_str(self, remote, text):
        remote_path = self.remote_path+"/"+remote
        k = Key(self.bucket)
        k.key = remote_path
        k.set_contents_from_string(text)

    def upload_dir(self, local, remote):
        remote_path = self.remote_path+"/"+remote
        if not os.path.exists(local):
            return

        for fn in os.listdir(local):
            full_fn = os.path.join(local, fn)
            if os.path.isfile(full_fn):
                k = Key(self.bucket)
                k.key = os.path.join(remote_path, fn)
                k.set_contents_from_filename(full_fn)

    def upload_file(self, local, remote):
        remote_path = self.remote_path+"/"+remote
        k = Key(self.bucket)
        k.key = remote_path
        k.set_contents_from_filename(local)

def drop_prefix(prefix, value):
    assert value[:len(prefix)] == prefix
    return value[len(prefix):]

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
    print("cmd={}".format(cmd))
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
    print("cmd={}".format(cmd))
    retcode = subprocess.call(cmd)
    print("retcode={}".format(retcode))
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
    print("cmd={}".format(cmd))
    retcode = subprocess.call(cmd)
    if retcode != 0:
        remote.upload_str(completion_path, json.dumps(dict(state="failed", retcode=retcode)))
    else:
        remote.upload_dir("results", "results")
        remote.upload_str("task-completions/gather", json.dumps(dict(state="success")))

#########################

def run_job(job_json):
    #print("submitting:{}".format(json.dumps(job_json, indent=2)))
    job_id = job_json["Job"]["ID"]
    print("submitting: {}".format(job_id))
    r = requests.post(nomad_url+"/v1/job/"+job_id, json=job_json)
    assert r.status_code == 200, "Got status {}".format(r.status_code)

    index = None
    while True:
        params = {}
        if index != None:
            params['index'] = index
        r = requests.get(nomad_url+"/v1/job/"+job_id, params=params)
        assert r.status_code == 200
        index=r.headers["X-Nomad-Index"]
        job = r.json()
        #print("Got job state: {}".format(json.dumps(job, indent=2)))
        status = job['Status']
        if status == 'dead':
            break

def make_job_submission(job_id, cmds):
    AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY=os.getenv("AWS_SECRET_ACCESS_KEY")

    task_groups = []
    for cmd in cmds:
        name = cmd["name"]
        args = cmd["args"]
        task_groups.append( {
            "Name": name,
            "Count": 1,
            "Constraints": None,
            "RestartPolicy": {
                "Attempts": 15,
                "Interval": 604800000000000,
                "Delay": 15000000000,
                "Mode": "delay"
            },
            "Tasks": [
                    {
                        "Name": name,
                        "Driver": "raw_exec",
                        "Config": {
                            "Command": phlock2_path,
                            "Args": args
                        },
                        "Env": {
                            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
                            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY
                        },
                        "Services": [],
                        "Constraints": None,
                        "Resources": {
                            "CPU": 100,
                            "MemoryMB": 10,
                            "DiskMB": 300,
                            "IOPS": 0,
                            "Networks": []
                        },
                        "Meta": None,
                        "KillTimeout": 5000000000,
                        "LogConfig": {
                            "MaxFiles": 10,
                            "MaxFileSizeMB": 10
                        },
                        "Artifacts": []
                    }
            ],
            "Meta": None
        })

    # cmd_args is a list of {name:..., args:...}.  One task per cmd will be generated
    job = {"Job":{
    "Region": "global",
    "ID": job_id,
    "ParentID": "",
    "Name": job_id,
    "Type": "batch",
    "Priority": 50,
    "AllAtOnce": False,
    "Datacenters": [
        datacenter
    ],
    "Constraints": None,
    "TaskGroups": task_groups,
    "Update": {
        "Stagger": 0,
        "MaxParallel": 0
    },
    "Periodic": None,
	}}

    return job

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
    
import argparse

def do_execute(args):
    if args.remote == None:
        remote = None
        print args.download
        assert args.download == None
        assert args.upload == None
    else:
        remote = Remote(args.remote)
        for download in args.download:
            remote.download(download)

    start_time = timestamp()
    retcode = subprocess.call(args.args)
    end_time = timestamp()

    with open(args.output, "wt") as fd:
        json.dump(dict(retcode=retcode, start=start_time, end=end_time), fd)

    if remote != None:
        for upload in args.upload:
            remote.upload(upload)

def add_execute(subparsers):
    parser = subparsers.add_parser("execute")
    parser.set_defaults(func=do_execute)
    parser.add_argument('-u', '--upload', action='append')
    parser.add_argument('-d', '--download', action='append')
    parser.add_argument('-r', '--remote')
    parser.add_argument('output')
    parser.add_argument('args', nargs=argparse.REMAINDER)

def add_scatter(subparsers):
    parser = subparsers.add_parser("scatter")
    parser.set_defaults(func=do_scatter)
    parser.add_argument('fn_name')
    parser.add_argument('remote_url')

def add_map(subparsers):
    parser = subparsers.add_parser("scatter")
    parser.set_defaults(func=do_map)
    parser.add_argument('fn_name')
    parser.add_argument('task_index')
    parser.add_argument('remote_url')

def add_gather(subparsers):
    parser = subparsers.add_parser("scatter")
    parser.set_defaults(func=do_gather)
    parser.add_argument('fn_name')
    parser.add_argument('remote_url')

def main(args=None):
    parse = argparse.ArgumentParser()
    subparsers = parse.add_subparsers()
    add_execute(subparsers)
    add_scatter(subparsers)
    add_map(subparsers)
    add_gather(subparsers)

    args = parse.parse_args()
    args.func(args)

if __name__ == "__main__":
    main(sys.argv[1:])
