import os
import re
import subprocess
import json
from phlock2.remote import Remote
import hashlib
import argparse
import logging

try:
    from configparser import ConfigParser
except:
    from ConfigParser import ConfigParser

log = logging.getLogger(__name__)

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

def do_map(map_fn, remote, rcode, is_test):
    indices = find_map_indices_not_run(remote, is_test)
    if len(indices) == 0:
        return

    submit_map(map_fn, indices, remote, rcode)

    indices = find_map_indices_not_run(remote, is_test)
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

def find_map_indices_not_run(remote, is_test):
    indices = set()

    input_prefix = remote.remote_path+"/map-in"
    for key in remote.bucket.list(prefix=input_prefix):
        fn = drop_prefix(input_prefix+"/", key.key)
        m = re.match("(\\d+)", fn)
        if m != None:
            indices.add(m.group(1))

    if is_test:
        # only take first 5 examples
        x=list(indices)
        x.sort()
        indices = set(x[:5])

    output_prefix = remote.remote_path+"/map-out"
    for key in remote.bucket.list(prefix=output_prefix):
        fn = drop_prefix(output_prefix+"/", key.key)
        m = re.match("(\\d+)", fn)
        if m != None and m.group(1) in indices:
            indices.remove(m.group(1))

    return indices


def submit_config(url):
    command = ["docker",
               "run",
               "-e", 'AWS_ACCESS_KEY_ID='+ os.getenv('AWS_ACCESS_KEY_ID'),
               "-e", 'AWS_SECRET_ACCESS_KEY=' + os.getenv('AWS_SECRET_ACCESS_KEY'),
               "-w", "/work", "-t", "-i",
               "enrichment", "python", "/helper.py", "exec-config", url]
    log.info("Running %s", " ".join(command))
    subprocess.check_call(command)

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
    do_map(map_fn, remote, rcode, args.test)
    do_gather(gather_fn, remote, rcode)


def load_config(path):
    full_path = os.path.expanduser(path)

    config = ConfigParser()
    config.read(full_path)
    return dict(config.items('config'))

def add_sub_sg(subparser):
    parser = subparser.add_parser("subsg")
    parser.set_defaults(func=submit_scatter_gather)
    parser.add_argument("func_prefix")
    parser.add_argument("remote_url")
    parser.add_argument("filename")
    parser.add_argument("--test", action="store_true")

def main():
    logging.basicConfig(level=logging.INFO)

    parse = argparse.ArgumentParser()
    subparsers = parse.add_subparsers()
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
    main()
