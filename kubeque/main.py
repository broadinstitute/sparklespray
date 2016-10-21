import time
import random
import logging
import os
import json
import re
import sys
import hashlib

from kubeque.kubesub import submit_job

from contextlib import contextmanager

from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.key import Key

log = logging.getLogger(__name__)

try:
    from configparser import ConfigParser
except:
    from ConfigParser import ConfigParser

CLAIM_TIMEOUT = 30

from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, JSONAttribute
)

def get_fake_s3_port_from_url(url):
    m = re.match("fakes3://(\\d+)/.*", url)
    if m is None:
        return None
    return int(m.group(1))

class IO:
    def __init__(self, accesskey, secretaccesskey, cas_url_prefix, fake_s3_port):
        self.buckets = {}
        self.fake_s3_port = None
        if fake_s3_port is not None:
            self.fake_s3_port = int(fake_s3_port)
            self.connection = S3Connection("", "", is_secure=False, port=self.fake_s3_port, host='localhost', calling_format=OrdinaryCallingFormat())
        else:
            self.connection = S3Connection(accesskey, secretaccesskey)
        self.cas_url_prefix = cas_url_prefix

    def _get_bucket_and_path(self, path):
        m = re.match("^fakes3://(\\d+)/([^/]+)/(.*)$", path)
        if m is not None:
            fakes3port  = m.group(1)
            bucket_name = m.group(2)
            path = m.group(3)
        else:
            m = re.match("^s3://([^/]+)/(.*)$", path)
            assert m != None, "invalid remote path: {}".format(path)
            bucket_name = m.group(1)
            path = m.group(2)

        if bucket_name in self.buckets:
            bucket = self.buckets[bucket_name]
        else:
            bucket = self.connection.get_bucket(bucket_name)
        return bucket, path

    def get(self, src_url, dst_filename, must=True):
        log.info("get %s -> %s", src_url, dst_filename)
        bucket, path = self._get_bucket_and_path(src_url)
        key = bucket.get_key(path)
        if key is not None:
            key.get_contents_to_filename(dst_filename)
        else:
            assert not must, "Could not find {}".format(path)

    def get_as_str(self, src_url):
        bucket, path = self._get_bucket_and_path(src_url)
        key = bucket.get_key(path, validate=False)
        return key.get_contents_as_string().decode("utf8")

    def put(self, src_filename, dst_url, must=True):
        log.info("put %s -> %s", src_filename, dst_url)
        if must:
            assert os.path.exists(src_filename)

        bucket, path = self._get_bucket_and_path(dst_url)
        key = Key(bucket)
        key.key = path
        key.set_contents_from_filename(src_filename)

    def _get_url_prefix(self):
        if self.fake_s3_port is None:
            return "s3://"
        else:
            return "fakes3://{}/".format(self.fake_s3_port)

    def write_file_to_cas(self, filename):
        m = hashlib.sha256()
        with open(filename, "rb") as fd:
            for chunk in iter(lambda: fd.read(10000), b""):
                m.update(chunk)
        hash = m.hexdigest()
        dst_url = self.cas_url_prefix+hash
        bucket, path = self._get_bucket_and_path(dst_url)
        key = Key(bucket)
        key.key = path
        key.set_contents_from_filename(filename)
        return self._get_url_prefix()+bucket.name+"/"+path         

    def write_str_to_cas(self, text):
        text = text.encode("utf8")
        hash = hashlib.sha256(text).hexdigest()
        dst_url = self.cas_url_prefix+"/"+hash
        bucket, path = self._get_bucket_and_path(dst_url)
        key = Key(bucket)
        key.key = path
        key.set_contents_from_string(text)
        return self._get_url_prefix()+bucket.name+"/"+path
        
    def write_json_to_cas(self, obj):
        obj_str = json.dumps(obj)
        return self.write_str_to_cas(obj_str)

def create_tables(prefix, _region, _host):
    class Task(Model):
        class Meta:
            table_name = prefix+"Task"
            region = _region
            host = _host
        
        # will be of the form: job_id + task_index
        task_id = UnicodeAttribute(hash_key=True)

        task_index = NumberAttribute()
        job_id = UnicodeAttribute()
        status = UnicodeAttribute() # one of: pending, claimed, success, failed, lost
        owner = UnicodeAttribute(null=True)
        args = UnicodeAttribute()
        history = JSONAttribute() # list of records (timestamp, status)  (maybe include owner?)
        version = NumberAttribute(default=1)

    class Job(Model):
        class Meta:
            table_name = prefix+"Job"
            region = _region
            host = _host

        job_id = UnicodeAttribute(hash_key=True)
        tasks = JSONAttribute()

    if not Job.exists():
        print("creating job")
        Job.create_table(wait=True, read_capacity_units=1, write_capacity_units=1)

    if not Task.exists():
        print("creating task")
        Task.create_table(wait=True, read_capacity_units=1, write_capacity_units=1)

    return Task, Job

class JobQueue:
    def __init__(self, prefix, region, host):
        self.Task, self.Job = create_tables(prefix, region, host) 

    def _find_pending(self, job_id):
        print("_find_pending")
        tasks = self.Task.scan(status__eq = "pending", job_id__eq = job_id)
        return list(tasks)

    def get_tasks(self, job_id):
        tasks = self.Task.scan(job_id__eq = job_id)
        return tasks

    def get_status_counts(self, job_id):
        import collections
        counts = collections.defaultdict(lambda: 0)
        tasks = self.Task.scan(job_id__eq = job_id)
        for task in tasks:
            counts[task.status] += 1
        return counts

    def reset(self, job_id):
        tasks = self.Task.scan(job_id__eq = job_id)
        now = time.time()
        for task in tasks:
            original_version = task.version
            task.version = original_version + 1
            task.owner = None
            task.status = "pending"
            task.history.append( dict(timestamp=now, status="reset") )
            updated = task.save(version__eq = original_version)

    def submit(self, job_id, args):
        tasks = []
        now = time.time()
        with self.Task.batch_write() as batch:
            for i, arg in enumerate(args):
                task_id = "{}.{}".format(job_id, i)
                task = self.Task(task_id, task_index=i, job_id=job_id, status="pending", args=arg, history=[dict(timestamp=now, status="pending")])
                tasks.append(task)
                batch.save(task)

        job = self.Job(job_id, tasks=[t.task_id for t in tasks])
        job.save()


    def claim_task(self, job_id, new_owner):
        claim_start = time.time()
        while True:
            # fetch all pending with this job_id\n",
            tasks = self._find_pending(job_id)
            if len(tasks) == 0:
                return None

            now = time.time()
            if now - claim_start > CLAIM_TIMEOUT:
                raise Exception("Timeout attempting to claim task")

            task = random.choice(tasks)
            original_version = task.version
            task.version = original_version + 1
            task.owner = new_owner
            task.status = "claimed"
            task.history.append( dict(timestamp=now, status="claimed", owner=new_owner) )
            updated = task.save(version__eq = original_version)
            if updated is not None:
                return task.task_id, task.args

            # add exponential backoff?
            print("Update failed")
            time.sleep(random.uniform(0, 1))

    def task_completed(self, task_id, was_successful):
        if was_successful:
            new_status = "success"
        else:
            new_status = "failed"
        self._update_status(task_id, new_status)

    def _update_status(self, task_id, new_status):
        task = self.Task.get(task_id, consistent_read=True)
        now = time.time()
        original_version = task.version
        task.version += 1
        task.history.append( dict(timestamp=now, status=new_status) )
        task.status = new_status
        task.owner = None
        print("updating status. asserting version ==", original_version, "new", task.version)
        updated = task.save(version__eq = original_version)
        if updated is None:
            # I suppose this is not technically correct. Could be a simultaneous update of "success" or "failed" and "lost"
            raise Exception("Detected concurrent update, which should not be possible")

    def owner_lost(self, owner):
        tasks = self.Task.scan(owner == owner)
        for task in tasks:
            self._update_status(task_id, "lost")

def consumer_run_loop(jq, job_id, owner_name, execute_callback):
    while True:
        claimed = jq.claim_task(job_id, owner_name)
        print("claimed:", claimed)
        if claimed is None:
            break
        task_id, args = claimed
        print("task_id:", task_id, "args:", args)
        execute_callback(task_id, args)
        jq.task_completed(task_id, True)


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

        return dict(src_url=src_url, dst=url['dst'])

    src_expanded = [ rewrite_download(x) for x in downloads ]

    return [rewrite_url_in_dict(x, "src_url", default_url_prefix) for x in src_expanded]

def upload_config_for_consume(io, config):
    consume_config = {}
    for key in ['cas_url_prefix', 'fake_s3_port', 'dynamodb_prefix', 'dynamodb_region', 'dynamodb_host']:
        consume_config[key] = config.get(key)

    config_url = io.write_str_to_cas(json.dumps(consume_config))
    return config_url

def submit(jq, io, job_id, spec, dry_run, config):
    default_url_prefix = config.get("default_url_prefix")
    default_job_url_prefix = default_url_prefix+job_id+"/"

    tasks = spec.expand_spec(default_url_prefix, default_job_url_prefix)
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


def submit_cmd(jq, io, args):
    if args.file:
        assert len(args.command) == 0
        spec = json.load(open(args.file, "rt"))
    else:
        assert len(args.command) != 0
        spec = make_spec_from_command(args.command)

    job_id = args.name
    if job_id is None:
        job_id = new_id()

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
            io.get(ul['dst_url'], localpath)

def exec_command_(command):
    return os.system(command)

def write_result_file(command_result_path, retcode, local_to_url_mapping):
    with open(command_result_path, "wt") as fd:
        fd.write(json.dumps({"return_code": retcode, "files": local_to_url_mapping}))

def resolve_uploads(uploads):
    resolved = []
    for ul in uploads:
        if os.path.exists(ul['src']):
            resolved.append(dict(src=ul['src'], dst_url=ul['dst_url']))
    return resolved

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
        spec = json.loads(io.get_as_str(json_url))
        with redirect_output_to_file(spec['log_path']):
            for dl in spec['downloads']:
                io.get(dl['src_url'], dl['dst'])
            retcode = exec_command_(spec['command'])

            local_to_url_mapping = resolve_uploads(spec['uploads'])
            for ul in local_to_url_mapping:
                io.put(ul['src'], ul['dst_url'])

            write_result_file(spec['command_result_path'], retcode, local_to_url_mapping)
            io.put(spec['command_result_path'], spec['command_result_url'])

    consumer_run_loop(jq, args.jobid, args.name, exec_task)

import argparse

def main(argv=None):
    logging.basicConfig(level=logging.INFO)

    parse = argparse.ArgumentParser()
    parse.add_argument("--config", default="~/.phlock2")
    subparser = parse.add_subparsers()

    parser = subparser.add_parser("sub")
    parser.set_defaults(func=submit_cmd)
    parser.add_argument("--file", "-f")
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

