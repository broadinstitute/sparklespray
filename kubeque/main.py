import time
import logging
import os
import json
import sys

from kubeque import kubesub as kube
from kubeque.kubesub import submit_job_spec, create_kube_job_spec
from kubeque.gcp import create_gcs_job_queue, IO, STATUS_PENDING
from kubeque.hasher import CachingHashFunction

from kubeque.spec import make_spec_from_command, SrcDstPair
import csv
import copy

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
    assert not ("//" in url[4:]), "url=%s, default_url_prefix=%s"%(url, a)
    return url

def rewrite_url_in_dict(d, prop_name, default_url_prefix):
    if not (prop_name in d):
        return d

    d = dict(d)
    url = d[prop_name] 
    d[prop_name] = rewrite_url_with_prefix(url, default_url_prefix) 
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

        dst = os.path.normpath(url['dst'])
        # only allow paths to be relative to working directory
        assert not (dst.startswith("../"))
        assert not (dst.startswith("/"))

        return dict(src_url=src_url, dst=dst, executable=url.get("executable", False))

    src_expanded = [ rewrite_download(x) for x in downloads ]

    return [rewrite_url_in_dict(x, "src_url", default_url_prefix) for x in src_expanded]

def upload_config_for_consume(io, config):
    consume_config = {}
    for key in ['cas_url_prefix', 'project', "cluster_name"]:
        consume_config[key] = config[key]

    log.debug("consume_config: %s", consume_config)
    config_url = io.write_str_to_cas(json.dumps(consume_config))
    return config_url


def expand_tasks(spec, io, default_url_prefix, default_job_url_prefix):
    common = spec['common']
    common['downloads'] = rewrite_downloads(io, common.get('downloads', []), default_url_prefix)
    common['uploads'] = rewrite_uploads(common.get('uploads', []), default_job_url_prefix)

    tasks = []
    for task_i, spec_task in enumerate(spec['tasks']):
        task_url_prefix = "{}/{}".format(default_job_url_prefix, task_i+1)
        task = expand_task_spec(common, spec_task)
        task['downloads'] = rewrite_downloads(io, task['downloads'], default_url_prefix)
        task['uploads'] = rewrite_uploads(task['uploads'], task_url_prefix)
        task['stdout_url'] = rewrite_url_with_prefix(task['stdout_url'], task_url_prefix)
        task['command_result_url'] = rewrite_url_with_prefix(task['command_result_url'], task_url_prefix)
        task['parameters'] = spec_task['parameters']

        assert set(spec_task.keys()).issubset(task.keys()), "task before expand: {}, after expand: {}".format(spec_task.keys(), task.keys())

        tasks.append(task)
    return tasks

def submit(jq, io, job_id, spec, dry_run, config, skip_kube_submit):
    # where to take this from? arg with a default of 1?
    if dry_run:
        skip_kube_submit = True

    default_url_prefix = config.get("default_url_prefix", "")
    if default_url_prefix.endswith("/"):
        default_url_prefix = default_url_prefix[:-1]
    default_job_url_prefix = default_url_prefix+"/"+job_id

    tasks = expand_tasks(spec, io, default_url_prefix, default_job_url_prefix)
    task_spec_urls = []
    
    # TODO: When len(tasks) is a fair size (>100) this starts taking a noticable amount of time.
    # Perhaps store tasks in a single blob?  Or do write with multiple requests in parallel? 
    for task in tasks:    
        if not dry_run:
            url = io.write_json_to_cas(task)
            task_spec_urls.append(url)
        else:
            log.debug("task post expand: %s", json.dumps(task, indent=2))

    log.info("job_id: %s", job_id)
    if not dry_run:
        config_url = upload_config_for_consume(io, config)

        cas_url_prefix = config['cas_url_prefix']
        project = config['project']
        kubeque_command = ["kubeque-consume", config_url, job_id, "--project", project, "--cas_url_prefix", cas_url_prefix, "--cache_dir", "/host-var/kubeque-obj-cache"]
        parallelism = len(tasks)
        resources = spec["resources"]
        image = spec['image']
        kube_job_spec = create_kube_job_spec(job_id, parallelism, image, kubeque_command,
                                             cpu_request=resources.get("cpu", config['default_resource_cpu']),
                                             mem_limit=resources.get("memory", config["default_resource_memory"]))

        jq.submit(job_id, task_spec_urls, kube_job_spec)
        if not skip_kube_submit:
            #[("GOOGLE_APPLICATION_CREDENTIALS", "/google_creds/cred")], [("kube")]
            submit_job_spec(kube_job_spec)
        else:
            image = spec['image']
            log.info("Skipping submission.  You can execute tasks locally via:\n   %s", " ".join(["gcloud", "docker", "--", "run", "-v", os.path.expanduser("~/.config/gcloud")+":/google-creds", "-e", "GOOGLE_APPLICATION_CREDENTIALS=/google-creds/application_default_credentials.json", image] + kubeque_command + ["--nodename", "local"]))
        return config_url

def load_config(config_file):
    config_file = os.path.expanduser(config_file)

    config = ConfigParser()
    config.read(config_file)
    config = dict(config.items('config'))

    return [config] + list(load_config_from_dict(config))

def load_config_from_dict(config):
    io = IO(config['project'], config['cas_url_prefix'])
    jq = create_gcs_job_queue(config['project'], config['cluster_name'])

    return jq, io

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
            #assert os.path.exists(src)

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

        hash_db = CachingHashFunction(config.get("cache_db_path", ".kubeque-cached-file-hashes"))
        upload_map, spec = make_spec_from_command(args.command,
            image,
            dest_url=default_url_prefix+job_id, 
            cas_url=cas_url_prefix,
            parameters=parameters, 
            resources=args.resources,
            hash_function=hash_db.hash_filename,
            extra_files=expand_files_to_upload(args.push))
        hash_db.persist()

        log.info("upload_map = %s", upload_map)
        for filename, dest in upload_map.items():
            io.put(filename, dest, skip_if_exists=True)

    log.debug("spec: %s", json.dumps(spec, indent=2))
    submit(jq, io, job_id, spec, args.dryrun, config, args.skip_kube_submit)

    if not (args.dryrun or args.skip_kube_submit) and args.wait_for_completion:
        log.info("Waiting for job to terminate")
        watch(jq, job_id)
        if args.fetch:
            log.info("Job completed, downloading results to %s", args.fetch)
            fetch_cmd_(jq, io, job_id, args.fetch)
        else:
            log.info("Job completed.  You can download results by executing: kubeque fetch %s DEST_DIR", job_id)

def reset_cmd(jq, io, args):
    for jobid in jq.get_jobids(args.jobid_pattern):
        log.info("reseting %s", jobid)
        jq.reset(jobid, args.owner)
        if args.resubmit:
            pending_count = jq.get_status_counts(jobid)[STATUS_PENDING]
            kube_job_spec_json = jq.get_kube_job_spec(jobid)
            kube_job_spec = json.loads(kube_job_spec_json)
            # correct parallelism to reflect the remaining jobs
            kube_job_spec["spec"]["parallelism"] = pending_count
            import random, string
            name = kube_job_spec["metadata"]["name"]
            name += "-" + (''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(5)))
            kube_job_spec["metadata"]["name"] = name
            submit_job_spec(json.dumps(kube_job_spec))

def status_cmd(jq, io, args):
    jobid_pattern = args.jobid_pattern
    if not jobid_pattern:
        jobid_pattern = "*"

    for jobid in jq.get_jobids(jobid_pattern):
        if args.detailed:
            for task in jq.get_tasks(jobid):
                log.info("task_id: %s\n"
                         "  status: %s, failure_reason: %s\n"
                         "  started on pod: %s\n"
                         "  args: %s, history: %s", task.task_id,
                         task.status, task.failure_reason, task.owner, task.args, task.history)
        else:
            counts = jq.get_status_counts(jobid)
            status_str = ", ".join([ "{}: {}".format(status, count) for status, count in counts.items()])
            log.info("%s: %s", jobid, status_str)

def fetch_cmd(jq, io, args):
    fetch_cmd_(jq, io, args.jobid, args.dest)

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
        log.debug("task %d spec: %s", task.task_index+1, spec)

        if include_index:
            dest = os.path.join(dest_root, str(task.task_index+1))
            if not os.path.exists(dest):
                os.mkdir(dest)
        else:
            dest = dest_root

        # save parameters taken from spec
        with open(os.path.join(dest, "parameters.json"), "wt") as fd:
            fd.write(json.dumps(spec['parameters']))
        command_result_json = io.get_as_str(spec['command_result_url'], must=False)
        if command_result_json is None:
            log.warning("Results did not appear to be written yet at %s", spec['command_result_url'])
        else:
            get(spec['stdout_url'], os.path.join(dest, "stdout.txt"))
            command_result = json.loads(command_result_json)
            log.debug("command_result: %s", json.dumps(command_result))
            for ul in command_result['files']:
                assert not (ul['src'].startswith("/")), "Source must be a relative path"
                assert not (ul['src'].startswith("../")), "Source must not refer to parent dir"
                localpath = os.path.join(dest, ul['src'])
                log.info("Downloading to %s", localpath)
                get(ul['dst_url'], localpath)


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
    # TODO: Maybe rename as "clean" and also remove all pods/jobs which are complete _and_ are not referenced by a job
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

def stop_cmd(config):
    kube.stop_cluster(config['cluster_name'])

def add_node_pool_cmd(config, args):
    import uuid
    if args.name:
        node_pool_name = args.name
    else:
        if args.preemptable:
            prefix = "preempt"
        else:
            prefix = "reserve"
        node_pool_name = prefix + "-" + uuid.uuid4().hex[:6]
    kube.add_node_pool(config['cluster_name'], node_pool_name, args.machinetype, args.count, args.min, args.max, args.autoscale, args.preemptable)

def resize_node_pool_cmd(config, args):
    raise Exception("unimplemented")

def rm_node_pool_cmd(config, args):

    kube.rm_node_pool(config['cluster_name'], args.node_pool_name)

def kill_cmd(jq, args):
    jobids = jq.get_jobids(args.jobid_pattern)
    for jobid in jobids:
        kube.stop_job(jobid)
        # TODO: stop just marks the job as it shouldn't run any more.  tasks will still be claimed.
        # do we let the re-claimer transition these to pending?  Or do we cancel pending and mark claimed as failed?
        # probably we should

def peek_cmd(args):
    kube.peek(args.pod_name, args.lines)

import argparse

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
    parser.add_argument("--resources", "-r")
    parser.add_argument("--file", "-f", help="Job specification file (in JSON).  Only needed if command is not specified.")
    parser.add_argument("--push", "-u", action="append", default=[], help="Path to a local file which should be uploaded to working directory of command before execution starts.  If filename starts with a '@' the file is interpreted as a list of files which need to be uploaded.")
    parser.add_argument("--image", "-i", help="Name of docker image to run job within.  Defaults to value from kubeque config file.")
    parser.add_argument("--name", "-n", help="The name to assign to the job")
    parser.add_argument("--seq", type=int, help="Parameterize the command by index.  Submitting with --seq=10 will submit 10 commands with a parameter index varied from 1 to 10")
    parser.add_argument("--params", help="Parameterize the command by the rows in the specified CSV file.  If the CSV file has 5 rows, then 5 commands will be submitted.")
    parser.add_argument("--fetch", help="After run is complete, automatically download the results")
    parser.add_argument("--dryrun", action="store_true", help="Don't actually submit the job but just print what would have been done")
    parser.add_argument("--skipkube", action="store_true", dest="skip_kube_submit", help="Do all steps except submitting the job to kubernetes")
    parser.add_argument("--no-wait", action="store_false", dest="wait_for_completion", help="Exit immediately after submission instead of waiting for job to complete")
    parser.add_argument("command", nargs=argparse.REMAINDER)

    parser = subparser.add_parser("reset", help="Mark any 'claimed' or 'failed' jobs as ready for execution again.  Useful largely only during debugging issues with job submission.")
    parser.set_defaults(func=reset_cmd)
    parser.add_argument("jobid_pattern")
    parser.add_argument("--owner")
    parser.add_argument("--resubmit", action="store_true")

    parser = subparser.add_parser("status", help="Print the status for the tasks which make up the specified job")
    parser.set_defaults(func=status_cmd)
    parser.add_argument("--detailed", action="store_true", help="List attributes of each task")
    parser.add_argument("jobid_pattern", nargs="?")

    parser = subparser.add_parser("remove", help="Remove completed jobs from the database of jobs")
    parser.set_defaults(func=remove_cmd)
    parser.add_argument("jobid_pattern")

    parser = subparser.add_parser("kill", help="Terminate the specified job")
    parser.set_defaults(func=kill_cmd)
    parser.add_argument("jobid_pattern")

    parser = subparser.add_parser("start", help="Start kubernetes cluster")
    parser.set_defaults(func=start_cmd)

    parser = subparser.add_parser("stop", help="Shutdown kubernets cluster")
    parser.set_defaults(func=stop_cmd)

    parser = subparser.add_parser("fetch", help="Download results from a completed job")
    parser.set_defaults(func=fetch_cmd)
    parser.add_argument("jobid")
    parser.add_argument("dest", help="The path to the directory where the results will be downloaded")

    parser = subparser.add_parser("add-node-pool")
    parser.set_defaults(func=add_node_pool_cmd)
    parser.add_argument("machinetype")
    parser.add_argument("--name")
    parser.add_argument("--count", "-n", default=1)
    parser.add_argument("--max")
    parser.add_argument("--min")
    parser.add_argument("--autoscale", action="store_true")
    parser.add_argument("--preemptable", action="store_true")

    parser = subparser.add_parser("resize-node-pool")
    parser.set_defaults(func=resize_node_pool_cmd)
    parser.add_argument("name")
    parser.add_argument("--count", "-n", default=1)
    parser.add_argument("--max")
    parser.add_argument("--min")
    parser.add_argument("--autoscale", action="store_true")
    parser.add_argument("--preemptable", action="store_true")

    parser = subparser.add_parser("rm-node-pool")
    parser.set_defaults(func=rm_node_pool_cmd)
    parser.add_argument("node_pool_name")

    parser = subparser.add_parser("peek")
    parser.set_defaults(func=peek_cmd)
    parser.add_argument("pod_name")
    parser.add_argument("--lines", default=100, type=int)

    args = parse.parse_args(argv)

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if not hasattr(args, 'func'):
        parse.print_help()
        sys.exit(1)
    
    func_param_names = get_func_parameters(args.func)
    if len(set(["config", "jq", "io"]).intersection(func_param_names)) > 0:
        config_path = get_config_path(args.config)
        config, jq, io = load_config(config_path)
    func_params = {}
    if "args" in func_param_names:
        func_params["args"] = args
    if "config" in func_param_names:
        func_params["config"] = config
    if "io" in func_param_names:
        func_params["io"] = io
    if "jq" in func_param_names:
        func_params["jq"] = jq

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

