import os
import json
from ..log import log
from ..job_queue import JobQueue, Job
from ..io_helper import IO
from ..log import log
from .shared import _resolve_jobid

def fetch_cmd(jq: JobQueue, io: IO, args):
    jobid = _resolve_jobid(jq, args.jobid)
    if args.dest is None:
        dest = jobid
    else:
        dest = args.dest
    fetch_cmd_(jq, io, jobid, dest, flat=args.flat)


def fetch_cmd_(
    jq: JobQueue, io: IO, jobid: str, dest_root: str, force=False, flat=False
):
    def get(src, dst, **kwargs):
        if os.path.exists(dst) and not force:
            log.warning("%s exists, skipping download", dst)
        return io.get(src, dst, **kwargs)

    tasks = jq.task_storage.get_tasks(jobid)

    if not os.path.exists(dest_root):
        os.mkdir(dest_root)

    include_index = not flat

    for task in tasks:
        spec = json.loads(io.get_as_str_must(task.args))
        log.debug("task %d spec: %s", task.task_index, spec)

        if include_index:
            dest = os.path.join(dest_root, str(task.task_index))
            if not os.path.exists(dest):
                os.mkdir(dest)
        else:
            dest = dest_root

        # save parameters taken from spec
        # with open(os.path.join(dest, "parameters.json"), "wt") as fd:
        #     fd.write(json.dumps(spec['parameters']))
        command_result_json = io.get_as_str(spec["command_result_url"], must=False)
        to_download = []
        if command_result_json is None:
            log.warning(
                "Results did not appear to be written yet at %s",
                spec["command_result_url"],
            )
        else:
            get(spec["stdout_url"], os.path.join(dest, "stdout.txt"))
            command_result = json.loads(command_result_json)
            log.debug("command_result: %s", json.dumps(command_result))
            for ul in command_result["files"]:
                to_download.append((ul["src"], ul["dst_url"]))

        for src, dst_url in to_download:
            if include_index:
                localpath = os.path.join(dest_root, str(task.task_index), src)
            else:
                localpath = os.path.join(dest_root, src)
            pdir = os.path.dirname(localpath)
            if not os.path.exists(pdir):
                os.makedirs(pdir)
            get(dst_url, localpath)


def add_fetch_cmd(subparser):
    parser = subparser.add_parser("fetch", help="Download results from a completed job")
    parser.set_defaults(func=fetch_cmd)
    parser.add_argument("jobid")
    parser.add_argument(
        "--flat",
        action="store_true",
        help="Instead of writing each task into a seperate directory, write all files into the destination directory",
    )
    parser.add_argument(
        "--dest",
        help="The path to the directory where the results will be downloaded. If omitted a directory will be created with the job id",
    )
