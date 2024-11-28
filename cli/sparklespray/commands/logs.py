from ..print_failures import print_failures
from .shared import _resolve_jobid
from ..job_queue import JobQueue
from ..io_helper import IO


def logs_cmd(jq: JobQueue, io: IO, args):
    jobid = _resolve_jobid(jq, args.jobid)
    print_failures(jq, io, jobid, args.all)


def add_logs_cmd(subparser):
    parser = subparser.add_parser("logs", help="Print out logs from failed tasks")
    parser.set_defaults(func=logs_cmd)
    parser.add_argument("jobid")
    parser.add_argument(
        "--all",
        help="If set, show paths to logs for all tasks instead of just failures",
        action="store_true",
    )
