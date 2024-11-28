from ..task_store import (
    STATUS_FAILED,
    STATUS_CLAIMED,
    STATUS_PENDING,
    STATUS_KILLED,
    STATUS_COMPLETE,
)
from ..job_queue import JobQueue, Job
from ..cluster_service import Cluster
from ..io_helper import IO
from ..resize_cluster import GetPreempted
from ..config import get_config_path, load_config, create_services, Config, BadConfig
from ..log import log
from .shared import _get_jobids_from_pattern, _resolve_jobid
import json
import sys
from ..task_store import Task
import dataclasses
import csv


def show_cmd(jq: JobQueue, io: IO, args):
    jobid = _resolve_jobid(jq, args.jobid)
    retcode = args.exitcode

    if args.incomplete:
        tasks = []
        for status in [STATUS_FAILED, STATUS_CLAIMED, STATUS_PENDING, STATUS_KILLED]:
            tasks.extend(jq.task_storage.get_tasks(jobid, status=status))
    else:
        tasks = jq.task_storage.get_tasks(jobid)

    if retcode is not None:

        def retcode_matches(exit_code):
            return exit_code is not None and int(exit_code) == retcode

        before_count = len(tasks)
        tasks = [task for task in tasks if retcode_matches(task.exit_code)]
        log.info(
            "Filtered {} tasks to {} tasks with exit code {}".format(
                before_count, len(tasks), retcode
            )
        )

    if len(tasks) == 0:
        log.error("No tasks found")
        return 1
    else:
        log.info("Getting parameters from %d tasks" % len(tasks))

        rows = []

        def make_simple_row(task: Task):
            row = {}
            row["sparklespray_task_id"] = task.task_id
            row["sparklespray_exit_code"] = task.exit_code
            row["sparklespray_failure_reason"] = task.failure_reason
            row["sparklespray_status"] = task.status

            if args.params:
                task_spec = json.loads(io.get_as_str_must(task.args))
                task_parameters = task_spec.get("parameters", {})
                row.update(task_parameters)

            return row

        def make_full_row(task: Task):
            row = dataclasses.asdict(task)
            task_spec = json.loads(io.get_as_str_must(task.args))
            row["args_url"] = task.args
            row["args"] = task_spec
            return row

        def write_json_rows(rows, fd):
            fd.write(json.dumps(rows, indent=3) + "\n")

        def write_csv_rows(rows, fd):
            # find the union of all keys
            keys = set()
            for p in rows:
                keys.update(p.keys())

            columns = list(keys)
            columns.sort()

            w = csv.writer(fd)
            w.writerow(columns)
            for p in rows:
                row = [str(p.get(column, "")) for column in columns]
                w.writerow(row)

        if args.detailed:
            for task in tasks:
                rows.append(make_full_row(task))
            write = write_json_rows
        else:
            for task in tasks:
                rows.append(make_simple_row(task))
            if args.csv:
                write = write_csv_rows
            else:
                write = write_json_rows

        if args.out:
            with open(args.out, "wt") as fd:
                write(rows, fd)
        else:
            write(rows, sys.stdout)


def add_show_cmd(subparser):
    parser = subparser.add_parser(
        "show", help="Write to a csv file the parameters for each task"
    )
    parser.set_defaults(func=show_cmd)
    parser.add_argument("jobid")
    parser.add_argument(
        "--out",
        help="The filename to write the output to. If not specified, writes to stdout",
    )
    parser.add_argument(
        "--params",
        help="If set, include the parameters for each task",
        action="store_true",
    )
    parser.add_argument("--csv", help="If set, will write as csv", action="store_true")
    parser.add_argument(
        "--detailed",
        help="If set, will write full details as json",
        action="store_true",
    )
    parser.add_argument(
        "--incomplete",
        "-i",
        help="By default, will list all parameters. If this flag is present, only those tasks which are not complete will be written to the csv",
        action="store_true",
    )
    parser.add_argument(
        "--exitcode",
        "-e",
        help="Only include those tasks with this return code",
        type=int,
    )
