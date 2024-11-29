from typing import List, Optional, Dict
import re
import json
import csv
from ..job_queue import JobQueue
import sys
from ..io_helper import IO
from ..cluster_service import Cluster
from ..node_req_store import AddNodeReqStore
import dataclasses
from .shared import _resolve_jobid

# def logs_cmd(jq: JobQueue, io: IO, args):
#     jobid = _resolve_jobid(jq, args.jobid)
#     tasks = jq.task_storage.get_tasks(jobid)
#     if not args.all:
#         tasks = [t for t in tasks if t.status == STATUS_FAILED or (
#             t.exit_code is not None and str(t.exit_code) != "0")]
#     print("You can view any of these logs by using: gsutil cat <log_path>")
#     print("task_id\texit_code\tlog_path\t")
#     for t in tasks:
#         print("{}\t{}\t{}".format(t.task_id, t.exit_code, t.log_url))


def list_cmd(jq: JobQueue, io, args):
    job_id = _resolve_jobid(jq, args.jobid)
    fields = None
    if args.fields is not None:
        fields = args.fields.split(",")
    filters = []
    if args.filters is not None:
        filters = args.filters
    list_tasks(jq, io, job_id, args.params, fields, filters, args.format, args.output)


def add_list_cmd(subparser):
    parser = subparser.add_parser("list", help="List tasks within a job")
    parser.set_defaults(func=list_cmd)
    parser.add_argument("jobid")
    parser.add_argument(
        "--filter",
        help="only include records matching this filter",
        action="append",
        dest="filters",
    )
    parser.add_argument("--fields", help="Only include these fields")
    parser.add_argument(
        "--format", default="csv", help="Output format, either 'json' or 'csv'"
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Name of file to write to. If not specified, writes to stdout",
    )
    parser.add_argument(
        "--params",
        help="Only write out parameters from original --params submission",
        action="store_true",
    )


from typing import Any


def list_tasks(
    jq: JobQueue,
    io: IO,
    job_id: str,
    params_only: bool,
    fields: Optional[List[str]],
    filter_expressions: List[str],
    output_mode: str,
    output_filename: str,
):
    # only expand "args" if we request or filter by a field inside args
    if fields is None:
        needs_full_task_def = True
    else:
        needs_full_task_def = False
        for field in fields:
            if field.startswith("args."):
                needs_full_task_def = True

    for f in filter_expressions:
        if f.startswith("args."):
            needs_full_task_def = True

    # or if we want the user provided parameters, we can only get those by expanding args
    if params_only:
        needs_full_task_def = True

    def to_record(task, task_specs_str) -> Dict[str, Any]:
        row = dataclasses.asdict(task)
        if needs_full_task_def:
            assert task_specs_str is not None
            task_spec = json.loads(task_specs_str)
            row["args_url"] = task.args
            row["args"] = task_spec
        return row

    tasks = jq.task_storage.get_tasks(job_id)

    task_spec_strs = {}
    if needs_full_task_def:
        task_spec_strs = io.bulk_get_as_str([task.args for task in tasks])

    records = []
    for i, task in enumerate(tasks):
        # print("{}/{}".format(i, len(tasks)))
        records.append(to_record(task, task_spec_strs.get(task.args)))

    # perform the filtering before applying params_only so we can do things like "find parameters of failed tasks"
    filtered = process_records(records, fields, filter_expressions)

    if params_only:
        filtered = [record["args"]["parameters"] for record in filtered]  # type: ignore

    write(filtered, output_mode, output_filename)


def process_records(records, fields, filter_expressions):
    filters = [make_predicate(f) for f in filter_expressions]

    # make a predicate which represents all predicates being satisified
    def predicate(record):
        for filter in filters:
            if not filter(record):
                return False
        return True

    # perform filtering
    filtered = [rec for rec in records if predicate(rec)]

    # project out a subset of columns if requested
    if fields is not None:
        filtered = [project(rec, fields) for rec in filtered]

    return filtered


def _get(d: dict, path: str) -> Optional[str]:
    "Given a dotted path, traverse through nested dictionaries to return field. If any step is missing, return None"
    elements = path.split(".")
    for e in elements[:-1]:
        assert isinstance(d, dict)
        d2 = d.get(e)
        if d2 is None:
            return None
        assert isinstance(d2, dict)
        d = d2
    return d.get(elements[-1])


def _set(d: dict, path: str, value: str):
    elements = path.split(".")
    for e in elements[:-1]:
        if e not in d:
            d[e] = {}
        d = d[e]
    d[elements[-1]] = value


def project(d: dict, fields: List[str]):
    result: Dict[str, str] = {}
    for field in fields:
        v = _get(d, field)
        _set(result, field, "" if v is None else v)
    return result


def make_predicate(filter_expression):
    m = re.match("([A-Za-z0-9._]+)(=|!=)(.*)", filter_expression)
    assert m is not None, "Could not parse '{}'".format(filter_expression)
    var = m.group(1)
    op = m.group(2)
    value = m.group(3)
    if op == "=":
        return lambda record: _get(record, var) == value
    else:
        assert op == "!="
        return lambda record: _get(record, var) != value


def flatten(d: dict):
    result = {}
    for k, v in d.items():
        if isinstance(v, dict):
            for inner_k, inner_v in flatten(v).items():
                result[k + "." + inner_k] = inner_v
        else:
            result[k] = str(v)
    return result


def write_csv(records, fd):
    # flatten all records
    records = [flatten(x) for x in records]

    # find all keys
    keys = set()
    for rec in records:
        keys.update(rec.keys())

    columns = list(keys)
    columns.sort()

    w = csv.writer(fd)
    w.writerow(columns)
    for rec in records:
        w.writerow([rec.get(column, "") for column in columns])


def write_json(records, fd):
    json.dump(records, fd, indent=4)


def write(records, mode, filename):
    if filename is None:
        fd = sys.stdout
    else:
        fd = open(filename, "wt")

    if mode == "csv":
        write_csv(records, fd)
    else:
        assert mode == "json"
        write_json(records, fd)


# def add_list_nodes_cmd(subparser):
#     parser = subparser.add_parser("list-nodes", help="List tasks within a job")
#     parser.set_defaults(func=list_nodes_cmd)
#     parser.add_argument("jobid")
#     parser.add_argument(
#         "--filter",
#         help="only include records matching this filter",
#         action="append",
#         dest="filters",
#     )
#     parser.add_argument("--fields", help="Only include these fields")
#     parser.add_argument(
#         "--format", default="csv", help="Output format, either 'json' or 'csv'"
#     )
#     parser.add_argument(
#         "--output",
#         "-o",
#         help="Name of file to write to. If not specified, writes to stdout",
#     )


# def list_nodes_cmd(jq: JobQueue, cluster: Cluster, io, args):
#     job_id = _resolve_jobid(jq, args.jobid)
#     fields = None
#     if args.fields is not None:
#         fields = args.fields.split(",")

#     filters: List[str] = []
#     if args.filters is not None:
#         filters = args.filters
#         assert isinstance(filters, list)

#     job = jq.get_job(job_id)
#     assert job is not None
#     cluster_id = job.cluster

#     list_nodes(
#         cluster_id,
#         cluster.node_req_store,
#         io,
#         job_id,
#         fields,
#         filters,
#         args.format,
#         args.output,
#     )


# def list_nodes(
#     cluster_id: str,
#     node_req_store,
#     io: IO,
#     job_id: str,
#     fields: Optional[List[str]],
#     filter_expressions: List[str],
#     output_mode: str,
#     output_filename: str,
# ):
#     def to_record(node_req: AddNodeReqStore):
#         row = dataclasses.asdict(node_req)
#         return row

#     node_reqs = node_req_store.get_node_reqs(cluster_id)

#     records = []
#     for _, node_req in enumerate(node_reqs):
#         records.append(to_record(node_req))

#     filtered = process_records(records, fields, filter_expressions)

#     write(filtered, output_mode, output_filename)
