import re
import logging
import os
import json
import sys
import attr
import sparklespray
from .task_store import (
    STATUS_FAILED,
    STATUS_CLAIMED,
    STATUS_PENDING,
    STATUS_KILLED,
    STATUS_COMPLETE,
)
from .util import get_timestamp, url_join
from .job_store import JOB_STATUS_KILLED
from .job_queue import JobQueue
from .cluster_service import Cluster
from .io import IO
from .watch import watch
from .resize_cluster import GetPreempted
from . import txtui
from .validate import validate_cmd
import csv
import argparse

from .config import get_config_path, load_config, load_only_config_dict

from .log import log
from . import txtui
from .gcp_setup import setup_project, grant


def logs_cmd(jq: JobQueue, io: IO, args):
    jobid = _resolve_jobid(jq, args.jobid)
    tasks = jq.task_storage.get_tasks(jobid)
    if not args.all:
        tasks = [
            t
            for t in tasks
            if t.status == STATUS_FAILED
            or (t.exit_code is not None and str(t.exit_code) != "0")
        ]
    print("You can view any of these logs by using: gsutil cat <log_path>")
    print("task_id\texit_code\tlog_path\t")
    for t in tasks:
        print("{}\t{}\t{}".format(t.task_id, t.exit_code, t.log_url))


def show_cmd(jq: JobQueue, io: IO, args):
    jobid = _resolve_jobid(jq, args.jobid)
    retcode = args.exitcode

    # job = jq.get_job(jobid)
    # job = attr.asdict(job)

    if args.incomplete:
        tasks = []
        for status in [STATUS_FAILED, STATUS_CLAIMED, STATUS_PENDING, STATUS_KILLED]:
            tasks.extend(jq.get_tasks(jobid, status=status))
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
        sys.exit(1)
    else:
        log.info("Getting parameters from %d tasks" % len(tasks))

        rows = []

        def make_simple_row(task):
            row = {}
            row["sparklespray_task_id"] = task.task_id
            row["sparklespray_exit_code"] = task.exit_code
            row["sparklespray_failure_reason"] = task.failure_reason
            row["sparklespray_status"] = task.status

            if args.params:
                task_spec = json.loads(io.get_as_str(task.args))
                task_parameters = task_spec.get("parameters", {})
                row.update(task_parameters)

            return row

        def make_full_row(task):
            row = attr.asdict(task)
            task_spec = json.loads(io.get_as_str(task.args))
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


def reset_cmd(jq: JobQueue, io: IO, cluster: Cluster, args):
    jobid_pattern = args.jobid_pattern
    if "." in jobid_pattern:
        task = jq.task_storage.get_task(jobid_pattern)
        print(f"reseting task from {task.status} -> STATUS_PENDING")
        jq._reset_task(task, STATUS_PENDING)
    else:
        for jobid in _get_jobids_from_pattern(jq, jobid_pattern):
            if args.all:
                statuses_to_clear = [
                    STATUS_CLAIMED,
                    STATUS_FAILED,
                    STATUS_COMPLETE,
                    STATUS_KILLED,
                ]
            else:
                statuses_to_clear = [STATUS_CLAIMED, STATUS_FAILED, STATUS_KILLED]
            log.info(
                "reseting %s by changing tasks with statuses (%s) -> %s",
                jobid,
                ",".join(statuses_to_clear),
                STATUS_PENDING,
            )
            updated = jq.reset(jobid, None, statuses_to_clear=statuses_to_clear)
            log.info("updated %d tasks", updated)

            cluster.cleanup_node_reqs(jobid)
            log.info("Cleaned up old node requests")



def _summarize_task_statuses(tasks):
    import collections

    complete = True
    counts = collections.defaultdict(lambda: 0)
    for task in tasks:
        if task.status == STATUS_COMPLETE:
            label = "{}(code={})".format(task.status, task.exit_code)
        elif task.status == STATUS_FAILED:
            label = "{}({})".format(task.status, task.failure_reason)
        else:
            label = task.status
        counts[label] += 1

        if not _is_terminal_status(task.status):
            complete = False

    labels = list(counts.keys())
    labels.sort()
    status_str = ", ".join(["{}: {}".format(l, counts[l]) for l in labels])
    return status_str, complete


def _get_jobids_from_pattern(jq, jobid_pattern):
    if not jobid_pattern:
        jobid_pattern = "*"

    if jobid_pattern == "LAST":
        job = jq.get_last_job()
        return [job.job_id]
    else:
        return jq.get_jobids(jobid_pattern)


def _resolve_jobid(jq, jobid):
    if jobid == "LAST":
        job = jq.get_last_job()
        return job.job_id
    else:
        return jobid


def status_cmd(jq: JobQueue, io: IO, cluster: Cluster, args):
    jobids = _get_jobids_from_pattern(jq, args.jobid_pattern)

    for jobid in jobids:
        # if args.detailed or args.failures:
        #     for task in jq.get_tasks(jobid):
        #         if args.failures and task.status != STATUS_FAILED:
        #             continue

        #         command_result_json = None
        #         if task.command_result_url is not None:
        #             command_result_json = io.get_as_str(task.command_result_url, must=False)
        #         if command_result_json is not None:
        #             command_result = json.loads(command_result_json)
        #             command_result_block = "\n  command result: {}".format(json.dumps(command_result, indent=4))
        #         else:
        #             command_result_block = ""

        #         log.info("task_id: %s\n"
        #                     "  status: %s, exit_code: %s, failure_reason: %s\n"
        #                     "  started on pod: %s\n"
        #                     "  args: %s, history: %s%s\n"
        #                     "  cluster: %s", task.task_id,
        #                     task.status, task.exit_code, task.failure_reason, task.owner, task.args, task.history,
        #                     command_result_block, task.cluster)
        # else:
        tasks = cluster.task_store.get_tasks(jobid)
        status, complete = _summarize_task_statuses(tasks)
        txtui.user_print(f"{jobid}: {status}")
        if args.stats:
            task_times = []

            command_result_urls = []
            claim_count_per_task = []
            for task in tasks:
                command_result_urls.append(task.command_result_url)

                claimed_time = None
                task_time = None
                claimed_count = 0
                for entry in task.history:
                    if entry.status == STATUS_CLAIMED:
                        claimed_time = entry.timestamp
                        claimed_count += 1
                    elif entry.status == STATUS_COMPLETE:
                        task_time = entry.timestamp - claimed_time
                claim_count_per_task.append(claimed_count)

                if task_time is not None:
                    task_times.append(task_time)

            claim_count_per_task.sort()
            n = len(claim_count_per_task)
            txtui.user_print(
                "Number of times a task was claimed quantiles: {}, {}, {}, {}, {}, mean: {:1f}".format(
                    claim_count_per_task[0],
                    claim_count_per_task[int(n / 4)],
                    claim_count_per_task[int(n / 2)],
                    claim_count_per_task[int(n * 3 / 4)],
                    claim_count_per_task[n - 1],
                    sum(claim_count_per_task) / n,
                )
            )

            task_times.sort()
            n = len(task_times)
            txtui.user_print(
                "task count: {}, execution time quantiles (in minutes): {:.1f}, {:.1f}, {:.1f}, {:.1f}, {:.1f}, mean: {:.1f}".format(
                    n,
                    task_times[0] / 60,
                    task_times[int(n / 4)] / 60,
                    task_times[int(n / 2)] / 60,
                    task_times[int(n * 3 / 4)] / 60,
                    task_times[n - 1] / 60,
                    sum(task_times) / n / 60,
                )
            )

            txtui.user_print("Getting memory stats...")
            results = io.bulk_get_as_str(command_result_urls).values()
            results = [json.loads(body) for body in results if body is not None]
            max_memory_size = [x["resource_usage"]["max_memory_size"] for x in results]
            max_memory_size.sort()
            n = len(max_memory_size)
            txtui.user_print(
                "max memory quantiles: {}, {}, {}, {}, {}, mean: {}".format(
                    max_memory_size[0],
                    max_memory_size[int(n / 4)],
                    max_memory_size[int(n / 2)],
                    max_memory_size[int(n * 3 / 4)],
                    max_memory_size[n - 1],
                    sum(max_memory_size) / n,
                )
            )


#            import pdb
#            pdb.set_trace()


def fetch_cmd(jq, io, args):
    jobid = _resolve_jobid(jq, args.jobid)
    if args.dest is None:
        dest = jobid
    else:
        dest = args.dest
    fetch_cmd_(jq, io, jobid, dest, flat=args.flat)


def fetch_cmd_(jq, io, jobid, dest_root, force=False, flat=False):
    def get(src, dst, **kwargs):
        if os.path.exists(dst) and not force:
            log.warning("%s exists, skipping download", dst)
        return io.get(src, dst, **kwargs)

    tasks = jq.get_tasks(jobid)

    if not os.path.exists(dest_root):
        os.mkdir(dest_root)

    include_index = not flat

    for task in tasks:
        spec = json.loads(io.get_as_str(task.args))
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


def _is_terminal_status(status):
    return status in [STATUS_FAILED, STATUS_COMPLETE]


def _is_complete(status_counts):
    all_terminal = True
    for status in status_counts.keys():
        if not _is_terminal_status(status):
            all_terminal = True
    return all_terminal


def _resub_preempted(cluster, jq, jobid):
    tasks = jq.get_tasks(jobid, STATUS_CLAIMED)
    for task in tasks:
        _update_if_owner_missing(cluster, jq, task)


def _update_claimed_are_still_running(jq, cluster, job_id):
    get_preempted = GetPreempted(min_bad_time=0)
    state = cluster.get_state(job_id)
    state.update()
    task_ids = get_preempted(state)
    if len(task_ids) > 0:
        log.info(
            "Resetting tasks which appear to have been preempted: %s",
            ", ".join(task_ids),
        )
        for task_id in task_ids:
            jq.reset_task(task_id)
    return task_ids


def clean(
    cluster: Cluster,
    jq: JobQueue,
    job_id: str,
    force: bool = False,
    force_pending: bool = False,
    only_nodes: bool = False,
):
    cluster.cleanup_node_reqs(job_id)
    if only_nodes:
        return

    if not force:
        # Check to not remove tasks that are claimed and still running
        tasks = cluster.task_store.get_tasks(job_id, status=STATUS_CLAIMED)
        if len(tasks) > 0:
            # if some tasks are still marked 'claimed' verify that the owner is still running
            reset_task_ids = _update_claimed_are_still_running(jq, cluster, job_id)

            still_running = []
            for task in tasks:
                if task.task_id not in reset_task_ids:
                    still_running.append(task.task_id)

            log.info(
                "reset_task_ids=%s, still_running=%s", reset_task_ids, still_running
            )
            if len(still_running) > 0:
                log.warning(
                    "job %s is still running (%d tasks), cannot remove",
                    job_id,
                    len(still_running),
                )
                return False

        if not force_pending:
            # Check to not remove tasks that are pending (waiting to be claimed)
            job = cluster.job_store.get_job(job_id)
            tasks = cluster.task_store.get_tasks(job_id, status=STATUS_PENDING)
            nb_tasks_pending = len(tasks)
            if nb_tasks_pending > 0 and cluster.has_active_node_requests(job.cluster):
                log.warning(
                    "Job {} has {} tasks that are still pending and active requests for VMs. If you want to force cleaning them,"
                    " please use 'sparkles kill {}'".format(
                        job_id, nb_tasks_pending, job_id
                    )
                )
                return False

    cluster.delete_job(job_id)
    return True


def clean_cmd(cluster: Cluster, jq, args):
    jobids = _get_jobids_from_pattern(jq, args.jobid_pattern)
    for jobid in jobids:
        log.info("Deleting %s", jobid)
        clean(
            cluster,
            jq,
            jobid,
            args.force,
            args.force_pending,
            only_nodes=args.only_nodes,
        )


def _update_if_owner_missing(cluster, jq, task):
    if task.status != STATUS_CLAIMED:
        return
    if not cluster.is_owner_running(task.owner):
        job = jq.get_job(task.job_id)
        if job.status == JOB_STATUS_KILLED:
            new_status = STATUS_KILLED
        else:
            new_status = STATUS_PENDING
        log.info(
            "Task %s is owned by %s which does not appear to be running, resetting status from 'claimed' to '%s'",
            task.task_id,
            task.owner,
            new_status,
        )
        jq.reset_task(task.task_id, status=new_status)


def kill_cmd(jq: JobQueue, cluster, args):
    jobids = _get_jobids_from_pattern(jq, args.jobid_pattern)
    if len(jobids) == 0:
        log.warning("No jobs found matching pattern")
    for jobid in jobids:
        # TODO: stop just marks the job as it shouldn't run any more.  tasks will still be claimed.
        log.info("Marking %s as killed", jobid)
        ok, job = jq.kill_job(jobid)
        assert ok
        if not args.keepcluster:
            cluster.stop_cluster(job.cluster)
            tasks = jq.get_tasks_for_cluster(job.cluster, STATUS_CLAIMED)
            for task in tasks:
                _update_if_owner_missing(cluster, jq, task)

        # if there are any sit sitting at pending, mark them as killed
        tasks = jq.task_storage.get_tasks(jobid, status=STATUS_PENDING)
        txtui.user_print("Marking {} pending tasks as killed".format(len(tasks)))
        for task in tasks:
            jq.reset_task(task.task_id, status=STATUS_KILLED)


def version_cmd():
    log.info("version command ran")
    print(sparklespray.__version__)


def get_func_parameters(func):
    import inspect

    return inspect.getfullargspec(func)[0]


def dump_operation_cmd(cluster: Cluster, args):
    operation = cluster.get_raw_operation_details(args.operation_id)
    print(json.dumps(operation, indent="  "))


def grant_cmd(args, config):
    credentials = config
    role = args.role
    project_id = args.project
    service_acct = config.get("credentials").service_account_email
    grant(service_acct, project_id, role)


def setup_cmd(args, config):
    default_url_prefix = config["default_url_prefix"]
    m = re.match("^gs://([^/]+)(?:/.*)?$", default_url_prefix)
    assert m != None, "invalid remote path: {}".format(default_url_prefix)
    bucket_name = m.group(1)

    setup_project(config["project"], config["service_account_key"], bucket_name)


def sparkles_main():
    # disable stdout/stderr buffering to work better when run non-interactively
    import sys, io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, line_buffering=True)
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, line_buffering=True)

    retcode = main()
    if retcode is not None:
        sys.exit(retcode)


def main(argv=None):
    import warnings

    warnings.filterwarnings(
        "ignore", "Your application has authenticated using end user credentials"
    )

    from .submit import add_submit_cmd
    from .watch import add_watch_cmd
    from .list import add_list_cmd, add_list_nodes_cmd

    parse = argparse.ArgumentParser()
    parse.add_argument("--config", default=None)
    parse.add_argument(
        "--debug", action="store_true", help="If set, debug messages will be output"
    )
    subparser = parse.add_subparsers()

    add_submit_cmd(subparser)
    add_list_cmd(subparser)
    add_list_nodes_cmd(subparser)

    parser = subparser.add_parser(
        "validate", help="Run a series of tests to confirm the configuration is valid"
    )
    parser.set_defaults(func=validate_cmd)

    parser = subparser.add_parser(
        "reset",
        help="Mark any 'claimed', 'killed' or 'failed' or jobs with non-zero exit jobs as ready for execution again.  Useful largely only during debugging issues with job submission. Potentially also useful for retrying after transient failures.",
    )
    parser.set_defaults(func=reset_cmd)
    parser.add_argument("jobid_pattern")
    parser.add_argument(
        "--all",
        action="store_true",
        help="If set, will mark all tasks as 'pending', not just 'claimed', 'killed' or 'failed' tasks. The first parameter can be either a job ID or an individual task ID",
    )

    parser = subparser.add_parser(
        "grant",
        help="Grants additional rights to service account that sparkles is using",
    )
    parser.add_argument("project")
    parser.add_argument("role")
    parser.set_defaults(func=grant_cmd)

    parser = subparser.add_parser(
        "setup",
        help="Configures the google project chosen in the config to be compatible with sparklespray. (requires gcloud installed in path)",
    )
    parser.set_defaults(func=setup_cmd)

    parser = subparser.add_parser(
        "dump-operation",
        help="primarily used for debugging. If a sparkles cannot turn on a node, this can be used to dump the details of the operation which requested the node.",
    )
    parser.set_defaults(func=dump_operation_cmd)
    parser.add_argument("operation_id")

    parser = subparser.add_parser("logs", help="Print out logs from failed tasks")
    parser.set_defaults(func=logs_cmd)
    parser.add_argument("jobid")
    parser.add_argument(
        "--all",
        help="If set, show paths to logs for all tasks instead of just failures",
        action="store_true",
    )

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

    #    parser = subparser.add_parser("retry", help="Resubmit any 'failed' jobs for execution again. (often after increasing memory required)")
    #    parser.set_defaults(func=retry_cmd)
    #    parser.add_argument("jobid_pattern")
    #    parser.add_argument("--resources", "-r", help="Update the resource requirements that should be used when re-running job. (ie: -r memory=5G,cpu=2) ")
    #    parser.add_argument("--owner", help="if specified, only tasks with this owner will be retried")
    #    parser.add_argument("--no-wait", action="store_false", dest="wait_for_completion", help="Exit immediately after submission instead of waiting for job to complete")

    parser = subparser.add_parser(
        "status", help="Print the status for the tasks which make up the specified job"
    )
    parser.add_argument("--stats", action="store_true")
    parser.set_defaults(func=status_cmd)
    parser.add_argument("jobid_pattern", nargs="?")

    add_watch_cmd(subparser)

    parser = subparser.add_parser(
        "clean",
        help="Remove jobs which are not currently running from the database of jobs",
    )
    parser.set_defaults(func=clean_cmd)
    parser.add_argument(
        "--force",
        "-f",
        help="If set, will delete job regardless of whether it is running or not",
        action="store_true",
    )
    parser.add_argument(
        "jobid_pattern",
        nargs="?",
        help="If specified will only attempt to remove jobs that match this pattern",
    )
    parser.add_argument(
        "--force_pending",
        "-p",
        help="If set, will delete pending jobs",
        action="store_true",
    )
    parser.add_argument(
        "--only-nodes",
        action="store_true",
        help="If set, will not delete the job, only completed node requests.",
    )

    parser = subparser.add_parser("kill", help="Terminate the specified job")
    parser.set_defaults(func=kill_cmd)
    parser.add_argument(
        "--keepcluster",
        action="store_true",
        help="If set will also terminate the nodes that the job is using to run. (This could impact other running jobs that use the same docker image)",
    )
    parser.add_argument("jobid_pattern")

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

    parser = subparser.add_parser("version", help="print the version and exit")
    parser.set_defaults(func=version_cmd)

    args = parse.parse_args(argv)

    txtui.config_logging(100 if args.debug else 0)

    if not hasattr(args, "func"):
        parse.print_help()
        sys.exit(1)

    if args.func == setup_cmd:
        # special case, because this is the one command which must work before the service account
        # is set up.
        config = load_only_config_dict(args.config, verbose=True)
        args.func(args, config)
    else:
        func_param_names = get_func_parameters(args.func)
        if (
            len(set(["config", "jq", "io", "cluster"]).intersection(func_param_names))
            > 0
        ):
            config, jq, io, cluster = load_config(args.config)
        func_params = {}
        if "args" in func_param_names:
            func_params["args"] = args
        if "config" in func_param_names:
            func_params["config"] = config
        if "io" in func_param_names:
            func_params["io"] = io
        if "jq" in func_param_names:
            func_params["jq"] = jq
        if "cluster" in func_param_names:
            func_params["cluster"] = cluster

        return args.func(**func_params)


if __name__ == "__main__":
    main(sys.argv[1:])
