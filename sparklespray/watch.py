import os
import subprocess
from .txtui import user_print
from .log import log
from . import txtui
from .cluster_service import Cluster
from .job_queue import JobQueue
from .io import IO
from .resize_cluster import ResizeCluster, GetPreempted
import time
import sys
import logging
import contextlib
import json
from .logclient import LogMonitor, CommunicationError
from typing import Callable
from .cluster_service import ClusterState
from .txtui import print_log_content
# from google.gax.errors import RetryError


class RetryError(Exception):
    pass


def add_watch_cmd(subparser):
    parser = subparser.add_parser("watch", help="Monitor the job")
    parser.set_defaults(func=watch_cmd)
    parser.add_argument("jobid")
    parser.add_argument("--nodes", "-n", type=int,
                        help="The target number of workers")
    parser.add_argument(
        "--loglive", help="tail the first running task we can find", action="store_true")


def watch_cmd(jq: JobQueue, io: IO, cluster: Cluster, args):
    from .main import _resolve_jobid
    jobid = _resolve_jobid(jq, args.jobid)
    watch(io, jq, jobid, cluster, target_nodes=args.nodes, loglive=args.loglive)


@contextlib.contextmanager
def _exception_guard(deferred_msg, reset=None):
    try:
        yield
    except OSError as ex:
        # consider these as non-fatal
        msg = deferred_msg()
        log.exception(msg)
        log.warning("Ignoring exception and continuing...")
        if reset is not None:
            reset()
    except RetryError as ex:
        msg = deferred_msg()
        log.exception(msg)
        log.warning("Ignoring exception and continuing...")
        if reset is not None:
            reset()


def print_error_lines(lines):
    from termcolor import colored, cprint
    for line in lines:
        print(colored(line, "red"))


def dump_stdout_if_single_task(jq, io, jobid):
    tasks = jq.get_tasks(jobid)
    if len(tasks) != 1:
        return
    task = list(tasks)[0]
    spec = json.loads(io.get_as_str(task.args))
    stdout_lines = io.get_as_str(spec['stdout_url']).split("\n")
    stdout_lines = stdout_lines[-100:]
    print_error_lines(stdout_lines)


def _watch(job_id: str, state: ClusterState, initial_poll_delay: float,
           max_poll_delay: float, loglive: bool, cluster: Cluster,
           poll_cluster: Callable[[], None],
           flush_remaining: Callable[[str, int], None]):

    log_monitor = None
    prev_summary = None

    while True:
        with _exception_guard(lambda: "summarizing status of job {} threw exception".format(job_id)):
            state.update()

        if state.is_done():
            break

        summary = state.get_summary()
        if prev_summary != summary:
            user_print(summary)
            prev_summary = summary

            poll_delay = initial_poll_delay
        else:
            # if the status hasn't changed since last time then slow down polling
            poll_delay = min(poll_delay * 1.5, max_poll_delay)

        if state.failed_node_req_count > 3:
            log.error("%d node requests failed. Aborting...",
                      state.failed_node_req_count)
            break

        poll_cluster()

        if log_monitor is None:
            if loglive:
                running = list(state.get_running_tasks())
                if len(running) > 0:
                    task = running[0]
                    if task.monitor_address is not None:
                        log.info("Obtained monitor address for task %s: %s",
                                 task.task_id, task.monitor_address)
                        log_monitor = LogMonitor(
                            cluster.client, task.monitor_address, task.task_id)
                        print_log_content(None,
                                          "[starting tail of log {}]".format(log_monitor.task_id), from_sparkles=True)
        else:
            if state.is_task_running(log_monitor.task_id):
                with _exception_guard(lambda: "polling log file threw exception"):
                    try:
                        log_monitor.poll()
                    except CommunicationError as err:
                        log.info("Got error from log_monitor.poll() -> %s", err)
                        log_monitor = None
                        print_log_content(None,
                                          "[Got communication error getting logs, stopping watch of logs from {}]".format(log_monitor.task_id), from_sparkles=True)
            else:
                print_log_content(None,
                                  "[{} is no longer running, tail of log stopping]".format(log_monitor.task_id), from_sparkles=True)
                flush_remaining(log_monitor.task_id, log_monitor.offset)
                log_monitor = None

        time.sleep(poll_delay)

    # waiting is done, try to flush any remaining logs
    if log_monitor is not None:
        flush_remaining(log_monitor.task_id, log_monitor.offset)
    elif len(state.get_tasks()) == 1:
        task = state.get_tasks()[0]
        flush_remaining(task.task_id, 0)


def start_docker_process(job_spec_str: str, consume_exe: str, work_dir: str):
    job_spec = json.loads(job_spec_str)
    actions = job_spec['pipeline']['actions']
    # action 0 is curl downloading consume
    consume_action = actions[1]
    docker_image = consume_action["imageUri"]
    docker_command = consume_action["commands"]
    docker_portmapping = consume_action["portMappings"]

    assert docker_command[0] == "/mnt/consume"

    # TODO: Add options for google creds
    docker_options = [
        "-v", os.path.expanduser("~/.config/gcloud") + ":/google-creds",
        "-e", "GOOGLE_APPLICATION_CREDENTIALS=/google-creds/application_default_credentials.json",
        "-v", f"{consume_exe}:/mnt/consume", "-v", f"{work_dir}:/mnt"]

    for src_port, dst_port in docker_portmapping.items():
        docker_options.extend(["-p", f"{src_port}:{dst_port}"])

    cmd = ["docker", "run"] + \
        docker_options + [docker_image] + docker_command

    with open("sparkles-docker.log", "a") as docker_log:
        docker_log.write("Executing: {}".format(cmd))
        proc = subprocess.Popen(
            cmd, stderr=subprocess.STDOUT, stdout=docker_log)

    return proc


class DockerFailedException(Exception):
    pass


def local_watch(io: IO, jq: JobQueue, job_id: str, consume_exe: str, work_dir: str, cluster: Cluster, initial_poll_delay=1.0, max_poll_delay=30.0):
    loglive = True

    job = cluster.job_store.get_job(job_id)

    state = cluster.get_state(job_id)
    state.update()

    proc = start_docker_process(job.kube_job_spec, consume_exe, work_dir)

    def poll_cluster():
        if proc.poll() is not None:
            raise DockerFailedException("Docker process prematurely died")

    try:
        _watch(job_id, state, initial_poll_delay,
               max_poll_delay, loglive, cluster, poll_cluster,
               make_log_flush_remainder(io, jq))

        failures = state.get_failed_task_count()
        successes = state.get_successful_task_count()
        txtui.user_print(
            f"Job finished. {successes} tasks completed successfully, {failures} tasks failed")

        successful_execution = failures == 0

    except KeyboardInterrupt:
        print("Interrupted -- Aborting...")
        successful_execution = False

    if proc.poll() is not None:
        log.warning(f"Sending terminate signal to {proc}")
        proc.terminate()

    return successful_execution


def make_log_flush_remainder(io, jq):
    def log_flush_remainder(task_id, offset):
        task = jq.task_storage.get_task(task_id)
        remainder = io.get_as_str_starting_at(task.log_url, offset)
        if remainder != "":
            txtui.print_log_content(None, remainder)
    return log_flush_remainder


def watch(io: IO, jq: JobQueue, job_id: str, cluster: Cluster, target_nodes=None, initial_poll_delay=1.0, max_poll_delay=30.0, loglive=None):
    job = jq.get_job(job_id)

    if target_nodes is None:
        target_nodes = job.target_node_count

    resize_cluster = ResizeCluster(target_nodes,
                                   max_preemptable_attempts=job.max_preemptable_attempts)
    get_preempted = GetPreempted()

    state = cluster.get_state(job_id)
    state.update()

    task_count = len(state.get_tasks())

    if loglive is None and task_count == 1:
        loglive = True
        log.info("Only one task, so tailing log")

    try:
        def poll_cluster():
            with _exception_guard(lambda: "restarting preempted nodes threw exception"):
                task_ids = get_preempted(state)
                if len(task_ids) > 0:
                    log.warning(
                        "Resetting tasks which appear to have been preempted: %s", ", ".join(task_ids))
                    for task_id in task_ids:
                        jq.reset_task(task_id)

            with _exception_guard(lambda: "rescaling cluster threw exception"):
                resize_cluster(state, cluster.get_cluster_mod(job_id))

        _watch(job_id, state, initial_poll_delay,
               max_poll_delay, loglive, cluster, poll_cluster,
               make_log_flush_remainder(io, jq))

        failures = state.get_failed_task_count()
        successes = state.get_successful_task_count()
        txtui.user_print(
            f"Job finished. {successes} tasks completed successfully, {failures} tasks failed")

        if failures > 0 and len(job.tasks) == 1:
            log.warning(
                "Job failed, and there was only one task, so dumping the tail of the output from that task")
            dump_stdout_if_single_task(jq, io, job_id)

        return failures == 0
    except KeyboardInterrupt:
        print("Interrupted -- Exiting, but your job will continue to run unaffected.")
        sys.exit(1)
