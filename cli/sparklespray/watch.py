import time
import sys
import logging
import contextlib
import json
from .logclient import LogMonitor, CommunicationError
from typing import Callable, Optional
from .cluster_service import ClusterState
from .txtui import print_log_content
import datetime
import subprocess
import os
from .task_store import STATUS_COMPLETE
import collections
from .config import Config

# from google.gax.errors import RetryError


class RetryError(Exception):
    pass


class TooManyNodeFailures(Exception):
    pass


from .resize_cluster import ResizeCluster, GetPreempted
from .io_helper import IO
from .job_queue import JobQueue
from .cluster_service import Cluster
from . import txtui
from .job_store import Job

from .log import log
from .txtui import user_print


def add_watch_cmd(subparser):
    parser = subparser.add_parser("watch", help="Monitor the job")
    parser.set_defaults(func=watch_cmd)
    parser.set_defaults(loglive=True)
    parser.add_argument("jobid")
    parser.add_argument("--nodes", "-n", type=int, help="The target number of workers")
    parser.add_argument(
        "--verify",
        action="store_true",
        help="If set, before watching will confirm all finished jobs wrote their output. Any jobs whose output is missing will be reset",
    )
    parser.add_argument(
        "--loglive",
        help="Stream output (on by default)",
        action="store_true",
        dest="loglive",
    )
    parser.add_argument(
        "--no-loglive",
        help="tail the first running task we can find",
        action="store_false",
        dest="loglive",
    )


def watch_cmd(jq: JobQueue, io: IO, cluster: Cluster, config: Config, args):
    from .main import _resolve_jobid

    jobid = _resolve_jobid(jq, args.jobid)
    if args.verify:
        check_completion(jq, io, jobid)

    max_preemptable_attempts_scale = config.max_preemptable_attempts_scale

    watch(
        io,
        jq,
        jobid,
        cluster,
        target_nodes=args.nodes,
        loglive=args.loglive,
        max_preemptable_attempts_scale=max_preemptable_attempts_scale,
    )


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


def flush_stdout_from_complete_task(jq, io, task_id, offset):
    task = jq.task_storage.get_task(task_id)
    spec = json.loads(io.get_as_str(task.args))

    attempts = 0
    while True:
        rest_of_stdout = io.get_as_str(spec["stdout_url"], start=offset, must=False)
        if rest_of_stdout is not None:
            break
        log.warning(
            "Log %s doesn't appear to exist yet. Will try again...", spec["stdout_url"]
        )
        time.sleep(5)
        attempts += 1
        if attempts > 10:
            log.warning("Log file never did appear. Giving up trying to tail log")
            return

    print_log_content(datetime.datetime.now(), rest_of_stdout)


from .startup_failure_tracker import StartupFailureTracker


def _watch(
    job_id: str,
    state: ClusterState,
    initial_poll_delay: float,
    max_poll_delay: float,
    loglive: bool,
    cluster: Cluster,
    poll_cluster: Callable[[], None],
    flush_stdout_from_complete_task: Optional[Callable[[str, int], None]] = None,
):
    log_monitor = None
    prev_summary = None

    startup_failure_tracker = StartupFailureTracker(state.get_completed_node_names())

    # how many completions do we need to see before we can estimate the rate of completion?
    min_completion_timestamps = 10
    # how many completions do we want to use to estimate our rate at most
    max_completion_timestamps = 50
    # a log of the timestamp of each completion in order it occurred
    completion_timestamps = []
    prev_completion_count = None

    poll_delay = initial_poll_delay
    while True:
        with _exception_guard(
            lambda: "summarizing status of job {} threw exception".format(job_id)
        ):
            state.update()

        if state.is_done():
            break

        completion_count = state.get_failed_task_count() + state.get_successful_task_count()
        if prev_completion_count is None:
            prev_completion_count = completion_count
        else:
            now = time.time()
            for _ in range(completion_count - prev_completion_count):
                completion_timestamps.append(now)
            # if we have too many samples, drop the oldest
            while len(completion_timestamps) > max_completion_timestamps:
                del completion_timestamps[0]
            prev_completion_count = completion_count

        completion_rate = None
        if len(completion_timestamps) > min_completion_timestamps:
            completion_window = time.time() - completion_timestamps[0]
            if completion_window > 0:
                # only compute the completion rate if the time over which we measured is non-zero
                # otherwise we get a division by zero
                completion_rate = len(completion_timestamps)/completion_window

        summary = state.get_summary(completion_rate=completion_rate)
        if prev_summary != summary:
            user_print(summary)
            prev_summary = summary

            poll_delay = initial_poll_delay
        else:
            # if the status hasn't changed since last time then slow down polling
            poll_delay = min(poll_delay * 1.5, max_poll_delay)

        startup_failure_tracker.update(
            time.time(), state.tasks, state.get_completed_node_names()
        )
        if startup_failure_tracker.is_too_many_failures(time.time()):
            raise TooManyNodeFailures()

        poll_cluster()

        # check each of the failed nodes to see if we have no tasks

        if log_monitor is None:
            if loglive:
                running = list(state.get_running_tasks())
                if len(running) > 0:
                    task = running[0]
                    if task.monitor_address is not None:
                        log.info(
                            "Obtained monitor address for task %s: %s",
                            task.task_id,
                            task.monitor_address,
                        )
                        log_monitor = LogMonitor(
                            cluster.client, task.monitor_address, task.task_id
                        )
                        print_log_content(
                            None,
                            "[starting tail of log {}]".format(log_monitor.task_id),
                            from_sparkles=True,
                        )
        else:
            if state.is_task_running(log_monitor.task_id):
                with _exception_guard(lambda: "polling log file threw exception"):
                    try:
                        log_monitor.poll()
                    except CommunicationError as ex:
                        log.info(
                            "Got error polling log. shutting down log watch: {}".format(
                                ex
                            )
                        )
                        log_monitor.close()
                        log_monitor = None

            else:
                print_log_content(
                    None,
                    "[{} is no longer running, tail of log stopping]".format(
                        log_monitor.task_id
                    ),
                    from_sparkles=True,
                )
                if flush_stdout_from_complete_task:
                    flush_stdout_from_complete_task(
                        log_monitor.task_id, log_monitor.offset
                    )
                log_monitor = None

        time.sleep(poll_delay)


# TODO: Finish implementing. Use to start docker instance and watch progress
import tempfile
import json


def start_docker_process(job_spec_str: str, consume_exe: str, work_dir: str):
    job_spec = json.loads(job_spec_str)

    print(json.dumps(job_spec, indent=2))
    # the goal of this is to emulate how the google pipelines API will interpret this
    # job.

    # start by setting up the volumes
    volume_by_name = {}
    volumes = job_spec["pipeline"]["resources"]["virtualMachine"]["volumes"]
    tmpdirs = []
    for volume in volumes:
        if "persistentDisk" in volume:
            tmpdir = tempfile.TemporaryDirectory(dir=".", prefix="pdtmp-")
            tmpdirs.append(tmpdir)
            path = os.path.abspath(tmpdir.name)
            print(f"Created temp folder {path} for volume")
        elif "existingDisk" in volume:
            path = volume["existingDisk"]["disk"]
            path = os.path.abspath(path)
            assert os.path.exists(path)
        else:
            raise Exception("Unknown type of volume")
        print(f"Using {path} for the volume {volume['volume']}")
        volume_by_name[volume["volume"]] = path

    proc = None
    actions = job_spec["pipeline"]["actions"]
    for index, action in enumerate(actions):
        docker_command = [
            "docker",
            "run",
            "--platform",
            "linux/amd64",
            "-v",
            os.path.expanduser("~/.config/gcloud") + ":/google-creds",
            "-e",
            "GOOGLE_APPLICATION_CREDENTIALS=/google-creds/application_default_credentials.json",
        ]

        for mount in action["mounts"]:
            docker_command.extend(
                ["-v", f"{volume_by_name[mount['disk']]}:{mount['path']}"]
            )

        portMappings = action.get("portMappings")
        if portMappings:
            for src_port, dst_port in portMappings.items():
                docker_command.extend(["-p", f"{src_port}:{dst_port}"])

        docker_command.append(action["imageUri"])
        docker_command.extend(action["commands"])

        with open("sparkles-docker.log", "a") as docker_log:
            # docker_log = sys.stdout
            docker_log.write(f"Executing: {docker_command}\n")
            proc = subprocess.Popen(
                docker_command, stderr=subprocess.STDOUT, stdout=docker_log
            )

            if index < len(actions):
                # if this isn't the last job (the consume action) run now and wait for completion
                retcode = proc.wait()
                if retcode != 0:
                    raise Exception(
                        f"docker command {docker_command} failed: retcode = {retcode}"
                    )
                continue

    assert proc is not None
    return proc


class DockerFailedException(Exception):
    pass


def local_watch(
    job_id: str,
    consume_exe: str,
    work_dir: str,
    cluster: Cluster,
    initial_poll_delay=1.0,
    max_poll_delay=30.0,
):
    loglive = True

    job = cluster.job_store.get_job(job_id)
    assert isinstance(job, Job)

    state = cluster.get_state(job_id)
    state.update()

    assert isinstance(job.kube_job_spec, str)
    proc = start_docker_process(job.kube_job_spec, consume_exe, work_dir)

    def poll_cluster():
        if proc.poll() is not None:
            raise DockerFailedException("Docker process prematurely died")

    try:
        _watch(
            job_id,
            state,
            initial_poll_delay,
            max_poll_delay,
            loglive,
            cluster,
            poll_cluster,
        )

        failures = state.get_failed_task_count()
        successes = state.get_successful_task_count()
        txtui.user_print(
            f"Job finished. {successes} tasks completed successfully, {failures} tasks failed"
        )

        successful_execution = failures == 0

    except KeyboardInterrupt:
        print("Interrupted -- Aborting...")
        successful_execution = False

    if proc.poll() is not None:
        log.warning(f"Sending terminate signal to {proc}")
        proc.terminate()

    return successful_execution


def watch(
    io: IO,
    jq: JobQueue,
    job_id: str,
    cluster: Cluster,
    target_nodes=None,
    max_preemptable_attempts_scale=None,
    initial_poll_delay=1.0,
    max_poll_delay=30.0,
    loglive=None,
):
    job = jq.get_job(job_id)
    assert job is not None
    flush_stdout_calls = [0]

    def flush_stdout(task_id, offset):
        flush_stdout_from_complete_task(jq, io, task_id, offset)
        flush_stdout_calls[0] += 1

    if target_nodes is None:
        target_nodes = job.target_node_count

    if max_preemptable_attempts_scale is None:
        max_preemptable_attempts = job.max_preemptable_attempts
    else:
        max_preemptable_attempts = target_nodes * max_preemptable_attempts_scale

    log.info(
        "targeting %s nodes. First %s nodes will be preemptive (from job: target_node_count=%s, max_preemptable_attempts=%s)",
        target_nodes,
        max_preemptable_attempts,
        job.target_node_count,
        job.max_preemptable_attempts,
    )
    resize_cluster = ResizeCluster(
        target_nodes, max_preemptable_attempts=max_preemptable_attempts
    )
    get_preempted = GetPreempted()

    state = cluster.get_state(job_id)
    state.update()

    check_attempts = 0
    while len(state.get_tasks()) == 0:
        log.warning(
            "Did not see any tasks for %s, sleeping and will check again...", job_id
        )
        time.sleep(5)
        state.update()
        check_attempts += 1
        if check_attempts > 20:
            raise Exception("Even after waiting a while no tasks ever appeared")

    if loglive is None:
        loglive = True

    try:

        def poll_cluster():
            with _exception_guard(lambda: "restarting preempted nodes threw exception"):
                task_ids = get_preempted(state)
                if len(task_ids) > 0:
                    log.warning(
                        "Resetting tasks which appear to have been preempted: %s",
                        ", ".join(task_ids),
                    )
                    for task_id in task_ids:
                        jq.reset_task(task_id)

            with _exception_guard(lambda: "rescaling cluster threw exception"):
                resize_cluster(state, cluster.get_cluster_mod(job_id))

        _watch(
            job_id,
            state,
            initial_poll_delay,
            max_poll_delay,
            loglive,
            cluster,
            poll_cluster,
            flush_stdout_from_complete_task=flush_stdout,
        )

        failures = state.get_failed_task_count()
        successes = state.get_successful_task_count()
        txtui.user_print(
            f"Job finished. {successes} tasks completed successfully, {failures} tasks failed"
        )

        if failures > 0:
            log.warning(
                "At least one task failed. Dumping stdout from one of the failures."
            )
            failed_tasks = state.get_failed_tasks()
            flush_stdout(failed_tasks[0].task_id, 0)

        elif (
            flush_stdout_calls[0] == 0 and loglive
        ):  # if we want the logs and yet we've never written out a single log, pick one at random and write it out
            log.info("Dumping arbitrary successful task stdout")
            successful_tasks = state.get_successful_tasks()
            flush_stdout(successful_tasks[0].task_id, 0)

        return failures == 0
    except KeyboardInterrupt:
        print("Interrupted -- Exiting, but your job will continue to run unaffected.")
        return 20


def check_completion(jq: JobQueue, io: IO, job_id: str):
    successful_count = 0
    completed_count = 0

    tasks = jq.task_storage.get_tasks(job_id)
    for task in tasks:
        if task.status == STATUS_COMPLETE:
            if (completed_count % 100) == 0:
                print(
                    "Verified {} out of {} completed tasks successfully wrote output".format(
                        successful_count, len(tasks)
                    )
                )
            completed_count += 1
            if io.exists(task.command_result_url):
                # print("task {} completed successfully".format(task.task_id))
                successful_count += 1
            else:
                print(
                    "task {} missing {}, resetting".format(
                        task.task_id, task.command_result_url
                    )
                )
                # look up owner -> operation id -> dump log
                jq.reset_task(task.task_id)
