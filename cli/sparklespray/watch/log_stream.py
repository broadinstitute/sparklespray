import time
import sys
import logging
import contextlib
import json
from ..logclient import LogMonitor, CommunicationError
from typing import Callable, Optional
from ..cluster_service import ClusterState
from ..txtui import print_log_content
import datetime
import subprocess
import os
from ..task_store import STATUS_COMPLETE
import collections
from ..config import Config
import tempfile
import json
from ..task_store import Task, STATUS_FAILED
from ..cluster_service import NodeReq
from .shared import _summarize_task_statuses
from typing import Dict
from collections import defaultdict
from ..node_req_store import NODE_REQ_CLASS_NORMAL, NODE_REQ_CLASS_PREEMPTIVE
from ..resize_cluster import ResizeCluster, GetPreempted
from ..io_helper import IO
from ..job_queue import JobQueue
from ..cluster_service import Cluster
from .. import txtui
from ..job_store import Job

from ..log import log
from ..txtui import user_print
from ..startup_failure_tracker import StartupFailureTracker
from ..task_store import _is_terminal_status, Task
from ..node_req_store import REQUESTED_NODE_STATES, NodeReq
from typing import List
from .runner import PeriodicTask, NextPoll, StopPolling
from .shared import _count_incomplete_tasks, _exception_guard

class StreamLogs:
    # Stream output from one of the running processes.
    def __init__(self, cluster):
        self.log_monitor = None
        self.cluster = cluster

    def start_logging(self, state):
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
                    self.cluster.client, task.monitor_address, task.task_id
                )
                print_log_content(
                    None,
                    "[starting tail of log {}]".format(log_monitor.task_id),
                    from_sparkles=True,
                )

    def do_next_read(self):
        with _exception_guard(lambda: "polling log file threw exception"):
            try:
                self.log_monitor.poll()
            except CommunicationError as ex:
                log.warning(
                    "Got error polling log. shutting down log watch: {}".format(
                        ex
                    )
                )
                self.log_monitor.close()
                self.log_monitor = None

    def do_last_read(self):
        print_log_content(
            None,
            "[{} is no longer running, tail of log stopping]".format(
                self.log_monitor.task_id
            ),
            from_sparkles=True,
        )

        self.flush_stdout_from_complete_task(
                self.log_monitor.task_id, self.log_monitor.offset
            )
        self.log_monitor = None


    def flush_stdout_from_complete_task(self,task_id, offset):
        task = self.jq.task_storage.get_task(task_id)
        spec = json.loads(self.io.get_as_str(task.args))

        attempts = 0
        while True:
            rest_of_stdout = self.io.get_as_str(spec["stdout_url"], start=offset, must=False)
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


    def poll(self, state):
        if self.log_monitor is None:
            self.start_logging()
        else:
            if state.is_task_running(self.log_monitor.task_id):
                self.do_next_read()
            else:
                self.do_last_read()
        return NextPoll(1)

    def finish(self):
        if self.log_monitor is not None:
            self.do_last_read()

