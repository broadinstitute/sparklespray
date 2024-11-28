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
from .runner import PeriodicTask, NextPoll, StopPolling, ClusterStateQuery
from .shared import _count_incomplete_tasks, _exception_guard

class PrintStatus(PeriodicTask):
    def __init__(self, initial_poll_delay, max_poll_delay):
        self.poll_delay = self.initial_poll_delay = initial_poll_delay
        self.max_poll_delay = max_poll_delay
        self.prev_summary = None

    def poll(self, state : ClusterStateQuery):
        summary = format_summary(state.get_tasks(), state.get_nodes())
        if self.prev_summary != summary:
            user_print(summary)
            self.prev_summary = summary

            self.poll_delay = self.initial_poll_delay
        else:
            # if the status hasn't changed since last time then slow down polling
            self.poll_delay = min(self.poll_delay * 1.5, self.max_poll_delay)
        return NextPoll(self.poll_delay)


def format_summary(tasks: List[Task], node_reqs: List[NodeReq]):
    # compute status of tasks
    by_status: Dict[str, int] = defaultdict(lambda: 0)
    for t in tasks:
        if t.status == STATUS_COMPLETE:
            label = "{}(code={})".format(t.status, t.exit_code)
        elif t.status == STATUS_FAILED:
            label = "{}({})".format(t.status, t.failure_reason)
        else:
            label = t.status
        by_status[label] += 1
    statuses = sorted(by_status.keys())
    task_status = ", ".join(
        ["{} ({})".format(status, by_status[status]) for status in statuses]
    )

    # compute status of workers
    by_status = defaultdict(lambda: 0)
    to_desc = {
        NODE_REQ_CLASS_NORMAL: "non-preempt",
        NODE_REQ_CLASS_PREEMPTIVE: "preemptible",
    }
    for r in node_reqs:
        label = "{}(type={})".format(r.status, to_desc[r.node_class])
        by_status[label] += 1
    statuses = sorted(by_status.keys())
    node_status = ", ".join(
        [f"{status} ({by_status[status]})" for status in statuses]
    )

    msg = f"tasks: {task_status}, worker nodes: {node_status}"
    # if completion_rate:
    #     incomplete_task_count = self.get_incomplete_task_count()
    #     remaining_estimate_in_seconds = float(incomplete_task_count)/completion_rate
    #     msg += f", eta: {(remaining_estimate_in_seconds/60):.1f} minutes to complete remaining {incomplete_task_count} tasks"
    return msg