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

from dataclasses import dataclass
class ClusterStateQuery:
    def __init__(self, now : float, _get_tasks, _get_nodes):
        self.now = now
        self._get_tasks = _get_tasks
        self._get_nodes = _get_nodes

    def get_time(self):
        return self.now

    def get_tasks(self):
        return self._get_tasks()

    def get_nodes(self):
        return self._get_nodes()

@dataclass
class NextPoll:
    "Return from PeriodicTask.poll() to indicate when this task should be run again"
    delay : float

class StopPolling:
    "Return from PeriodicTask.poll to indicate that all tasks should stop"

class PeriodicTask:
    "This is an abstract class for tasks which should do some work (via calling poll()), pause for some duration and then poll() will be called again."

    def poll(self, state : ClusterStateQuery):
        "Function to be implemented with behavior for this task. Task should return None, if this task should stop, or NextPoll(delay) if it should be called again after `delay` seconds."
        return None
    
    def finish(self, state : ClusterStateQuery):
        "Function called after job is done and no more calls to poll() will be made"

import functools
@functools.total_ordering
class ScheduledTask:
    def __init__(self, timestamp, task):
        self.timestamp = timestamp
        self.task = task
    
    def __lt__(self, other):
        return self.timestamp < other.timestamp
    def __eq__(self, other):
        return self.timestamp == other.timestamp

from typing import List
import heapq

class RateLimitedCall:
    def __init__(self, callback, min_delay):
        self.callback = callback
        self.prev_value = None
        self.prev_call_timestamp = None
        self.min_delay = min_delay

    def __call__(self):
        now = time.time()
        if (self.prev_call_timestamp is None) or ((now -self.prev_call_timestamp) > self.min_delay):
            self.prev_call_timestamp = now
            #print("calling", self.callback)
            self.prev_value = self.callback()
        return self.prev_value


from ..task_store import TaskStore
def run_tasks(job_id: str, cluster_id: str, tasks: List[PeriodicTask], cluster: Cluster):
    now = time.time()
    timeline = []

    # schedule all tasks to run immediately
    for task in tasks:
        heapq.heappush(timeline, ScheduledTask(now, task))

    get_tasks = RateLimitedCall(lambda: cluster.task_store.get_tasks(job_id), 1)
    get_nodes = RateLimitedCall(lambda: cluster.node_req_store.get_node_reqs(cluster_id), 1)

    while True:
        now = time.time()

        # get the next task whose time is soonest
        scheduled_task = heapq.heappop(timeline)
        
        # if it's not yet time for this task, sleep until then
        if scheduled_task.timestamp > now:
            time.sleep(scheduled_task.timestamp - now)

        # now, run the task and find out if we need to run it again later
        #print("calling", scheduled_task.task)
        next_action = scheduled_task.task.poll(ClusterStateQuery(now, get_tasks, get_nodes))
        #print("next action", next_action)

        if next_action is not None:
            if isinstance(next_action, StopPolling):
                break
            else:
                assert isinstance(next_action, NextPoll)
                # add it to the schedule at the appropriate time
                heapq.heappush(timeline, ScheduledTask(now+next_action.delay, scheduled_task.task))

    # now that we're done polling, call finish() on all tasks
    final_state = ClusterStateQuery(time.time(), get_tasks, get_nodes)
    for task in tasks:
        task.finish(final_state)
