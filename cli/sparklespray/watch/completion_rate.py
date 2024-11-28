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

class EstimateRateOfCompletion:
    def __init__(self):
        # how many completions do we need to see before we can estimate the rate of completion?
        self.min_completion_timestamps = 10
        # how many completions do we want to use to estimate our rate at most
        self.max_completion_timestamps = 50
        # a log of the timestamp of each completion in order it occurred
        self.completion_timestamps = []
        self.prev_completion_count = None
        self.completion_timestamps = []

    def poll(self, state):
        completion_count = state.get_failed_task_count() + state.get_successful_task_count()
        if prev_completion_count is None:
            prev_completion_count = completion_count
        else:
            now = state.get_time()
            for _ in range(completion_count - prev_completion_count):
                self.completion_timestamps.append(now)
            # if we have too many samples, drop the oldest
            while len(self.completion_timestamps) > self.max_completion_timestamps:
                del self.completion_timestamps[0]
            prev_completion_count = completion_count

        self.completion_rate = None
        if len(self.completion_timestamps) > self.min_completion_timestamps:
            completion_window = state.get_time() - self.completion_timestamps[0]
            if completion_window > 0:
                # only compute the completion rate if the time over which we measured is non-zero
                # otherwise we get a division by zero
                self.completion_rate = len(self.completion_timestamps)/completion_window
