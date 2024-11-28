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
from .shared import _count_incomplete_tasks, _exception_guard, _count_preempt_attempt, _count_requested_nodes

class ResizeCluster(PeriodicTask):
    # adjust cluster size
    # Given a (target size, a restart-preempt budget, current number of outstanding operations, current number of pending tasks)
    # decide whether to add more add_node operations or remove add_node operations.
    def __init__(
        self,
        target_node_count: int,
        max_preemptable_attempts: int,
        cluster_mod,
        seconds_between_modifications: int = 60,
    ) -> None:
        self.target_node_count = target_node_count
        self.max_preemptable_attempts = max_preemptable_attempts
        self.seconds_between_modifications = seconds_between_modifications
        self.cluster_mod = cluster_mod

    def poll(self, state: ClusterStateQuery):
        modified = False

        # cap our target by the number of tasks which have not finished
        target_node_count = min(
            self.target_node_count, _count_incomplete_tasks(state.get_tasks())
        )

        requested_nodes = _count_requested_nodes(state.get_nodes()) # state.get_requested_node_count()
        print("target_node_count > requested_nodes", target_node_count , requested_nodes)
        if target_node_count > requested_nodes:
            # Is our target higher than what we have now? Then add that many nodes
            remaining_preempt_attempts = (
                self.max_preemptable_attempts - _count_preempt_attempt(state.get_nodes())
            )
            nodes_to_add = target_node_count - requested_nodes
            if nodes_to_add > 0:
                print(
                    "Currently targeting having {} nodes running, but we've only requested {} nodes. Adding {}, remaining_preempt_attempts={}".format(
                        target_node_count,
                        requested_nodes,
                        nodes_to_add,
                        remaining_preempt_attempts,
                    )
                )
                for _ in range(nodes_to_add):
                    preemptable = remaining_preempt_attempts > 0
                    if preemptable:
                        remaining_preempt_attempts -= 1
                    self.cluster_mod.add_node(preemptable=preemptable)
                    modified = True

        elif target_node_count < requested_nodes:
            # We have requested too many. Start cancelling
            needs_cancel = requested_nodes - target_node_count
            if needs_cancel > 0:
                self.cluster_mod.cancel_nodes(state, needs_cancel)
                modified = True

        if modified:
            self.last_modification = state.get_time()

        return NextPoll(self.seconds_between_modifications)
