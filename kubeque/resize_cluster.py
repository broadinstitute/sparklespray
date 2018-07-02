import time
from typing import List
from .node_req_store import REQUESTED_NODE_STATES, NODE_REQ_CLASS_PREEMPTIVE, NODE_REQ_SUBMITTED
from .task_store import INCOMPLETE_TASK_STATES
from .job_queue import JobQueue
from .node_service import NodeService
from .task_store import Task
from .node_req_store import NodeReq
from .cluster_service import Cluster, ClusterState


class ClusterMod:
    def __init__(self, job_id : str, cluster : Cluster, debug_log_prefix : str) -> None:
        self.job_id = job_id
        self.cluster = cluster
        self.debug_log_prefix = debug_log_prefix

    def add_node(self, preemptable : bool) -> None:
        self.cluster.add_node(self.job_id, preemptable, self.debug_log_prefix)

    def cancel_nodes(self, state : ClusterState, count : int) -> None:
        pending_node_reqs = [x for x in state.node_reqs if x.status == NODE_REQ_SUBMITTED ]
        pending_node_reqs.sort(key=lambda x: x.sequence)
        pending_node_reqs = list(reversed(pending_node_reqs))
        if count < len(pending_node_reqs):
            pending_node_reqs = pending_node_reqs[:count]
        for x in pending_node_reqs:
            self.cluster.cancel_add_node(x.operation_id)

class GetPreempted:
    def __init__(self, get_time=time.time, min_bad_time=30):
        self.first_time_task_reported_bad = {}
        self.get_time = get_time
        self.min_bad_time = min_bad_time

    def __call__(self, state : ClusterState) -> List[str]:
        tasks_to_reset = []
        task_ids = state.get_running_tasks_with_invalid_owner()
        self.first_time_task_reported_bad
        next_times = {}
        for task_id in task_ids:
            first_time = self.first_time_task_reported_bad.get(task_id, self.get_time())
            if self.get_time() - first_time > self.min_bad_time:
                tasks_to_reset.append(task_id)
            else:
                next_times[task_id] = first_time

        self.first_time_task_reported_bad = next_times

        return tasks_to_reset

class ResizeCluster:
    # adjust cluster size
    # Given a (target size, a restart-preempt budget, current number of outstanding operations, current number of pending tasks)
    # decide whether to add more add_node operations or remove add_node operations.
    def __init__(self, target_node_count : int, max_preemptable_attempts : int, seconds_between_modifications : int =60, get_time=time.time) -> None:
        self.target_node_count = target_node_count
        self.max_preemptable_attempts = max_preemptable_attempts

        self.last_modification = None
        self.seconds_between_modifications = seconds_between_modifications
        self.get_time = get_time

    def __call__(self, state : ClusterState, cluster_mod : ClusterMod) -> None:
        if self.last_modification is not None and (self.get_time() - self.last_modification) < self.seconds_between_modifications:
            return

        modified = False
        # cap our target by the number of tasks which have not finished
        target_node_count = min(self.target_node_count, state.get_incomplete_task_count())

        requested_nodes = state.get_requested_node_count()
        if target_node_count > requested_nodes:
            # Is our target higher than what we have now? Then add that many nodes
            remaining_preempt_attempts = self.max_preemptable_attempts - state.get_preempt_attempt_count()
            for i in range(target_node_count - requested_nodes):
                preemptable = remaining_preempt_attempts > 0
                if preemptable:
                    remaining_preempt_attempts -= 1

                cluster_mod.add_node(preemptable=preemptable)
                modified = True

        elif target_node_count < requested_nodes:
            # We have requested too many. Start cancelling
            needs_cancel = (requested_nodes - target_node_count)
            if needs_cancel > 0 :
                cluster_mod.cancel_nodes(state, needs_cancel)
                modified = True

        if modified:
            self.last_modification = self.get_time()


