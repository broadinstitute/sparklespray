from .runner_types import PeriodicTask, NextPoll, ClusterStateQuery
from .shared import (
    _count_incomplete_tasks,
    _count_preempt_attempt,
    _count_active_nodes,
)
from ..cluster_service import Cluster
from .runner import StopPolling


class ResizeCluster(PeriodicTask):
    # adjust cluster size
    # Given a (target size, a restart-preempt budget, current number of outstanding operations, current number of pending tasks)
    # decide whether to add more add_node operations or remove add_node operations.
    def __init__(
        self,
        cluster: Cluster,
        target_node_count: int,
        max_preemptable_attempts: int,
        seconds_between_modifications: int = 60,
    ) -> None:
        self.target_node_count = target_node_count
        self.max_preemptable_attempts = max_preemptable_attempts
        self.seconds_between_modifications = seconds_between_modifications
        self.cluster = cluster

    def poll(self, state: ClusterStateQuery):
        modified = False

        # cap our target by the number of tasks which have not finished
        target_node_count = min(
            self.target_node_count, _count_incomplete_tasks(state.get_tasks())
        )

        requested_nodes = _count_active_nodes(state.get_nodes())
        additional_nodes = max(0, target_node_count - requested_nodes)

        if additional_nodes > 0:
            # we haven't requested anything, so request something now
            self.cluster.add_nodes(additional_nodes)
            modified = True
            return None
        # print("target_node_count > requested_nodes", target_node_count, requested_nodes)
        # if target_node_count > requested_nodes:
        #     # Is our target higher than what we have now? Then add that many nodes
        #     remaining_preempt_attempts = (
        #         self.max_preemptable_attempts
        #         - _count_preempt_attempt(state.get_nodes())
        #     )
        #     nodes_to_add = target_node_count - requested_nodes
        #     if nodes_to_add > 0:
        #         print(
        #             "Currently targeting having {} nodes running, but we've only requested {} nodes. Adding {}, remaining_preempt_attempts={}".format(
        #                 target_node_count,
        #                 requested_nodes,
        #                 nodes_to_add,
        #                 remaining_preempt_attempts,
        #             )
        #         )
        #         for _ in range(nodes_to_add):
        #             preemptable = remaining_preempt_attempts > 0
        #             if preemptable:
        #                 remaining_preempt_attempts -= 1
        #             self.cluster_mod.add_node(preemptable=preemptable)
        #         modified = True

        # elif target_node_count < requested_nodes:
        #     # We have requested too many. Start cancelling
        #     needs_cancel = requested_nodes - target_node_count
        #     if needs_cancel > 0:
        #         # FIXME: I'm pretty sure the next line doesn't work...
        #         self.cluster_mod.cancel_nodes(state, needs_cancel)
        #         modified = True

        if modified:
            self.last_modification = state.get_time()

        return NextPoll(self.seconds_between_modifications)
