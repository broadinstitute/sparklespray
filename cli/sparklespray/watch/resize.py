from .runner_types import PeriodicTask, NextPoll, ClusterStateQuery
from .shared import (
    _count_incomplete_tasks,
    _count_preempt_attempt,
    _count_active_nodes,
)
from ..cluster_service import Cluster
from .runner import StopPolling
import math
from typing import Protocol


class HasAddNodes(Protocol):
    def add_nodes(self, count: int, max_retry_count: int, preemptible: bool) -> str:
        ...


class ResizeCluster(PeriodicTask):
    # adjust cluster size
    # Given a (target size, a restart-preempt budget)
    # decide whether to add more add_node operations
    def __init__(
        self,
        cluster: HasAddNodes,
        target_node_count: int,
        max_preemptable_attempts: int,
        seconds_between_modifications: int = 60,
    ) -> None:
        self.target_node_count = target_node_count
        self.max_preemptable_attempts = max_preemptable_attempts
        self.seconds_between_modifications = seconds_between_modifications
        self.cluster = cluster
        self.preemptable_created = 0
        self.non_preemptable_created = 0

    def poll(self, state: ClusterStateQuery):
        modified = False

        tasks = state.get_tasks()

        # cap our target by the number of tasks which have not finished
        target_node_count = min(self.target_node_count, _count_incomplete_tasks(tasks))

        # Let's give ourselves a budget of 20% more then the target node count in case there's some spurious failures
        max_non_preemptable_attempts = int(math.ceil(target_node_count * 1.20))

        requested_nodes = _count_active_nodes(state.get_nodes())
        additional_nodes = max(0, target_node_count - requested_nodes)

        if additional_nodes > 0:
            # figure out how many nodes can be pre-emptable by looking at max_preemptable_attempts. The
            # rest will be non-preemptable
            additional_preemptable_nodes = min(
                [
                    additional_nodes,
                    self.max_preemptable_attempts - self.preemptable_created,
                ]
            )
            additional_non_preemptable_nodes = (
                additional_nodes - additional_preemptable_nodes
            )

            if additional_preemptable_nodes > 0:
                self.cluster.add_nodes(additional_preemptable_nodes, 3, True)
                self.preemptable_created += additional_preemptable_nodes

            if additional_non_preemptable_nodes > 0:
                assert (
                    self.non_preemptable_created <= max_non_preemptable_attempts
                ), f"Sparkles would like to start {additional_non_preemptable_nodes} additional nodes, but we've already created {self.non_preemptable_created} non-preemptable nodes (max attempts allowed currently {max_non_preemptable_attempts}). This suggests we might be recreating nodes infinitely. Aborting"

                self.cluster.add_nodes(additional_non_preemptable_nodes, 3, False)
                self.non_preemptable_created += additional_non_preemptable_nodes

            modified = True

        if modified:
            self.last_modification = state.get_time()

        return NextPoll(self.seconds_between_modifications)
