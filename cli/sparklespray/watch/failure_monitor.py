import collections

from ..log import log
from .runner_types import PeriodicTask
from dataclasses import dataclass


@dataclass
class NodesCompleted:
    timestamp: float
    processed_count: int  # number of nodes which completed _and_ had processed some tasks
    zero_processed_count: int  # number of nodes which completed _and_ had not processed any tasks



class StartupFailureMonitor(PeriodicTask):
    "Are nodes failing to start?"

    def __init__(self, completed_node_names):
        self.previously_completed = set(completed_node_names)
        self.prev_completed_node_names_count = None
        self.event_log = []

    def update(self, timestamp, tasks, completed_node_names):
        # if len(self.event_log) == 0:
        #     self.event_log.append(StartEvent(timestamp))

        # fast path: if no names have been added, just return because nothing to do
        if self.prev_completed_node_names_count == len(completed_node_names):
            return

        # record the count for next time
        self.prev_completed_node_names_count = len(completed_node_names)

        # find the number of tasks per node
        task_count_by_node_name = collections.defaultdict(lambda: 0)
        for task in tasks:
            if task.owner is None:
                continue
            node_name = task.owner.split("/")[-1]
            task_count_by_node_name[node_name] += 1

        # find nodes which have newly completed
        newly_completed = []
        for node_name in completed_node_names:
            if node_name not in self.previously_completed:
                newly_completed.append(node_name)

        # for each completion, check to see how many tasks were run
        nodes_finished_without_running_anything = 0
        nodes_finished_processing_at_least_one = 0
        for node_name in newly_completed:
            tasks_started = task_count_by_node_name[node_name]
            log.info(
                "Node %s completed after executing %d tasks", node_name, tasks_started
            )
            if tasks_started == 0:
                nodes_finished_without_running_anything += 1
            else:
                nodes_finished_processing_at_least_one += 1

        if (
            nodes_finished_without_running_anything
            + nodes_finished_processing_at_least_one
        ) > 0:
            self.event_log.append(
                NodesCompleted(
                    timestamp,
                    nodes_finished_processing_at_least_one,
                    nodes_finished_without_running_anything,
                )
            )

        # remember which nodes we've checked
        self.previously_completed.update(newly_completed)

        # keep track of the last 15 minutes of history
        oldest_timestamp = timestamp - (15 * 60)
        self.event_log = [x for x in self.event_log if x.timestamp > oldest_timestamp]

    def is_too_many_failures(self, timestamp):
        good = 0
        bad = 0
        max_bad = 10
        grace_period_elapsed = False

        for event in self.event_log:
            good += event.processed_count
            bad += event.zero_processed_count

            if good == 0 and bad >= max_bad:
                # this means no nodes were started that successfully started processing data
                # but at least 10 nodes started which failed without any processing

                # now, it's possible that nodes were started but the queue was drained before they
                # came online. If that's the case, give ourselves a grace period of 30 seconds.
                # if the queue is empty then we won't be starting any more nodes, and we'll realize
                # we're done before that period elapses.
                if (timestamp - event.timestamp) > 30:
                    grace_period_elapsed = True

        return grace_period_elapsed and (good == 0 and bad >= max_bad)
