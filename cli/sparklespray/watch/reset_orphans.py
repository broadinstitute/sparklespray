from .runner_types import PeriodicTask, NextPoll, ClusterStateQuery
from ..cluster_service import Cluster
from ..job_queue import JobQueue
from .shared import (
    _count_running_nodes,
)
from ..reset import identify_orphans
from typing import Any, Dict
from dataclasses import dataclass
from time import time
from ..task_store import STATUS_CLAIMED

@dataclass
class StablizedSetElement():
    update_count: int 
    first_update: float

class StablizedSet():
    "a set of values which have been consistently present in each call to `update()`. Call `values()` to get those elements which have been present a sufficiently long time"

    def __init__(self, min_update_count=2, min_time_elapsed=30):
        self.min_update_count = min_update_count
        self.min_time_elapsed = min_time_elapsed
        self.latest_values = []
        self.per_value : Dict[Any, StablizedSetElement] = {}

    def update(self, values):
        now = time()

        # add/update all values in this batch
        for value in values:
            existing = self.per_value.get(value)
            if existing is None:
                self.per_value[value] = StablizedSetElement(update_count=1, first_update=now)
            else:
                existing.update_count += 1

        # remove anything which isn't included in this batch            
        missing = set(self.per_value.keys()).difference(values)
        for missing_value in missing:
            del self.per_value[missing_value]

        # figure out which values satify the min critera
        self.latest_values = [ value for value, stats in self.per_value.items() if stats.update_count >= self.min_update_count and stats.first_update - now >= self.min_time_elapsed]
        
    def values(self):
        return self.latest_values


class ResetOrphans(PeriodicTask):
    "Monitors for tasks which are reported as claimed by a node which is not running"
    def __init__(
        self,
        jq : JobQueue,
        cluster: Cluster,
        seconds_between_modifications: int = 60,
    ) -> None:
        self.cluster = cluster
        self.preemptable_created = 0
        self.jq = jq
        self. seconds_between_modifications = seconds_between_modifications
        self.likely_orphans = StablizedSet()

    def poll(self, state: ClusterStateQuery):
        tasks = state.get_tasks()

        claimed_tasks = [x for x in tasks if x.status == STATUS_CLAIMED]
        running_nodes = _count_running_nodes(state.get_nodes())

        # I'm trying to avoid making lots of unnecessary API calls because 
        # checking whether each host is running has to be done individually. 
        #
        # Most of the time we things are fine, so let's use the heuristic
        # that we'll only do this check when there are too few running nodes
        # to account for all the claimed nodes. This might happen transiently
        # as these statuses are not in sync, but this is just a heuristic
        # used to decide whether we want to bother checking each individual host.
        if True: # running_nodes < len( claimed_tasks ):
            orphan_summary = identify_orphans(self.cluster, claimed_tasks)

            # use likely_orphans as a low-pass filter. That is to say, only consider 
            # something an orphan if we've repeatedly identified it as an orphan. This 
            # is to help ignore transient situtations like a a task was claimed when we 
            # polled the tasks, but completed before we polled the workers.
            if len(orphan_summary.orphaned)> 0:
                print(f"Identified {len(orphan_summary.orphaned)} likely orphaned tasks")
            self.likely_orphans.update( [x.task_id for x in orphan_summary.orphaned] )
            confidently_orphaned_task_ids = self.likely_orphans.values()

            if len(confidently_orphaned_task_ids) > 0:
                print(f"Reseting {len(confidently_orphaned_task_ids)} orphaned tasks")

                for task_id in confidently_orphaned_task_ids:
                    self.jq.reset_task(task_id)

        return NextPoll(self.seconds_between_modifications)
