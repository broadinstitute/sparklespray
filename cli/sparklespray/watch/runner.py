import time
from ..cluster_service import Cluster

from typing import List
from .runner_types import PeriodicTask, NextPoll, StopPolling

from .runner_types import (
    PeriodicTask,
    ScheduledTask,
    RateLimitedCall,
    ClusterStateQuery,
)
import heapq


def run_tasks(
    job_id: str, cluster_id: str, tasks: List[PeriodicTask], cluster: Cluster
):
    now = time.time()
    timeline = []

    # schedule all tasks to run immediately
    for task in tasks:
        heapq.heappush(timeline, ScheduledTask(now, task))

    get_tasks = RateLimitedCall(lambda: cluster.task_store.get_tasks(job_id), 1)
    get_nodes = RateLimitedCall(
        lambda: cluster.get_node_reqs(), 1
    )

    while True:
        now = time.time()

        # get the next task whose time is soonest
        scheduled_task = heapq.heappop(timeline)

        # if it's not yet time for this task, sleep until then
        if scheduled_task.timestamp > now:
            time.sleep(scheduled_task.timestamp - now)

        # now, run the task and find out if we need to run it again later
        # print("calling", scheduled_task.task)
        next_action = scheduled_task.task.poll(
            ClusterStateQuery(now, get_tasks, get_nodes)
        )
        # print("next action", next_action)

        if next_action is not None:
            if isinstance(next_action, StopPolling):
                break
            else:
                assert isinstance(next_action, NextPoll)
                # add it to the schedule at the appropriate time
                heapq.heappush(
                    timeline,
                    ScheduledTask(now + next_action.delay, scheduled_task.task),
                )

    # now that we're done polling, call finish() on all tasks
    final_state = ClusterStateQuery(time.time(), get_tasks, get_nodes)
    for task in tasks:
        task.finish(final_state)