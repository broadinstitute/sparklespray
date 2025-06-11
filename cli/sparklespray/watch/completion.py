from .runner_types import PeriodicTask, NextPoll, StopPolling
from .shared import _count_incomplete_tasks, _count_active_nodes
import time
from ..errors import UserError, NoWorkersRunning


class StablizedValue:
    """Represents a value which is updated repeated, but get() only returns if value if update has been called with the same value for more than `min_update_count` times and min_time_update of time has passed"""

    def __init__(self, min_update_count=2, min_time_elapsed=30):
        self.last_change = None
        self.last_update = None
        self.stable_updates = 0
        self.value = None
        self.min_update_count = min_update_count
        self.min_time_elapsed = min_time_elapsed

    def update(self, now, value):
        if self.value != value:
            self.value = value
            self.last_change = now
            self.stable_updates = 1
        else:
            self.stable_updates += 1
        self.last_update = now

    def get(self, default=None):
        if (
            (self.last_update is not None)
            and ((self.last_update - self.last_change) >= self.min_time_elapsed)
            and (self.min_update_count >= self.stable_updates)
        ):
            return self.value
        return default


class CompletionMonitor(PeriodicTask):
    def __init__(self):
        self.cur_delay = self.initial_delay = 0.5
        self.max_delay = 20
        self.last_active_node_time = None

    def next_poll_delay(self):
        self.cur_delay = min(self.max_delay, self.cur_delay * 2)
        return NextPoll(self.cur_delay)

    def poll(self, state):
        # Is everything done? If so, terminate loop.
        now = state.get_time()
        if self.last_active_node_time is None:
            self.last_active_node_time = now

        tasks = state.get_tasks()
        nodes = state.get_nodes()
        if _count_active_nodes(nodes) > 0:
            self.last_active_node_time = now

        if _count_incomplete_tasks(tasks) == 0:
            return StopPolling()

        if now - self.last_active_node_time > 120: # if 2 minutes with no nodes running has gone by we have a problem
            raise NoWorkersRunning()

        return self.next_poll_delay()
