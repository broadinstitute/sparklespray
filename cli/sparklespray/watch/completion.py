from .runner_types import PeriodicTask, NextPoll, StopPolling
from .shared import _count_incomplete_tasks, _count_active_nodes
import time

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

        if now - self.last_active_node_time > 10:
            raise Exception("No remaining nodes running, but tasks are not complete. Are nodes not restarting?")

        return self.next_poll_delay()
