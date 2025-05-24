from .runner_types import PeriodicTask, NextPoll, StopPolling
from .shared import _count_incomplete_tasks, _count_active_nodes


class CompletionMonitor(PeriodicTask):
    def __init__(self):
        self.cur_delay = self.initial_delay = 0.5
        self.max_delay = 20
        self.no_active_nodes_count = 0

    def next_poll_delay(self):
        self.cur_delay = min(self.max_delay, self.cur_delay * 2)
        return NextPoll(self.cur_delay)

    def poll(self, state):
        # Is everything done? If so, terminate loop.
        tasks = state.get_tasks()
        nodes = state.get_nodes()
        if _count_active_nodes(nodes) == 0:
            self.no_active_nodes_count += 1
            if self.no_active_nodes_count > 10:
                return StopPolling()
        if _count_incomplete_tasks(tasks) == 0:
            return StopPolling()
        return self.next_poll_delay()
