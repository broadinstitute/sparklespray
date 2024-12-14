from dataclasses import dataclass
import functools
from typing import List
import time
from typing import Union


class ClusterStateQuery:
    def __init__(self, now: float, _get_tasks, _get_nodes):
        self.now = now
        self._get_tasks = _get_tasks
        self._get_nodes = _get_nodes

    def get_time(self):
        return self.now

    def get_tasks(self):
        return self._get_tasks()

    def get_nodes(self):
        return self._get_nodes()


@dataclass
class NextPoll:
    "Return from PeriodicTask.poll() to indicate when this task should be run again"
    delay: float


class StopPolling:
    "Return from PeriodicTask.poll to indicate that all tasks should stop"


class PeriodicTask:
    "This is an abstract class for tasks which should do some work (via calling poll()), pause for some duration and then poll() will be called again."

    def poll(self, state: ClusterStateQuery) -> Union[None, NextPoll, StopPolling]:
        "Function to be implemented with behavior for this task. Task should return None, if this task should stop, or NextPoll(delay) if it should be called again after `delay` seconds."
        return None

    def finish(self, state: ClusterStateQuery):
        "Function called after job is done and no more calls to poll() will be made"

    def cleanup(self):
        "Release any resources after all tasks are done. (Different then finish in that it is called both on successful completions and when exceptions are raised)"

@functools.total_ordering
class ScheduledTask:
    def __init__(self, timestamp, task):
        self.timestamp = timestamp
        self.task = task

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    def __eq__(self, other):
        return self.timestamp == other.timestamp


class RateLimitedCall:
    def __init__(self, callback, min_delay):
        self.callback = callback
        self.prev_value = None
        self.prev_call_timestamp = None
        self.min_delay = min_delay

    def __call__(self):
        now = time.time()
        if (self.prev_call_timestamp is None) or (
            (now - self.prev_call_timestamp) > self.min_delay
        ):
            self.prev_call_timestamp = now
            # print("calling", self.callback)
            self.prev_value = self.callback()
        return self.prev_value
