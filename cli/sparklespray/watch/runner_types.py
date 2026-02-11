from dataclasses import dataclass
import functools
from typing import Dict, List, TYPE_CHECKING, Union
import time

if TYPE_CHECKING:
    from ..task_store import Task, TaskStore


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


class IncrementalTaskFetcher:
    """Fetches only changed tasks and merges with cached state.

    On first call, fetches all tasks for the job. On subsequent calls,
    only fetches tasks that have been updated since the last fetch
    (using the last_updated field) and merges them into the cache.

    Because Datastore composite indexes are eventually consistent, we use
    a padding interval when querying to avoid missing recently updated tasks.
    This means we may fetch some duplicates, but since we update a dict keyed
    by task_id, duplicates are harmless.

    As an additional safety net, we periodically do a full fetch of all tasks
    to ensure we never miss an update even if eventual consistency delays
    exceed the padding interval.
    """

    # Padding to account for Datastore eventual consistency on composite indexes.
    # 10 minutes should be sufficient based on observed propagation delays.
    INDEX_CONSISTENCY_PADDING = 10 * 60  # seconds

    # Maximum time between full fetches as a safety net.
    # Even if incremental fetches miss something, a full fetch will catch it.
    MAX_TIME_UNTIL_FULL_REFRESH = 30 * 60  # seconds

    def __init__(self, task_store: "TaskStore", job_id: str, min_delay: float = 1.0):
        self.task_store = task_store
        self.job_id = job_id
        self.min_delay = min_delay
        self.cached_tasks: Dict[str, "Task"] = {}
        self.last_updated_watermark: float = 0.0
        self.prev_call_timestamp: float = 0.0
        self.last_full_fetch_timestamp: float = 0.0

    def __call__(self) -> List["Task"]:
        now = time.time()
        if (now - self.prev_call_timestamp) < self.min_delay:
            return list(self.cached_tasks.values())

        self.prev_call_timestamp = now

        # Determine if we need a full fetch (first call or safety net refresh)
        needs_full_fetch = (
            not self.cached_tasks
            or (now - self.last_full_fetch_timestamp) > self.MAX_TIME_UNTIL_FULL_REFRESH
        )

        if needs_full_fetch:
            # Full fetch: get all tasks
            tasks = self.task_store.get_tasks(job_id=self.job_id)
            self.cached_tasks = {t.task_id: t for t in tasks}
            self._update_watermark(tasks)
            self.last_full_fetch_timestamp = now
        else:
            # Incremental fetch: only changed tasks.
            # Subtract padding to handle eventual consistency - we may get
            # duplicates but won't miss recently updated tasks.
            query_since = self.last_updated_watermark - self.INDEX_CONSISTENCY_PADDING
            changed = self.task_store.get_tasks_updated_since(self.job_id, query_since)
            for task in changed:
                self.cached_tasks[task.task_id] = task
            self._update_watermark(changed)

        return list(self.cached_tasks.values())

    def _update_watermark(self, tasks: List["Task"]) -> None:
        """Update the watermark to the max last_updated from the given tasks."""
        for task in tasks:
            if task.last_updated is not None:
                if task.last_updated > self.last_updated_watermark:
                    self.last_updated_watermark = task.last_updated
