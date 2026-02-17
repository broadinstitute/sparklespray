from dataclasses import dataclass
import functools
from queue import Queue, Empty
from typing import Dict, List, TYPE_CHECKING, Union, Optional
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

    def on_pubsub_notify(self, state: ClusterStateQuery):
        "Called when we get a pub/sub response which indicates something that we care about may have changed."

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
    checks a queue of task IDs that have changed (populated by pub/sub
    listeners) and fetches those specific tasks by ID.

    As a safety net, periodically does a full fetch of all tasks to ensure
    we never miss an update even if a pub/sub message is lost.
    """

    # Maximum time between full fetches as a safety net.
    # Even if pub/sub misses something, a full fetch will catch it.
    MAX_TIME_UNTIL_FULL_REFRESH = 30 * 60  # seconds

    def __init__(
        self,
        task_store: "TaskStore",
        job_id: str,
        changed_task_queue: Optional[Queue] = None,
        min_delay: float = 1.0,
    ):
        self.task_store = task_store
        self.job_id = job_id
        self.changed_task_queue = changed_task_queue
        self.min_delay = min_delay
        self.cached_tasks: Dict[str, "Task"] = {}
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
            self.last_full_fetch_timestamp = now
            # Clear any pending items in the queue since we just did a full fetch
            self._drain_queue()
        else:
            # Incremental fetch: get task IDs from the queue and fetch them by ID
            changed_task_ids = self._drain_queue()
            if changed_task_ids:
                for task_id in changed_task_ids:
                    task = self.task_store.get_task(task_id)
                    if task is not None:
                        self.cached_tasks[task_id] = task

        return list(self.cached_tasks.values())

    def _drain_queue(self) -> List[str]:
        """Drain all task IDs from the changed_task_queue.

        Returns a list of unique task IDs that were in the queue.
        """
        if self.changed_task_queue is None:
            return []

        task_ids = set()
        while True:
            try:
                task_id = self.changed_task_queue.get_nowait()
                task_ids.add(task_id)
            except Empty:
                break
        return list(task_ids)
