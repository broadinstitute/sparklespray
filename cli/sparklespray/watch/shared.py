import contextlib
from ..task_store import Task, STATUS_CLAIMED, STATUS_FAILED, STATUS_COMPLETE
from ..cluster_service import NodeReq
from ..node_req_store import NODE_REQ_CLASS_PREEMPTIVE

from ..log import log
from ..task_store import _is_terminal_status, Task
from ..node_req_store import REQUESTED_NODE_STATES, NodeReq
from typing import List


class TooManyNodeFailures(Exception):
    pass


class RetryError(Exception):
    pass


@contextlib.contextmanager
def _exception_guard(deferred_msg, reset=None):
    try:
        yield
    except OSError as ex:
        # consider these as non-fatal
        msg = deferred_msg()
        log.exception(msg)
        log.warning("Ignoring exception and continuing...")
        if reset is not None:
            reset()
    except RetryError as ex:
        msg = deferred_msg()
        log.exception(msg)
        log.warning("Ignoring exception and continuing...")
        if reset is not None:
            reset()


def _is_task_running(task_id: str, tasks: List[Task]):
    matching_tasks = [task for task in tasks if task.task_id == task_id]
    if len(matching_tasks) == 0:
        return False
    assert len(matching_tasks) == 1
    matching_task = matching_tasks[0]
    return matching_task.status == STATUS_CLAIMED


def _only_running_tasks(tasks: List[Task]):
    return [task for task in tasks if task.status == STATUS_CLAIMED]


def _only_failed_tasks(tasks: List[Task]):
    return [
        task
        for task in tasks
        if (task.status == STATUS_COMPLETE and task.exit_code != 0)
    ]


def _only_successful_tasks(tasks: List[Task]):
    return [
        task
        for task in tasks
        if (task.status == STATUS_COMPLETE and task.exit_code == 0)
    ]


def _only_completed_tasks(tasks: List[Task]):
    return [task for task in tasks if task.status == STATUS_COMPLETE]


def _count_incomplete_tasks(tasks: List[Task]):
    return sum([1 for task in tasks if not _is_terminal_status(task.status)])


def _count_requested_nodes(node_reqs: List[NodeReq]):
    return sum([1 for o in node_reqs if o.status in REQUESTED_NODE_STATES])


def _count_preempt_attempt(node_reqs: List[NodeReq]) -> int:
    return sum([1 for o in node_reqs if o.node_class == NODE_REQ_CLASS_PREEMPTIVE])
