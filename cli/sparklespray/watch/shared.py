import contextlib
from ..task_store import Task
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


def _count_incomplete_tasks(tasks: List[Task]):
    return sum([1 for task in tasks if not _is_terminal_status(task.status)])


def _count_requested_nodes(node_reqs: List[NodeReq]):
    return sum([1 for o in node_reqs if o.status in REQUESTED_NODE_STATES])


def _count_preempt_attempt(node_reqs: List[NodeReq]) -> int:
    return sum([1 for o in node_reqs if o.node_class == NODE_REQ_CLASS_PREEMPTIVE])
