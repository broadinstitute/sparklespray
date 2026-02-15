from dataclasses import dataclass
from typing import Optional

NODE_REQ_SUBMITTED = "submitted"
NODE_REQ_STAGING = "staging"
NODE_REQ_RUNNING = "running"
NODE_REQ_COMPLETE = "complete"
NODE_REQ_FAILED = "failed"

REQUESTED_NODE_STATES = set([NODE_REQ_SUBMITTED, NODE_REQ_RUNNING, NODE_REQ_STAGING])
FINAL_NODE_STATES = set([NODE_REQ_FAILED, NODE_REQ_COMPLETE])

NODE_REQ_CLASS_PREEMPTIVE = "preemptable"
NODE_REQ_CLASS_NORMAL = "normal"


def is_terminal_state(status: str):
    return status in FINAL_NODE_STATES


@dataclass
class NodeReq:
    operation_id: str
    cluster_id: str
    status: str
    node_class: str
    sequence: str
    job_id: str
    instance_name: Optional[str]
