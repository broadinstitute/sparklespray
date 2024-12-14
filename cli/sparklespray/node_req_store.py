from google.cloud import datastore
from .datastore_batch import ImmediateBatch, Batch, Batcher
from typing import List
from dataclasses import dataclass
import dataclasses
from typing import Optional
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


def node_req_to_entity(client: datastore.Client, o: NodeReq) -> datastore.Entity:
    assert o.operation_id is not None
    entity_key = client.key("NodeReq", o.operation_id)
    entity = datastore.Entity(key=entity_key)
    entity["cluster_id"] = o.cluster_id
    entity["job_id"] = o.job_id
    entity["status"] = o.status
    entity["node_class"] = o.node_class
    entity["sequence"] = o.sequence
    entity["instance_name"] = o.instance_name
    return entity




def entity_to_node_req(entity: datastore.Entity) -> NodeReq:
    # def get_str(prop: str ) -> Optional[str]:
    #     val = entity.get(prop)
    #     assert val is None or isinstance(val, str)
    #     return val
    assert entity.key is not None
    return NodeReq(
        operation_id=entity.key.name,
        cluster_id=entity["cluster_id"],
        status=entity["status"],
        node_class=entity["node_class"],
        sequence=entity["sequence"],
        job_id=entity["job_id"],
        instance_name=entity["instance_name"],
    )
