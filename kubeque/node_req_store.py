from google.cloud import datastore
import attr
from typing import List

NODE_REQ_SUBMITTED = "submitted"
NODE_REQ_RUNNING = "running"
NODE_REQ_COMPLETE = "complete"

REQUESTED_NODE_STATES = set([NODE_REQ_SUBMITTED, NODE_REQ_RUNNING])

NODE_REQ_CLASS_PREEMPTIVE = "preemptable"
NODE_REQ_CLASS_NORMAL = "normal"


@attr.s
class NodeReq(object):
    operation_id = attr.ib()
    job_id = attr.ib()
    status = attr.ib()
    node_class = attr.ib()
    sequence = attr.ib()

def node_req_to_entity(client :datastore.Client, o : NodeReq) -> datastore.Entity:
    assert o.operation_id is not None
    entity_key = client.key("NodeReq", o.operation_id)
    entity = datastore.Entity(key=entity_key)
    entity['job_id'] = o.job_id
    entity['status'] = o.status
    entity['node_class'] = o.node_class
    entity['sequence'] = o.sequence
    return entity

def entity_to_node_req(entity : datastore.Entity) -> NodeReq:
    return NodeReq(operation_id=entity.key.name,
        job_id = entity['job_id'],
        status=entity['status'],
        node_class = entity['node_class'],
                   sequence=entity['sequence'])

class AddNodeReqStore:
    def __init__(self, client : datastore.Client) -> None:
        self.client = client

    def add_node_req(self, req : NodeReq):
        self.client.put(node_req_to_entity(self.client, req))

    def get_node_reqs(self, job_id : str, status : str = None) -> List[NodeReq]:
        query = self.client.query(kind="NodeReq")
        query.add_filter("job_id", "=", job_id)
        if status is not None:
            query.add_filter("status", "=", status)
        results = []
        for entity in query.fetch():
            node_req = entity_to_node_req(entity)
            results.append(node_req)
        return results

    def delete(self, operation_id : str, batch=None) -> None:
        key = self.client.key("NodeReq", operation_id)

        if batch is None:
            self.client.delete(key)
        else:
            batch.delete(key)

                # def get_pending_node_req_count(self, job_id):
    #     return len(self.get_node_reqs(job_id, status=NODE_REQ_SUBMITTED))
    #
    # def update_node_reqs(self, job_id, cluster):
    #     # only need to worry about things that are submitted and have not yet been fulfilled
    #     node_reqs = self.get_node_reqs(job_id, status=NODE_REQ_SUBMITTED)
    #     for node_req in node_reqs:
    #         new_status = cluster.get_node_req_status(node_req.operation_id)
    #         if new_status != node_req.status:
    #             log.info("Changing status of node request %s from %s to %s", node_req.operation_id, node_req.status, new_status)
    #             node_req.status = new_status
    #             self.client.put(node_req_to_entity(self.client, node_req))
