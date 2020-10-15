from google.cloud import datastore
import attr
from .datastore_batch import ImmediateBatch, Batch
from typing import List

NODE_REQ_SUBMITTED = "submitted"
NODE_REQ_STAGING = "staging"
NODE_REQ_RUNNING = "running"
NODE_REQ_COMPLETE = "complete"
NODE_REQ_FAILED = "failed"

REQUESTED_NODE_STATES = set([NODE_REQ_SUBMITTED, NODE_REQ_RUNNING, NODE_REQ_STAGING])
FINAL_NODE_STATES = set([NODE_REQ_FAILED, NODE_REQ_COMPLETE])

NODE_REQ_CLASS_PREEMPTIVE = "preemptable"
NODE_REQ_CLASS_NORMAL = "normal"


@attr.s
class NodeReq(object):
    operation_id = attr.ib()
    cluster_id = attr.ib()
    status = attr.ib()
    node_class = attr.ib()
    sequence = attr.ib()
    job_id = attr.ib()
    instance_name = attr.ib(default=None)


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
    return NodeReq(
        operation_id=entity.key.name,
        cluster_id=entity["cluster_id"],
        status=entity["status"],
        node_class=entity["node_class"],
        sequence=entity["sequence"],
        job_id=entity.get("job_id"),
        instance_name=entity["instance_name"],
    )


class AddNodeReqStore:
    def __init__(self, client: datastore.Client) -> None:
        self.client = client
        self.immediate_batch = ImmediateBatch(self.client)

    def add_node_req(self, req: NodeReq):
        self.client.put(node_req_to_entity(self.client, req))

    def get_node_reqs(self, cluster_id: str, status: str = None) -> List[NodeReq]:
        query = self.client.query(kind="NodeReq")
        query.add_filter("cluster_id", "=", cluster_id)
        if status is not None:
            query.add_filter("status", "=", status)
        results = []
        for entity in query.fetch():
            node_req = entity_to_node_req(entity)
            results.append(node_req)
        return results

    def update_node_req_status(self, operation_id, status, instance_name):
        entity = self.client.get(self.client.key("NodeReq", operation_id))
        entity["status"] = status
        if instance_name is not None:
            entity["instance_name"] = instance_name
        self.client.put(entity)

    def cleanup_cluster(self, cluster_id: str, batch: Batch = None) -> None:
        if batch is None:
            batch = self.immediate_batch

        query = self.client.query(kind="NodeReq")
        query.add_filter("cluster_id", "=", cluster_id)
        query.add_filter("status", "=", NODE_REQ_COMPLETE)
        for entity in query.fetch():
            self.client.delete(entity.key)

        query = self.client.query(kind="NodeReq")
        query.add_filter("cluster_id", "=", cluster_id)
        query.add_filter("status", "=", NODE_REQ_FAILED)
        for entity in query.fetch():
            self.client.delete(entity.key)

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
