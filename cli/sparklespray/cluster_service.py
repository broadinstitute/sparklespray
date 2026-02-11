from .task_store import (
    STATUS_FAILED,
    STATUS_COMPLETE,
)
from .node_req_store import (
    NodeReq,
    NODE_REQ_SUBMITTED,
    NODE_REQ_CLASS_PREEMPTIVE,
    NODE_REQ_CLASS_NORMAL,
    NODE_REQ_COMPLETE,
    REQUESTED_NODE_STATES,
    NODE_REQ_FAILED,
    REQUESTED_NODE_STATES,
    FINAL_NODE_STATES,
)

# from .node_service import NodeService, MachineSpec
from .job_store import JobStore
from .task_store import TaskStore
from typing import List, Optional, Set
from google.cloud import datastore
import time
from .util import get_timestamp

from .log import log

from .batch_api import ClusterAPI, JobSpec
from dataclasses import dataclass
from typing import Protocol

CLUSTER_HEARTBEAT_COLLECTION = "ClusterHeartbeat"

# If a heartbeat is older than this, it's considered stale and can be taken over
SECONDS_UNTIL_STALE_HEARTBEAT = 60 * 10  # 10 minutes


@dataclass
class ClusterHeartbeat:
    """Records the last heartbeat from a watch process monitoring a cluster."""

    cluster_id: str
    watch_run_uuid: str
    timestamp: float


def heartbeat_to_entity(
    client: datastore.Client, o: ClusterHeartbeat
) -> datastore.Entity:
    entity_key = client.key(CLUSTER_HEARTBEAT_COLLECTION, o.cluster_id)
    entity = datastore.Entity(key=entity_key)
    entity["watch_run_uuid"] = o.watch_run_uuid
    entity["timestamp"] = o.timestamp
    return entity


def entity_to_heartbeat(entity: datastore.Entity) -> ClusterHeartbeat:
    assert entity.key is not None
    return ClusterHeartbeat(
        cluster_id=entity.key.name,
        watch_run_uuid=entity["watch_run_uuid"],
        timestamp=entity["timestamp"],
    )


class MinConfig(Protocol):
    project: str
    debug_log_prefix: str

    @property
    def location(self) -> str:
        ...


from .job_queue import JobQueue


def create_cluster(
    config: MinConfig, jq: JobQueue, datastore_client, cluster_api, job_id
):
    job = jq.get_job_must(job_id)

    return Cluster(
        config.project,
        config.location,
        job.cluster,
        job_id,
        jq.job_storage,
        jq.task_storage,
        datastore_client,
        cluster_api,
        config.debug_log_prefix,
    )


class Cluster:
    """
    Manages a compute cluster for executing distributed tasks.

    The Cluster class provides an interface for managing compute resources in Google Cloud,
    including provisioning nodes, tracking node requests, and monitoring task execution.
    It serves as the bridge between the job/task storage layer and the actual compute
    infrastructure.

    Attributes:
        project: Google Cloud project ID
        client: Datastore client for storage operations
        job_store: Storage for job metadata
        task_store: Storage for task metadata and status
        debug_log_prefix: Prefix for debug log files
        cluster_api: API for interacting with the batch service
        job_id: ID of the job associated with this cluster
        location: Google Cloud region where the cluster is deployed
        _cluster_id: Cached cluster ID (lazily loaded)
    """

    def __init__(
        self,
        project: str,
        location: str,
        cluster_id: str,
        job_id: str,
        job_store: JobStore,
        task_store: TaskStore,
        client: datastore.Client,
        cluster_api: ClusterAPI,
        debug_log_prefix: str,
    ) -> None:
        self.project = project
        self.client = client
        self.job_store = job_store
        self.task_store = task_store
        self.debug_log_prefix = debug_log_prefix
        self.cluster_api = cluster_api
        self.job_id = job_id
        self.location = location
        self._cluster_id = None

    @property
    def cluster_id(self):
        if self._cluster_id is None:
            job = self.job_store.get_job_must(self.job_id)
            self._cluster_id = job.cluster
        return self._cluster_id

    def get_node_reqs(self):
        return self.cluster_api.get_node_reqs(
            self.project, self.location, self.cluster_id
        )

    def add_nodes(
        self, count: int, max_retry_count: int, preemptible: bool
    ):  # job_id: str, preemptible: bool, debug_log_url: str):
        job = self.job_store.get_job_must(self.job_id)

        job_spec = JobSpec.model_validate_json(job.kube_job_spec)

        job_spec.preemptible = preemptible

        return self.cluster_api.create_job(
            self.project, self.location, job_spec, count, max_retry_count
        )

    def has_active_node_requests(self):
        node_reqs = self.cluster_api.get_node_reqs(
            self.project, self.location, self.cluster_id
        )
        for node_req in node_reqs:
            if node_req.status in REQUESTED_NODE_STATES:
                return True
        return False

    def stop_cluster(self):
        self.cluster_api.delete_node_reqs(self.project, self.location, self.cluster_id)

    def delete_complete_requests(self):
        self.cluster_api.delete_node_reqs(
            self.project, self.location, self.cluster_id, only_terminal_reqs=True
        )

    def is_owner_live(self, owner):
        return self.cluster_api.is_instance_running(owner)

    def heartbeat(
        self,
        watch_run_uuid: str,
        expected_uuid: Optional[str] = None,
    ) -> bool:
        """Record a heartbeat for this cluster from the current watch process.

        This stores a ClusterHeartbeat entity in Datastore with the cluster ID
        as the entity key. Uses a transaction to atomically check and update,
        preventing race conditions between multiple watch processes.

        Args:
            watch_run_uuid: UUID identifying this watch process execution
            expected_uuid: If provided, only update if the current heartbeat's
                watch_run_uuid matches this value (or if no heartbeat exists).
                If None, always update unconditionally.

        Returns:
            True if the heartbeat was successfully recorded, False if the
            expected_uuid check failed (another process owns the heartbeat).
        """
        key = self.client.key(CLUSTER_HEARTBEAT_COLLECTION, self.cluster_id)
        took_over_stale = False

        def update_in_transaction(transaction):
            nonlocal took_over_stale
            entity = transaction.get(key)
            now = time.time()

            # Check if we should proceed based on expected_uuid
            if expected_uuid is not None and entity is not None:
                current_uuid = entity.get("watch_run_uuid")
                if current_uuid != expected_uuid:
                    # Check if the heartbeat is stale
                    current_timestamp = entity.get("timestamp", 0)
                    if (now - current_timestamp) > SECONDS_UNTIL_STALE_HEARTBEAT:
                        # Heartbeat is stale, take over
                        took_over_stale = True
                    else:
                        # Heartbeat is fresh and belongs to another process
                        return False

            # Create or update the heartbeat
            new_heartbeat = ClusterHeartbeat(
                cluster_id=self.cluster_id,
                watch_run_uuid=watch_run_uuid,
                timestamp=now,
            )
            new_entity = heartbeat_to_entity(self.client, new_heartbeat)
            transaction.put(new_entity)
            return True

        with self.client.transaction() as transaction:
            result = update_in_transaction(transaction)

        if took_over_stale:
            log.warning(
                "An old job watcher crashed? Taking over this job as its new watcher."
            )

        return result

    def get_heartbeat(self) -> Optional[ClusterHeartbeat]:
        """Get the current heartbeat for this cluster, if any.

        Returns:
            ClusterHeartbeat if one exists, None otherwise
        """
        key = self.client.key(CLUSTER_HEARTBEAT_COLLECTION, self.cluster_id)
        entity = self.client.get(key)
        if entity is None:
            return None
        return entity_to_heartbeat(entity)

    def clear_heartbeat(self, watch_run_uuid: str) -> bool:
        """Clear the heartbeat for this cluster, but only if it belongs to us.

        Uses a transaction to atomically check the UUID and delete, preventing
        accidental deletion of another process's heartbeat.

        Args:
            watch_run_uuid: Only delete if the current heartbeat's UUID matches

        Returns:
            True if the heartbeat was deleted (or didn't exist),
            False if the heartbeat belongs to a different watch process.
        """
        key = self.client.key(CLUSTER_HEARTBEAT_COLLECTION, self.cluster_id)

        def delete_in_transaction(transaction):
            entity = transaction.get(key)

            if entity is None:
                # No heartbeat exists, nothing to clear
                return True

            current_uuid = entity.get("watch_run_uuid")
            if current_uuid != watch_run_uuid:
                # Heartbeat belongs to another process, don't delete
                return False

            transaction.delete(key)
            return True

        with self.client.transaction() as transaction:
            return delete_in_transaction(transaction)


class CachingCaller:
    def __init__(self, fn, expiry_time=5):
        self.prev = {}
        self.expiry_time = expiry_time
        self.fn = fn

    def __call__(self, *args):
        now = time.time()
        immutable_args = tuple(args)
        if immutable_args in self.prev:
            value, timestamp = self.prev[immutable_args]
            if timestamp + self.expiry_time < now:
                return value

        value = self.fn(*args)

        self.prev[immutable_args] = (value, now)

        return value
