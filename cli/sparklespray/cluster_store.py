from google.cloud import datastore
from google.cloud import pubsub_v1
from google.api_core import exceptions
from dataclasses import dataclass
from typing import Optional
import logging

log = logging.getLogger(__name__)

CLUSTER_COLLECTION = "SparklesV6Cluster"


@dataclass
class ClusterConfig:
    cluster_id: str
    incoming_topic: str
    response_topic: str


def cluster_config_to_entity(
    client: datastore.Client, config: ClusterConfig
) -> datastore.Entity:
    entity_key = client.key(CLUSTER_COLLECTION, config.cluster_id)
    entity = datastore.Entity(key=entity_key)
    entity["incoming_topic"] = config.incoming_topic
    entity["response_topic"] = config.response_topic
    return entity


def entity_to_cluster_config(entity: datastore.Entity) -> ClusterConfig:
    return ClusterConfig(
        cluster_id=entity.key.name,
        incoming_topic=entity.get("incoming_topic", ""),
        response_topic=entity.get("response_topic", ""),
    )


def _create_topic_if_not_exists(
    publisher: pubsub_v1.PublisherClient, topic_path: str
) -> None:
    """Create a pub/sub topic if it doesn't already exist."""
    try:
        publisher.create_topic(name=topic_path)
        log.info(f"Created pub/sub topic: {topic_path}")
    except exceptions.AlreadyExists:
        log.debug(f"Pub/sub topic already exists: {topic_path}")


def _delete_topic_if_exists(
    publisher: pubsub_v1.PublisherClient, topic_path: str
) -> None:
    """Delete a pub/sub topic if it exists."""
    try:
        publisher.delete_topic(topic=topic_path)
        log.info(f"Deleted pub/sub topic: {topic_path}")
    except exceptions.NotFound:
        log.debug(f"Pub/sub topic not found (already deleted?): {topic_path}")


class ClusterStore:
    def __init__(
        self, client: datastore.Client, project_id: str, pubsub_topics: str = "shared"
    ) -> None:
        self.client = client
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()
        self.pubsub_topics = pubsub_topics

    def _make_topic_path(self, topic_name: str) -> str:
        return self.publisher.topic_path(self.project_id, topic_name)

    def get(self, cluster_id: str) -> Optional[ClusterConfig]:
        entity_key = self.client.key(CLUSTER_COLLECTION, cluster_id)
        entity = self.client.get(entity_key)
        if entity is None:
            return None
        return entity_to_cluster_config(entity)

    def set(self, config: ClusterConfig) -> None:
        entity = cluster_config_to_entity(self.client, config)
        self.client.put(entity)

    def create_cluster(self, cluster_id: str) -> ClusterConfig:
        """Create a new cluster with its pub/sub topics.

        Creates the incoming and response pub/sub topics, then stores
        the cluster config in Datastore.

        If pubsub_topics is "shared", uses fixed topic names "sparkles-inbound"
        and "sparkles-outbound" that are shared across all clusters.
        If pubsub_topics is "per-cluster", generates unique topic names based
        on cluster_id.
        """
        # Generate topic names based on mode
        if self.pubsub_topics == "shared":
            incoming_topic_name = "sparkles-inbound"
            response_topic_name = "sparkles-outbound"
        else:
            # per-cluster mode
            incoming_topic_name = f"sparkles-{cluster_id}-incoming"
            response_topic_name = f"sparkles-{cluster_id}-response"

        incoming_topic_path = self._make_topic_path(incoming_topic_name)
        response_topic_path = self._make_topic_path(response_topic_name)

        # Create the pub/sub topics
        _create_topic_if_not_exists(self.publisher, incoming_topic_path)
        _create_topic_if_not_exists(self.publisher, response_topic_path)

        # Create and store the cluster config
        config = ClusterConfig(
            cluster_id=cluster_id,
            incoming_topic=incoming_topic_name,
            response_topic=response_topic_name,
        )
        self.set(config)

        log.info(
            f"Created cluster {cluster_id} with topics: {incoming_topic_name}, {response_topic_name}"
        )
        return config

    def delete_cluster(self, cluster_id: str) -> None:
        """Delete a cluster and its pub/sub topics.

        Deletes the pub/sub topics (unless using shared topics) and removes
        the cluster config from Datastore.
        """
        # Get the cluster config to find the topic names
        config = self.get(cluster_id)
        if config is None:
            log.debug(f"Cluster {cluster_id} not found, nothing to delete")
            return

        # Only delete the pub/sub topics if using per-cluster mode
        # Shared topics are kept permanently
        if self.pubsub_topics != "shared":
            if config.incoming_topic:
                incoming_topic_path = self._make_topic_path(config.incoming_topic)
                _delete_topic_if_exists(self.publisher, incoming_topic_path)

            if config.response_topic:
                response_topic_path = self._make_topic_path(config.response_topic)
                _delete_topic_if_exists(self.publisher, response_topic_path)

        # Delete the cluster config from Datastore
        entity_key = self.client.key(CLUSTER_COLLECTION, cluster_id)
        self.client.delete(entity_key)

        log.info(f"Deleted cluster {cluster_id}")
