from google.cloud import datastore
from dataclasses import dataclass
from typing import Optional

CLUSTER_COLLECTION = "Cluster"


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


class ClusterStore:
    def __init__(self, client: datastore.Client) -> None:
        self.client = client

    def get(self, cluster_id: str) -> Optional[ClusterConfig]:
        entity_key = self.client.key(CLUSTER_COLLECTION, cluster_id)
        entity = self.client.get(entity_key)
        if entity is None:
            return None
        return entity_to_cluster_config(entity)

    def set(self, config: ClusterConfig) -> None:
        entity = cluster_config_to_entity(self.client, config)
        self.client.put(entity)
