from google.cloud import datastore

import random, string

from typing import Tuple

class KeyStore:
    def __init__(self, client : datastore.Client) -> None:
        self.client = client

    def get_cert_and_key(self) -> Tuple[bytes, bytes]:
        entity_key = self.client.key("ClusterKeys", "sparklespray")
        entity = self.client.get(entity_key)
        if entity is None:
            return None, None
        return entity['cert'], entity['private_key']

    def set_cert_and_key(self, cert : bytes, key : bytes):
        entity_key = self.client.key("ClusterKeys", "sparklespray")
        entity = datastore.Entity(key=entity_key, exclude_from_indexes=('cert', 'private_key', 'shared_secret'))
        entity['cert'] = cert
        entity['private_key'] = key
        entity['shared_secret'] = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(20))
        self.client.put(entity)
