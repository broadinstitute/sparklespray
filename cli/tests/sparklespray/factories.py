import json
import os
from unittest.mock import MagicMock
from typing import Dict, List, Optional, Any

from sparklespray.io_helper import IO


class DatastoreClientSimulator:
    """
    A simulator for the Google Cloud Datastore client that stores entities in memory.
    """
    def __init__(self):
        self.entities: Dict[str, Dict[str, Any]] = {}
        self.keys = []
        self.next_id = 1

    def key(self, kind, id=None):
        """Create a key for the given kind and ID."""
        if id is None:
            id = self.next_id
            self.next_id += 1
        key = MagicMock()
        key.kind = kind
        key.id = id
        key.name = str(id)
        key.__str__ = lambda self: f"{kind}({id})"
        self.keys.append(key)
        return key

    def get(self, key):
        """Get an entity by key."""
        key_str = f"{key.kind}:{key.name}"
        if key_str in self.entities:
            entity = MagicMock()
            for k, v in self.entities[key_str].items():
                setattr(entity, k, v)
            entity.key = key
            return entity
        return None

    def put(self, entity):
        """Store an entity."""
        key = entity.key
        key_str = f"{key.kind}:{key.name}"
        
        # Convert entity to dict for storage
        entity_dict = {}
        for k in dir(entity):
            if not k.startswith('_') and k != 'key':
                entity_dict[k] = getattr(entity, k)
        
        self.entities[key_str] = entity_dict
        return key

    def delete(self, key):
        """Delete an entity by key."""
        key_str = f"{key.kind}:{key.name}"
        if key_str in self.entities:
            del self.entities[key_str]

    def query(self, kind=None):
        """Create a query for the given kind."""
        query = MagicMock()
        query.kind = kind
        
        def fetch():
            results = []
            for key_str, entity_dict in self.entities.items():
                if key_str.startswith(f"{kind}:"):
                    entity = MagicMock()
                    for k, v in entity_dict.items():
                        setattr(entity, k, v)
                    key_name = key_str.split(':')[1]
                    entity.key = self.key(kind, key_name)
                    results.append(entity)
            return results
        
        query.fetch = fetch
        return query


class MockIO(IO):
    """Mock IO helper for testing."""
    def __init__(self):
        self.files = {}
        self.exists_results = {}
        self.bulk_exists_results = {}
        
    def exists(self, path):
        return self.exists_results.get(path, False)
        
    def bulk_exists_check(self, paths):
        if self.bulk_exists_results:
            return self.bulk_exists_results
        return {path: False for path in paths}
        
    def put(self, src_filename, dst_url, must=True, skip_if_exists=False):
        if os.path.exists(src_filename):
            with open(src_filename, 'rb') as f:
                self.files[dst_url] = f.read()
        else:
            self.files[dst_url] = b"mock content"
        return dst_url
        
    def write_json_to_cas(self, data):
        url = f"gs://mock-cas/{hash(json.dumps(data, sort_keys=True))}"
        self.files[url] = json.dumps(data).encode('utf-8')
        return url
        
    def write_file_to_cas(self, filename):
        url = f"gs://mock-cas/{hash(filename)}"
        self.files[url] = b"mock content"
        return url
        
    def get_child_keys(self, prefix):
        return [k for k in self.files.keys() if k.startswith(prefix)]
