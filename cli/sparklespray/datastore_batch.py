from google.cloud import datastore
from typing import Set, Any, List


class Batch:
    def __init__(self, client: datastore.Client, batch_size: int = 300) -> None:
        self.deletes = set()  # type: Set[Any]
        self.puts = []  # type: List[Any]
        self.batch_size = batch_size
        self.client = client

    def __repr__(self):
        return f"<Batch {len(self.deletes)} deletes, {len(self.puts)} puts>"

    def delete(self, key) -> None:
        self.deletes.add(key)

    def put(self, entity) -> None:
        self.puts.append(entity)

    def flush(self) -> None:
        deletes = list(self.deletes)
        for chunk_start in range(0, len(deletes), self.batch_size):
            key_batch = deletes[chunk_start : chunk_start + self.batch_size]
            self.client.delete_multi(key_batch)

        puts = self.puts
        for chunk_start in range(0, len(puts), self.batch_size):
            batch = puts[chunk_start : chunk_start + self.batch_size]
            self.client.put_multi(batch)


class ImmediateBatch:
    def __init__(self, client):
        self.client = client

    def delete(self, key):
        self.client.delete(key)

    def put(self, entity):
        self.client.put(entity)

    def flush(self):
        pass
