from google.cloud import datastore
from typing import Set, Any

class Batch:
    def __init__(self, client: datastore.Client, batch_size : int=300) -> None:
        self.deletes = set() # type: Set[Any]
        self.batch_size = batch_size
        self.client = client

    def delete(self, key) -> None:
        self.deletes.add(key)

    def flush(self) -> None:
        deletes = list(self.deletes)
        for chunk_start in range(0, len(deletes), self.batch_size):
            key_batch = deletes[chunk_start:chunk_start+self.batch_size]
            self.client.delete_multi(key_batch)
