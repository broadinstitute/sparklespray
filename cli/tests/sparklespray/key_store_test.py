from sparklespray.key_store import KeyStore
from dataclasses import dataclass
from google.cloud import datastore


@dataclass(frozen=True)
class MockKey:
    kind: str
    id: str


class MockClient:
    def __init__(self):
        self.objs_by_id = {}

    def key(self, kind, id):
        return MockKey(kind, id)

    def get(self, key):
        assert isinstance(key, MockKey)
        return self.objs_by_id.get(key)

    def put(self, entity):
        assert isinstance(entity, datastore.Entity)
        self.objs_by_id[entity.key] = entity


from typing import cast
from google.cloud.datastore import Client
def test_key_store():
    mock_client = cast(Client, MockClient())
    ks = KeyStore(mock_client)

    cert, key = ks.get_cert_and_key()
    assert cert is None
    assert key is None

    ks.set_cert_and_key(b"cert", b"key")
    cert, key = ks.get_cert_and_key()
    assert cert == b"cert"
    assert key == b"key"
