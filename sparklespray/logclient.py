from .pb_pb2_grpc import MonitorStub
from .pb_pb2 import ReadOutputRequest
import grpc
import datetime
import logging
from .txtui import print_log_content

from .log import log


class LogMonitor:
    def __init__(self, datastore_client, node_address, task_id):
        log.info("connecting to %s", node_address)
        entity_key = datastore_client.key("ClusterKeys", "sparklespray")
        entity = datastore_client.get(entity_key)

        cert = entity['cert']
        self.shared_secret = entity['shared_secret']
        creds = grpc.ssl_channel_credentials(cert)
        channel = grpc.secure_channel(node_address, creds,
                                      options=(('grpc.ssl_target_name_override', 'sparkles.server',),))
        self.stub = MonitorStub(channel)
        self.task_id = task_id
        self.offset = 0

    def poll(self):
        while True:
            response = self.stub.ReadOutput(ReadOutputRequest(taskId=self.task_id, offset=self.offset, size=100000),
                                            metadata=[('shared-secret', self.shared_secret)])

            payload = response.data.decode('utf8')
            if payload != "":
                print_log_content(datetime.datetime.now(), payload)

            self.offset += len(response.data)

            if response.endOfFile:
                break
