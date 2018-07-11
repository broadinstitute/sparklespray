from termcolor import colored, cprint
from kubeque.pb_pb2_grpc import MonitorStub
from kubeque.pb_pb2 import ReadOutputRequest
import grpc
import datetime
import logging

log = logging.getLogger(__name__)


def print_log_content(timestamp, payload):
    if payload[-1] == "\n":
        payload = payload[:-1]
    payload_lines = payload.split("\n")
    if payload_lines[-1] == "":
        del payload_lines[-1]
    prefix = None
    for line in payload_lines:
        if prefix is None:
            prefix = "[{}]".format(timestamp.strftime("%H:%M:%S"))
            print(colored(prefix, "green"), colored(line, "yellow"))
        else:
            print(colored(" "*len(prefix), "white"), colored(line, "yellow"))


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
