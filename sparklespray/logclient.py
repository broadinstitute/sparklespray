from .pb_pb2_grpc import MonitorStub
from .pb_pb2 import ReadOutputRequest, GetProcessStatusRequest
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

        self.prev_mem_total = 0

    def poll(self):
        while True:
            try:
                response = self.stub.ReadOutput(ReadOutputRequest(taskId=self.task_id, offset=self.offset, size=100000),
                                                timeout=10,
                                                metadata=[('shared-secret', self.shared_secret)])
            except grpc.RpcError as rpc_error:
                # TODO: Might be caught in an infinite loop. Could be good to add an exponential delay before retrying. And stop after a number of retries
                log.debug(
                    "Received a RpcError {}. Retrying to contact the VM".format(rpc_error))
                continue

            payload = response.data.decode('utf8')
            if payload != "":
                print_log_content(datetime.datetime.now(), payload)

            self.offset += len(response.data)

            if response.endOfFile:
                break

        try:
            response = self.stub.GetProcessStatus(GetProcessStatusRequest(), timeout=10,
                                                  metadata=[('shared-secret', self.shared_secret)])
        except grpc.RpcError as rpc_error:
            # TODO: Might be caught in an infinite loop. Could be good to add an exponential delay before retrying. And stop after a number of retries
            log.debug(
                "Received a RpcError {}. Retrying to contact the VM".format(rpc_error))

        mem_total = response.totalMemory + response.totalData + \
            response.totalShared + response.totalResident
        per_gb = (1024*1024*1024.0)
        if abs(self.prev_mem_total - mem_total) > 0.01 * per_gb:
            self.prev_mem_total = mem_total

            print_log_content(datetime.datetime.now(), "Processes running in container: %s, total memory used: %.3f GB, data memory used: %.3f GB, shared used %.3f GB, resident %.3f GB" % (
                response.processCount, response.totalMemory / per_gb, response.totalData / per_gb, response.totalShared / per_gb, response.totalResident / per_gb), from_sparkles=True)
