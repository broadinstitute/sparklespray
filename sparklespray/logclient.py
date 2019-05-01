from queue import Queue, Empty
import threading
from .pb_pb2_grpc import MonitorStub
from .pb_pb2 import ReadOutputRequest, GetProcessStatusRequest
import grpc
import datetime
import logging
from .txtui import print_log_content

from .log import log

# Give all grpc calls a timeout of 10 seconds to complete
GRPC_TIMEOUT = 10.0


class CommunicationError(Exception):
    pass


class GRPCError(CommunicationError):
    pass


class Timeout(CommunicationError):
    pass


# this is ridiculously complicated but it seems like the timeout option on grpc isn't reliable. So instead, make the call on a different thread
# so as worst, we can leak the hanging thread and signal the main thread with an exception.
class WrappedStub:
    def __init__(self, timeout):
        in_queue = Queue()
        out_queue = Queue()

        self.in_queue = in_queue
        self.out_queue = out_queue
        self.timeout = timeout

    def _worker_loop(self, channel):
        stub = MonitorStub(channel)
        while True:
            cmd = self.in_queue.get()
            if cmd == "dispose":
                break
            else:
                name, args, kwargs = cmd
                try:
                    result = getattr(stub, name)(*args, **kwargs)
                except Exception as ex:
                    self.out_queue.put(("exception", ex))
                else:
                    self.out_queue.put(("result", result))

    def start(self, channel):
        t = threading.Thread(target=self._worker_loop, args=[channel])
        t.daemon = True
        t.start()

    def _blocking_call(self, method, args, kwargs):
        self.in_queue.put((method, args, kwargs))
        try:
            type, value = self.out_queue.get(timeout=self.timeout)
        except Empty:
            raise Timeout("Timeout trying in call to {}".format(method))
        if type == "exception":
            raise value
        return value

    def ReadOutput(self, *args, **kwargs):
        return self._blocking_call("ReadOutput", args, kwargs)

    def GetProcessStatus(self, *args, **kwargs):
        return self._blocking_call("GetProcessStatus", args, kwargs)

    def dispose(self):
        self.in_queue.put("dispose")


class LogMonitor:
    def __init__(self, datastore_client, node_address, task_id):
        entity_key = datastore_client.key("ClusterKeys", "sparklespray")
        entity = datastore_client.get(entity_key)

        cert = entity['cert']
        self.shared_secret = entity['shared_secret']
        creds = grpc.ssl_channel_credentials(cert)

        log.info("connecting to %s", node_address)
        channel = grpc.secure_channel(node_address, creds,
                                      options=(('grpc.ssl_target_name_override', 'sparkles.server',),))

        self.stub = WrappedStub(GRPC_TIMEOUT*2)
        self.stub.start(channel)
        self.task_id = task_id
        self.offset = 0

        self.prev_mem_total = 0

    def close(self):
        self.stub.dispose()

    def poll(self):
        while True:
            try:
                response = self.stub.ReadOutput(ReadOutputRequest(taskId=self.task_id, offset=self.offset, size=100000),
                                                metadata=[('shared-secret', self.shared_secret)], timeout=GRPC_TIMEOUT)
            except grpc.RpcError as rpc_error:
                # TODO: Might be caught in an infinite loop. Could be good to add an exponential delay before retrying. And stop after a number of retries
                log.debug(
                    "Received a RpcError {}. Retrying to contact the VM".format(rpc_error))
                raise GRPCError(rpc_error)

            payload = response.data.decode('utf8')
            if payload != "":
                print_log_content(datetime.datetime.now(), payload)

            self.offset += len(response.data)

            if response.endOfFile:
                break

        try:
            response = self.stub.GetProcessStatus(GetProcessStatusRequest(),
                                                  metadata=[('shared-secret', self.shared_secret)], timeout=GRPC_TIMEOUT)
        except grpc.RpcError as rpc_error:
            # TODO: Might be caught in an infinite loop. Could be good to add an exponential delay before retrying. And stop after a number of retries
            log.debug(
                "Received a RpcError {}. Retrying to contact the VM".format(rpc_error))
            raise GRPCError(rpc_error)

        mem_total = response.totalMemory + response.totalData + \
            response.totalShared + response.totalResident
        per_gb = (1024*1024*1024.0)
        if abs(self.prev_mem_total - mem_total) > 0.01 * per_gb:
            self.prev_mem_total = mem_total

            print_log_content(datetime.datetime.now(), "Processes running in container: %s, total memory used: %.3f GB, data memory used: %.3f GB, shared used %.3f GB, resident %.3f GB" % (
                response.processCount, response.totalMemory / per_gb, response.totalData / per_gb, response.totalShared / per_gb, response.totalResident / per_gb), from_sparkles=True)
