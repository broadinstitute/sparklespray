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

class MonitorDictAPI:
    "Basically the same contract as MonitorStub, but consumes typed dicts and returns typed dicts"
    def __init__(self, channel, shared_secret):
        self.shared_secret = shared_secret
        self.stub = MonitorStub(channel)

    def read_output(self, task_id: str, offset:int, size:int):
        try:
            response = self.stub.ReadOutput(
                ReadOutputRequest(
                    taskId=task_id, offset=offset, size=size
                ),
                metadata=[("shared-secret", self.shared_secret)],
                timeout=GRPC_TIMEOUT,
            )
            return {"success": True, "data":response.data, "end_of_file": response.endOfFile}
        except grpc.RpcError as rpc_error:
            return {"success": False, "error": str(rpc_error)}
    
    def get_process_status(self):
        try:
            response = self.stub.GetProcessStatus(
                GetProcessStatusRequest(),
                metadata=[("shared-secret", self.shared_secret)],
                timeout=GRPC_TIMEOUT,
            )
        except grpc.RpcError as rpc_error:
            return {"success": False, "error": str(rpc_error)}

        return {"success": True, "process_count": response.processCount, "total_memory": response.totalMemory, "total_data": response.totalData, "total_shared":response.totalShared,"total_resident": response.totalResident}

def _worker_loop(in_queue, cert, node_address, shared_secret):
    creds = grpc.ssl_channel_credentials(cert)

    log.info("connecting to %s", node_address)
    channel = grpc.secure_channel(
        node_address,
        creds,
        options=(("grpc.ssl_target_name_override", "sparkles.server"),),
    )

    monitor = MonitorDictAPI(channel, shared_secret)

    while True:
        cmd = in_queue.get()
        if cmd == "dispose":
            break
        else:
            name, args, kwargs, out_queue = cmd
            result = getattr(monitor, name)(*args, **kwargs)
            out_queue.put(result)

    channel.close()

class WrappedStub:
    def __init__(self, timeout):
        in_queue = Queue()

        self.in_queue = in_queue
        self.timeout = timeout

    def start(self, cert, node_address, shared_secret):
        t = threading.Thread(target=_worker_loop, args=[self.in_queue, cert, node_address, shared_secret])
        t.daemon = True
        t.start()

    def _blocking_call(self, method, args, kwargs):
        if self.in_queue is None:
            raise CommunicationError("disconnected WrappedStub")
        out_queue = Queue()
        self.in_queue.put((method, args, kwargs, out_queue))
        try:
            value = out_queue.get(timeout=self.timeout)
        except Empty:
            self.dispose()
            self.in_queue = None
            raise Timeout("Timeout trying in call to {}".format(method))
        if not value['success']:
            raise CommunicationError(value["error"])
        return value

    def read_output(self, *args, **kwargs):
        return self._blocking_call("read_output", args, kwargs)

    def get_process_status(self, *args, **kwargs):
        return self._blocking_call("get_process_status", args, kwargs)

    def dispose(self):
        if self.in_queue is not None:
            self.in_queue.put("dispose")


class LogMonitor:
    def __init__(self, datastore_client, node_address, task_id):
        entity_key = datastore_client.key("ClusterKeys", "sparklespray")
        entity = datastore_client.get(entity_key)

        cert = entity["cert"]

        self.stub = WrappedStub(GRPC_TIMEOUT * 2)
        self.stub.start(cert, node_address, entity["shared_secret"])
        self.task_id = task_id
        self.offset = 0

        self.prev_mem_total = 0

    def close(self):
        self.stub.dispose()

    def poll(self):
        while True:
            response = self.stub.read_output(
                task_id=self.task_id, offset=self.offset, size=100000)

            payload = response["data"].decode("utf8")
            if payload != "":
                print_log_content(datetime.datetime.now(), payload)

            self.offset += len(response["data"])

            if response["end_of_file"]:
                break

        response = self.stub.get_process_status(        )

        mem_total = (
            response["total_memory"]
            + response["total_data"]
            + response["total_shared"]
            + response["total_resident"]
        )
        per_gb = 1024 * 1024 * 1024.0
        if abs(self.prev_mem_total - mem_total) > 0.01 * per_gb:
            self.prev_mem_total = mem_total

            print_log_content(
                datetime.datetime.now(),
                "Processes running in container: %s, total memory used: %.3f GB, data memory used: %.3f GB, shared used %.3f GB, resident %.3f GB"
                % (
                    response["process_count"],
                    response["total_data"] / per_gb,
                    response["total_data"] / per_gb,
                    response["total_shared"] / per_gb,
                    response["total_resident"] / per_gb,
                ),
                from_sparkles=True,
            )
