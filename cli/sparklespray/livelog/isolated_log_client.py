import sys
from queue import Queue, Empty
import threading
from ..pb_pb2_grpc import MonitorStub
from ..pb_pb2 import ReadOutputRequest, GetProcessStatusRequest
import grpc
from .pickle_pipe import Reader, Writer
import importlib

# this is ridiculously complicated but it seems like the timeout option on grpc isn't reliable. So instead, 
# spawn a child process which makes all grpc calls. If any call takes too long, we simply kill the process.

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
#                timeout=GRPC_TIMEOUT,
            )
            return {"success": True, "data":response.data, "end_of_file": response.endOfFile}
        except grpc.RpcError as rpc_error:
            return {"success": False, "error": str(rpc_error)}
    
    def get_process_status(self):
        try:
            response = self.stub.GetProcessStatus(
                GetProcessStatusRequest(),
                metadata=[("shared-secret", self.shared_secret)],
#                timeout=GRPC_TIMEOUT,
            )
        except grpc.RpcError as rpc_error:
            return {"success": False, "error": str(rpc_error)}

        return {"success": True, "process_count": response.processCount, "total_memory": response.totalMemory, "total_data": response.totalData, "total_shared":response.totalShared,"total_resident": response.totalResident}

from .pickle_pipe import log

def _worker_loop(in_queue : Queue, out_queue: Queue, monitor):
    while True:
        # log("waiting for command")
        cmd = in_queue.get()
        # log("got cmd")
        if cmd == "dispose":
            # log("dispose")
            break
        else:
            name, args, kwargs = cmd
            result = getattr(monitor, name)(*args, **kwargs)
            # log("cmd complete")
            out_queue.put(result)
            # log("result queued")

    # log("shutting down")
    monitor.close()

class Shutdown:
    pass

def start_worker_loop(reader : Reader, writer : Writer, monitor):
    out_queue = Queue()
    in_queue = Queue()

    def read_from_stdin():
        while True:
            try:
                item = reader.read_obj()
            except EOFError:
                break
            in_queue.put(item)
        # let the other thread know it should shutdown
        out_queue.put(Shutdown())

    def write_to_stdout():
        while True:
            item = out_queue.get()
            if isinstance(item, Shutdown):
                break
            writer.write_obj(item)

    # put IO into their own threads to avoid possible deadlocks from the other side not communicating well
    # and blocking read/writes. I think this is really not necessary, but it's also harmless.
    threading.Thread(target=read_from_stdin, daemon=True).start()
    threading.Thread(target=write_to_stdout, daemon=True).start()

    _worker_loop(in_queue, out_queue, monitor)

def create_monitor(init_params):
    cert, node_address, shared_secret = init_params

    creds = grpc.ssl_channel_credentials(cert)

    channel = grpc.secure_channel(
        node_address,
        creds,
        options=(("grpc.ssl_target_name_override", "sparkles.server"),),
    )

#    log("connected")
    return MonitorDictAPI(channel, shared_secret)



if __name__ == "__main__":
    # log("starting. reading init params")
    reader = Reader(sys.stdin)
    writer = Writer(sys.stdout.buffer)
    init_params = reader.read_obj()

    constructor_name = sys.argv[1]
    constructor_module_name, constructor_function_name = constructor_name.split(":")

    module = importlib.import_module(constructor_module_name)
    constructor_function = getattr(module, constructor_function_name)

    monitor = constructor_function(init_params)
    
    try:
        start_worker_loop(reader, writer, monitor)
    except KeyboardInterrupt:
        # if we get a ^C just silently shutdown
        pass
