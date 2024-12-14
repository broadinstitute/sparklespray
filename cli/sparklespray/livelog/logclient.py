import datetime
import logging
from ..txtui import print_log_content
from ..log import log
from termcolor import colored
import fcntl, os
import select
import threading
import subprocess
import sys
from .pickle_pipe import create_nonblocking_file_obj, Reader, Writer

# Give all grpc calls a timeout of 10 seconds to complete
GRPC_TIMEOUT = 10.0


class CommunicationError(Exception):
    pass


class GRPCError(CommunicationError):
    pass


class Timeout(CommunicationError):
    pass


def print_from_stream(fd, color):
    def print_loop():
        nb_fd = create_nonblocking_file_obj(fd)

        while True:
            select.select([nb_fd], [], [])
            data = nb_fd.read()
            if len(data) == 0:
                break
            print(colored(data.decode("utf8"), color))

    t = threading.Thread(target=print_loop, daemon=True)
    t.start()
    return t

def _start_isolated_log_client(entry_point, init_params):
    python_executable = sys.executable
    command = [python_executable, "-m", "sparklespray.livelog.isolated_log_client", entry_point]
    print(f"Executing: {' '.join(command)}")
    proc = subprocess.Popen(command, executable=python_executable, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    t = print_from_stream(proc.stderr, color="red")

    writer = Writer(proc.stdin)
    reader = Reader(proc.stdout)

    print("writing init params")
    writer.write_obj( init_params )
    return proc, reader, writer, t

from . import isolated_log_client

class SafeRemoteCaller:
    def __init__(self, entry_point, timeout):
        self.timeout = timeout
        self.proc = None
        self.reader = None
        self.writer = None
        self.entry_point = entry_point

    def start(self, init_params):
        self.proc, self.reader, self.writer, self.stderr_read_thread = _start_isolated_log_client(self.entry_point, init_params)

    def _blocking_call(self, method, args, kwargs):
        if self.proc is None:
            raise CommunicationError("disconnected WrappedStub")
        self.writer.write_obj( (method, args, kwargs) )
        try:
            value = self.reader.read_obj(timeout=self.timeout)
        except Timeout:
            self.dispose()
            raise Timeout("Timeout trying in call to {}".format(method))
        if not value['success']:
            raise CommunicationError(value["error"])
        return value

    def read_output(self, *args, **kwargs):
        return self._blocking_call("read_output", args, kwargs)

    def get_process_status(self, *args, **kwargs):
        return self._blocking_call("get_process_status", args, kwargs)

    def dispose(self):
        assert self.proc is not None
        # if the child process is still running, kill it
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()
        assert self.proc.poll() is not None
        self.proc=None
        if self.stderr_read_thread is not None:
            self.stderr_read_thread.join()
            self.stderr_read_thread = None


class LogMonitor:
    def __init__(self, datastore_client, node_address, task_id):
        entity_key = datastore_client.key("ClusterKeys", "sparklespray")
        entity = datastore_client.get(entity_key)

        cert = entity["cert"]

        self.stub = SafeRemoteCaller(f"{isolated_log_client.__name__}:create_monitor", GRPC_TIMEOUT * 2)
        self.stub.start((cert, node_address, entity["shared_secret"]))
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

