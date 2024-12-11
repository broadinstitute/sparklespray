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

from .isolated_log_client import write_obj, read_obj
from termcolor import colored
import fcntl, os
import select
import threading
import subprocess
import sys

def spit_out_stderr(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

    while True:
        select.select([fd], [], [])
        data = fd.read()
        if len(data) == 0:
            break
        print(colored(data.decode("utf8"), "red"))


def _start_isolated_log_client(init_params):
    python_executable = sys.executable
    command = [python_executable, "-m", "sparklespray.isolated_log_client"]
    print(f"Executing: {' '.join(command)}")
    proc = subprocess.Popen(command, executable=python_executable, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # make stdout_pipe a non-blocking, non-buffered file-like object for reading
    flags = fcntl.fcntl(proc.stdout, fcntl.F_GETFL)
    fcntl.fcntl(proc.stdout, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    stdout_pipe = os.fdopen(proc.stdout.fileno(), mode="rb", buffering=0)

    threading.Thread(target=lambda: spit_out_stderr(proc.stderr), daemon=True).start()

    print("writing init params")
    write_obj(proc.stdin, init_params )
    return proc, stdout_pipe

class WrappedStub:
    def __init__(self, timeout):
        self.timeout = timeout
        self.proc = None

    def start(self, cert, node_address, shared_secret):
        self.proc, self.stdout_pipe=_start_isolated_log_client((cert, node_address, shared_secret))

    def start_mock(self):
        self.proc, self.stdout_pipe=_start_isolated_log_client("mock")

    def _blocking_call(self, method, args, kwargs):
        if self.proc is None:
            raise CommunicationError("disconnected WrappedStub")
        write_obj(self.proc.stdin, (method, args, kwargs) )
        try:
            value = read_obj(self.stdout_pipe, timeout=self.timeout)
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
            self.proc=None

if __name__ == "__main__":
    stub = WrappedStub(3)
    stub.start_mock()
    result = stub._blocking_call("echo", ["example"], {})
    print(result)


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
