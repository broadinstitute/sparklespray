import datetime
from ..txtui import print_log_content
from ..pubsub_client import PubSubMonitorClient


class CommunicationError(Exception):
    pass


class Timeout(CommunicationError):
    pass


class LogMonitor:
    """Monitors log output from a running task.

    Uses a shared PubSubMonitorClient to communicate with workers.
    The client is passed in and managed externally (not closed by this class).
    """

    def __init__(
        self,
        client: PubSubMonitorClient,
        task_id: str,
        worker_id: str,
    ):
        self.client = client
        self.task_id = task_id
        self.worker_id = worker_id
        self.offset = 0
        self.prev_mem_total = 0

    def poll(self):
        while True:
            response = self.client.read_output(
                task_id=self.task_id,
                offset=self.offset,
                size=100000,
                worker_id=self.worker_id,
            )

            if not response.get("success"):
                raise CommunicationError(response.get("error", "Unknown error"))

            payload = response["data"].decode("utf8")
            if payload != "":
                print_log_content(datetime.datetime.now(), payload)

            self.offset += len(response["data"])

            if response["end_of_file"]:
                break

        response = self.client.get_process_status(worker_id=self.worker_id)

        if not response.get("success"):
            raise CommunicationError(response.get("error", "Unknown error"))

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
