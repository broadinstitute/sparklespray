import datetime
from ..txtui import print_log_content
from .pubsub_client import PubSubMonitorClient

# Default timeout for pub/sub communication
PUBSUB_TIMEOUT = 20.0


class CommunicationError(Exception):
    pass


class Timeout(CommunicationError):
    pass


class LogMonitor:
    def __init__(
        self, project_id: str, incoming_topic: str, response_topic: str, task_id: str
    ):
        self.client = PubSubMonitorClient(
            project_id=project_id,
            incoming_topic=incoming_topic,
            response_topic=response_topic,
            timeout=PUBSUB_TIMEOUT,
        )
        self.task_id = task_id
        self.offset = 0
        self.prev_mem_total = 0

    def close(self):
        self.client.close()

    def poll(self):
        while True:
            response = self.client.read_output(
                task_id=self.task_id, offset=self.offset, size=100000
            )

            if not response.get("success"):
                raise CommunicationError(response.get("error", "Unknown error"))

            payload = response["data"].decode("utf8")
            if payload != "":
                print_log_content(datetime.datetime.now(), payload)

            self.offset += len(response["data"])

            if response["end_of_file"]:
                break

        response = self.client.get_process_status()

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
