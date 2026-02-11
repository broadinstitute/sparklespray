from .runner_types import PeriodicTask, NextPoll
from ..cluster_service import Cluster, SECONDS_UNTIL_STALE_HEARTBEAT
from ..errors import UserError


class HeartbeatLost(UserError):
    """Raised when this watch process can no longer maintain its heartbeat."""

    pass


class Heartbeat(PeriodicTask):
    """Periodically records a heartbeat to indicate this watch process is alive.

    This allows detection of crashed watch processes - if the heartbeat becomes
    stale, another watch process can take over.

    If the heartbeat cannot be recorded (e.g., another process took over),
    this task raises HeartbeatLost to stop the watch process.
    """

    def __init__(
        self,
        cluster: Cluster,
        watch_run_uuid: str,
        delay: float = SECONDS_UNTIL_STALE_HEARTBEAT / 2,
    ):
        self.cluster = cluster
        self.watch_run_uuid = watch_run_uuid
        self.delay = delay

    def poll(self, state):
        success = self.cluster.heartbeat(
            self.watch_run_uuid,
            expected_uuid=self.watch_run_uuid,
        )
        if not success:
            raise HeartbeatLost(
                "Another watch process has taken over this job. Exiting."
            )

        return NextPoll(self.delay)
