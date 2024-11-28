import time
import json
from ..logclient import LogMonitor, CommunicationError
from ..txtui import print_log_content
import datetime
import json

from ..log import log
from .runner_types import NextPoll, ClusterStateQuery
from .shared import _exception_guard, _only_running_tasks
from ..cluster_service import Cluster
from .. import txtui 
from .shared import _only_failed_tasks, _only_running_tasks, _only_completed_tasks

class StreamLogs:
    # Stream output from one of the running processes.
    def __init__(self, stream_logs : bool, cluster : Cluster, io):
        self.log_monitor = None
        self.cluster = cluster
        self.stream_logs = stream_logs
        self.complete_tasks_printed = 0
        self.io = io

    def start_logging(self, state : ClusterStateQuery):
        running = _only_running_tasks(state.get_tasks())
        if len(running) > 0:
            task = running[0]
            if task.monitor_address is not None:
                log.info(
                    "Obtained monitor address for task %s: %s",
                    task.task_id,
                    task.monitor_address,
                )
                log_monitor = LogMonitor(
                    self.cluster.client, task.monitor_address, task.task_id
                )
                print_log_content(
                    None,
                    "[starting tail of log {}]".format(log_monitor.task_id),
                    from_sparkles=True,
                )

    def do_next_read(self):
        with _exception_guard(lambda: "polling log file threw exception"):
            try:
                self.log_monitor.poll()
            except CommunicationError as ex:
                log.warning(
                    "Got error polling log. shutting down log watch: {}".format(ex)
                )
                self.log_monitor.close()
                self.log_monitor = None

    def do_last_read(self):
        print_log_content(
            None,
            "[{} is no longer running, tail of log stopping]".format(
                self.log_monitor.task_id
            ),
            from_sparkles=True,
        )

        self.flush_stdout_from_complete_task(
            self.log_monitor.task_id, self.log_monitor.offset
        )
        self.log_monitor = None

    def flush_stdout_from_complete_task(self, task_id, offset):
        task = self.cluster.task_store.get_task(task_id)
        spec = json.loads(self.io.get_as_str(task.args))

        attempts = 0
        while True:
            rest_of_stdout = self.io.get_as_str(
                spec["stdout_url"], start=offset, must=False
            )
            if rest_of_stdout is not None:
                break
            log.warning(
                "Log %s doesn't appear to exist yet. Will try again...",
                spec["stdout_url"],
            )
            time.sleep(5)
            attempts += 1
            if attempts > 10:
                log.warning("Log file never did appear. Giving up trying to tail log")
                return

        print_log_content(datetime.datetime.now(), rest_of_stdout)
        self.complete_tasks_printed += 1

    def poll(self, state : ClusterStateQuery):
        if self.stream_logs:
            if self.log_monitor is None:
                self.start_logging(state)
            else:
                if state.is_task_running(self.log_monitor.task_id):
                    self.do_next_read()
                else:
                    self.do_last_read()
            return NextPoll(1)
        else:
            return None

    def finish(self, state):
        if self.log_monitor is not None:
            self.do_last_read()
        
        self._print_final_summary(state)

    def _print_final_summary(self, state:ClusterStateQuery):
        tasks = state.get_tasks()
        failed_tasks = _only_failed_tasks(tasks)
        successful_tasks = _only_completed_tasks(tasks)

        assert len(tasks) == len(failed_tasks) + len(successful_tasks), "Everything should be either failed or completed by this point"

        txtui.user_print(
            f"Job finished. {len(successful_tasks)} tasks completed successfully, {len(failed_tasks)} tasks failed"
        )

        if len(failed_tasks) > 0:
            log.warning(
                "At least one task failed. Dumping stdout from one of the failures."
            )
            failed_tasks = state.get_failed_tasks()
            self.flush_stdout_from_complete_task(failed_tasks[0].task_id, 0)
        
        # if we want the logs and yet we've never written out a single log, pick one at random and write it out
        elif (
            self.complete_tasks_printed == 0 and self.stream_logs
        ):  
            log.info("Dumping arbitrary successful task stdout")
            self.flush_stdout_from_complete_task(successful_tasks[0].task_id, 0)



# class FlushStdout:
#     def __init__(self, jq, io):
#         self.flush_stdout_calls = 0
#         self.jq = jq
#         self.io = io

#     def __call__(self, task_id, offset):
#         flush_stdout_from_complete_task(self.jq, self.io, task_id, offset)
#         self.flush_stdout_calls += 1


