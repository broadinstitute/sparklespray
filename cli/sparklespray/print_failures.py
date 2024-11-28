from .task_store import STATUS_FAILED
from .job_queue import JobQueue
from .io_helper import IO
import json


def print_failures(jq: JobQueue, io: IO, jobid, show_all: bool):
    tasks = jq.task_storage.get_tasks(jobid)
    if not show_all:
        all_task_count = len(tasks)
        tasks = [
            t
            for t in tasks
            if t.status == STATUS_FAILED
            or (t.exit_code is not None and str(t.exit_code) != "0")
        ]
        print(
            f"{len(tasks)} out of {all_task_count} tasks failed. Printing information about those failed tasks:\n"
        )

    from datetime import datetime

    def format_history(history):
        return "\n".join(
            [
                f"      {(datetime.utcfromtimestamp(entry.timestamp).strftime('%Y-%m-%d %H:%M:%S'))} {entry.status}"
                for entry in history
            ]
        )

    for t in tasks:
        task_spec = json.loads(io.get_as_str_must(t.args))

        print(
            f"""  task_index: {t.task_index}
    task_id: {t.task_id}
    command: {task_spec['command']}
    parameters: {task_spec['parameters']}
    exit_code: {t.exit_code}
    log_url: {t.log_url}
    history: 
{format_history(t.history)}
"""
        )
    print("You can view the logs from any of these by executing: gsutil cat <log_path>")
