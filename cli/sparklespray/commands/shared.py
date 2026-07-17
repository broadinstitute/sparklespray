from ..task_store import (
    STATUS_COMPLETE,
    STATUS_FAILED,
    STATUS_CLAIMED,
    STATUS_PENDING,
    STATUS_KILLED,
    is_terminal_status,
)
from ..log import log


def _get_jobids_from_pattern(jq, jobid_pattern):
    if not jobid_pattern:
        jobid_pattern = "*"

    if jobid_pattern == "LAST":
        job = jq.get_last_job()
        return [job.job_id]
    else:
        return jq.get_jobids(jobid_pattern)


def _resolve_jobid(jq, jobid):
    if jobid == "LAST":
        job = jq.get_last_job()
        return job.job_id
    else:
        return jobid


def _get_filtered_tasks(jq, jobid, incomplete, exitcode):
    if incomplete:
        tasks = []
        for status in [STATUS_FAILED, STATUS_CLAIMED, STATUS_PENDING, STATUS_KILLED]:
            tasks.extend(jq.task_storage.get_tasks(jobid, status=status))
    else:
        tasks = jq.task_storage.get_tasks(jobid)

    if exitcode is not None:

        def retcode_matches(exit_code):
            return exit_code is not None and int(exit_code) == exitcode

        before_count = len(tasks)
        tasks = [task for task in tasks if retcode_matches(task.exit_code)]
        log.info(
            "Filtered {} tasks to {} tasks with exit code {}".format(
                before_count, len(tasks), exitcode
            )
        )

    return tasks


def _summarize_task_statuses(tasks):
    import collections

    complete = True
    counts = collections.defaultdict(lambda: 0)
    for task in tasks:
        if task.status == STATUS_COMPLETE:
            label = "{}(code={})".format(task.status, task.exit_code)
        elif task.status == STATUS_FAILED:
            label = "{}({})".format(task.status, task.failure_reason)
        else:
            label = task.status
        counts[label] += 1

        if not is_terminal_status(task.status):
            complete = False

    labels = list(counts.keys())
    labels.sort()
    status_str = ", ".join(["{}: {}".format(l, counts[l]) for l in labels])
    return status_str, complete
