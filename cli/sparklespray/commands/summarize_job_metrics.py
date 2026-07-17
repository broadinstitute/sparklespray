import json
import statistics

from ..task_store import STATUS_CLAIMED, STATUS_COMPLETE
from ..job_queue import JobQueue
from ..io_helper import IO
from .. import txtui
from .shared import _resolve_jobid, _get_filtered_tasks


def _percentile(values, pct):
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=100, method="inclusive")[pct - 1]


def _print_metric_stats(name, values, unit="", note=None):
    if not values:
        txtui.user_print(f"{name}: no data available")
        return

    values = sorted(values)
    txtui.user_print(
        "{name} (n={n}): min={min:.1f}{unit}, p5={p5:.1f}{unit}, median={median:.1f}{unit}, "
        "mean={mean:.1f}{unit}, p95={p95:.1f}{unit}, max={max:.1f}{unit}".format(
            name=name,
            n=len(values),
            min=values[0],
            p5=_percentile(values, 5),
            median=statistics.median(values),
            mean=statistics.mean(values),
            p95=_percentile(values, 95),
            max=values[-1],
            unit=unit,
        )
    )
    if note:
        txtui.user_print(f"    {note}")


def _pick_time_unit(values_sec):
    if statistics.median(values_sec) < 0.1 * 60:
        return 1, " sec"
    return 60, " min"


def _pick_memory_unit(values_bytes):
    KB = 1024
    MB = KB * 1024
    GB = MB * 1024
    median = statistics.median(values_bytes)
    if median < MB:
        return KB, " KB"
    elif median < GB:
        return MB, " MB"
    else:
        return GB, " GB"


def _print_time_stats(name, values_sec):
    if not values_sec:
        _print_metric_stats(name, values_sec)
        return
    divisor, unit = _pick_time_unit(values_sec)
    _print_metric_stats(name, [v / divisor for v in values_sec], unit=unit)


def _print_memory_stats(name, values_bytes):
    if not values_bytes:
        _print_metric_stats(name, values_bytes)
        return
    divisor, unit = _pick_memory_unit(values_bytes)
    _print_metric_stats(name, [v / divisor for v in values_bytes], unit=unit)


def summarize_job_metrics_cmd(jq: JobQueue, io: IO, args):
    jobid = _resolve_jobid(jq, args.jobid)

    tasks = _get_filtered_tasks(jq, jobid, args.incomplete, args.exitcode)

    if len(tasks) == 0:
        txtui.user_print("No tasks found")
        return 1

    claim_counts = []
    runtimes = []
    wait_times = []
    completed_command_result_urls = []

    for task in tasks:
        claimed_time = None
        complete_time = None
        claim_count = 0
        for entry in task.history:
            if entry.status == STATUS_CLAIMED:
                claimed_time = entry.timestamp
                claim_count += 1
            elif entry.status == STATUS_COMPLETE:
                complete_time = entry.timestamp
        claim_counts.append(claim_count)

        if complete_time is not None and claimed_time is not None:
            runtimes.append(complete_time - claimed_time)
            wait_times.append(claimed_time - task.history[0].timestamp)

        if task.exit_code is not None:
            completed_command_result_urls.append(task.command_result_url)

    max_memory_sizes = []
    user_cpu_times = []
    if completed_command_result_urls:
        txtui.user_print(
            f"Fetching result data for {len(completed_command_result_urls)} completed tasks..."
        )
        results = io.bulk_get_as_str(completed_command_result_urls).values()
        for body in results:
            if body is None:
                continue
            result = json.loads(body)
            usage = result.get("resource_usage")
            if usage is None:
                continue
            max_memory_sizes.append(usage["max_memory_size"])
            user_cpu_time = usage["user_cpu_time"]
            user_cpu_times.append(user_cpu_time["Sec"] + user_cpu_time["Usec"] / 1e6)

    txtui.user_print(f"Task summary for job {jobid} ({len(tasks)} tasks)")
    _print_metric_stats(
        "Claimed events per task",
        claim_counts,
        note="Expect at least 1 per task; values > 1 indicate a task was preempted and restarted.",
    )
    _print_time_stats("Runtime", runtimes)
    _print_time_stats("Wait time before running", wait_times)
    _print_memory_stats("Max RSS", max_memory_sizes)
    _print_time_stats("User CPU time", user_cpu_times)


def add_summarize_job_metrics_cmd(subparser):
    parser = subparser.add_parser(
        "summarize-job-metrics",
        help="Print a statistical summary of the tasks which make up the specified job",
    )
    parser.set_defaults(func=summarize_job_metrics_cmd)
    parser.add_argument("jobid")
    parser.add_argument(
        "--incomplete",
        "-i",
        help="If set, only summarize those tasks which are not complete",
        action="store_true",
    )
    parser.add_argument(
        "--exitcode",
        "-e",
        help="Only include those tasks with this return code",
        type=int,
    )
