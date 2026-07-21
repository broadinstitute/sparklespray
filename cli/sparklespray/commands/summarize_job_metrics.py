import collections
import json
import statistics

from ..task_store import STATUS_CLAIMED, STATUS_COMPLETE
from ..job_queue import JobQueue
from ..io_helper import IO
from .. import txtui
from .shared import _resolve_jobid, _get_filtered_tasks

TOP_N = 5


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


def _format_duration(seconds):
    if seconds < 60:
        return f"{seconds:.1f} sec"
    elif seconds < 3600:
        return f"{seconds / 60:.1f} min"
    else:
        return f"{seconds / 3600:.1f} hours"


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


def _print_top_n(label, records, divisor, unit):
    if not records:
        return
    top = sorted(records, key=lambda r: r["value"], reverse=True)[:TOP_N]
    txtui.user_print(f"Top {len(top)} tasks with largest {label}:")
    for r in top:
        txtui.user_print(
            '   {value:.1f}{unit} taskID={task_id} command="{command}"'.format(
                value=r["value"] / divisor,
                unit=unit,
                task_id=r["task_id"],
                command=r["command"],
            )
        )


def summarize_job_metrics_cmd(jq: JobQueue, io: IO, args):
    jobid = _resolve_jobid(jq, args.jobid)

    tasks = _get_filtered_tasks(jq, jobid, args.incomplete, args.exitcode)

    if len(tasks) == 0:
        txtui.user_print("No tasks found")
        return 1

    claim_counts = []
    runtime_records = []
    wait_times = []
    completed_tasks = []

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
            runtime_records.append(
                {"task_id": task.task_id, "value": complete_time - claimed_time}
            )
            wait_times.append(claimed_time - task.history[0].timestamp)

        if task.exit_code is not None:
            completed_tasks.append(task)

    max_memory_records = []
    user_cpu_percents = []
    total_memory_values = []
    number_of_cpus_values = []
    machine_types = []
    commands_by_task_id = {}
    if completed_tasks:
        txtui.user_print(
            f"Fetching result data for {len(completed_tasks)} completed tasks..."
        )
        results_by_url = io.bulk_get_as_str(
            [task.command_result_url for task in completed_tasks]
        )
        for task in completed_tasks:
            body = results_by_url.get(task.command_result_url)
            if body is None:
                continue
            result = json.loads(body)
            commands_by_task_id[task.task_id] = result.get("command", "")

            machine_stats = result.get("machine_stats")
            if machine_stats is None:
                continue
            max_memory_records.append(
                {
                    "task_id": task.task_id,
                    "value": machine_stats["max_memory_in_use"],
                }
            )
            user_cpu_percents.append(machine_stats["max_user_cpu_percent_usage"])
            total_memory_values.append(machine_stats["memory_total"])
            number_of_cpus_values.append(machine_stats["number_of_cpus"])
            if machine_stats["machine_type"]:
                machine_types.append(machine_stats["machine_type"])

    for r in runtime_records + max_memory_records:
        r["command"] = commands_by_task_id.get(r["task_id"], "")

    if machine_types:
        machine_type_counts = collections.Counter(machine_types)
        if len(machine_type_counts) == 1:
            machine_type_str = next(iter(machine_type_counts))
        else:
            machine_type_str = ", ".join(
                f"{mt} ({n})" for mt, n in machine_type_counts.most_common()
            )
    else:
        machine_type_str = "unknown"
    txtui.user_print(f"machine type: {machine_type_str}")

    runtimes = [r["value"] for r in runtime_records]
    total_runtime_str = _format_duration(sum(runtimes)) if runtimes else "no data available"
    txtui.user_print(f"total runtime: {total_runtime_str}")

    txtui.user_print(f"Task summary for job {jobid} ({len(tasks)} tasks)")
    if total_memory_values:
        divisor, unit = _pick_memory_unit(total_memory_values)
        mean_total_memory = statistics.mean(total_memory_values) / divisor
        txtui.user_print(
            f"Mean total machine memory: {mean_total_memory:.1f}{unit} (n={len(total_memory_values)})"
        )
    if number_of_cpus_values:
        mean_number_of_cpus = statistics.mean(number_of_cpus_values)
        txtui.user_print(
            f"Mean number of cpus: {mean_number_of_cpus:.1f} (n={len(number_of_cpus_values)})"
        )
    txtui.user_print("")

    if wait_times:
        divisor, unit = _pick_time_unit(wait_times)
        _print_metric_stats(
            "Wait time before running", [v / divisor for v in wait_times], unit=unit
        )
    else:
        _print_metric_stats("Wait time before running", wait_times)
    _print_metric_stats(
        "Claimed events per task",
        claim_counts,
        note="Expect at least 1 per task; values > 1 indicate a task was preempted and restarted.",
    )
    txtui.user_print("")

    if user_cpu_percents:
        _print_metric_stats("User CPU usage", user_cpu_percents, unit="%")
    else:
        _print_metric_stats("User CPU usage", user_cpu_percents)
    txtui.user_print("")

    if runtimes:
        divisor, unit = _pick_time_unit(runtimes)
        _print_metric_stats("Runtime", [v / divisor for v in runtimes], unit=unit)
        _print_top_n("runtime", runtime_records, divisor, unit)
    else:
        _print_metric_stats("Runtime", runtimes)
    txtui.user_print("")

    max_memory_values = [r["value"] for r in max_memory_records]
    if max_memory_values:
        divisor, unit = _pick_memory_unit(max_memory_values)
        _print_metric_stats(
            "Max memory used", [v / divisor for v in max_memory_values], unit=unit
        )
        _print_top_n("max memory used", max_memory_records, divisor, unit)
    else:
        _print_metric_stats("Max memory used", max_memory_values)


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
