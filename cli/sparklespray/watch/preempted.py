# reset_preempted = ResetPreempted()
# get_preempted = GetPreempted()
#         task_ids = get_preempted(state)
#         if len(task_ids) > 0:
#             log.warning(
#                 "Resetting tasks which appear to have been preempted: %s",
#                 ", ".join(task_ids),
#             )
#             for task_id in task_ids:
#                 jq.reset_task(task_id)

#     with _exception_guard(lambda: "rescaling cluster threw exception"):
#         resize_cluster(state, cluster.get_cluster_mod(job_id))
class RestartPreemptedTasks:
    "Have any nodes unexpectedly shutdown? If so, change the status of the tasks they owned to pending."


class GetPreempted:
    def __init__(self, get_time=time.time, min_bad_time=30):
        self.first_time_task_reported_bad = {}
        self.get_time = get_time
        self.min_bad_time = min_bad_time

    def __call__(self, state: ClusterState) -> List[str]:
        tasks_to_reset = []
        task_ids = state.get_running_tasks_with_invalid_owner()
        next_times = {}
        for task_id in task_ids:
            first_time = self.first_time_task_reported_bad.get(task_id, self.get_time())
            if self.get_time() - first_time >= self.min_bad_time:
                tasks_to_reset.append(task_id)
            else:
                next_times[task_id] = first_time

        self.first_time_task_reported_bad = next_times

        return tasks_to_reset
