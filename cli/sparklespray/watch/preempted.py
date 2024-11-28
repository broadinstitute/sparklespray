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
