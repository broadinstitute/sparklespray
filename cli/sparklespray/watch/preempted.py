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
from .runner_types import ClusterStateQuery
from ..job_queue import JobQueue
from typing import List, Dict, Set
from ..task_store import Task, STATUS_CLAIMED
from ..node_req_store import (NodeReq, NODE_REQ_COMPLETE, NODE_REQ_FAILED, NODE_REQ_CLASS_PREEMPTIVE, NODE_REQ_SUBMITTED,
                    NODE_REQ_STAGING,
                    NODE_REQ_RUNNING,)
from ..log import log

class RestartPreemptedTasks:
    "Have any nodes unexpectedly shutdown? If so, change the status of the tasks they owned to pending."
    def __init__(self, jq : JobQueue):
        self.debounce_filter = DebounceFilter(30, 3)
        self.jq = jq
        self.unknown_instances = set()

    def poll(self, state : ClusterStateQuery):
        suspicious_tasks = get_tasks_with_dead_owner(self.unknown_instances, state.get_tasks(), state.get_nodes())
        task_ids_needing_reset = self.debounce_filter(state.get_time(), [task.id for task in suspicious_tasks])
        if len(task_ids_needing_reset) > 0:
            log.warning(
                "Resetting tasks which appear to have been preempted: %s",
                ", ".join(task_ids_needing_reset),
            )

            for task_id in task_ids_needing_reset:
                self.jq.reset_task(task_id)


def get_tasks_with_dead_owner(unknown_instance_names: Set[str], tasks: List[Task], node_reqs: List[NodeReq]):
    node_req_by_instance_name: Dict[str, NodeReq] = {}
    for node_req in node_reqs:
        if node_req.instance_name is not None:
            assert node_req.instance_name not in node_req_by_instance_name
            node_req_by_instance_name[node_req.instance_name] = node_req

    tasks_needing_reset = []

    for task in tasks:
        if task.status != STATUS_CLAIMED:
            continue

        # if it's claimed, to should definitely have the instance name populated
        instance_name = task.get_instance_name()
        assert instance_name is not None

        # if the task is claimed by a node which we don't see, something weird is going on.
        # warn us that's the case, but only do it once.
        if instance_name not in node_req_by_instance_name:
            if instance_name not in unknown_instance_names:
                log.warning(
                    "instance {} was not listed among {} nodes".format(
                        instance_name, len(node_req_by_instance_name)
                    )
                )
                # remember we don't know what this instance is and we should ignore it
                unknown_instance_names.add(instance_name)
                continue

        # Okay, here's the normal situation: we have a claimed task and we can find the node_req it corresponds to
        node_req = node_req_by_instance_name[instance_name]

        if node_req.status in [
                NODE_REQ_SUBMITTED,
                NODE_REQ_STAGING,
                NODE_REQ_RUNNING,
            ]:
            # if the node is running, everything is fine. Move onto the next task
            continue

        # if the node is no longer running, then we know the task isn't _really_ running either
        assert node_req.status in [NODE_REQ_COMPLETE, NODE_REQ_FAILED]
        log.warning(
            f"task {task.task_id} status = {task.status}, but node_req was {node_req.status}")
        
        # now, this should only happen when the node is preemptive, so check that's the case
        if node_req.node_class != NODE_REQ_CLASS_PREEMPTIVE:
            log.error(
                "instance %s terminated but task %s was reported to still be using instance and the instance was not preemptiable",
                instance_name,
                task.task_id,
            )

        tasks_needing_reset.append(task.task_id)

    return tasks_needing_reset

class DebounceFilter:
    # first_appearance_per_id : Dict[str, Tuple]
    "A low-pass filter. Intended to be call repeatedly with a set of IDs and will return those IDs which have been consistently passed in. More specifically, if an ID is present for every call within the last `duration` seconds, then the ID is returned from the call"
    def __init__(self, duration, min_invocation_count):
        self.duration = duration
        self.min_invocation_count = min_invocation_count
        self.first_appearance_per_id = {}
        self.invocation_count = 0
    
    def __call__(self, now, ids):
        self.invocation_count += 1

        # remove those ids which weren't included this time
        prev = set(self.first_appearance_per_id.keys())
        disappeared_ids = prev.difference(ids)
        for id in disappeared_ids:
            del self.first_appearance_per_id[id]
        
        new_ids = set(ids).difference(prev)
        for id in new_ids:
            self.first_appearance_per_id[id] = (now, self.invocation_count)

        # find the IDs which first appeared over `duration` seconds ago and 
        # at least `min_invocation_count` calls ago
        result = []
        for id, first_appearence in self.first_appearance_per_id.items():
            first_time, first_invocation = first_appearence
            if first_time < (now - self.duration) and first_invocation < (self.invocation_count - self.min_invocation_count):
                result.append(id)
        return result

