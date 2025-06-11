from .cluster_service import Cluster
from .job_queue import JobQueue
from dataclasses import dataclass
from typing import List
from .task_store import Task

@dataclass
class ResetSummary:
    running: List[Task]
    orphaned : List[Task]

    @property
    def running_count(self):
        return len(self.running)
    
    @property
    def orphaned_count(self):
        return len(self.orphaned)

def identify_orphans(cluster: Cluster, possibly_running: List[Task]):
    running = []
    orphaned = []
    for task in possibly_running:
        if cluster.is_owner_live(task.owner):
            running.append(task)
        else:
            orphaned.append(task)
    return ResetSummary(running, orphaned)

def reset_orphaned_tasks(job_id: str, cluster: Cluster, jq: JobQueue):
    # todo: refactor into a method which updates job status
    possibly_running = jq.get_claimed_tasks(job_id)
    summary = identify_orphans(cluster, possibly_running)

    print(f"Resetting status to PENDING for {summary.orphaned_count} orphaned tasks")
    for task in summary.orphaned:
        jq.reset_task(task.task_id)

    return summary
