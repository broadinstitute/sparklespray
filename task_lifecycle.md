interface Event {
type: string
timestamp: string
expiry: string
}

interface TaskEvent extends Event {
task_id: string
job_id: string
}

interface TaskClaimedEvent extends TaskEvent {
type: "task_claimed"
}

interface TaskStagedEvent extends TaskEvent {
type: "task_staged"
}

interface TaskCompleteEvent extends TaskEvent {
type: "task_complete"
}

interface TaskOrphanedEvent extends TaskEvent {
type: "task_orphaned"
}

interface TaskStartedEvent extends TaskEvent {
type: "task_started"
}

interface TaskExecutedEvent extends TaskEvent {
type: "task_executed"
exit_code: int
max_mem_in_gb: float
}

interface TaskFailedEvent extends TaskEvent {
type: "task_failed"
failure_reason: string
}

# The lifecycle of a task is:

# 0. Intially, all tasks have status "pending" until they are claimed

# by a worker.

# 1. task_claimed: A worker has claimed this task. The task's state has transitioned from "pending" to "claimed. The first step of the worker is first downloading any files which are needed by the task.

# 2. task_staged: The worker has finished downloading and staging

# any required files. The execution of the task then starts.

# 3. task_executed: Once execution of a task has completed, the

# worker starts uploading all new files.

# 4. task_completed: This event occurs after the upload completes and the task is assigned the terminal status of "completed"

#

# Now at any point after the task is claimed, it's possible for the

# worker which had claimed the task to be interrupted, resulting in

# a task_orphaned event which causes the tasks's status to be reset back to pending. It then restarts back at step 0 of the life cycle until the task is claimed again.

#

# Similarly, at any point after a task is claimed, a task_failed

# event might occur. At which time, the task transitions to the

# terminal "failed" state.
