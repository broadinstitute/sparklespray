export interface BaseEvent {
  id: string;
  type: string;
  timestamp: string;
  expiry: string;
}

export interface TaskEvent extends BaseEvent {
  task_id: string;
  job_id: string;
  cluster_id: string;
}

export interface ClusterEvent extends BaseEvent {
  cluster_id: string;
}

export interface ClusterStartedEvent extends ClusterEvent {
  type: "cluster_started";
}

export interface ClusterStoppedEvent extends ClusterEvent {
  type: "cluster_stopped";
}

export interface WorkerEvent extends ClusterEvent {
  worker_id: string;
}

export interface WorkerStartedEvent extends WorkerEvent {
  type: "worker_started";
}

export interface WorkerStoppedEvent extends WorkerEvent {
  type: "worker_stopped";
}

export interface JobStartedEvent extends BaseEvent {
  type: "job_started";
  job_id: string;
  cluster_id: string;
  task_count: number;
}

export interface JobKilledEvent extends BaseEvent {
  type: "job_killed";
  job_id: string;
}

export interface TaskClaimedEvent extends TaskEvent {
  type: "task_claimed";
}

export interface TaskExecStartedEvent extends TaskEvent {
  type: "task_exec_started";
}

export interface TaskExecCompleteEvent extends TaskEvent {
  type: "task_exec_complete";
  exit_code: number;
}

export interface TaskCompleteEvent extends TaskEvent {
  type: "task_complete";
  download_bytes: number;
  upload_bytes: number;
  exit_code: number;
  max_mem_in_gb: number;
  user_cpu_sec: number;
  system_cpu_sec: number;
  max_memory_bytes: number;
  shared_memory_bytes: number;
  unshared_memory_bytes: number;
  block_input_ops: number;
  block_output_ops: number;
}

export interface TaskOrphanedEvent extends TaskEvent {
  type: "task_orphaned";
}

export interface TaskFailedEvent extends TaskEvent {
  type: "task_failed";
  failure_reason: string;
}

export interface TaskKilledEvent extends TaskEvent {
  type: "task_killed";
}

export type AnyTaskEvent =
  | TaskClaimedEvent
  | TaskExecStartedEvent
  | TaskExecCompleteEvent
  | TaskCompleteEvent
  | TaskOrphanedEvent
  | TaskFailedEvent
  | TaskKilledEvent;

export type AnyEvent =
  | ClusterStartedEvent
  | ClusterStoppedEvent
  | WorkerStartedEvent
  | WorkerStoppedEvent
  | JobStartedEvent
  | JobKilledEvent
  | AnyTaskEvent;
