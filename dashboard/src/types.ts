export interface BaseEvent {
  event_id: string;
  type: string;
  timestamp: string;
  expiry: string;
}

export interface WorkerEvent extends BaseEvent {
  worker_id: string;
  workpool_id: string;
}

export interface WorkerStartedEvent extends WorkerEvent {
  type: "worker_started";
}

export interface WorkerStoppedEvent extends WorkerEvent {
  type: "worker_stopped";
}

export interface JobCreatedEvent extends BaseEvent {
  type: "job_created";
  job_id: string;
  workpool_id: string;
}

export interface JobTerminatedEvent extends BaseEvent {
  type: "job_terminated";
  job_id: string;
  workpool_id: string;
}

export interface TaskStateUpdateEvent extends BaseEvent {
  type: "task_state_update";
  task_id: string;
  job_id: string;
  old_state: string;
  new_state: string;
}

export type AnyTaskEvent = TaskStateUpdateEvent;

export type AnyEvent =
  | WorkerStartedEvent
  | WorkerStoppedEvent
  | JobCreatedEvent
  | JobTerminatedEvent
  | TaskStateUpdateEvent;

export interface BackendJobSummary {
  job_id: string;
  workpool_id: string;
  created_at: string;
  status: string;
  tasks: { state: string; count: number }[];
  labels: { name: string; value: string }[];
  expiry: string;
  // computed client-side from tasks[]
  taskCount: number;
  successCount: number;
  failureCount: number;
}

export interface JobDetail {
  job_id: string;
  name: string;
  workpool_id: string;
  created_at: string;
  task_count: number;
  labels: { name: string; value: string }[];
  metadata: Record<string, string>;
}

export interface JobSummaryHistoryEntry {
  job_id: string;
  workpool_id: string;
  timestamp: string;
  status: string;
  tasks: { state: string; count: number }[];
}

export interface WorkPoolDetail {
  workpool_id: string;
  machine_type: string;
  region: string;
  zones: string[];
  root_dir: string;
  sparkles_worker_gcs_path: string;
  resources: { name: string; value: number }[];
  empty_volumes: { mount_point: string; type: string; size_in_gb: number }[];
  labels: { name: string; value: string }[];
  max_worker_count: number;
  max_preemptible_worker_attempts: number;
  state: string;
  state_message: string;
  last_incident_at: string | null;
  incident_count: number;
  expiry: string;
}

export interface WorkPoolSummaryHistoryEntry {
  workpool_id: string;
  timestamp: string;
  expected_preemptible_workers: number;
  expected_nonpreemptible_workers: number;
  unhealthy_batch_count: number;
  batch_api_request_counts: { status: string; count: number }[];
  preemptible_workers: { status: string; count: number }[];
  nonpreemptible_workers: { status: string; count: number }[];
  tasks: { status: string; count: number }[];
}

export interface TimeSeriesPoint {
  time: number;
  label: string;
  value: number;
}

export interface VolumeDataPoint {
  location: string;
  totalGb: number;
  usedGb: number;
}

export interface ResourceDataPoint {
  time: number;
  label: string;
  processCount: number;
  totalMemoryGb: number;
  totalDataGb: number;
  totalSharedGb: number;
  totalResidentGb: number;
  cpuUser: number;
  cpuSystem: number;
  cpuIdle: number;
  cpuIowait: number;
  memTotalGb: number;
  memAvailableGb: number;
  memFreeGb: number;
  memPressureSomeAvg10: number;
  memPressureFullAvg10: number;
  volumes: VolumeDataPoint[];
}

export interface StdoutLine {
  time: number;
  text: string;
}

export interface TaskSummaryRecord {
  task_id: string;
  task_index: number;
  status: string;
  exit_code: number | null;
  resource_usage?: {
    elapsed_seconds: number;
    max_memory_bytes: number;
    cpu_user_usec: number;
    cpu_system_usec: number;
    block_read_bytes: number;
    block_write_bytes: number;
    oom_killed: boolean;
  };
}
