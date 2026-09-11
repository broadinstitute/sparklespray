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
  // true for a normal, self-reported shutdown; false when task recovery
  // marked the worker a zombie (heartbeat expired without a clean shutdown).
  cleanly_terminated: boolean;
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

export interface WorkpoolStateChangeEvent extends BaseEvent {
  type: "workpool_state_change";
  workpool_id: string;
  new_state: string;
  state_message: string;
}

export interface BatchFailedEvent extends BaseEvent {
  type: "batch_failed";
  workpool_id: string;
  state_message: string;
}

export interface BatchSucceededEvent extends BaseEvent {
  type: "batch_succeeded";
  workpool_id: string;
}

export interface WorkpoolIncidentEvent extends BaseEvent {
  type: "workpool_incident";
  workpool_id: string;
  state_message: string;
  /** Machine-readable category of the anomaly, e.g. "zombie". */
  incident_type?: string;
}

export type AnyTaskEvent = TaskStateUpdateEvent;

export type AnyEvent =
  | WorkerStartedEvent
  | WorkerStoppedEvent
  | JobCreatedEvent
  | JobTerminatedEvent
  | TaskStateUpdateEvent
  | WorkpoolStateChangeEvent
  | BatchFailedEvent
  | BatchSucceededEvent
  | WorkpoolIncidentEvent;

// A raw event as returned by GET /api/v1/events, before it's known which
// event type it is. All fields beyond the base ones are optional since
// which are populated depends on `type`. Used by generic event-log UI (the
// Events tab) that doesn't need to know each event shape up front.
export interface RawEvent extends BaseEvent {
  worker_id?: string;
  workpool_id?: string;
  task_id?: string;
  job_id?: string;
  old_state?: string;
  new_state?: string;
  state_message?: string;
  cleanly_terminated?: boolean;
  incident_type?: string;
}

export interface BackendJobSummary {
  job_id: string;
  workpool_id: string;
  created_at: string;
  status: string;
  /** The job's overall state (pending/in_progress/.../success/error/failed/
   * killed) -- see monitor.JobStatus on the Go side. Named separately from
   * the (unused, likely stale) `status` field above since that's what the
   * backend actually serializes this as. */
  state: string;
  tasks: { state: string; count: number }[];
  labels: { name: string; value: string }[];
  expiry: string;
  /** Timestamp of the monitor's most recent write to this job's summary --
   * for a job in a terminal state, this is effectively when it finished. */
  last_updated: string;
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
  /** Empty when the workpool runs its VMs in the backend's own project. */
  project_id: string;
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
  max_workers_per_request: number;
  max_zombies_before_abort: number;
  max_consecutive_failed_batches: number;
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

// MetricMetadata mirrors v100.MetricMetadata (GET /api/v1/metrics): one
// record per metric a metric_update entry (MetricSample) and/or a task's
// resource_usage (ResourceUsage) may carry, described independently of any
// particular task's samples so the UI can build a metric picker and chart
// labels without hardcoding either struct's field list.
export interface MetricMetadata {
  key: string;
  name: string;
  description: string;
  units: "percent" | "bytes" | "usec" | "count" | "seconds" | "none";
  type: "gauge" | "counter" | "categorical";
  /** Default visibility for the per-task time series view (MetricsPanel).
   * Absent/undefined means "don't show this metric by default". Lower is
   * higher priority among the metrics that are shown. A metric can be
   * in_metric_sample AND in_resource_usage at once (most container_*
   * counters are) with a different default answer on each view -- see
   * resource_usage_default_position. */
  default_position?: number;
  /** Default visibility for the per-job distributions view (PerfOverview),
   * same semantics as default_position but for that view. */
  resource_usage_default_position?: number;
  /** Which of MetricSample/ResourceUsage this key can appear on -- filter by
   * whichever one a given view is rendering against. */
  in_metric_sample: boolean;
  in_resource_usage: boolean;
}

// MetricValue is one metric's raw reading within a single ResourceDataPoint.
// props distinguishes multiple instances of the same metric key sharing one
// sample -- currently only used for per-volume disk metrics, where props is
// e.g. { location: "/tmp" }.
export interface MetricValue {
  key: string;
  value: number;
  props?: Record<string, string>;
}

export interface ResourceDataPoint {
  time: number;
  label: string;
  metrics: MetricValue[];
}

export interface StdoutLine {
  time: number;
  text: string;
}

// ResourceUsageSummary mirrors v100.ResourceUsage's current field names.
// elapsed_seconds/exit_code/oom_killed come from `docker inspect` and are
// always present when resource_usage is present at all; the container_*
// cgroup counters are optional/nullable (server-side nil, e.g. because the
// container's cgroup was torn down before the final read) rather than the
// old `-1` sentinel.
export interface ResourceUsageSummary {
  elapsed_seconds: number;
  exit_code: number;
  oom_killed: boolean;
  container_memory_peak_bytes?: number;
  container_memory_limit_bytes?: number;
  container_memory_major_faults?: number;
  container_memory_workingset_refaults?: number;
  container_memory_oom_kill_count?: number;
  container_cpu_usage_usec?: number;
  container_cpu_user_usec?: number;
  container_cpu_system_usec?: number;
  container_cpu_throttled_usec?: number;
  container_cpu_throttled_periods?: number;
  container_cpu_stall_some_usec?: number;
  container_cpu_stall_full_usec?: number;
  container_memory_stall_some_usec?: number;
  container_memory_stall_full_usec?: number;
  container_io_stall_some_usec?: number;
  container_io_stall_full_usec?: number;
  container_io_read_bytes?: number;
  container_io_write_bytes?: number;
  container_io_read_ops?: number;
  container_io_write_ops?: number;
  container_pids_peak?: number;
  [key: string]: unknown;
}

export interface TaskSummaryRecord {
  task_id: string;
  task_index: number;
  status: string;
  exit_code: number | null;
  resource_usage?: ResourceUsageSummary;
}
