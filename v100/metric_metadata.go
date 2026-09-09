package v100

// MetricUnits identifies the physical unit a metric's raw value is stored
// in. The dashboard uses this (together with MetricType) to decide how to
// scale and label a metric for display -- e.g. "bytes" auto-scales to GB,
// and a "counter" metric's unit becomes "<units>/s" once turned into a rate
// (only meaningful for a time series -- see MetricType).
type MetricUnits string

const (
	MetricUnitsPercent MetricUnits = "percent"
	MetricUnitsBytes   MetricUnits = "bytes"
	MetricUnitsUsec    MetricUnits = "usec"
	MetricUnitsCount   MetricUnits = "count"
	MetricUnitsSeconds MetricUnits = "seconds"
	// MetricUnitsNone is for a metric with no physical unit to scale or
	// label -- a "categorical" metric's value is a label, not a quantity.
	MetricUnitsNone MetricUnits = "none"
)

// MetricType says how a metric's value(s) should be interpreted.
//
//   - "gauge": the raw value is meaningful on its own at a single point in
//     time (a percentage, a current byte count, a high-water mark). Display
//     it as-is.
//   - "counter": in a time series (MetricSample), the raw value only ever
//     grows, so a single reading says little on its own -- display it as a
//     rate (delta value over delta time) by differencing consecutive
//     samples. In a one-shot summary (ResourceUsage) there's no second
//     reading to difference against, so it's just the cumulative total for
//     the task's whole run -- still meaningful, just not a rate.
//   - "categorical": the value is one of a small set of discrete values
//     (an exit code, a boolean), not a continuous quantity -- distributions
//     should show a count of tasks per distinct value, not percentiles.
type MetricType string

const (
	MetricTypeGauge       MetricType = "gauge"
	MetricTypeCounter     MetricType = "counter"
	MetricTypeCategorical MetricType = "categorical"
)

// MetricMetadata describes one metric a client may plot, independent of any
// particular task's samples. Served by GET /api/v1/metrics so the dashboard
// can build its metric picker and chart labels without hardcoding the
// MetricSample/ResourceUsage field lists.
type MetricMetadata struct {
	// Key matches a field's JSON name on MetricSample and/or ResourceUsage
	// (see InMetricSample/InResourceUsage below), except for two synthetic
	// "host_volume_*" keys: HostVolumes is an array, and each element yields
	// two per-volume metric values (see the dashboard-api.md note on
	// volumes) rather than a single scalar field.
	Key         string      `json:"key"`
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Units       MetricUnits `json:"units"`
	Type        MetricType  `json:"type"`

	// DefaultPosition/ResourceUsageDefaultPosition each recommend to clients
	// whether and where to show this metric by default: nil means "don't
	// show it by default", a lower number means higher priority among the
	// metrics that are shown. Clients may override this; it's only a
	// default.
	//
	// Two separate fields, not one shared across both views, because a
	// metric can be in_metric_sample AND in_resource_usage at once (most
	// container_* counters are) with a different default-visibility answer
	// on each view -- e.g. container_memory_peak_bytes is a default on the
	// per-job distributions page but not on the per-task time series, which
	// defaults to container_memory_current_bytes instead. DefaultPosition is
	// the per-task time series (MetricSample) view's answer;
	// ResourceUsageDefaultPosition is the per-job distributions
	// (ResourceUsage) view's.
	DefaultPosition              *int `json:"default_position,omitempty"`
	ResourceUsageDefaultPosition *int `json:"resource_usage_default_position,omitempty"`

	// InMetricSample is true if this key can appear in a periodic
	// MetricSample (the per-task time series). InResourceUsage is true if it
	// can appear in the one-shot ResourceUsage final summary (distributed
	// across a job's tasks). Most container_* counters are true for both --
	// a client filters this table by whichever struct it's rendering
	// against.
	InMetricSample  bool `json:"in_metric_sample"`
	InResourceUsage bool `json:"in_resource_usage"`
}

// defaultPosition is a small helper so the table below can write defaultPosition(1)
// instead of repeating "&x" boilerplate for each of the handful of metrics
// that have one.
func defaultPosition(n int) *int {
	return &n
}

// MetricMetadataTable describes every metric MetricSample or ResourceUsage
// can carry. It is a static, hand-maintained list -- deliberately not
// derived by reflecting over either struct -- so each entry can carry a real
// name/description/unit instead of just a field name, matching the rest of
// this package's very explicit style (see setContainerCounters).
var MetricMetadataTable = []MetricMetadata{
	{
		Key:             "host_cpu_user_pct",
		Name:            "User CPU",
		Description:     "Percentage of total CPU time across all host cores spent in user mode since the previous sample.",
		Units:           MetricUnitsPercent,
		Type:            MetricTypeGauge,
		DefaultPosition: defaultPosition(1),
		InMetricSample:  true,
	},
	{
		Key:            "host_cpu_system_pct",
		Name:           "System CPU",
		Description:    "Percentage of total CPU time across all host cores spent in kernel mode since the previous sample.",
		Units:          MetricUnitsPercent,
		Type:           MetricTypeGauge,
		InMetricSample: true,
	},
	{
		Key:            "host_cpu_idle_pct",
		Name:           "Idle CPU",
		Description:    "Percentage of total CPU time across all host cores that was idle since the previous sample.",
		Units:          MetricUnitsPercent,
		Type:           MetricTypeGauge,
		InMetricSample: true,
	},
	{
		Key:            "host_cpu_iowait_pct",
		Name:           "I/O-wait CPU",
		Description:    "Percentage of total CPU time across all host cores spent waiting on I/O since the previous sample.",
		Units:          MetricUnitsPercent,
		Type:           MetricTypeGauge,
		InMetricSample: true,
	},
	{
		Key:            "host_cpu_count",
		Name:           "Host CPU Count",
		Description:    "Number of logical cores on the host, for interpreting the CPU percentages above.",
		Units:          MetricUnitsCount,
		Type:           MetricTypeGauge,
		InMetricSample: true,
	},
	{
		Key:            "host_memory_total_bytes",
		Name:           "Host Memory (total)",
		Description:    "Total physical memory on the host.",
		Units:          MetricUnitsBytes,
		Type:           MetricTypeGauge,
		InMetricSample: true,
	},
	{
		Key:            "host_memory_available_bytes",
		Name:           "Host Memory (available)",
		Description:    "Memory available on the host without swapping, per /proc/meminfo's MemAvailable.",
		Units:          MetricUnitsBytes,
		Type:           MetricTypeGauge,
		InMetricSample: true,
	},
	{
		Key:            "host_cpu_stall_some_usec",
		Name:           "Host CPU Stall",
		Description:    "Cumulative microseconds at least one host task was stalled waiting on CPU.",
		Units:          MetricUnitsUsec,
		Type:           MetricTypeCounter,
		InMetricSample: true,
	},
	{
		Key:            "host_cpu_stall_full_usec",
		Name:           "Host CPU Stall (full)",
		Description:    "Cumulative microseconds all non-idle host tasks were stalled waiting on CPU at once.",
		Units:          MetricUnitsUsec,
		Type:           MetricTypeCounter,
		InMetricSample: true,
	},
	{
		Key:            "host_memory_stall_some_usec",
		Name:           "Host Memory Stall",
		Description:    "Cumulative microseconds at least one host task was stalled on memory pressure.",
		Units:          MetricUnitsUsec,
		Type:           MetricTypeCounter,
		InMetricSample: true,
	},
	{
		Key:            "host_memory_stall_full_usec",
		Name:           "Host Memory Stall (full)",
		Description:    "Cumulative microseconds all non-idle host tasks were stalled on memory pressure at once.",
		Units:          MetricUnitsUsec,
		Type:           MetricTypeCounter,
		InMetricSample: true,
	},
	{
		Key:            "host_io_stall_some_usec",
		Name:           "Host I/O Stall",
		Description:    "Cumulative microseconds at least one host task was stalled on I/O.",
		Units:          MetricUnitsUsec,
		Type:           MetricTypeCounter,
		InMetricSample: true,
	},
	{
		Key:            "host_io_stall_full_usec",
		Name:           "Host I/O Stall (full)",
		Description:    "Cumulative microseconds all non-idle host tasks were stalled on I/O at once.",
		Units:          MetricUnitsUsec,
		Type:           MetricTypeCounter,
		InMetricSample: true,
	},
	{
		Key:            "host_volume_total_bytes",
		Name:           "Disk Total",
		Description:    "Total capacity of a host mount point sampled for this task (one series per volume).",
		Units:          MetricUnitsBytes,
		Type:           MetricTypeGauge,
		InMetricSample: true,
	},
	{
		Key:            "host_volume_used_bytes",
		Name:           "Disk Used",
		Description:    "Used space on a host mount point sampled for this task (one series per volume).",
		Units:          MetricUnitsBytes,
		Type:           MetricTypeGauge,
		InMetricSample: true,
	},
	{
		Key:             "container_memory_current_bytes",
		Name:            "Container Memory",
		Description:     "Current memory usage of the task's container, from the cgroup's memory.current.",
		Units:           MetricUnitsBytes,
		Type:            MetricTypeGauge,
		DefaultPosition: defaultPosition(2),
		InMetricSample:  true,
		// Not in ResourceUsage: by the time the final summary is built, the
		// task has already exited, so there's no "current" memory to report.
	},
	{
		Key:                          "container_memory_peak_bytes",
		Name:                         "Container Memory (peak)",
		Description:                  "High-water mark of the container's memory usage over its lifetime so far.",
		Units:                        MetricUnitsBytes,
		Type:                         MetricTypeGauge,
		ResourceUsageDefaultPosition: defaultPosition(2),
		InMetricSample:               true,
		InResourceUsage:              true,
	},
	{
		Key:             "container_memory_limit_bytes",
		Name:            "Container Memory (limit)",
		Description:     "The container's memory limit, from the cgroup's memory.max.",
		Units:           MetricUnitsBytes,
		Type:            MetricTypeGauge,
		InMetricSample:  true,
		InResourceUsage: true,
	},
	{
		Key:             "container_cpu_usage_usec",
		Name:            "Container CPU",
		Description:     "Cumulative CPU time consumed by the container.",
		Units:           MetricUnitsUsec,
		Type:            MetricTypeCounter,
		InMetricSample:  true,
		InResourceUsage: true,
	},
	{
		Key:                          "container_cpu_user_usec",
		Name:                         "Container CPU (user)",
		Description:                  "Cumulative CPU time consumed by the container in user mode.",
		Units:                        MetricUnitsUsec,
		Type:                         MetricTypeCounter,
		ResourceUsageDefaultPosition: defaultPosition(3),
		InMetricSample:               true,
		InResourceUsage:              true,
	},
	{
		Key:                          "container_cpu_system_usec",
		Name:                         "Container CPU (system)",
		Description:                  "Cumulative CPU time consumed by the container in kernel mode.",
		Units:                        MetricUnitsUsec,
		Type:                         MetricTypeCounter,
		ResourceUsageDefaultPosition: defaultPosition(4),
		InMetricSample:               true,
		InResourceUsage:              true,
	},
	{
		Key:             "container_cpu_throttled_usec",
		Name:            "Container CPU Throttled",
		Description:     "Cumulative microseconds the container's CPU was throttled by its cgroup limit.",
		Units:           MetricUnitsUsec,
		Type:            MetricTypeCounter,
		InMetricSample:  true,
		InResourceUsage: true,
	},
	{
		Key:             "container_cpu_throttled_periods",
		Name:            "Container CPU Throttled Periods",
		Description:     "Cumulative count of CPU scheduling periods during which the container was throttled.",
		Units:           MetricUnitsCount,
		Type:            MetricTypeCounter,
		InMetricSample:  true,
		InResourceUsage: true,
	},
	{
		Key:             "container_memory_major_faults",
		Name:            "Container Major Faults",
		Description:     "Cumulative count of major page faults (requiring a disk read) in the container.",
		Units:           MetricUnitsCount,
		Type:            MetricTypeCounter,
		InMetricSample:  true,
		InResourceUsage: true,
	},
	{
		Key:             "container_memory_workingset_refaults",
		Name:            "Container Workingset Refaults",
		Description:     "Cumulative count of page refaults for pages that were recently evicted, in the container.",
		Units:           MetricUnitsCount,
		Type:            MetricTypeCounter,
		InMetricSample:  true,
		InResourceUsage: true,
	},
	{
		Key:             "container_memory_oom_kill_count",
		Name:            "Container OOM Kills",
		Description:     "Cumulative count of processes OOM-killed inside the container's cgroup, including children.",
		Units:           MetricUnitsCount,
		Type:            MetricTypeCounter,
		InMetricSample:  true,
		InResourceUsage: true,
	},
	{
		Key:             "container_pids_peak",
		Name:            "Container Process Count (peak)",
		Description:     "High-water mark of the number of processes/threads in the container's cgroup.",
		Units:           MetricUnitsCount,
		Type:            MetricTypeGauge,
		InMetricSample:  true,
		InResourceUsage: true,
	},
	{
		Key:             "container_cpu_stall_some_usec",
		Name:            "Container CPU Stall",
		Description:     "Cumulative microseconds at least one task in the container was stalled waiting on CPU.",
		Units:           MetricUnitsUsec,
		Type:            MetricTypeCounter,
		InMetricSample:  true,
		InResourceUsage: true,
	},
	{
		Key:             "container_cpu_stall_full_usec",
		Name:            "Container CPU Stall (full)",
		Description:     "Cumulative microseconds all non-idle tasks in the container were stalled waiting on CPU at once.",
		Units:           MetricUnitsUsec,
		Type:            MetricTypeCounter,
		InMetricSample:  true,
		InResourceUsage: true,
	},
	{
		Key:             "container_memory_stall_some_usec",
		Name:            "Container Memory Stall",
		Description:     "Cumulative microseconds at least one task in the container was stalled on memory pressure.",
		Units:           MetricUnitsUsec,
		Type:            MetricTypeCounter,
		InMetricSample:  true,
		InResourceUsage: true,
	},
	{
		Key:             "container_memory_stall_full_usec",
		Name:            "Container Memory Stall (full)",
		Description:     "Cumulative microseconds all non-idle tasks in the container were stalled on memory pressure at once.",
		Units:           MetricUnitsUsec,
		Type:            MetricTypeCounter,
		InMetricSample:  true,
		InResourceUsage: true,
	},
	{
		Key:             "container_io_stall_some_usec",
		Name:            "Container I/O Stall",
		Description:     "Cumulative microseconds at least one task in the container was stalled on I/O.",
		Units:           MetricUnitsUsec,
		Type:            MetricTypeCounter,
		InMetricSample:  true,
		InResourceUsage: true,
	},
	{
		Key:             "container_io_stall_full_usec",
		Name:            "Container I/O Stall (full)",
		Description:     "Cumulative microseconds all non-idle tasks in the container were stalled on I/O at once.",
		Units:           MetricUnitsUsec,
		Type:            MetricTypeCounter,
		InMetricSample:  true,
		InResourceUsage: true,
	},
	{
		Key:                          "container_io_read_bytes",
		Name:                         "Container I/O Read",
		Description:                  "Cumulative bytes read from block devices by the container.",
		Units:                        MetricUnitsBytes,
		Type:                         MetricTypeCounter,
		ResourceUsageDefaultPosition: defaultPosition(5),
		InMetricSample:               true,
		InResourceUsage:              true,
	},
	{
		Key:                          "container_io_write_bytes",
		Name:                         "Container I/O Write",
		Description:                  "Cumulative bytes written to block devices by the container.",
		Units:                        MetricUnitsBytes,
		Type:                         MetricTypeCounter,
		ResourceUsageDefaultPosition: defaultPosition(6),
		InMetricSample:               true,
		InResourceUsage:              true,
	},
	{
		Key:             "container_io_read_ops",
		Name:            "Container I/O Read Ops",
		Description:     "Cumulative count of read operations issued to block devices by the container.",
		Units:           MetricUnitsCount,
		Type:            MetricTypeCounter,
		InMetricSample:  true,
		InResourceUsage: true,
	},
	{
		Key:             "container_io_write_ops",
		Name:            "Container I/O Write Ops",
		Description:     "Cumulative count of write operations issued to block devices by the container.",
		Units:           MetricUnitsCount,
		Type:            MetricTypeCounter,
		InMetricSample:  true,
		InResourceUsage: true,
	},

	// ResourceUsage-only: these describe the one-shot final summary, not the
	// periodic time series, and have no MetricSample equivalent.
	{
		Key:                          "elapsed_seconds",
		Name:                         "Execution Time",
		Description:                  "Wall-clock duration of the task's container, end minus start.",
		Units:                        MetricUnitsSeconds,
		Type:                         MetricTypeGauge,
		ResourceUsageDefaultPosition: defaultPosition(1),
		InResourceUsage:              true,
	},
	{
		Key:             "exit_code",
		Name:            "Exit Code",
		Description:     "The container's process exit code.",
		Units:           MetricUnitsNone,
		Type:            MetricTypeCategorical,
		InResourceUsage: true,
	},
	{
		Key:             "oom_killed",
		Name:            "OOM Killed",
		Description:     "Whether the container's main process was OOM-killed.",
		Units:           MetricUnitsNone,
		Type:            MetricTypeCategorical,
		InResourceUsage: true,
	},
}
