package v100

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// MetricUpdateEventType is the TaskLog document-kind discriminator for metric
// samples. It is a Firestore query dependency (the dashboard filters on it),
// so it is deliberately left unchanged by the metric redesign; the post-exit
// sample is distinguished by MetricSample.Final rather than a separate type.
const MetricUpdateEventType = "metric_update"

// MetricSchemaCurrent identifies the metric field layout. Readers must skip
// samples carrying a different value: an older document decoded into the
// current struct reads as all zeros -- i.e. as a container that used no CPU --
// which is considerably worse than a decode error.
const MetricSchemaCurrent int32 = 2

// HostVolume is disk usage for one mount point, in bytes.
//
// Bytes rather than gigabytes because the field then stays an int64: Firestore's
// DataTo refuses to decode a stored double into an int64 field, so a float unit
// here would permanently fix the wire type. Unit conversion belongs at display.
type HostVolume struct {
	Location   string `firestore:"location" json:"location"`
	TotalBytes int64  `firestore:"total_bytes" json:"total_bytes"`
	UsedBytes  int64  `firestore:"used_bytes" json:"used_bytes"`
}

// MetricSample is one metric_update entry in the events file, the TaskLog
// collection, and the dashboard API.
//
// Fields are named <scope>_<resource>_<measure>_<unit> with no abbreviations,
// and the container_* fields carry the same names in ResourceUsage, so a
// periodic sample and the final post-exit summary are directly comparable.
//
// PSI stalls are stored as cumulative total= microseconds rather than the
// avg10/avg60/avg300 the kernel also offers. An average has a ~10s ramp, which
// makes it meaningless for a task that runs for three seconds, whereas totals
// difference exactly over any interval and compose with the post-exit read of
// the same counter -- so the last sample and the final read leave no gap.
//
// Any value that could not be read is metricUnavailable (-1), which is
// deliberately distinct from a genuine zero.
type MetricSample struct {
	TaskID       string    `firestore:"task_id" json:"task_id"`
	Type         string    `firestore:"type" json:"type"`
	Timestamp    time.Time `firestore:"timestamp" json:"timestamp"`
	Expiry       time.Time `firestore:"expiry" json:"expiry"`
	MetricSchema int32     `firestore:"metric_schema" json:"metric_schema"`
	Seq          int32     `firestore:"seq" json:"seq"`
	Final        bool      `firestore:"final" json:"final"`

	// Host CPU, as a percentage of total CPU time across all cores over the
	// interval since the previous sample.
	HostCPUUserPct   float64 `firestore:"host_cpu_user_pct" json:"host_cpu_user_pct"`
	HostCPUSystemPct float64 `firestore:"host_cpu_system_pct" json:"host_cpu_system_pct"`
	HostCPUIdlePct   float64 `firestore:"host_cpu_idle_pct" json:"host_cpu_idle_pct"`
	HostCPUIowaitPct float64 `firestore:"host_cpu_iowait_pct" json:"host_cpu_iowait_pct"`

	HostMemoryTotalBytes     int64 `firestore:"host_memory_total_bytes" json:"host_memory_total_bytes"`
	HostMemoryAvailableBytes int64 `firestore:"host_memory_available_bytes" json:"host_memory_available_bytes"`

	HostCPUStallSomeUSec    int64 `firestore:"host_cpu_stall_some_usec" json:"host_cpu_stall_some_usec"`
	HostCPUStallFullUSec    int64 `firestore:"host_cpu_stall_full_usec" json:"host_cpu_stall_full_usec"`
	HostMemoryStallSomeUSec int64 `firestore:"host_memory_stall_some_usec" json:"host_memory_stall_some_usec"`
	HostMemoryStallFullUSec int64 `firestore:"host_memory_stall_full_usec" json:"host_memory_stall_full_usec"`
	HostIOStallSomeUSec     int64 `firestore:"host_io_stall_some_usec" json:"host_io_stall_some_usec"`
	HostIOStallFullUSec     int64 `firestore:"host_io_stall_full_usec" json:"host_io_stall_full_usec"`

	HostVolumes []HostVolume `firestore:"host_volumes" json:"host_volumes,omitempty"`

	// ContainerPresent is false when the container's cgroup could not be
	// located: it has not been created yet (normal for the first sample of a
	// task), it has already been torn down, or this task runs without a
	// container at all. Every container_* field is unavailable in that case.
	ContainerPresent bool `firestore:"container_present" json:"container_present"`

	ContainerMemoryCurrentBytes int64 `firestore:"container_memory_current_bytes" json:"container_memory_current_bytes"`
	ContainerMemoryPeakBytes    int64 `firestore:"container_memory_peak_bytes" json:"container_memory_peak_bytes"`
	ContainerMemoryLimitBytes   int64 `firestore:"container_memory_limit_bytes" json:"container_memory_limit_bytes"`

	ContainerCPUUsageUSec  int64 `firestore:"container_cpu_usage_usec" json:"container_cpu_usage_usec"`
	ContainerCPUUserUSec   int64 `firestore:"container_cpu_user_usec" json:"container_cpu_user_usec"`
	ContainerCPUSystemUSec int64 `firestore:"container_cpu_system_usec" json:"container_cpu_system_usec"`

	// These are cumulative or high-water counters, so they are carried in
	// every sample rather than only in the final one: knowing *when* a task
	// started being throttled, or when its children were OOM-killed, is more
	// useful than only learning that it happened. Reading them here also means
	// the final summary is a pure projection of the last sample and needs no
	// second pass over the cgroup.
	ContainerCPUThrottledUSec         int64 `firestore:"container_cpu_throttled_usec" json:"container_cpu_throttled_usec"`
	ContainerCPUThrottledPeriods      int64 `firestore:"container_cpu_throttled_periods" json:"container_cpu_throttled_periods"`
	ContainerMemoryMajorFaults        int64 `firestore:"container_memory_major_faults" json:"container_memory_major_faults"`
	ContainerMemoryWorkingsetRefaults int64 `firestore:"container_memory_workingset_refaults" json:"container_memory_workingset_refaults"`
	ContainerMemoryOOMKillCount       int64 `firestore:"container_memory_oom_kill_count" json:"container_memory_oom_kill_count"`
	ContainerPidsPeak                 int64 `firestore:"container_pids_peak" json:"container_pids_peak"`

	ContainerCPUStallSomeUSec    int64 `firestore:"container_cpu_stall_some_usec" json:"container_cpu_stall_some_usec"`
	ContainerCPUStallFullUSec    int64 `firestore:"container_cpu_stall_full_usec" json:"container_cpu_stall_full_usec"`
	ContainerMemoryStallSomeUSec int64 `firestore:"container_memory_stall_some_usec" json:"container_memory_stall_some_usec"`
	ContainerMemoryStallFullUSec int64 `firestore:"container_memory_stall_full_usec" json:"container_memory_stall_full_usec"`
	ContainerIOStallSomeUSec     int64 `firestore:"container_io_stall_some_usec" json:"container_io_stall_some_usec"`
	ContainerIOStallFullUSec     int64 `firestore:"container_io_stall_full_usec" json:"container_io_stall_full_usec"`

	ContainerIOReadBytes  int64 `firestore:"container_io_read_bytes" json:"container_io_read_bytes"`
	ContainerIOWriteBytes int64 `firestore:"container_io_write_bytes" json:"container_io_write_bytes"`
	ContainerIOReadOps    int64 `firestore:"container_io_read_ops" json:"container_io_read_ops"`
	ContainerIOWriteOps   int64 `firestore:"container_io_write_ops" json:"container_io_write_ops"`
}

type cpuStats struct {
	User   int64
	System int64
	Idle   int64
	Iowait int64
}

// cpuPercentages converts the raw jiffie counters from two /proc/stat
// snapshots into percentages of total CPU time (summed across all cores)
// spent in each state between the two samples.
func cpuPercentages(prev, cur *cpuStats) (user, system, idle, iowait float64) {
	deltaUser := float64(cur.User - prev.User)
	deltaSystem := float64(cur.System - prev.System)
	deltaIdle := float64(cur.Idle - prev.Idle)
	deltaIowait := float64(cur.Iowait - prev.Iowait)

	total := deltaUser + deltaSystem + deltaIdle + deltaIowait
	if total <= 0 {
		return 0, 0, 0, 0
	}

	return deltaUser / total * 100,
		deltaSystem / total * 100,
		deltaIdle / total * 100,
		deltaIowait / total * 100
}

func getCPUStats() (*cpuStats, error) {
	data, err := os.ReadFile(filepath.Join(procRoot, "stat"))
	if err != nil {
		return nil, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 6 {
			continue
		}
		user, _ := strconv.ParseInt(fields[1], 10, 64)
		nice, _ := strconv.ParseInt(fields[2], 10, 64)
		system, _ := strconv.ParseInt(fields[3], 10, 64)
		idle, _ := strconv.ParseInt(fields[4], 10, 64)
		iowait, _ := strconv.ParseInt(fields[5], 10, 64)
		return &cpuStats{User: user + nice, System: system, Idle: idle, Iowait: iowait}, nil
	}
	return nil, nil
}

type systemMemory struct {
	Total     int64
	Available int64
	Free      int64
}

func getSystemMemory() (*systemMemory, error) {
	data, err := os.ReadFile(filepath.Join(procRoot, "meminfo"))
	if err != nil {
		return nil, err
	}
	m := &systemMemory{}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		val, _ := strconv.ParseInt(fields[1], 10, 64)
		val *= 1024 // kB → bytes
		switch fields[0] {
		case "MemTotal:":
			m.Total = val
		case "MemAvailable:":
			m.Available = val
		case "MemFree:":
			m.Free = val
		}
	}
	return m, nil
}

// getHostVolumes reports disk usage in bytes for each distinct path given.
// Paths that cannot be stat'ed are omitted rather than reported as zero.
func getHostVolumes(paths ...string) []HostVolume {
	seen := make(map[string]bool)
	var volumes []HostVolume
	for _, p := range paths {
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		var stat syscall.Statfs_t
		if err := syscall.Statfs(p, &stat); err != nil {
			continue
		}
		total := int64(stat.Blocks * uint64(stat.Bsize))
		free := int64(stat.Bfree * uint64(stat.Bsize))
		volumes = append(volumes, HostVolume{Location: p, TotalBytes: total, UsedBytes: total - free})
	}
	return volumes
}

// containerMetrics resolves a container's cgroup and reads its counters.
//
// present is false when the cgroup could not be located, which is a normal
// condition rather than an error: the container may not have been created yet
// (expected for the first sample of every task), it may already have been torn
// down, or the task may be running without a container at all.
func containerMetrics(cg *containerCgroup) (counters containerCounters, present bool) {
	if cg == nil {
		return unavailableContainerCounters(), false
	}
	dir := cg.resolve()
	if dir == "" {
		return unavailableContainerCounters(), false
	}
	return readContainerCounters(dir), true
}

// setContainerCounters copies one cgroup read onto the sample.
func (s *MetricSample) setContainerCounters(c containerCounters, present bool) {
	s.ContainerPresent = present

	s.ContainerMemoryCurrentBytes = c.MemoryCurrentBytes
	s.ContainerMemoryPeakBytes = c.MemoryPeakBytes
	s.ContainerMemoryLimitBytes = c.MemoryLimitBytes

	s.ContainerCPUUsageUSec = c.CPUUsageUSec
	s.ContainerCPUUserUSec = c.CPUUserUSec
	s.ContainerCPUSystemUSec = c.CPUSystemUSec
	s.ContainerCPUThrottledUSec = c.CPUThrottledUSec
	s.ContainerCPUThrottledPeriods = c.CPUThrottledPeriods

	s.ContainerMemoryMajorFaults = c.MemoryMajorFaults
	s.ContainerMemoryWorkingsetRefaults = c.MemoryWorkingsetRefaults
	s.ContainerMemoryOOMKillCount = c.MemoryOOMKillCount
	s.ContainerPidsPeak = c.PidsPeak

	s.ContainerCPUStallSomeUSec, s.ContainerCPUStallFullUSec = c.CPUStall.SomeUSec, c.CPUStall.FullUSec
	s.ContainerMemoryStallSomeUSec, s.ContainerMemoryStallFullUSec = c.MemoryStall.SomeUSec, c.MemoryStall.FullUSec
	s.ContainerIOStallSomeUSec, s.ContainerIOStallFullUSec = c.IOStall.SomeUSec, c.IOStall.FullUSec

	s.ContainerIOReadBytes = c.IOReadBytes
	s.ContainerIOWriteBytes = c.IOWriteBytes
	s.ContainerIOReadOps = c.IOReadOps
	s.ContainerIOWriteOps = c.IOWriteOps
}

// collectMetricSample takes one host-and-container metric sample for a task.
//
// prev is the cpuStats snapshot from the previous sample (nil for the first),
// used to turn the cumulative /proc/stat counters into a percentage of CPU
// time spent in each state since then. cg may be nil for tasks that run
// without a container. The returned cpuStats is what the caller should pass as
// prev next time.
func collectMetricSample(taskID, workDir string, prev *cpuStats, cg *containerCgroup, seq int32, final bool) (*MetricSample, *cpuStats) {
	now := time.Now()
	s := &MetricSample{
		TaskID:       taskID,
		Type:         MetricUpdateEventType,
		Timestamp:    now,
		Expiry:       now.Add(taskEventLogTTL),
		MetricSchema: MetricSchemaCurrent,
		Seq:          seq,
		Final:        final,
		HostVolumes:  getHostVolumes("/", workDir),

		HostMemoryTotalBytes:     metricUnavailable,
		HostMemoryAvailableBytes: metricUnavailable,
	}

	cur, err := getCPUStats()
	if err != nil || cur == nil {
		cur = prev
	} else if prev != nil {
		s.HostCPUUserPct, s.HostCPUSystemPct, s.HostCPUIdlePct, s.HostCPUIowaitPct = cpuPercentages(prev, cur)
	}

	if sysMem, err := getSystemMemory(); err == nil {
		s.HostMemoryTotalBytes = sysMem.Total
		s.HostMemoryAvailableBytes = sysMem.Available
	}

	cpuStall := readPressureFile(filepath.Join(procRoot, "pressure", "cpu"))
	memoryStall := readPressureFile(filepath.Join(procRoot, "pressure", "memory"))
	ioStall := readPressureFile(filepath.Join(procRoot, "pressure", "io"))
	s.HostCPUStallSomeUSec, s.HostCPUStallFullUSec = cpuStall.SomeUSec, cpuStall.FullUSec
	s.HostMemoryStallSomeUSec, s.HostMemoryStallFullUSec = memoryStall.SomeUSec, memoryStall.FullUSec
	s.HostIOStallSomeUSec, s.HostIOStallFullUSec = ioStall.SomeUSec, ioStall.FullUSec

	s.setContainerCounters(containerMetrics(cg))

	return s, cur
}

// MetricSampler drives metric collection outside the normal per-task polling
// loop in OpenTaskEventLog, for callers in other packages such as "sparkles
// dev test-profile-command".
//
// It exists as a type rather than a bare function because sampling is
// stateful: the CPU baseline and sequence number carry across samples, and the
// container's cgroup handle caches its resolved path so repeated samples do
// not re-run docker inspect.
type MetricSampler struct {
	taskID  string
	workDir string
	cgroup  *containerCgroup
	prevCPU *cpuStats
	seq     int32
}

// NewMetricSampler returns a sampler for a task. containerName may be empty
// for a task with no container, in which case only host metrics are reported.
func NewMetricSampler(taskID, workDir, containerName string) *MetricSampler {
	m := &MetricSampler{taskID: taskID, workDir: workDir}
	if containerName != "" {
		m.cgroup = newContainerCgroup(containerName)
	}
	// Seed the CPU baseline so the first sample covers the interval since the
	// sampler was created rather than being skipped for lack of a baseline.
	m.prevCPU, _ = getCPUStats()
	return m
}

// Sample takes one sample, advancing the sampler's CPU baseline and sequence.
func (m *MetricSampler) Sample(final bool) *MetricSample {
	s, cur := collectMetricSample(m.taskID, m.workDir, m.prevCPU, m.cgroup, m.seq, final)
	m.prevCPU = cur
	m.seq++
	return s
}

// NextMetricsDelay exposes the adaptive sampling schedule so external drivers
// can match the cadence the worker actually uses.
func NextMetricsDelay(cur time.Duration) time.Duration {
	return nextMetricsDelay(cur)
}
