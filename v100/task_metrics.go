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
//
// Bumped to 3 when the unavailable-metric sentinel changed from a literal -1
// to an omitted/nil field: a schema-2 document decoded into the current
// struct would read its -1s as real (very negative) values rather than as
// "unavailable".
const MetricSchemaCurrent int32 = 3

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
// Any value that could not be read is a nil pointer, omitted from both the
// JSON and Firestore encodings, which is deliberately distinct from a
// genuine zero. There is no separate "container present" flag: a task that
// never had a container, or whose container's cgroup could not be located,
// simply has every container_* field nil -- the same convention as any other
// unavailable metric, rather than a second signal that can (and, in a known
// bug this replaced, did) disagree with it.
type MetricSample struct {
	TaskID       string    `firestore:"task_id" json:"task_id"`
	Type         string    `firestore:"type" json:"type"`
	Timestamp    time.Time `firestore:"timestamp" json:"timestamp"`
	Expiry       time.Time `firestore:"expiry" json:"expiry"`
	MetricSchema int32     `firestore:"metric_schema" json:"metric_schema"`
	Seq          int32     `firestore:"seq" json:"seq"`
	Final        bool      `firestore:"final" json:"final"`

	// Host CPU, as a percentage of total CPU time across all cores over the
	// interval since the previous sample. Nil for the first sample, which has
	// no previous /proc/stat snapshot to difference against.
	HostCPUUserPct   *float64 `firestore:"host_cpu_user_pct,omitempty" json:"host_cpu_user_pct,omitempty"`
	HostCPUSystemPct *float64 `firestore:"host_cpu_system_pct,omitempty" json:"host_cpu_system_pct,omitempty"`
	HostCPUIdlePct   *float64 `firestore:"host_cpu_idle_pct,omitempty" json:"host_cpu_idle_pct,omitempty"`
	HostCPUIowaitPct *float64 `firestore:"host_cpu_iowait_pct,omitempty" json:"host_cpu_iowait_pct,omitempty"`

	// HostCPUCount is the number of logical cores backing the percentages
	// above, so e.g. "25% user" can be read as a fraction of however many
	// cores the host actually has rather than assumed to be one.
	HostCPUCount *int64 `firestore:"host_cpu_count,omitempty" json:"host_cpu_count,omitempty"`

	HostMemoryTotalBytes     *int64 `firestore:"host_memory_total_bytes,omitempty" json:"host_memory_total_bytes,omitempty"`
	HostMemoryAvailableBytes *int64 `firestore:"host_memory_available_bytes,omitempty" json:"host_memory_available_bytes,omitempty"`

	HostCPUStallSomeUSec    *int64 `firestore:"host_cpu_stall_some_usec,omitempty" json:"host_cpu_stall_some_usec,omitempty"`
	HostCPUStallFullUSec    *int64 `firestore:"host_cpu_stall_full_usec,omitempty" json:"host_cpu_stall_full_usec,omitempty"`
	HostMemoryStallSomeUSec *int64 `firestore:"host_memory_stall_some_usec,omitempty" json:"host_memory_stall_some_usec,omitempty"`
	HostMemoryStallFullUSec *int64 `firestore:"host_memory_stall_full_usec,omitempty" json:"host_memory_stall_full_usec,omitempty"`
	HostIOStallSomeUSec     *int64 `firestore:"host_io_stall_some_usec,omitempty" json:"host_io_stall_some_usec,omitempty"`
	HostIOStallFullUSec     *int64 `firestore:"host_io_stall_full_usec,omitempty" json:"host_io_stall_full_usec,omitempty"`

	HostVolumes []HostVolume `firestore:"host_volumes" json:"host_volumes,omitempty"`

	ContainerMemoryCurrentBytes *int64 `firestore:"container_memory_current_bytes,omitempty" json:"container_memory_current_bytes,omitempty"`
	ContainerMemoryPeakBytes    *int64 `firestore:"container_memory_peak_bytes,omitempty" json:"container_memory_peak_bytes,omitempty"`
	ContainerMemoryLimitBytes   *int64 `firestore:"container_memory_limit_bytes,omitempty" json:"container_memory_limit_bytes,omitempty"`

	ContainerCPUUsageUSec  *int64 `firestore:"container_cpu_usage_usec,omitempty" json:"container_cpu_usage_usec,omitempty"`
	ContainerCPUUserUSec   *int64 `firestore:"container_cpu_user_usec,omitempty" json:"container_cpu_user_usec,omitempty"`
	ContainerCPUSystemUSec *int64 `firestore:"container_cpu_system_usec,omitempty" json:"container_cpu_system_usec,omitempty"`

	// These are cumulative or high-water counters, so they are carried in
	// every sample rather than only in the final one: knowing *when* a task
	// started being throttled, or when its children were OOM-killed, is more
	// useful than only learning that it happened. Reading them here also means
	// the final summary is a pure projection of the last sample and needs no
	// second pass over the cgroup.
	ContainerCPUThrottledUSec         *int64 `firestore:"container_cpu_throttled_usec,omitempty" json:"container_cpu_throttled_usec,omitempty"`
	ContainerCPUThrottledPeriods      *int64 `firestore:"container_cpu_throttled_periods,omitempty" json:"container_cpu_throttled_periods,omitempty"`
	ContainerMemoryMajorFaults        *int64 `firestore:"container_memory_major_faults,omitempty" json:"container_memory_major_faults,omitempty"`
	ContainerMemoryWorkingsetRefaults *int64 `firestore:"container_memory_workingset_refaults,omitempty" json:"container_memory_workingset_refaults,omitempty"`
	ContainerMemoryOOMKillCount       *int64 `firestore:"container_memory_oom_kill_count,omitempty" json:"container_memory_oom_kill_count,omitempty"`
	ContainerPidsPeak                 *int64 `firestore:"container_pids_peak,omitempty" json:"container_pids_peak,omitempty"`

	ContainerCPUStallSomeUSec    *int64 `firestore:"container_cpu_stall_some_usec,omitempty" json:"container_cpu_stall_some_usec,omitempty"`
	ContainerCPUStallFullUSec    *int64 `firestore:"container_cpu_stall_full_usec,omitempty" json:"container_cpu_stall_full_usec,omitempty"`
	ContainerMemoryStallSomeUSec *int64 `firestore:"container_memory_stall_some_usec,omitempty" json:"container_memory_stall_some_usec,omitempty"`
	ContainerMemoryStallFullUSec *int64 `firestore:"container_memory_stall_full_usec,omitempty" json:"container_memory_stall_full_usec,omitempty"`
	ContainerIOStallSomeUSec     *int64 `firestore:"container_io_stall_some_usec,omitempty" json:"container_io_stall_some_usec,omitempty"`
	ContainerIOStallFullUSec     *int64 `firestore:"container_io_stall_full_usec,omitempty" json:"container_io_stall_full_usec,omitempty"`

	ContainerIOReadBytes  *int64 `firestore:"container_io_read_bytes,omitempty" json:"container_io_read_bytes,omitempty"`
	ContainerIOWriteBytes *int64 `firestore:"container_io_write_bytes,omitempty" json:"container_io_write_bytes,omitempty"`
	ContainerIOReadOps    *int64 `firestore:"container_io_read_ops,omitempty" json:"container_io_read_ops,omitempty"`
	ContainerIOWriteOps   *int64 `firestore:"container_io_write_ops,omitempty" json:"container_io_write_ops,omitempty"`
}

// int64OrNA converts a metricUnavailable-sentineled int64 read from the
// cgroup/proc layer into the pointer form MetricSample exposes: nil when the
// value could not be read, a pointer to it otherwise.
func int64OrNA(v int64) *int64 {
	if v == metricUnavailable {
		return nil
	}
	return &v
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

// getCPUCount returns the number of logical cores, counted from /proc/stat's
// per-core "cpuN" lines rather than a separate syscall -- distinct from the
// aggregate "cpu " line cpuPercentages derives its totals from -- so it
// reads from the same source those percentages do, host or test fixture
// alike.
func getCPUCount() (int64, error) {
	data, err := os.ReadFile(filepath.Join(procRoot, "stat"))
	if err != nil {
		return 0, err
	}
	var count int64
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "cpu") || strings.HasPrefix(line, "cpu ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if _, err := strconv.Atoi(strings.TrimPrefix(fields[0], "cpu")); err == nil {
			count++
		}
	}
	return count, nil
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
// A cgroup that could not be located is not an error: the container may not
// have been created yet (expected for the first sample of every task), it may
// already have been torn down, or the task may be running without a
// container at all. unavailableContainerCounters() already reports every
// counter as unavailable in that case, so callers don't need a separate
// "present" flag to know -- the fields themselves say so.
func containerMetrics(cg *containerCgroup) containerCounters {
	if cg == nil {
		return unavailableContainerCounters()
	}
	dir := cg.resolve()
	if dir == "" {
		return unavailableContainerCounters()
	}
	return readContainerCounters(dir)
}

// setContainerCounters copies one cgroup read onto the sample.
func (s *MetricSample) setContainerCounters(c containerCounters) {
	s.ContainerMemoryCurrentBytes = int64OrNA(c.MemoryCurrentBytes)
	s.ContainerMemoryPeakBytes = int64OrNA(c.MemoryPeakBytes)
	s.ContainerMemoryLimitBytes = int64OrNA(c.MemoryLimitBytes)

	s.ContainerCPUUsageUSec = int64OrNA(c.CPUUsageUSec)
	s.ContainerCPUUserUSec = int64OrNA(c.CPUUserUSec)
	s.ContainerCPUSystemUSec = int64OrNA(c.CPUSystemUSec)
	s.ContainerCPUThrottledUSec = int64OrNA(c.CPUThrottledUSec)
	s.ContainerCPUThrottledPeriods = int64OrNA(c.CPUThrottledPeriods)

	s.ContainerMemoryMajorFaults = int64OrNA(c.MemoryMajorFaults)
	s.ContainerMemoryWorkingsetRefaults = int64OrNA(c.MemoryWorkingsetRefaults)
	s.ContainerMemoryOOMKillCount = int64OrNA(c.MemoryOOMKillCount)
	s.ContainerPidsPeak = int64OrNA(c.PidsPeak)

	s.ContainerCPUStallSomeUSec, s.ContainerCPUStallFullUSec = int64OrNA(c.CPUStall.SomeUSec), int64OrNA(c.CPUStall.FullUSec)
	s.ContainerMemoryStallSomeUSec, s.ContainerMemoryStallFullUSec = int64OrNA(c.MemoryStall.SomeUSec), int64OrNA(c.MemoryStall.FullUSec)
	s.ContainerIOStallSomeUSec, s.ContainerIOStallFullUSec = int64OrNA(c.IOStall.SomeUSec), int64OrNA(c.IOStall.FullUSec)

	s.ContainerIOReadBytes = int64OrNA(c.IOReadBytes)
	s.ContainerIOWriteBytes = int64OrNA(c.IOWriteBytes)
	s.ContainerIOReadOps = int64OrNA(c.IOReadOps)
	s.ContainerIOWriteOps = int64OrNA(c.IOWriteOps)
}

// mergeMetricFields fills every nil pointer-typed metric field on dst from
// the corresponding field on fallback, when fallback has a non-nil value
// there. Fields already non-nil on dst are left untouched; fallback == nil
// is a no-op.
//
// HostVolumes and the identity/bookkeeping fields (TaskID, Type, Timestamp,
// Expiry, MetricSchema, Seq, Final) are excluded: they aren't "a metric that
// may be unavailable," so a merge doesn't apply to them.
func mergeMetricFields(dst, fallback *MetricSample) {
	if fallback == nil {
		return
	}
	if dst.HostCPUUserPct == nil {
		dst.HostCPUUserPct = fallback.HostCPUUserPct
	}
	if dst.HostCPUSystemPct == nil {
		dst.HostCPUSystemPct = fallback.HostCPUSystemPct
	}
	if dst.HostCPUIdlePct == nil {
		dst.HostCPUIdlePct = fallback.HostCPUIdlePct
	}
	if dst.HostCPUIowaitPct == nil {
		dst.HostCPUIowaitPct = fallback.HostCPUIowaitPct
	}
	if dst.HostCPUCount == nil {
		dst.HostCPUCount = fallback.HostCPUCount
	}
	if dst.HostMemoryTotalBytes == nil {
		dst.HostMemoryTotalBytes = fallback.HostMemoryTotalBytes
	}
	if dst.HostMemoryAvailableBytes == nil {
		dst.HostMemoryAvailableBytes = fallback.HostMemoryAvailableBytes
	}
	if dst.HostCPUStallSomeUSec == nil {
		dst.HostCPUStallSomeUSec = fallback.HostCPUStallSomeUSec
	}
	if dst.HostCPUStallFullUSec == nil {
		dst.HostCPUStallFullUSec = fallback.HostCPUStallFullUSec
	}
	if dst.HostMemoryStallSomeUSec == nil {
		dst.HostMemoryStallSomeUSec = fallback.HostMemoryStallSomeUSec
	}
	if dst.HostMemoryStallFullUSec == nil {
		dst.HostMemoryStallFullUSec = fallback.HostMemoryStallFullUSec
	}
	if dst.HostIOStallSomeUSec == nil {
		dst.HostIOStallSomeUSec = fallback.HostIOStallSomeUSec
	}
	if dst.HostIOStallFullUSec == nil {
		dst.HostIOStallFullUSec = fallback.HostIOStallFullUSec
	}
	if dst.ContainerMemoryCurrentBytes == nil {
		dst.ContainerMemoryCurrentBytes = fallback.ContainerMemoryCurrentBytes
	}
	if dst.ContainerMemoryPeakBytes == nil {
		dst.ContainerMemoryPeakBytes = fallback.ContainerMemoryPeakBytes
	}
	if dst.ContainerMemoryLimitBytes == nil {
		dst.ContainerMemoryLimitBytes = fallback.ContainerMemoryLimitBytes
	}
	if dst.ContainerCPUUsageUSec == nil {
		dst.ContainerCPUUsageUSec = fallback.ContainerCPUUsageUSec
	}
	if dst.ContainerCPUUserUSec == nil {
		dst.ContainerCPUUserUSec = fallback.ContainerCPUUserUSec
	}
	if dst.ContainerCPUSystemUSec == nil {
		dst.ContainerCPUSystemUSec = fallback.ContainerCPUSystemUSec
	}
	if dst.ContainerCPUThrottledUSec == nil {
		dst.ContainerCPUThrottledUSec = fallback.ContainerCPUThrottledUSec
	}
	if dst.ContainerCPUThrottledPeriods == nil {
		dst.ContainerCPUThrottledPeriods = fallback.ContainerCPUThrottledPeriods
	}
	if dst.ContainerMemoryMajorFaults == nil {
		dst.ContainerMemoryMajorFaults = fallback.ContainerMemoryMajorFaults
	}
	if dst.ContainerMemoryWorkingsetRefaults == nil {
		dst.ContainerMemoryWorkingsetRefaults = fallback.ContainerMemoryWorkingsetRefaults
	}
	if dst.ContainerMemoryOOMKillCount == nil {
		dst.ContainerMemoryOOMKillCount = fallback.ContainerMemoryOOMKillCount
	}
	if dst.ContainerPidsPeak == nil {
		dst.ContainerPidsPeak = fallback.ContainerPidsPeak
	}
	if dst.ContainerCPUStallSomeUSec == nil {
		dst.ContainerCPUStallSomeUSec = fallback.ContainerCPUStallSomeUSec
	}
	if dst.ContainerCPUStallFullUSec == nil {
		dst.ContainerCPUStallFullUSec = fallback.ContainerCPUStallFullUSec
	}
	if dst.ContainerMemoryStallSomeUSec == nil {
		dst.ContainerMemoryStallSomeUSec = fallback.ContainerMemoryStallSomeUSec
	}
	if dst.ContainerMemoryStallFullUSec == nil {
		dst.ContainerMemoryStallFullUSec = fallback.ContainerMemoryStallFullUSec
	}
	if dst.ContainerIOStallSomeUSec == nil {
		dst.ContainerIOStallSomeUSec = fallback.ContainerIOStallSomeUSec
	}
	if dst.ContainerIOStallFullUSec == nil {
		dst.ContainerIOStallFullUSec = fallback.ContainerIOStallFullUSec
	}
	if dst.ContainerIOReadBytes == nil {
		dst.ContainerIOReadBytes = fallback.ContainerIOReadBytes
	}
	if dst.ContainerIOWriteBytes == nil {
		dst.ContainerIOWriteBytes = fallback.ContainerIOWriteBytes
	}
	if dst.ContainerIOReadOps == nil {
		dst.ContainerIOReadOps = fallback.ContainerIOReadOps
	}
	if dst.ContainerIOWriteOps == nil {
		dst.ContainerIOWriteOps = fallback.ContainerIOWriteOps
	}
}

// updateLastGoodAccumulator folds a freshly measured, non-final sample into a
// running accumulator: the fresh sample's own values are kept exactly as
// measured, and only fields it left nil are filled in from the previous
// accumulator -- so the newest measurement of any given field always wins,
// while gaps get carried forward. sample itself is never mutated.
func updateLastGoodAccumulator(lastGoodAccumulator, sample *MetricSample) *MetricSample {
	acc := *sample // shallow copy; sample's own fields are left untouched
	mergeMetricFields(&acc, lastGoodAccumulator)
	return &acc
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
	}

	cur, err := getCPUStats()
	if err != nil || cur == nil {
		cur = prev
	} else if prev != nil {
		userPct, systemPct, idlePct, iowaitPct := cpuPercentages(prev, cur)
		s.HostCPUUserPct, s.HostCPUSystemPct, s.HostCPUIdlePct, s.HostCPUIowaitPct = &userPct, &systemPct, &idlePct, &iowaitPct
	}

	if sysMem, err := getSystemMemory(); err == nil {
		s.HostMemoryTotalBytes = &sysMem.Total
		s.HostMemoryAvailableBytes = &sysMem.Available
	}

	if count, err := getCPUCount(); err == nil {
		s.HostCPUCount = &count
	}

	cpuStall := readPressureFile(filepath.Join(procRoot, "pressure", "cpu"))
	memoryStall := readPressureFile(filepath.Join(procRoot, "pressure", "memory"))
	ioStall := readPressureFile(filepath.Join(procRoot, "pressure", "io"))
	s.HostCPUStallSomeUSec, s.HostCPUStallFullUSec = int64OrNA(cpuStall.SomeUSec), int64OrNA(cpuStall.FullUSec)
	s.HostMemoryStallSomeUSec, s.HostMemoryStallFullUSec = int64OrNA(memoryStall.SomeUSec), int64OrNA(memoryStall.FullUSec)
	s.HostIOStallSomeUSec, s.HostIOStallFullUSec = int64OrNA(ioStall.SomeUSec), int64OrNA(ioStall.FullUSec)

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
	taskID              string
	workDir             string
	cgroup              *containerCgroup
	prevCPU             *cpuStats
	seq                 int32
	lastGoodAccumulator *MetricSample
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
// The final sample has any field it couldn't read itself backfilled from the
// last sample that did -- see mergeMetricFields.
func (m *MetricSampler) Sample(final bool) *MetricSample {
	s, cur := collectMetricSample(m.taskID, m.workDir, m.prevCPU, m.cgroup, m.seq, final)
	m.prevCPU = cur
	m.seq++
	if final {
		mergeMetricFields(s, m.lastGoodAccumulator)
	} else {
		m.lastGoodAccumulator = updateLastGoodAccumulator(m.lastGoodAccumulator, s)
	}
	return s
}

// NextMetricsDelay exposes the adaptive sampling schedule so external drivers
// can match the cadence the worker actually uses.
func NextMetricsDelay(cur time.Duration) time.Duration {
	return nextMetricsDelay(cur)
}
