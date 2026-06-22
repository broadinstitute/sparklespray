package v100

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// VolumeUsage holds disk usage for one mount point.
type VolumeUsage struct {
	Location string  `firestore:"location" json:"location"`
	TotalGB  float64 `firestore:"total_gb" json:"total_gb"`
	UsedGB   float64 `firestore:"used_gb" json:"used_gb"`
}

// ResourceUsageEvent is one metric_update entry in the events file and TaskLog collection.
type ResourceUsageEvent struct {
	TaskID               string        `firestore:"task_id" json:"task_id"`
	Type                 string        `firestore:"type" json:"type"`
	Timestamp            time.Time     `firestore:"timestamp" json:"timestamp"`
	Expiry               time.Time     `firestore:"expiry" json:"expiry"`
	ProcessCount         int32         `firestore:"process_count" json:"process_count"`
	Volumes              []VolumeUsage `firestore:"volumes" json:"volumes,omitempty"`
	TotalMemory          int64         `firestore:"total_memory" json:"total_memory"`
	TotalData            int64         `firestore:"total_data" json:"total_data"`
	TotalShared          int64         `firestore:"total_shared" json:"total_shared"`
	TotalResident        int64         `firestore:"total_resident" json:"total_resident"`
	CpuUser              int64         `firestore:"cpu_user" json:"cpu_user"`
	CpuSystem            int64         `firestore:"cpu_system" json:"cpu_system"`
	CpuIdle              int64         `firestore:"cpu_idle" json:"cpu_idle"`
	CpuIowait            int64         `firestore:"cpu_iowait" json:"cpu_iowait"`
	MemTotal             int64         `firestore:"mem_total" json:"mem_total"`
	MemAvailable         int64         `firestore:"mem_available" json:"mem_available"`
	MemFree              int64         `firestore:"mem_free" json:"mem_free"`
	MemPressureSomeAvg10 int32         `firestore:"mem_pressure_some_avg10" json:"mem_pressure_some_avg10"`
	MemPressureFullAvg10 int32         `firestore:"mem_pressure_full_avg10" json:"mem_pressure_full_avg10"`
}

var pageSize = int64(os.Getpagesize())

type memoryUsage struct {
	procCount     int
	totalSize     int64
	totalData     int64
	totalShared   int64
	totalResident int64
}

func getMemoryUsage() (*memoryUsage, error) {
	filenames, err := filepath.Glob("/proc/*/statm")
	if err != nil {
		return nil, err
	}

	m := &memoryUsage{}
	for _, filename := range filenames {
		data, err := os.ReadFile(filename)
		if err != nil {
			continue
		}
		fields := strings.Fields(string(data))
		if len(fields) < 6 {
			continue
		}
		size, e1 := strconv.ParseInt(fields[0], 10, 64)
		resident, e2 := strconv.ParseInt(fields[1], 10, 64)
		shared, e3 := strconv.ParseInt(fields[2], 10, 64)
		data2, e4 := strconv.ParseInt(fields[5], 10, 64)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
			continue
		}
		m.procCount++
		m.totalSize += size
		m.totalResident += resident
		m.totalShared += shared
		m.totalData += data2
	}
	return m, nil
}

type cpuStats struct {
	User   int64
	System int64
	Idle   int64
	Iowait int64
}

func getCPUStats() (*cpuStats, error) {
	data, err := os.ReadFile("/proc/stat")
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
	data, err := os.ReadFile("/proc/meminfo")
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

type memoryPressure struct {
	SomeAvg10 int32
	FullAvg10 int32
}

func getMemoryPressure() *memoryPressure {
	data, err := os.ReadFile("/proc/pressure/memory")
	p := &memoryPressure{SomeAvg10: -1, FullAvg10: -1}
	if err != nil {
		return p
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		var target *int32
		switch fields[0] {
		case "some":
			target = &p.SomeAvg10
		case "full":
			target = &p.FullAvg10
		default:
			continue
		}
		for _, f := range fields[1:] {
			if strings.HasPrefix(f, "avg10=") {
				val, err := strconv.ParseFloat(strings.TrimPrefix(f, "avg10="), 64)
				if err == nil {
					*target = int32(val * 100)
				}
				break
			}
		}
	}
	return p
}

func getVolumeUsage(paths ...string) []VolumeUsage {
	seen := make(map[string]bool)
	var volumes []VolumeUsage
	for _, p := range paths {
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		var stat syscall.Statfs_t
		if err := syscall.Statfs(p, &stat); err != nil {
			continue
		}
		total := float64(stat.Blocks*uint64(stat.Bsize)) / (1024 * 1024 * 1024)
		free := float64(stat.Bfree*uint64(stat.Bsize)) / (1024 * 1024 * 1024)
		volumes = append(volumes, VolumeUsage{Location: p, TotalGB: total, UsedGB: total - free})
	}
	return volumes
}

// collectMetrics samples current system and process metrics for the given task.
func collectMetrics(taskID, workDir string) *ResourceUsageEvent {
	now := time.Now()
	event := &ResourceUsageEvent{
		TaskID:    taskID,
		Type:      "metric_update",
		Timestamp: now,
		Expiry:    now.Add(taskEventLogTTL),
		Volumes:   getVolumeUsage("/", workDir),
	}
	if mem, err := getMemoryUsage(); err == nil {
		event.ProcessCount = int32(mem.procCount)
		event.TotalMemory = mem.totalSize * pageSize
		event.TotalData = mem.totalData * pageSize
		event.TotalShared = mem.totalShared * pageSize
		event.TotalResident = mem.totalResident * pageSize
	}
	if cpu, err := getCPUStats(); err == nil && cpu != nil {
		event.CpuUser = cpu.User
		event.CpuSystem = cpu.System
		event.CpuIdle = cpu.Idle
		event.CpuIowait = cpu.Iowait
	}
	if sysMem, err := getSystemMemory(); err == nil {
		event.MemTotal = sysMem.Total
		event.MemAvailable = sysMem.Available
		event.MemFree = sysMem.Free
	}
	if p := getMemoryPressure(); p != nil {
		event.MemPressureSomeAvg10 = p.SomeAvg10
		event.MemPressureFullAvg10 = p.FullAvg10
	}
	return event
}
