package monitor

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// PageSize is the system page size in bytes
const PageSize = 4 * 1024

// MemoryUsage holds memory stats from /proc/*/statm
type MemoryUsage struct {
	TotalSize     int64 // sum of size of each process visible
	TotalData     int64
	TotalShared   int64
	TotalResident int64
	ProcCount     int // number of processes visible
}

// GetMemoryUsage reads memory stats from /proc/*/statm
func GetMemoryUsage() (*MemoryUsage, error) {
	filenames, err := filepath.Glob("/proc/*/statm")
	if err != nil {
		return nil, err
	}

	procCount := 0
	totalSize := int64(0)
	totalData := int64(0)
	totalShared := int64(0)
	totalResident := int64(0)

	for _, filename := range filenames {
		contents, err := os.ReadFile(filename)
		if err != nil {
			log.Printf("Could not read %s for memory stats, skipping...", filename)
			continue
		}

		strContents := string(contents)
		fields := strings.Split(strContents, " ")

		// from the /proc docs
		// 		size       (1) total program size
		// 		(same as VmSize in /proc/[pid]/status)
		// resident   (2) resident set size
		// 		(same as VmRSS in /proc/[pid]/status)
		// shared     (3) number of resident shared pages (i.e., backed by a file)
		// 		(same as RssFile+RssShmem in /proc/[pid]/status)
		// text       (4) text (code)
		// lib        (5) library (unused since Linux 2.6; always 0)
		// data       (6) data + stack
		// dt         (7) dirty pages (unused since Linux 2.6; always 0)
		size, err := strconv.ParseInt(fields[0], 10, 64)
		var resident int64
		var shared int64
		var data int64
		if err == nil {
			resident, err = strconv.ParseInt(fields[1], 10, 64)
		}
		if err == nil {
			shared, err = strconv.ParseInt(fields[2], 10, 64)
		}
		if err == nil {
			data, err = strconv.ParseInt(fields[5], 10, 64)
		}

		if err != nil {
			log.Printf("Could not parse statm: %s", err)
			continue
		}

		procCount++
		totalSize += size
		totalData += data
		totalShared += shared
		totalResident += resident
	}

	return &MemoryUsage{totalSize, totalData, totalShared, totalResident, procCount}, nil
}

// CPUStats holds CPU time counters from /proc/stat (in jiffies)
type CPUStats struct {
	User   int64
	System int64
	Idle   int64
	Iowait int64
}

// GetCPUStats reads CPU counters from /proc/stat
func GetCPUStats() (*CPUStats, error) {
	contents, err := os.ReadFile("/proc/stat")
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(contents), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "cpu ") {
			fields := strings.Fields(line)
			if len(fields) < 8 {
				return nil, fmt.Errorf("unexpected /proc/stat format")
			}
			// fields: cpu user nice system idle iowait irq softirq ...
			user, _ := strconv.ParseInt(fields[1], 10, 64)
			nice, _ := strconv.ParseInt(fields[2], 10, 64)
			system, _ := strconv.ParseInt(fields[3], 10, 64)
			idle, _ := strconv.ParseInt(fields[4], 10, 64)
			iowait, _ := strconv.ParseInt(fields[5], 10, 64)

			return &CPUStats{
				User:   user + nice, // combine user and nice
				System: system,
				Idle:   idle,
				Iowait: iowait,
			}, nil
		}
	}
	return nil, fmt.Errorf("cpu line not found in /proc/stat")
}

// SystemMemory holds system-wide memory info from /proc/meminfo (in bytes)
type SystemMemory struct {
	Total     int64
	Available int64
	Free      int64
}

// GetSystemMemory reads memory info from /proc/meminfo
func GetSystemMemory() (*SystemMemory, error) {
	contents, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return nil, err
	}

	mem := &SystemMemory{}
	lines := strings.Split(string(contents), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, _ := strconv.ParseInt(fields[1], 10, 64)
		value *= 1024 // convert from kB to bytes

		switch fields[0] {
		case "MemTotal:":
			mem.Total = value
		case "MemAvailable:":
			mem.Available = value
		case "MemFree:":
			mem.Free = value
		}
	}
	return mem, nil
}

// MemoryPressure holds PSI metrics from /proc/pressure/memory
type MemoryPressure struct {
	SomeAvg10 int32 // percentage * 100, or -1 if unavailable
	FullAvg10 int32
}

// GetMemoryPressure reads PSI memory pressure (gracefully returns -1 if unavailable)
func GetMemoryPressure() *MemoryPressure {
	contents, err := os.ReadFile("/proc/pressure/memory")
	if err != nil {
		// PSI not available (older kernel or not enabled)
		return &MemoryPressure{SomeAvg10: -1, FullAvg10: -1}
	}

	pressure := &MemoryPressure{SomeAvg10: -1, FullAvg10: -1}
	lines := strings.Split(string(contents), "\n")
	for _, line := range lines {
		// Format: some avg10=0.00 avg60=0.00 avg300=0.00 total=0
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		var target *int32
		if fields[0] == "some" {
			target = &pressure.SomeAvg10
		} else if fields[0] == "full" {
			target = &pressure.FullAvg10
		} else {
			continue
		}

		// Parse avg10=X.XX
		for _, field := range fields[1:] {
			if strings.HasPrefix(field, "avg10=") {
				valStr := strings.TrimPrefix(field, "avg10=")
				val, err := strconv.ParseFloat(valStr, 64)
				if err == nil {
					*target = int32(val * 100) // convert to percentage * 100
				}
				break
			}
		}
	}
	return pressure
}

// GetProcessStatusResponse for get_process_status responses
type GetProcessStatusResponse struct {
	Type                 string `json:"type"`
	ProcessCount         int32  `json:"process_count"`
	TotalMemory          int64 `json:"total_memory"`
	TotalData            int64 `json:"total_data"`
	TotalShared          int64 `json:"total_shared"`
	TotalResident        int64 `json:"total_resident"`
	CpuUser              int64 `json:"cpu_user"`
	CpuSystem            int64 `json:"cpu_system"`
	CpuIdle              int64 `json:"cpu_idle"`
	CpuIowait            int64 `json:"cpu_iowait"`
	MemTotal             int64 `json:"mem_total"`
	MemAvailable         int64 `json:"mem_available"`
	MemFree              int64 `json:"mem_free"`
	MemPressureSomeAvg10 int32 `json:"mem_pressure_some_avg10"`
	MemPressureFullAvg10 int32 `json:"mem_pressure_full_avg10"`
}

func GetResourceUsage() (*GetProcessStatusResponse, error) {

	mem, err := GetMemoryUsage()
	if err != nil {
		return nil, err
	}

	resp := &GetProcessStatusResponse{
		Type:          "process_status",
		TotalMemory:   mem.TotalSize * PageSize,
		TotalData:     mem.TotalData * PageSize,
		TotalShared:   mem.TotalShared * PageSize,
		TotalResident: mem.TotalResident * PageSize,
		ProcessCount:  int32(mem.ProcCount),
	}

	// Add CPU stats (best effort)
	if cpu, err := GetCPUStats(); err == nil {
		resp.CpuUser = cpu.User
		resp.CpuSystem = cpu.System
		resp.CpuIdle = cpu.Idle
		resp.CpuIowait = cpu.Iowait
	}

	// Add system memory info (best effort)
	if sysMem, err := GetSystemMemory(); err == nil {
		resp.MemTotal = sysMem.Total
		resp.MemAvailable = sysMem.Available
		resp.MemFree = sysMem.Free
	}

	// Add memory pressure
	pressure := GetMemoryPressure()
	resp.MemPressureSomeAvg10 = pressure.SomeAvg10
	resp.MemPressureFullAvg10 = pressure.FullAvg10

	return resp, nil
}
