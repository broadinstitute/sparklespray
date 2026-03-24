package monitor

import "time"

type StdoutUpdate struct {
	Type      string    `json:"type"`
	Content   []byte    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

type ResourceUsage struct {
	Type                 string    `json:"type"`
	Timestamp            time.Time `json:"timestamp"`
	ProcessCount         int32  `json:"process_count"`
	TotalMemory          int64  `json:"total_memory"`
	TotalData            int64  `json:"total_data"`
	TotalShared          int64  `json:"total_shared"`
	TotalResident        int64  `json:"total_resident"`
	CpuUser              int64  `json:"cpu_user"`
	CpuSystem            int64  `json:"cpu_system"`
	CpuIdle              int64  `json:"cpu_idle"`
	CpuIowait            int64  `json:"cpu_iowait"`
	MemTotal             int64  `json:"mem_total"`
	MemAvailable         int64  `json:"mem_available"`
	MemFree              int64  `json:"mem_free"`
	MemPressureSomeAvg10 int32  `json:"mem_pressure_some_avg10"`
	MemPressureFullAvg10 int32  `json:"mem_pressure_full_avg10"`
}
