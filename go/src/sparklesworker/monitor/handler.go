package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/broadinstitute/sparklesworker/control"
)

// ReadOutputRequest for read_output messages
type ReadOutputRequest struct {
	TaskID   string `json:"task_id"`
	Size     int32  `json:"size"`
	Offset   int64  `json:"offset"`
	WorkerID string `json:"worker_id"`
}

// GetProcessStatusRequest for get_process_status messages
type GetProcessStatusRequest struct {
	WorkerID string `json:"worker_id"`
}

// ReadOutputResponse for read_output responses
type ReadOutputResponse struct {
	Data      []byte `json:"data"`
	EndOfFile bool   `json:"end_of_file"`
}

// GetProcessStatusResponse for get_process_status responses
type GetProcessStatusResponse struct {
	ProcessCount         int32 `json:"process_count"`
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

// Handler handles control messages using a Monitor
type Handler struct {
	mon      *Monitor
	workerID string
}

// NewHandler creates a new message handler
func NewHandler(mon *Monitor, workerID string) *Handler {
	return &Handler{
		mon:      mon,
		workerID: workerID,
	}
}

// HandleMessage processes an incoming control message and returns a response
// Returns (response, shouldRespond) - if shouldRespond is false, no response is sent
func (h *Handler) HandleMessage(ctx context.Context, msg *control.Message) (*control.Response, bool) {
	log.Printf("Handling control message type=%s request_id=%s", msg.Type, msg.RequestID)

	response := &control.Response{
		Type:      msg.Type,
		RequestID: msg.RequestID,
	}

	switch msg.Type {
	case "read_output":
		return h.handleReadOutput(ctx, msg, response)
	case "get_process_status":
		return h.handleGetProcessStatus(ctx, msg, response)
	default:
		response.Error = fmt.Sprintf("unknown message type: %s", msg.Type)
		return response, true
	}
}

func (h *Handler) handleReadOutput(ctx context.Context, msg *control.Message, response *control.Response) (*control.Response, bool) {
	var req ReadOutputRequest
	if err := json.Unmarshal(msg.Payload, &req); err != nil {
		response.Error = fmt.Sprintf("failed to unmarshal read_output payload: %v", err)
		return response, true
	}

	// Check if this request is for this worker
	if req.WorkerID != "" && req.WorkerID != h.workerID {
		log.Printf("Ignoring read_output request for worker_id %s (this worker is %s)", req.WorkerID, h.workerID)
		return response, false
	}

	// Use the monitor's GetLogPath
	stdoutPath, ok := h.mon.GetLogPath(req.TaskID)
	if !ok {
		response.Error = fmt.Sprintf("unknown task: %s", req.TaskID)
		return response, true
	}

	// Read the file
	data, eof, err := readOutputFile(stdoutPath, req.Size, req.Offset)
	if err != nil {
		response.Error = fmt.Sprintf("failed to read output: %v", err)
		return response, true
	}

	response.Payload = ReadOutputResponse{
		Data:      data,
		EndOfFile: eof,
	}
	return response, true
}

func (h *Handler) handleGetProcessStatus(ctx context.Context, msg *control.Message, response *control.Response) (*control.Response, bool) {
	var req GetProcessStatusRequest
	if err := json.Unmarshal(msg.Payload, &req); err != nil {
		// Payload might be empty for backwards compatibility, continue
		log.Printf("Could not unmarshal get_process_status payload (may be empty): %v", err)
	}

	// Check if this request is for this worker
	if req.WorkerID != "" && req.WorkerID != h.workerID {
		log.Printf("Ignoring get_process_status request for worker_id %s (this worker is %s)", req.WorkerID, h.workerID)
		return response, false
	}

	mem, err := GetMemoryUsage()
	if err != nil {
		response.Error = fmt.Sprintf("failed to get memory usage: %v", err)
		return response, true
	}

	resp := GetProcessStatusResponse{
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

	response.Payload = resp
	return response, true
}

// readOutputFile reads from a file at the given offset
func readOutputFile(path string, size int32, offset int64) ([]byte, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()

	buffer := make([]byte, size)
	n, err := f.ReadAt(buffer, offset)
	buffer = buffer[:n]

	eof := err != nil && err == io.EOF
	if err != nil && err != io.EOF {
		return nil, false, err
	}

	return buffer, eof, nil
}
