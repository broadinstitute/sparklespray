package backend

import "time"

// Command type constants for the Type field of IncomingMessage.
const (
	// CmdCancelJob requests that the worker cancel all tasks belonging to a job.
	CmdCancelJob = "cancel_job"

	// CmdStartStatusStream requests that the worker begin streaming log output
	// for a running task back via the ControlResponseTopic.
	CmdStartStatusStream = "start_status_stream"

	// CmdStopStatusStream requests that the worker stop streaming log output
	// for a task that was previously started with CmdStartStatusStream.
	CmdStopStatusStream = "stop_status_stream"
)

// Response type constants for the Type field of OutgoingResponse.
const (
	// RespAck is a generic acknowledgement sent in reply to any command.
	RespAck = "ack"

	// RespLogStreamUpdate carries a chunk of log output for a streaming task.
	// These are published periodically until the task finishes or a
	// CmdStopStatusStream is received.
	RespLogStreamUpdate = "log_stream_update"

	RespResourceUsageUpdate = "resource_usage_update"
)

// --- Command payloads (carried in IncomingMessage.Payload) ---

// Command is the base struct embedded by all command types. Type identifies
// the command variant (one of the Cmd* constants) and ReqID ties each command
// to its responses so the sender can correlate AckResponse and LogStreamUpdate
// messages back to the originating request.
type Command struct {
	Type  string `json:"type"`
	ReqID string `json:"req_id"`
}

// CancelJobCommand requests cancellation of all tasks belonging to JobID.
type CancelJobCommand struct {
	Command
	JobID string `json:"job_id"`
}

// StartStatusStreamCommand requests that the worker start streaming log output
// for the task identified by TaskID.
type StartStatusStreamCommand struct {
	Command
	TaskID string `json:"task_id"`
}

// StopStatusStreamCommand requests that the worker stop streaming log output
// for the task identified by TaskID.
type StopStatusStreamCommand struct {
	Command
	TaskID string `json:"task_id"`
}

// --- Response payloads (carried in OutgoingResponse.Payload) ---

// ResponseBase is the base struct embedded by all response types. Type
// identifies the response variant (one of the Resp* constants) and ReqID
// echoes the originating command's ReqID so the caller can correlate
// responses back to their request.
type ResponseBase struct {
}

// AckResponse is a generic acknowledgement payload. It may optionally carry a
// human-readable message, but its presence alone signals success.
type AckResponse struct {
	Type    string `json:"type"`
	ReqID   string `json:"req_id"`
	Message string `json:"message,omitempty"`
}

// LogStreamUpdate carries a chunk of log content for a running task. Updates
// are published periodically on the ControlResponseTopic until the task
// finishes or a StopStatusStreamCommand is received.
type LogStreamUpdate struct {
	Type      string    `json:"type"`
	ReqID     string    `json:"req_id"`
	Timestamp time.Time `json:"timestamp"`
	TaskID    string    `json:"task_id"`
	Content   string    `json:"content"`
}

type ResourceUsageUpdate struct {
	Type                 string    `json:"type"`
	ReqID                string    `json:"req_id"`
	TaskID               string    `json:"task_id"`
	Timestamp            time.Time `json:"timestamp"`
	ProcessCount         int32     `json:"process_count"`
	TotalMemory          int64     `json:"total_memory"`
	TotalData            int64     `json:"total_data"`
	TotalShared          int64     `json:"total_shared"`
	TotalResident        int64     `json:"total_resident"`
	CpuUser              int64     `json:"cpu_user"`
	CpuSystem            int64     `json:"cpu_system"`
	CpuIdle              int64     `json:"cpu_idle"`
	CpuIowait            int64     `json:"cpu_iowait"`
	MemTotal             int64     `json:"mem_total"`
	MemAvailable         int64     `json:"mem_available"`
	MemFree              int64     `json:"mem_free"`
	MemPressureSomeAvg10 int32     `json:"mem_pressure_some_avg10"`
	MemPressureFullAvg10 int32     `json:"mem_pressure_full_avg10"`
}
