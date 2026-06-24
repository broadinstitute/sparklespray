package dev

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	v100 "github.com/broadinstitute/sparklespray/v100"
	"github.com/broadinstitute/sparklespray/v100/monitor"
	iamcredentials "google.golang.org/api/iamcredentials/v1"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"github.com/urfave/cli"
)

// ----- handler context -----

type dashboardServer struct {
	project string
	fs      *firestore.Client
	ps      *pubsub.Client
	// subscriberSA is the service account email used to generate short-lived
	// Pub/Sub tokens. If empty the subscription endpoint returns an error.
	subscriberSA string
}

// ----- helpers -----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("dashboard: writeJSON: %v", err)
	}
}

func writeError(w http.ResponseWriter, httpStatus int, code, msg string) {
	writeJSON(w, httpStatus, map[string]string{"error": msg, "code": code})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func parseRFC3339(s string) (time.Time, error) {
	return time.Parse(time.RFC3339, s)
}

func newSubID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "sparkles-dash-" + hex.EncodeToString(b)
}

// ----- GET /api/v1/workpools -----

type workpoolSummaryResponse struct {
	WorkpoolID     string    `json:"workpool_id"`
	MachineType    string    `json:"machine_type"`
	Region         string    `json:"region"`
	Status         string    `json:"status"`
	StatusMessage  string    `json:"status_message"`
	LastIncidentAt *string   `json:"last_incident_at"`
	IncidentCount  int       `json:"incident_count"`
	Expiry         time.Time `json:"expiry"`
}

func (s *dashboardServer) handleListWorkpools(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// Read v100.WorkPool directly to get the Expiry field not present in monitor.WorkPool.
	iter := s.fs.Collection(v100.WorkpoolCollection).Documents(ctx)
	defer iter.Stop()
	result := []workpoolSummaryResponse{}
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("dashboard: ListWorkpools iter: %v", err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to iterate workpools")
			return
		}
		var wp v100.WorkPool
		if err := snap.DataTo(&wp); err != nil {
			log.Printf("dashboard: ListWorkpools DataTo: %v", err)
			continue
		}
		resp := workpoolSummaryResponse{
			WorkpoolID:    wp.WorkpoolID,
			MachineType:   wp.MachineType,
			Region:        wp.Region,
			Status:        wp.Status,
			StatusMessage: wp.StatusMessage,
			IncidentCount: wp.IncidentCount,
			Expiry:        wp.Expiry,
		}
		if !wp.LastIncidentAt.IsZero() {
			s := wp.LastIncidentAt.Format(time.RFC3339)
			resp.LastIncidentAt = &s
		}
		result = append(result, resp)
	}
	writeJSON(w, http.StatusOK, result)
}

// ----- GET /api/v1/workpool/{workpool_id} -----

type workpoolDetailResponse struct {
	WorkpoolID            string               `json:"workpool_id"`
	MachineType           string               `json:"machine_type"`
	Region                string               `json:"region"`
	Zones                 []string             `json:"zones"`
	RootDir               string               `json:"root_dir"`
	SparklesWorkerGCSPath string               `json:"sparkles_worker_gcs_path"`
	Resources             []v100.ResourceEntry `json:"resources"`
	EmptyVolumes          []v100.EmptyVolume   `json:"empty_volumes"`
	MaxWorkerCount        int                  `json:"max_worker_count"`
	Status                string               `json:"status"`
	StatusMessage         string               `json:"status_message"`
	LastIncidentAt        *string              `json:"last_incident_at"`
	IncidentCount         int                  `json:"incident_count"`
	Expiry                time.Time            `json:"expiry"`
}

func (s *dashboardServer) handleGetWorkpool(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	workpoolID := r.PathValue("workpool_id")
	snap, err := s.fs.Collection(v100.WorkpoolCollection).Doc(workpoolID).Get(ctx)
	if err != nil {
		if grpcstatus.Code(err) == codes.NotFound {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "workpool not found")
			return
		}
		log.Printf("dashboard: GetWorkpool %s: %v", workpoolID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to get workpool")
		return
	}
	var wp v100.WorkPool
	if err := snap.DataTo(&wp); err != nil {
		log.Printf("dashboard: GetWorkpool DataTo: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to parse workpool")
		return
	}
	resp := workpoolDetailResponse{
		WorkpoolID:            wp.WorkpoolID,
		MachineType:           wp.MachineType,
		Region:                wp.Region,
		Zones:                 wp.Zones,
		RootDir:               wp.RootDir,
		SparklesWorkerGCSPath: wp.SparklesWorkerGCSPath,
		Resources:             wp.Resources,
		EmptyVolumes:          wp.EmptyVolumes,
		MaxWorkerCount:        wp.MaxWorkerCount,
		Status:                wp.Status,
		StatusMessage:         wp.StatusMessage,
		IncidentCount:         wp.IncidentCount,
		Expiry:                wp.Expiry,
	}
	if !wp.LastIncidentAt.IsZero() {
		s := wp.LastIncidentAt.Format(time.RFC3339)
		resp.LastIncidentAt = &s
	}
	writeJSON(w, http.StatusOK, resp)
}

// ----- GET /api/v1/workpool/{workpool_id}/batches -----

type batchRequestResponse struct {
	BatchID               string     `json:"batch_id"`
	JobID                 string     `json:"job_id"`
	WorkpoolID            string     `json:"workpool_id"`
	ExpectedVMCount       int        `json:"expected_vm_count"`
	Preemptible           bool       `json:"preemptible"`
	SubmittedAt           time.Time  `json:"submitted_at"`
	RunningSince          *time.Time `json:"running_since"`
	RegisteredWorkerCount int        `json:"registered_worker_count"`
	Status                string     `json:"status"`
	Unhealthy             bool       `json:"unhealthy"`
}

func (s *dashboardServer) handleListBatches(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	workpoolID := r.PathValue("workpool_id")
	store := monitor.NewFirestoreBatchRequestStore(s.fs)
	allStatuses := []monitor.BatchStatus{
		monitor.BatchStatusPending,
		monitor.BatchStatusStarted,
		monitor.BatchStatusCompleted,
		monitor.BatchStatusFailed,
	}
	batches, err := store.ListByWorkpool(ctx, workpoolID, allStatuses)
	if err != nil {
		log.Printf("dashboard: ListBatches %s: %v", workpoolID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list batches")
		return
	}
	result := make([]batchRequestResponse, 0, len(batches))
	for _, b := range batches {
		result = append(result, batchRequestResponse{
			BatchID:               b.BatchID,
			JobID:                 b.JobID,
			WorkpoolID:            b.WorkpoolID,
			ExpectedVMCount:       b.ExpectedVMCount,
			Preemptible:           b.Preemptible,
			SubmittedAt:           b.SubmittedAt,
			RunningSince:          b.RunningSince,
			RegisteredWorkerCount: b.RegisteredWorkerCount,
			Status:                string(b.Status),
			Unhealthy:             b.Unhealthy,
		})
	}
	writeJSON(w, http.StatusOK, result)
}

// ----- GET /api/v1/workpool/{workpool_id}/workers -----

type workerResponse struct {
	WorkerID        string    `json:"worker_id"`
	WorkpoolID      string    `json:"workpool_id"`
	BatchID         string    `json:"batch_id"`
	InstanceName    string    `json:"instance_name"`
	Status          string    `json:"status"`
	Expiry          time.Time `json:"expiry"`
	HeartbeatExpiry time.Time `json:"heartbeat_expiry"`
}

func (s *dashboardServer) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	workpoolID := r.PathValue("workpool_id")
	statusFilter := r.URL.Query().Get("status")
	if statusFilter == "" {
		statusFilter = "started"
	}

	q := s.fs.Collection("Workers").Where("workpool_id", "==", workpoolID)
	if statusFilter != "all" {
		q = q.Where("status", "==", statusFilter)
	}
	iter := q.Documents(ctx)
	defer iter.Stop()

	result := []workerResponse{}
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("dashboard: ListWorkers %s: %v", workpoolID, err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list workers")
			return
		}
		var wr v100.WorkerRecord
		if err := snap.DataTo(&wr); err != nil {
			log.Printf("dashboard: ListWorkers DataTo: %v", err)
			continue
		}
		result = append(result, workerResponse{
			WorkerID:        wr.WorkerID,
			WorkpoolID:      wr.WorkpoolID,
			BatchID:         wr.BatchID,
			InstanceName:    wr.InstanceName,
			Status:          wr.Status,
			Expiry:          wr.Expiry,
			HeartbeatExpiry: wr.HeartbeatExpiry,
		})
	}
	writeJSON(w, http.StatusOK, result)
}

// ----- GET /api/v1/jobs -----

type taskCountResponse struct {
	State string `json:"state"`
	Count int    `json:"count"`
}

type labelResponse struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type jobSummaryResponse struct {
	JobID      string              `json:"job_id"`
	WorkpoolID string              `json:"workpool_id"`
	CreatedAt  time.Time           `json:"created_at"`
	Status     string              `json:"status"`
	Tasks      []taskCountResponse `json:"tasks"`
	Labels     []labelResponse     `json:"labels"`
	Expiry     time.Time           `json:"expiry"`
}

func (s *dashboardServer) handleListJobs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

	var afterTime, beforeTime time.Time
	if v := q.Get("after"); v != "" {
		t, err := parseRFC3339(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid 'after' timestamp")
			return
		}
		afterTime = t
	}
	if v := q.Get("before"); v != "" {
		t, err := parseRFC3339(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid 'before' timestamp")
			return
		}
		beforeTime = t
	}
	workpoolID := q.Get("workpool_id")

	cq := s.fs.Collection("JobSummary").Query
	if workpoolID != "" {
		cq = cq.Where("workpool_id", "==", workpoolID)
	}
	if !afterTime.IsZero() {
		cq = cq.Where("created_at", ">", afterTime)
	}
	if !beforeTime.IsZero() {
		cq = cq.Where("created_at", "<=", beforeTime)
	}
	cq = cq.OrderBy("created_at", firestore.Desc)

	iter := cq.Documents(ctx)
	defer iter.Stop()

	result := []jobSummaryResponse{}
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("dashboard: ListJobs: %v", err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list jobs")
			return
		}
		var js monitor.JobSummary
		if err := snap.DataTo(&js); err != nil {
			log.Printf("dashboard: ListJobs DataTo: %v", err)
			continue
		}
		result = append(result, jobSummaryToResponse(&js))
	}
	writeJSON(w, http.StatusOK, result)
}

func jobSummaryToResponse(js *monitor.JobSummary) jobSummaryResponse {
	tasks := make([]taskCountResponse, len(js.Tasks))
	for i, t := range js.Tasks {
		tasks[i] = taskCountResponse{State: t.State, Count: t.Count}
	}
	labels := make([]labelResponse, len(js.Labels))
	for i, l := range js.Labels {
		labels[i] = labelResponse{Name: l.Name, Value: l.Value}
	}
	return jobSummaryResponse{
		JobID:      js.JobID,
		WorkpoolID: js.WorkpoolID,
		CreatedAt:  js.CreatedAt,
		Status:     string(js.Status),
		Tasks:      tasks,
		Labels:     labels,
		Expiry:     js.Expiry,
	}
}

// ----- GET /api/v1/job/{job_id} -----

type jobResponse struct {
	JobID      string               `json:"job_id"`
	Name       string               `json:"name"`
	WorkpoolID string               `json:"workpool_id"`
	CreatedAt  time.Time            `json:"created_at"`
	TaskCount  int                  `json:"task_count"`
	Resources  []v100.ResourceEntry `json:"resources"`
	Labels     []labelResponse      `json:"labels"`
}

func (s *dashboardServer) handleGetJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobID := r.PathValue("job_id")
	snap, err := s.fs.Collection(v100.JobCollection).Doc(jobID).Get(ctx)
	if err != nil {
		if grpcstatus.Code(err) == codes.NotFound {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
			return
		}
		log.Printf("dashboard: GetJob %s: %v", jobID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to get job")
		return
	}
	var job v100.Job
	if err := snap.DataTo(&job); err != nil {
		log.Printf("dashboard: GetJob DataTo: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to parse job")
		return
	}
	labels := make([]labelResponse, len(job.Labels))
	for i, l := range job.Labels {
		labels[i] = labelResponse{Name: l.Name, Value: l.Value}
	}
	writeJSON(w, http.StatusOK, jobResponse{
		JobID:      job.JobID,
		Name:       job.Name,
		WorkpoolID: job.WorkpoolID,
		CreatedAt:  job.CreatedAt,
		TaskCount:  job.TaskCount,
		Resources:  job.Resources,
		Labels:     labels,
	})
}

// ----- GET /api/v1/job/{job_id}/summary-history -----

type jobSummaryHistoryEntryResponse struct {
	JobID      string              `json:"job_id"`
	WorkpoolID string              `json:"workpool_id"`
	Timestamp  time.Time           `json:"timestamp"`
	Status     string              `json:"status"`
	Tasks      []taskCountResponse `json:"tasks"`
}

func (s *dashboardServer) handleGetJobSummaryHistory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobID := r.PathValue("job_id")
	iter := s.fs.Collection("JobSummaryHistory").
		Where("job_id", "==", jobID).
		OrderBy("timestamp", firestore.Asc).
		Documents(ctx)
	defer iter.Stop()

	result := []jobSummaryHistoryEntryResponse{}
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("dashboard: GetJobSummaryHistory %s: %v", jobID, err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to get summary history")
			return
		}
		var h monitor.JobSummaryHistory
		if err := snap.DataTo(&h); err != nil {
			log.Printf("dashboard: GetJobSummaryHistory DataTo: %v", err)
			continue
		}
		tasks := make([]taskCountResponse, len(h.Tasks))
		for i, t := range h.Tasks {
			tasks[i] = taskCountResponse{State: t.State, Count: t.Count}
		}
		result = append(result, jobSummaryHistoryEntryResponse{
			JobID:      h.JobID,
			WorkpoolID: h.WorkpoolID,
			Timestamp:  h.Timestamp,
			Status:     string(h.Status),
			Tasks:      tasks,
		})
	}
	writeJSON(w, http.StatusOK, result)
}

// ----- GET /api/v1/task/{task_id} -----

type resourceUsageResponse struct {
	StartTime       time.Time `json:"start_time"`
	EndTime         time.Time `json:"end_time"`
	ElapsedSeconds  float64   `json:"elapsed_seconds"`
	MaxMemoryBytes  int64     `json:"max_memory_bytes"`
	CPUUserUSec     int64     `json:"cpu_user_usec"`
	CPUSystemUSec   int64     `json:"cpu_system_usec"`
	BlockReadBytes  int64     `json:"block_read_bytes"`
	BlockWriteBytes int64     `json:"block_write_bytes"`
	ExitCode        int       `json:"exit_code"`
	OOMKilled       bool      `json:"oom_killed"`
}

type taskParameterResponse struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type taskResponse struct {
	TaskID         string                  `json:"task_id"`
	TaskIndex      int                     `json:"task_index"`
	JobID          string                  `json:"job_id"`
	WorkpoolID     string                  `json:"workpool_id"`
	Status         string                  `json:"status"`
	Command        []string                `json:"command"`
	DockerImage    string                  `json:"docker_image"`
	ResultPath     string                  `json:"result_path,omitempty"`
	LogPath        string                  `json:"log_path,omitempty"`
	OwningWorkerID string                  `json:"owning_worker_id,omitempty"`
	FailureReason  string                  `json:"failure_reason,omitempty"`
	Parameters     []taskParameterResponse `json:"parameters"`
	ExitCode       *int                    `json:"exit_code,omitempty"`
	ResourceUsage  *resourceUsageResponse  `json:"resource_usage,omitempty"`
}

type taskSummaryResponse struct {
	TaskID        string                 `json:"task_id"`
	TaskIndex     int                    `json:"task_index"`
	Status        string                 `json:"status"`
	ExitCode      *int                   `json:"exit_code,omitempty"`
	ResourceUsage *resourceUsageResponse `json:"resource_usage,omitempty"`
}

func (s *dashboardServer) handleGetTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	taskID := r.PathValue("task_id")
	snap, err := s.fs.Collection(v100.TaskCollection).Doc(taskID).Get(ctx)
	if err != nil {
		if grpcstatus.Code(err) == codes.NotFound {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "task not found")
			return
		}
		log.Printf("dashboard: GetTask %s: %v", taskID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to get task")
		return
	}
	var task v100.Task
	if err := snap.DataTo(&task); err != nil {
		log.Printf("dashboard: GetTask DataTo: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to parse task")
		return
	}
	params := make([]taskParameterResponse, len(task.Parameters))
	for i, p := range task.Parameters {
		params[i] = taskParameterResponse{Name: p.Name, Value: p.Value}
	}
	resp := taskResponse{
		TaskID:         task.TaskID,
		TaskIndex:      task.TaskIndex,
		JobID:          task.JobID,
		WorkpoolID:     task.WorkpoolID,
		Status:         task.Status,
		Command:        task.Command,
		DockerImage:    task.DockerImage,
		ResultPath:     task.ResultPath,
		LogPath:        task.LogPath,
		OwningWorkerID: task.OwningWorkerID,
		FailureReason:  task.FailureReason,
		Parameters:     params,
	}
	// ExitCode: only include for completed tasks (terminal states record it meaningfully).
	if !v100.IsActiveStatus(task.Status) && task.Status != v100.StatusPending {
		ec := task.ExitCode
		resp.ExitCode = &ec
	}
	if task.ResourceUsage != nil {
		ru := task.ResourceUsage
		resp.ResourceUsage = &resourceUsageResponse{
			StartTime:       ru.StartTime,
			EndTime:         ru.EndTime,
			ElapsedSeconds:  ru.ElapsedSeconds,
			MaxMemoryBytes:  ru.MaxMemoryBytes,
			CPUUserUSec:     ru.CPUUserUSec,
			CPUSystemUSec:   ru.CPUSystemUSec,
			BlockReadBytes:  ru.BlockReadBytes,
			BlockWriteBytes: ru.BlockWriteBytes,
			ExitCode:        ru.ExitCode,
			OOMKilled:       ru.OOMKilled,
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// ----- GET /api/v1/job/{job_id}/tasks -----

func (s *dashboardServer) handleGetJobTasks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobID := r.PathValue("job_id")
	q := r.URL.Query()

	var statusFilter []string
	if v := q.Get("status"); v != "" {
		for _, s := range strings.Split(v, ",") {
			if t := strings.TrimSpace(s); t != "" {
				statusFilter = append(statusFilter, t)
			}
		}
	}

	var updatedAfter time.Time
	if v := q.Get("updated_after"); v != "" {
		t, err := parseRFC3339(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid 'updated_after' timestamp")
			return
		}
		updatedAfter = t
	}

	cq := s.fs.Collection(v100.TaskCollection).Where("job_id", "==", jobID)
	if len(statusFilter) == 1 {
		cq = cq.Where("status", "==", statusFilter[0])
	}
	if !updatedAfter.IsZero() && len(statusFilter) <= 1 {
		cq = cq.Where("last_updated", ">", updatedAfter)
	}

	iter := cq.Documents(ctx)
	defer iter.Stop()

	result := []taskSummaryResponse{}
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("dashboard: GetJobTasks %s: %v", jobID, err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list tasks")
			return
		}
		var task v100.Task
		if err := snap.DataTo(&task); err != nil {
			log.Printf("dashboard: GetJobTasks DataTo: %v", err)
			continue
		}
		// Apply multi-value status filter and updated_after client-side.
		if len(statusFilter) > 1 {
			matched := false
			for _, sf := range statusFilter {
				if task.Status == sf {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		if !updatedAfter.IsZero() && !task.LastUpdated.IsZero() && !task.LastUpdated.After(updatedAfter) {
			continue
		}
		entry := taskSummaryResponse{
			TaskID:    task.TaskID,
			TaskIndex: task.TaskIndex,
			Status:    task.Status,
		}
		if !v100.IsActiveStatus(task.Status) && task.Status != v100.StatusPending {
			ec := task.ExitCode
			entry.ExitCode = &ec
		}
		if task.ResourceUsage != nil {
			ru := task.ResourceUsage
			entry.ResourceUsage = &resourceUsageResponse{
				StartTime:       ru.StartTime,
				EndTime:         ru.EndTime,
				ElapsedSeconds:  ru.ElapsedSeconds,
				MaxMemoryBytes:  ru.MaxMemoryBytes,
				CPUUserUSec:     ru.CPUUserUSec,
				CPUSystemUSec:   ru.CPUSystemUSec,
				BlockReadBytes:  ru.BlockReadBytes,
				BlockWriteBytes: ru.BlockWriteBytes,
				ExitCode:        ru.ExitCode,
				OOMKilled:       ru.OOMKilled,
			}
		}
		result = append(result, entry)
	}
	writeJSON(w, http.StatusOK, result)
}

// ----- GET /api/v1/task/{task_id}/log -----

// taskLogEntry is the unified JSON shape for both log_update and metric_update entries.
type taskLogEntry struct {
	TaskID    string    `json:"task_id"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	// log_update fields
	Content string `json:"content,omitempty"`
	// metric_update fields — use raw map so absent fields are truly absent
	ProcessCount         *int32              `json:"process_count,omitempty"`
	TotalMemory          *int64              `json:"total_memory,omitempty"`
	TotalData            *int64              `json:"total_data,omitempty"`
	TotalShared          *int64              `json:"total_shared,omitempty"`
	TotalResident        *int64              `json:"total_resident,omitempty"`
	CpuUser              *int64              `json:"cpu_user,omitempty"`
	CpuSystem            *int64              `json:"cpu_system,omitempty"`
	CpuIdle              *int64              `json:"cpu_idle,omitempty"`
	CpuIowait            *int64              `json:"cpu_iowait,omitempty"`
	MemTotal             *int64              `json:"mem_total,omitempty"`
	MemAvailable         *int64              `json:"mem_available,omitempty"`
	MemFree              *int64              `json:"mem_free,omitempty"`
	MemPressureSomeAvg10 *int32              `json:"mem_pressure_some_avg10,omitempty"`
	MemPressureFullAvg10 *int32              `json:"mem_pressure_full_avg10,omitempty"`
	Volumes              []v100.VolumeUsage  `json:"volumes,omitempty"`
}

func (s *dashboardServer) handleGetTaskLog(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	taskID := r.PathValue("task_id")
	q := r.URL.Query()

	var afterTime time.Time
	if v := q.Get("after"); v != "" {
		t, err := parseRFC3339(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid 'after' timestamp")
			return
		}
		afterTime = t
	}

	typesFilter := map[string]bool{}
	if v := q.Get("types"); v != "" {
		for _, t := range strings.Split(v, ",") {
			typesFilter[strings.TrimSpace(t)] = true
		}
	}

	cq := s.fs.Collection("TaskLog").Where("task_id", "==", taskID)
	if !afterTime.IsZero() {
		cq = cq.Where("timestamp", ">", afterTime)
	}
	if len(typesFilter) == 1 {
		for t := range typesFilter {
			cq = cq.Where("type", "==", t)
		}
	}
	cq = cq.OrderBy("timestamp", firestore.Asc)

	iter := cq.Documents(ctx)
	defer iter.Stop()

	entries := []taskLogEntry{}
	var lastTimestamp time.Time
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("dashboard: GetTaskLog %s: %v", taskID, err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to get task log")
			return
		}

		data := snap.Data()
		entryType, _ := data["type"].(string)

		// Apply multi-value type filter client-side (Firestore only supports "in" with array literal).
		if len(typesFilter) > 1 && !typesFilter[entryType] {
			continue
		}

		entry := taskLogEntry{
			TaskID: taskID,
			Type:   entryType,
		}
		if ts, ok := data["timestamp"].(time.Time); ok {
			entry.Timestamp = ts
			lastTimestamp = ts
		}

		switch entryType {
		case "log_update":
			entry.Content, _ = data["content"].(string)
		case "metric_update":
			if v, ok := data["process_count"].(int64); ok {
				v32 := int32(v)
				entry.ProcessCount = &v32
			}
			if v, ok := data["total_memory"].(int64); ok {
				entry.TotalMemory = &v
			}
			if v, ok := data["total_data"].(int64); ok {
				entry.TotalData = &v
			}
			if v, ok := data["total_shared"].(int64); ok {
				entry.TotalShared = &v
			}
			if v, ok := data["total_resident"].(int64); ok {
				entry.TotalResident = &v
			}
			if v, ok := data["cpu_user"].(int64); ok {
				entry.CpuUser = &v
			}
			if v, ok := data["cpu_system"].(int64); ok {
				entry.CpuSystem = &v
			}
			if v, ok := data["cpu_idle"].(int64); ok {
				entry.CpuIdle = &v
			}
			if v, ok := data["cpu_iowait"].(int64); ok {
				entry.CpuIowait = &v
			}
			if v, ok := data["mem_total"].(int64); ok {
				entry.MemTotal = &v
			}
			if v, ok := data["mem_available"].(int64); ok {
				entry.MemAvailable = &v
			}
			if v, ok := data["mem_free"].(int64); ok {
				entry.MemFree = &v
			}
			if v, ok := data["mem_pressure_some_avg10"].(int64); ok {
				v32 := int32(v)
				entry.MemPressureSomeAvg10 = &v32
			}
			if v, ok := data["mem_pressure_full_avg10"].(int64); ok {
				v32 := int32(v)
				entry.MemPressureFullAvg10 = &v32
			}
			if vols, ok := data["volumes"].([]interface{}); ok {
				for _, vi := range vols {
					if vm, ok := vi.(map[string]interface{}); ok {
						vol := v100.VolumeUsage{}
						vol.Location, _ = vm["location"].(string)
						if tg, ok := vm["total_gb"].(float64); ok {
							vol.TotalGB = tg
						}
						if ug, ok := vm["used_gb"].(float64); ok {
							vol.UsedGB = ug
						}
						entry.Volumes = append(entry.Volumes, vol)
					}
				}
			}
		}
		entries = append(entries, entry)
	}

	nextAfter := time.Now()
	if !lastTimestamp.IsZero() {
		nextAfter = lastTimestamp
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"entries":    entries,
		"next_after": nextAfter.Format(time.RFC3339),
	})
}

// ----- POST /api/v1/task/{task_id}/stream -----

func (s *dashboardServer) handleStreamTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	taskID := r.PathValue("task_id")

	snap, err := s.fs.Collection(v100.TaskCollection).Doc(taskID).Get(ctx)
	if err != nil {
		if grpcstatus.Code(err) == codes.NotFound {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "task not found")
			return
		}
		log.Printf("dashboard: StreamTask get %s: %v", taskID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to get task")
		return
	}
	var task v100.Task
	if err := snap.DataTo(&task); err != nil {
		log.Printf("dashboard: StreamTask DataTo: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to parse task")
		return
	}
	if task.OwningWorkerID == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "task has no owning worker")
		return
	}

	msg, _ := json.Marshal(map[string]string{
		"type":    "stream_task_updates",
		"task_id": taskID,
	})
	subID := fmt.Sprintf("sparkles-worker-in-%s", task.OwningWorkerID)
	publisher := s.ps.Publisher(subID)
	defer publisher.Stop()
	res := publisher.Publish(ctx, &pubsub.Message{Data: msg})
	if _, err := res.Get(ctx); err != nil {
		log.Printf("dashboard: StreamTask publish %s: %v", taskID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to publish stream message")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ----- GET /api/v1/events -----

type eventResponse struct {
	EventID    string    `json:"event_id"`
	Type       string    `json:"type"`
	Timestamp  time.Time `json:"timestamp"`
	Expiry     time.Time `json:"expiry"`
	WorkerID   string    `json:"worker_id,omitempty"`
	WorkpoolID string    `json:"workpool_id,omitempty"`
	TaskID     string    `json:"task_id,omitempty"`
	JobID      string    `json:"job_id,omitempty"`
	OldState   string    `json:"old_state,omitempty"`
	NewState   string    `json:"new_state,omitempty"`
}

func (s *dashboardServer) handleListEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

	var afterTime, beforeTime time.Time
	if v := q.Get("after"); v != "" {
		t, err := parseRFC3339(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid 'after' timestamp")
			return
		}
		afterTime = t
	}
	if v := q.Get("before"); v != "" {
		t, err := parseRFC3339(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid 'before' timestamp")
			return
		}
		beforeTime = t
	}

	limit := 1000
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid 'limit'")
			return
		}
		if n > 10000 {
			n = 10000
		}
		limit = n
	}

	jobID := q.Get("job_id")
	workpoolID := q.Get("workpool_id")
	taskID := q.Get("task_id")

	typesFilter := map[string]bool{}
	if v := q.Get("types"); v != "" {
		for _, t := range strings.Split(v, ",") {
			typesFilter[strings.TrimSpace(t)] = true
		}
	}

	cq := s.fs.Collection(v100.EventCollection).Query
	if !afterTime.IsZero() {
		cq = cq.Where("timestamp", ">", afterTime)
	}
	if !beforeTime.IsZero() {
		cq = cq.Where("timestamp", "<=", beforeTime)
	}
	// Apply equality filters only when a single field is provided to avoid
	// requiring composite indexes for every combination.
	if jobID != "" {
		cq = cq.Where("job_id", "==", jobID)
	} else if workpoolID != "" {
		cq = cq.Where("workpool_id", "==", workpoolID)
	} else if taskID != "" {
		cq = cq.Where("task_id", "==", taskID)
	}
	cq = cq.OrderBy("timestamp", firestore.Asc).Limit(limit)

	iter := cq.Documents(ctx)
	defer iter.Stop()

	events := []eventResponse{}
	var lastTimestamp time.Time
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("dashboard: ListEvents: %v", err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list events")
			return
		}
		var ev v100.EventRecord
		if err := snap.DataTo(&ev); err != nil {
			log.Printf("dashboard: ListEvents DataTo: %v", err)
			continue
		}
		// Client-side filter for types (and secondary equality filters when
		// multiple ID params are supplied).
		if len(typesFilter) > 0 && !typesFilter[ev.Type] {
			continue
		}
		if workpoolID != "" && jobID != "" && ev.WorkpoolID != workpoolID {
			continue
		}
		if taskID != "" && jobID != "" && ev.TaskID != taskID {
			continue
		}
		lastTimestamp = ev.Timestamp
		events = append(events, eventResponse{
			EventID:    ev.EventID,
			Type:       ev.Type,
			Timestamp:  ev.Timestamp,
			Expiry:     ev.Expiry,
			WorkerID:   ev.WorkerID,
			WorkpoolID: ev.WorkpoolID,
			TaskID:     ev.TaskID,
			JobID:      ev.JobID,
			OldState:   ev.OldState,
			NewState:   ev.NewState,
		})
	}

	resp := map[string]any{"events": events}
	if !lastTimestamp.IsZero() {
		resp["next_after"] = lastTimestamp.Format(time.RFC3339)
	}
	writeJSON(w, http.StatusOK, resp)
}

// ----- POST /api/v1/subscriptions -----

type subscriptionResponse struct {
	SubscriptionID     string `json:"subscription_id"`
	PullURL            string `json:"pull_url"`
	AckURL             string `json:"ack_url"`
	AuthorizationToken string `json:"authorization_token"`
}

func buildTypeFilter(types string) string {
	if types == "" {
		return ""
	}
	var clauses []string
	for _, t := range strings.Split(types, ",") {
		t = strings.TrimSpace(t)
		if t != "" {
			clauses = append(clauses, fmt.Sprintf(`attributes.type = "%s"`, t))
		}
	}
	return strings.Join(clauses, " OR ")
}

func (s *dashboardServer) handleCreateSubscription(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	typesParam := r.URL.Query().Get("types")
	filter := buildTypeFilter(typesParam)

	subID := newSubID()
	fullSubName := fmt.Sprintf("projects/%s/subscriptions/%s", s.project, subID)
	fullTopicName := fmt.Sprintf("projects/%s/topics/sparkles-events", s.project)
	ttl := durationpb.New(24 * time.Hour)

	sub := &pubsubpb.Subscription{
		Name:                     fullSubName,
		Topic:                    fullTopicName,
		AckDeadlineSeconds:       10,
		MessageRetentionDuration: ttl,
		ExpirationPolicy:         &pubsubpb.ExpirationPolicy{Ttl: ttl},
	}
	if filter != "" {
		sub.Filter = filter
	}

	if _, err := s.ps.SubscriptionAdminClient.CreateSubscription(ctx, sub); err != nil {
		log.Printf("dashboard: CreateSubscription: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to create subscription")
		return
	}

	token, err := s.generateSubscriberToken(ctx)
	if err != nil {
		log.Printf("dashboard: CreateSubscription token: %v", err)
		// Clean up the subscription we just created.
		_ = s.ps.SubscriptionAdminClient.DeleteSubscription(context.Background(),
			&pubsubpb.DeleteSubscriptionRequest{Subscription: fullSubName})
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to generate access token")
		return
	}

	base := fmt.Sprintf("https://pubsub.googleapis.com/v1/projects/%s/subscriptions/%s", s.project, subID)
	writeJSON(w, http.StatusOK, subscriptionResponse{
		SubscriptionID:     subID,
		PullURL:            base + ":pull",
		AckURL:             base + ":acknowledge",
		AuthorizationToken: token,
	})
}

func (s *dashboardServer) generateSubscriberToken(ctx context.Context) (string, error) {
	if s.subscriberSA == "" {
		return "", fmt.Errorf("subscriber service account not configured; set --subscriber-sa")
	}
	svc, err := iamcredentials.NewService(ctx)
	if err != nil {
		return "", fmt.Errorf("creating IAM credentials service: %w", err)
	}
	name := "projects/-/serviceAccounts/" + s.subscriberSA
	resp, err := svc.Projects.ServiceAccounts.GenerateAccessToken(name,
		&iamcredentials.GenerateAccessTokenRequest{
			Scope: []string{"https://www.googleapis.com/auth/pubsub"},
		}).Context(ctx).Do()
	if err != nil {
		return "", fmt.Errorf("generating access token: %w", err)
	}
	return resp.AccessToken, nil
}

// ----- POST /api/v1/subscriptions/{subscription_id}/unsubscribe -----

func (s *dashboardServer) handleDeleteSubscription(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	subID := r.PathValue("subscription_id")
	fullSubName := fmt.Sprintf("projects/%s/subscriptions/%s", s.project, subID)
	if err := s.ps.SubscriptionAdminClient.DeleteSubscription(ctx,
		&pubsubpb.DeleteSubscriptionRequest{Subscription: fullSubName}); err != nil {
		if grpcstatus.Code(err) == codes.NotFound {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
			return
		}
		log.Printf("dashboard: DeleteSubscription %s: %v", subID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to delete subscription")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ----- command entry point -----

func runDevDashboardBackend(c *cli.Context) error {
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}
	db := c.String("db")
	addr := c.String("addr")
	subscriberSA := c.String("subscriber-sa")

	ctx := context.Background()

	fsClient, err := firestore.NewClientWithDatabase(ctx, project, db)
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer fsClient.Close()

	psClient, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("creating pubsub client: %w", err)
	}
	defer psClient.Close()

	srv := &dashboardServer{
		project:      project,
		fs:           fsClient,
		ps:           psClient,
		subscriberSA: subscriberSA,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/workpools", srv.handleListWorkpools)
	mux.HandleFunc("GET /api/v1/workpool/{workpool_id}", srv.handleGetWorkpool)
	mux.HandleFunc("GET /api/v1/workpool/{workpool_id}/batches", srv.handleListBatches)
	mux.HandleFunc("GET /api/v1/workpool/{workpool_id}/workers", srv.handleListWorkers)
	mux.HandleFunc("GET /api/v1/jobs", srv.handleListJobs)
	mux.HandleFunc("GET /api/v1/job/{job_id}", srv.handleGetJob)
	mux.HandleFunc("GET /api/v1/job/{job_id}/summary-history", srv.handleGetJobSummaryHistory)
	mux.HandleFunc("GET /api/v1/job/{job_id}/tasks", srv.handleGetJobTasks)
	mux.HandleFunc("GET /api/v1/task/{task_id}", srv.handleGetTask)
	mux.HandleFunc("GET /api/v1/task/{task_id}/log", srv.handleGetTaskLog)
	mux.HandleFunc("POST /api/v1/task/{task_id}/stream", srv.handleStreamTask)
	mux.HandleFunc("GET /api/v1/events", srv.handleListEvents)
	mux.HandleFunc("POST /api/v1/subscriptions", srv.handleCreateSubscription)
	mux.HandleFunc("POST /api/v1/subscriptions/{subscription_id}/unsubscribe", srv.handleDeleteSubscription)

	log.Printf("dashboard-backend listening on %s", addr)
	return http.ListenAndServe(addr, corsMiddleware(mux))
}
