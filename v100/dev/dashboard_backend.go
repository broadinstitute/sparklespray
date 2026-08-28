package dev

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	v100 "github.com/broadinstitute/sparklespray/v100"
	"github.com/broadinstitute/sparklespray/v100/monitor"
	"github.com/google/uuid"
	"github.com/urfave/cli"
	iamcredentials "google.golang.org/api/iamcredentials/v1"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ----- handler context -----

type dashboardServer struct {
	project string
	fs      *firestore.Client
	ps      *pubsub.Client
	config  *SparklesConfig
}

// sparklesConfigCollection is the Firestore collection holding SparklesConfig
// documents, and sparklesConfigDocID is the ID of the single doc read at
// dashboard-backend startup.
const (
	sparklesConfigCollection = "SparklesConfig"
	sparklesConfigDocID      = "default"
)

// SparklesConfig holds settings needed to service requests that aren't yet
// part of the public API surface (openapi.yaml) and so can't be supplied by a
// caller. It is read from the Firestore doc
// SparklesConfig/default at dashboard-backend startup.
type SparklesConfig struct {
	// GCSPrefix is the GCS prefix under which task result and log paths are
	// written, e.g. "gs://my-bucket/results". Mirrors the --gcs-prefix flag
	// accepted by "dev submit" (see v100/dev/submit.go).
	GCSPrefix string `firestore:"gcs_prefix" json:"gcsPrefix"`
	// SubscriberSA is the service account email used to generate short-lived
	// Pub/Sub tokens for the subscription endpoint. If empty, that endpoint
	// returns an error.
	SubscriberSA string `firestore:"subscriber_sa" json:"subscriberSA"`
	// SparklesWorkerGCSPath is the default GCS path to the sparkles-worker
	// binary/image, used when a submitted workpool omits sparklesWorkerGCSPath.
	SparklesWorkerGCSPath string `firestore:"sparkles_worker_gcs_path" json:"sparklesWorkerGCSPath"`
	// ServiceAccount is the default GCP service account email worker VMs run
	// as, used when a submitted workpool omits serviceAccount.
	ServiceAccount string `firestore:"service_account" json:"serviceAccount"`
	// Region is the default GCP region worker VMs are provisioned in, used
	// when a submitted workpool omits region.
	Region string `firestore:"region" json:"region"`
	// Zones is the default set of GCP zones eligible for worker VM placement,
	// used when a submitted workpool omits zones.
	Zones []string `firestore:"zones" json:"zones"`
}

// loadSparklesConfig reads the SparklesConfig/default doc from Firestore. It
// returns an error (wrapping codes.NotFound semantics) if the doc doesn't exist,
// since dashboard-backend has no built-in defaults to fall back to.
func loadSparklesConfig(ctx context.Context, fs *firestore.Client) (*SparklesConfig, error) {
	snap, err := fs.Collection(sparklesConfigCollection).Doc(sparklesConfigDocID).Get(ctx)
	if err != nil {
		if grpcstatus.Code(err) == codes.NotFound {
			return nil, fmt.Errorf("%s/%s not found in Firestore; it must be created before starting dashboard-backend", sparklesConfigCollection, sparklesConfigDocID)
		}
		return nil, fmt.Errorf("reading %s/%s: %w", sparklesConfigCollection, sparklesConfigDocID, err)
	}
	var config SparklesConfig
	if err := snap.DataTo(&config); err != nil {
		return nil, fmt.Errorf("parsing %s/%s: %w", sparklesConfigCollection, sparklesConfigDocID, err)
	}
	return &config, nil
}

// ----- helpers -----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("dashboard: writeJSON: %v", err)
	}
}

// writeError writes a body matching openapi.yaml's Error schema: {"code": ..., "error": ...}.
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
	WorkpoolID     string          `json:"workpool_id"`
	MachineType    string          `json:"machine_type"`
	Region         string          `json:"region"`
	State          string          `json:"state"`
	StateMessage   string          `json:"state_message"`
	LastIncidentAt *string         `json:"last_incident_at"`
	IncidentCount  int             `json:"incident_count"`
	Labels         []labelResponse `json:"labels"`
	Expiry         time.Time       `json:"expiry"`
}

func (s *dashboardServer) handleListWorkpools(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Load all WorkPoolSummary docs (mutable state) keyed by workpool_id.
	summaries := map[string]monitor.WorkPoolSummary{}
	summaryIter := s.fs.Collection(monitor.CollectionWorkPoolSummary).Documents(ctx)
	defer summaryIter.Stop()
	for {
		snap, err := summaryIter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("dashboard: ListWorkpools summary iter: %v", err)
			break
		}
		var ws monitor.WorkPoolSummary
		if err := snap.DataTo(&ws); err == nil {
			summaries[ws.WorkpoolID] = ws
		}
	}

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
		wpLabels := make([]labelResponse, len(wp.Labels))
		for i, l := range wp.Labels {
			wpLabels[i] = labelResponse{Name: l.Name, Value: l.Value}
		}
		ws := summaries[wp.WorkpoolID]
		resp := workpoolSummaryResponse{
			WorkpoolID:    wp.WorkpoolID,
			MachineType:   wp.MachineType,
			Region:        wp.Region,
			State:         string(ws.State),
			StateMessage:  ws.StateMessage,
			IncidentCount: ws.IncidentCount,
			Labels:        wpLabels,
			Expiry:        wp.Expiry,
		}
		if !ws.LastIncidentAt.IsZero() {
			t := ws.LastIncidentAt.Format(time.RFC3339)
			resp.LastIncidentAt = &t
		}
		result = append(result, resp)
	}
	writeJSON(w, http.StatusOK, result)
}

// ----- GET /api/v1/workpool/{workpool_id} -----

type workpoolDetailResponse struct {
	WorkpoolID                   string               `json:"workpool_id"`
	MachineType                  string               `json:"machine_type"`
	Region                       string               `json:"region"`
	Zones                        []string             `json:"zones"`
	RootDir                      string               `json:"root_dir"`
	SparklesWorkerGCSPath        string               `json:"sparkles_worker_gcs_path"`
	Resources                    []v100.ResourceEntry `json:"resources"`
	EmptyVolumes                 []v100.EmptyVolume   `json:"empty_volumes"`
	Labels                       []labelResponse      `json:"labels"`
	MaxWorkerCount               int                  `json:"max_worker_count"`
	MaxPreemptibleWorkerAttempts int                  `json:"max_preemptible_worker_attempts"`
	State                        string               `json:"state"`
	StateMessage                 string               `json:"state_message"`
	LastIncidentAt               *string              `json:"last_incident_at"`
	IncidentCount                int                  `json:"incident_count"`
	Expiry                       time.Time            `json:"expiry"`
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

	// Read mutable state from WorkPoolSummary.
	var ws monitor.WorkPoolSummary
	if summarySnap, err := s.fs.Collection(monitor.CollectionWorkPoolSummary).Doc(workpoolID).Get(ctx); err == nil {
		_ = summarySnap.DataTo(&ws)
	}

	detailLabels := make([]labelResponse, len(wp.Labels))
	for i, l := range wp.Labels {
		detailLabels[i] = labelResponse{Name: l.Name, Value: l.Value}
	}
	resp := workpoolDetailResponse{
		WorkpoolID:                   wp.WorkpoolID,
		MachineType:                  wp.MachineType,
		Region:                       wp.Region,
		Zones:                        wp.Zones,
		RootDir:                      wp.RootDir,
		SparklesWorkerGCSPath:        wp.SparklesWorkerGCSPath,
		Resources:                    wp.Resources,
		EmptyVolumes:                 wp.EmptyVolumes,
		Labels:                       detailLabels,
		MaxWorkerCount:               wp.MaxWorkerCount,
		MaxPreemptibleWorkerAttempts: wp.MaxPreemptibleWorkerAttempts,
		State:                        string(ws.State),
		StateMessage:                 ws.StateMessage,
		IncidentCount:                ws.IncidentCount,
		Expiry:                       wp.Expiry,
	}
	if !ws.LastIncidentAt.IsZero() {
		t := ws.LastIncidentAt.Format(time.RFC3339)
		resp.LastIncidentAt = &t
	}
	writeJSON(w, http.StatusOK, resp)
}

// ----- GET /api/v1/workpool/{workpool_id}/summary -----

type statusCountResponse struct {
	State string `json:"state"`
	Count int    `json:"count"`
}

type workpoolSummaryDetailResponse struct {
	WorkpoolID                    string                `json:"workpool_id"`
	LastUpdated                   time.Time             `json:"last_updated"`
	ExpectedPreemptibleWorkers    int                   `json:"expected_preemptible_workers"`
	ExpectedNonpreemptibleWorkers int                   `json:"expected_nonpreemptible_workers"`
	UnhealthyBatchCount           int                   `json:"unhealthy_batch_count"`
	BatchAPIRequestCounts         []statusCountResponse `json:"batch_api_request_counts"`
	PreemptibleWorkers            []statusCountResponse `json:"preemptible_workers"`
	NonpreemptibleWorkers         []statusCountResponse `json:"nonpreemptible_workers"`
	Tasks                         []statusCountResponse `json:"tasks"`
}

func (s *dashboardServer) handleGetWorkpoolSummary(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	workpoolID := r.PathValue("workpool_id")
	snap, err := s.fs.Collection(monitor.CollectionWorkPoolSummary).Doc(workpoolID).Get(ctx)
	if err != nil {
		if grpcstatus.Code(err) == codes.NotFound {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "workpool summary not found")
			return
		}
		log.Printf("dashboard: GetWorkpoolSummary %s: %v", workpoolID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to get workpool summary")
		return
	}
	var ws monitor.WorkPoolSummary
	if err := snap.DataTo(&ws); err != nil {
		log.Printf("dashboard: GetWorkpoolSummary DataTo: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to parse workpool summary")
		return
	}
	resp := workpoolSummaryDetailResponse{
		WorkpoolID:                    ws.WorkpoolID,
		LastUpdated:                   ws.LastUpdated,
		ExpectedPreemptibleWorkers:    ws.ExpectedPreemptibleWorkers,
		ExpectedNonpreemptibleWorkers: ws.ExpectedNonpreemptibleWorkers,
		UnhealthyBatchCount:           ws.UnhealthyBatchCount,
	}
	for _, sc := range ws.BatchAPIRequestCounts {
		resp.BatchAPIRequestCounts = append(resp.BatchAPIRequestCounts, statusCountResponse{State: sc.State, Count: sc.Count})
	}
	for _, sc := range ws.PreemptibleWorkers {
		resp.PreemptibleWorkers = append(resp.PreemptibleWorkers, statusCountResponse{State: sc.State, Count: sc.Count})
	}
	for _, sc := range ws.NonpreemptibleWorkers {
		resp.NonpreemptibleWorkers = append(resp.NonpreemptibleWorkers, statusCountResponse{State: sc.State, Count: sc.Count})
	}
	for _, sc := range ws.Tasks {
		resp.Tasks = append(resp.Tasks, statusCountResponse{State: sc.State, Count: sc.Count})
	}
	writeJSON(w, http.StatusOK, resp)
}

// ----- GET /api/v1/workpool/{workpool_id}/summary-history -----

type workpoolSummaryHistoryEntryResponse struct {
	WorkpoolID                    string                `json:"workpool_id"`
	Timestamp                     time.Time             `json:"timestamp"`
	ExpectedPreemptibleWorkers    int                   `json:"expected_preemptible_workers"`
	ExpectedNonpreemptibleWorkers int                   `json:"expected_nonpreemptible_workers"`
	UnhealthyBatchCount           int                   `json:"unhealthy_batch_count"`
	BatchAPIRequestCounts         []statusCountResponse `json:"batch_api_request_counts"`
	PreemptibleWorkers            []statusCountResponse `json:"preemptible_workers"`
	NonpreemptibleWorkers         []statusCountResponse `json:"nonpreemptible_workers"`
	Tasks                         []statusCountResponse `json:"tasks"`
}

func (s *dashboardServer) handleGetWorkpoolSummaryHistory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	workpoolID := r.PathValue("workpool_id")
	iter := s.fs.Collection(monitor.CollectionWorkPoolSummaryHistory).
		Where("workpool_id", "==", workpoolID).
		OrderBy("timestamp", firestore.Asc).
		Documents(ctx)
	defer iter.Stop()

	result := []workpoolSummaryHistoryEntryResponse{}
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("dashboard: GetWorkpoolSummaryHistory %s: %v", workpoolID, err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to get workpool summary history")
			return
		}
		var h monitor.WorkPoolSummaryHistory
		if err := snap.DataTo(&h); err != nil {
			log.Printf("dashboard: GetWorkpoolSummaryHistory DataTo: %v", err)
			continue
		}
		entry := workpoolSummaryHistoryEntryResponse{
			WorkpoolID:                    h.WorkpoolID,
			Timestamp:                     h.Timestamp,
			ExpectedPreemptibleWorkers:    h.ExpectedPreemptibleWorkers,
			ExpectedNonpreemptibleWorkers: h.ExpectedNonpreemptibleWorkers,
			UnhealthyBatchCount:           h.UnhealthyBatchCount,
		}
		for _, sc := range h.BatchAPIRequestCounts {
			entry.BatchAPIRequestCounts = append(entry.BatchAPIRequestCounts, statusCountResponse{State: sc.State, Count: sc.Count})
		}
		for _, sc := range h.PreemptibleWorkers {
			entry.PreemptibleWorkers = append(entry.PreemptibleWorkers, statusCountResponse{State: sc.State, Count: sc.Count})
		}
		for _, sc := range h.NonpreemptibleWorkers {
			entry.NonpreemptibleWorkers = append(entry.NonpreemptibleWorkers, statusCountResponse{State: sc.State, Count: sc.Count})
		}
		for _, sc := range h.Tasks {
			entry.Tasks = append(entry.Tasks, statusCountResponse{State: sc.State, Count: sc.Count})
		}
		result = append(result, entry)
	}
	writeJSON(w, http.StatusOK, result)
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
	// TerminationReason is populated when status is "terminated" — it
	// explains why the monitor itself decided to kill the job (as opposed
	// to "failed", where GCP reported the failure).
	TerminationReason string          `json:"termination_reason,omitempty"`
	Labels            []labelResponse `json:"labels"`
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
		monitor.BatchStatusTerminated,
	}
	batches, err := store.ListByWorkpool(ctx, workpoolID, allStatuses)
	if err != nil {
		log.Printf("dashboard: ListBatches %s: %v", workpoolID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list batches")
		return
	}
	result := make([]batchRequestResponse, 0, len(batches))
	for _, b := range batches {
		batchLabels := make([]labelResponse, len(b.Labels))
		for i, l := range b.Labels {
			batchLabels[i] = labelResponse{Name: l.Name, Value: l.Value}
		}
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
			TerminationReason:     b.TerminationReason,
			Labels:                batchLabels,
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

// ----- GET /api/v1/worker/{worker_id} -----

func (s *dashboardServer) handleGetWorker(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	workerID := r.PathValue("worker_id")
	snap, err := s.fs.Collection("Workers").Doc(workerID).Get(ctx)
	if err != nil {
		if grpcstatus.Code(err) == codes.NotFound {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "worker not found")
			return
		}
		log.Printf("dashboard: GetWorker %s: %v", workerID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to get worker")
		return
	}
	var wr v100.WorkerRecord
	if err := snap.DataTo(&wr); err != nil {
		log.Printf("dashboard: GetWorker DataTo: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to parse worker")
		return
	}
	writeJSON(w, http.StatusOK, workerResponse{
		WorkerID:        wr.WorkerID,
		WorkpoolID:      wr.WorkpoolID,
		BatchID:         wr.BatchID,
		InstanceName:    wr.InstanceName,
		Status:          wr.Status,
		Expiry:          wr.Expiry,
		HeartbeatExpiry: wr.HeartbeatExpiry,
	})
}

// ----- GET /api/v1/batch/{batch_id} -----

func (s *dashboardServer) handleGetBatch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	batchID := r.PathValue("batch_id")
	store := monitor.NewFirestoreBatchRequestStore(s.fs)
	batch, err := store.Get(ctx, batchID)
	if err != nil {
		if grpcstatus.Code(err) == codes.NotFound {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "batch not found")
			return
		}
		log.Printf("dashboard: GetBatch %s: %v", batchID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to get batch")
		return
	}
	batchLabels := make([]labelResponse, len(batch.Labels))
	for i, l := range batch.Labels {
		batchLabels[i] = labelResponse{Name: l.Name, Value: l.Value}
	}
	writeJSON(w, http.StatusOK, batchRequestResponse{
		BatchID:               batch.BatchID,
		JobID:                 batch.JobID,
		WorkpoolID:            batch.WorkpoolID,
		ExpectedVMCount:       batch.ExpectedVMCount,
		Preemptible:           batch.Preemptible,
		SubmittedAt:           batch.SubmittedAt,
		RunningSince:          batch.RunningSince,
		RegisteredWorkerCount: batch.RegisteredWorkerCount,
		Status:                string(batch.Status),
		Unhealthy:             batch.Unhealthy,
		TerminationReason:     batch.TerminationReason,
		Labels:                batchLabels,
	})
}

// ----- POST /api/v1/job -----
//
// Request/response shapes mirror components/schemas/SubmitJobBody and
// SubmitJobResponse in openapi.yaml. The write path (workpool upsert, job +
// task creation, job summary, job_created event) mirrors devSubmit in
// v100/dev/submit.go, minus the CLI-only polling/printing loop.

// submitFileToLocalizeRequest mirrors openapi's FileToLocalize schema, which
// already matches v100.FileToLocalize's json tags.
type submitFileToLocalizeRequest = v100.FileToLocalize

// submitTaskRequest mirrors openapi's Task schema.
type submitTaskRequest struct {
	FilesToLocalize []submitFileToLocalizeRequest `json:"filesToLocalize"`
	Image           string                        `json:"image"`
	Command         []string                      `json:"command"`
}

// submitJobRequest mirrors openapi's SubmitJobBody schema.
type submitJobRequest struct {
	Name            string                        `json:"name"`
	Resources       []v100.ResourceEntry          `json:"resources"`
	FilesToLocalize []submitFileToLocalizeRequest `json:"filesToLocalize"`
	Labels          []v100.Label                  `json:"labels"`
	Tasks           []submitTaskRequest           `json:"tasks"`
	Workpool        WorkpoolSpec                  `json:"workpool"`
}

// submitJobResponse mirrors openapi's SubmitJobResponse schema.
type submitJobResponse struct {
	ID string `json:"id"`
}

// applyWorkpoolDefaults fills in fields of spec that were omitted from the
// submission with defaults, either fixed values or ones supplied by config
// (for settings not yet exposed by the openapi.yaml request schema).
func applyWorkpoolDefaults(spec *WorkpoolSpec, config *SparklesConfig) {
	if spec.RootDir == "" {
		spec.RootDir = "/mnt/sparkles"
	}
	if spec.SparklesWorkerGCSPath == "" {
		spec.SparklesWorkerGCSPath = config.SparklesWorkerGCSPath
	}
	if spec.ServiceAccount == "" {
		spec.ServiceAccount = config.ServiceAccount
	}
	if len(spec.Resources) == 0 {
		spec.Resources = []v100.ResourceEntry{{Name: "slots", Value: 1}}
	}
	if spec.EmptyVolumes == nil {
		spec.EmptyVolumes = []v100.EmptyVolume{}
	}
	if spec.Region == "" {
		spec.Region = config.Region
	}
	if len(spec.Zones) == 0 {
		spec.Zones = config.Zones
	}
	if spec.MaxWorkerCount == 0 {
		spec.MaxWorkerCount = 1
	}
	if spec.MaxPreemptibleWorkerAttempts == 0 {
		spec.MaxPreemptibleWorkerAttempts = 1
	}
	if spec.MaxWorkersPerRequest == 0 {
		spec.MaxWorkersPerRequest = 25
	}
	if spec.VMShutdownGracePeriodSec == 0 {
		spec.VMShutdownGracePeriodSec = 600
	}
	if spec.MaxZombiesBeforeAbort == 0 {
		spec.MaxZombiesBeforeAbort = 5
	}
	if spec.MaxConsecutiveFailedBatches == 0 {
		spec.MaxConsecutiveFailedBatches = 5
	}
}

// workpoolIDRe matches valid workpool IDs: at most 35 characters, starting
// with a lowercase letter, followed by lowercase letters, numbers, or '-'.
var workpoolIDRe = regexp.MustCompile(`^[a-z][a-z0-9-]{0,34}$`)

func (s *dashboardServer) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req submitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid JSON body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "'name' is required")
		return
	}
	if req.Workpool.MachineType == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "'workpool.machineType' is required")
		return
	}
	if len(req.Tasks) == 0 {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "'tasks' must contain at least one task")
		return
	}
	for _, t := range req.Tasks {
		if t.Image == "" {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", "each task requires 'image'")
			return
		}
		if len(t.Command) == 0 {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", "each task requires 'command'")
			return
		}
	}

	applyWorkpoolDefaults(&req.Workpool, s.config)

	workpoolID, err := resolveWorkpoolID(&req.Workpool)
	if err != nil {
		log.Printf("dashboard: SubmitJob resolveWorkpoolID: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to resolve workpool id")
		return
	}
	workpoolSpecHash, err := computeWorkpoolSpecHash(&req.Workpool)
	if err != nil {
		log.Printf("dashboard: SubmitJob computeWorkpoolSpecHash: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to hash workpool spec")
		return
	}

	if !workpoolIDRe.MatchString(workpoolID) {
		log.Printf("dashboard: SubmitJob: workpool.id %q is invalid", workpoolID)
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "'workpool.id' must be at most 35 characters, start with a lowercase letter, and contain only lowercase letters, numbers, and '-'")
		return
	}

	now := time.Now()
	workpool := v100.WorkPool{
		WorkpoolID:            workpoolID,
		MachineType:           req.Workpool.MachineType,
		RootDir:               req.Workpool.RootDir,
		SparklesWorkerGCSPath: req.Workpool.SparklesWorkerGCSPath,
		ServiceAccount:        req.Workpool.ServiceAccount,
		Resources:             req.Workpool.Resources,
		EmptyVolumes:          req.Workpool.EmptyVolumes,
		Labels:                req.Workpool.Labels,
		WorkpoolSpecHash:      workpoolSpecHash,
		Expiry:                now.Add(7 * 24 * time.Hour),
		Region:                req.Workpool.Region,
		Zones:                 req.Workpool.Zones,

		MaxWorkerCount:               req.Workpool.MaxWorkerCount,
		MaxPreemptibleWorkerAttempts: req.Workpool.MaxPreemptibleWorkerAttempts,
		MaxWorkersPerRequest:         req.Workpool.MaxWorkersPerRequest,

		VMShutdownGracePeriodSec:    req.Workpool.VMShutdownGracePeriodSec,
		MaxZombiesBeforeAbort:       req.Workpool.MaxZombiesBeforeAbort,
		MaxConsecutiveFailedBatches: req.Workpool.MaxConsecutiveFailedBatches,
	}
	if _, err := s.fs.Collection(v100.WorkpoolCollection).Doc(workpoolID).Set(ctx, workpool); err != nil {
		log.Printf("dashboard: SubmitJob writing workpool %s: %v", workpoolID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to write workpool")
		return
	}

	jobID := uuid.New().String()
	job := v100.Job{
		JobID:      jobID,
		Name:       req.Name,
		WorkpoolID: workpoolID,
		CreatedAt:  now,
		Expiry:     now.Add(7 * 24 * time.Hour),
		TaskCount:  len(req.Tasks),
		Resources:  req.Resources,
		Labels:     req.Labels,
	}

	// Pre-generate task IDs outside the transaction so retries are idempotent.
	taskIDs := make([]string, len(req.Tasks))
	for i := range taskIDs {
		taskIDs[i] = uuid.New().String()
	}

	err = s.fs.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := tx.Set(s.fs.Collection(v100.JobCollection).Doc(jobID), job); err != nil {
			return err
		}
		for i, t := range req.Tasks {
			taskPrefix := fmt.Sprintf("%s/%s/%d", s.config.GCSPrefix, req.Name, i)
			// Per-task files are localized in addition to job-level files.
			filesToLocalize := append([]v100.FileToLocalize{}, req.FilesToLocalize...)
			filesToLocalize = append(filesToLocalize, t.FilesToLocalize...)
			task := v100.Task{
				JobID:           jobID,
				TaskID:          taskIDs[i],
				TaskIndex:       i,
				WorkpoolID:      workpoolID,
				Status:          v100.StatusPending,
				Command:         t.Command,
				DockerImage:     t.Image,
				FilesToLocalize: filesToLocalize,
				Labels:          req.Labels,
				ResultPath:      taskPrefix,
				LogPath:         taskPrefix + "/stdout.txt",
				Expiry:          now.Add(7 * 24 * time.Hour),
			}
			if err := tx.Set(s.fs.Collection(v100.TaskCollection).Doc(taskIDs[i]), task); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("dashboard: SubmitJob writing job %s: %v", jobID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to write job and tasks")
		return
	}

	// Create the initial JobSummary so the monitor's job-summary poll can track it.
	summaryStore := monitor.NewFirestoreJobSummaryStore(s.fs)
	if err := summaryStore.Create(ctx, &monitor.JobSummary{
		JobID:       jobID,
		WorkpoolID:  workpoolID,
		CreatedAt:   now,
		Expiry:      now.Add(7 * 24 * time.Hour),
		LastUpdated: now,
		State:       monitor.JobStatusPending,
		Tasks:       []monitor.StateCount{{State: "pending", Count: len(req.Tasks)}},
		Labels:      toMonitorLabels(req.Labels),
	}); err != nil {
		log.Printf("dashboard: SubmitJob creating job summary %s: %v", jobID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to create job summary")
		return
	}

	// Publish job_created event so the monitor can react immediately.
	// Non-fatal: the monitor's periodic poll will pick up the job if this fails.
	ep := v100.NewEventPublisher(s.ps.Publisher("sparkles-events"), s.fs)
	defer ep.Stop()
	if err := ep.PublishJobCreated(ctx, v100.JobCreatedEvent{JobID: jobID, WorkpoolID: workpoolID}); err != nil {
		log.Printf("dashboard: SubmitJob publishing job_created event %s: %v", jobID, err)
	}

	writeJSON(w, http.StatusOK, submitJobResponse{ID: jobID})
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
	State      string              `json:"state"`
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
	// Only apply server-side ordering when there is no equality filter on workpool_id;
	// combining an equality filter with an order-by requires a composite index that
	// may not exist. When filtering by workpool_id the result set is small enough to
	// sort client-side (see handleListJobs response assembly below).
	if workpoolID == "" {
		cq = cq.OrderBy("created_at", firestore.Desc)
	}

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
	if workpoolID != "" {
		slices.SortFunc(result, func(a, b jobSummaryResponse) int {
			return b.CreatedAt.Compare(a.CreatedAt)
		})
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
		State:      string(js.State),
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

// ----- GET /api/v1/job/{job_id}/summary -----

func (s *dashboardServer) handleGetJobSummary(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobID := r.PathValue("job_id")
	snap, err := s.fs.Collection("JobSummary").Doc(jobID).Get(ctx)
	if err != nil {
		if grpcstatus.Code(err) == codes.NotFound {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "job summary not found")
			return
		}
		log.Printf("dashboard: GetJobSummary %s: %v", jobID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to get job summary")
		return
	}
	var js monitor.JobSummary
	if err := snap.DataTo(&js); err != nil {
		log.Printf("dashboard: GetJobSummary DataTo: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to parse job summary")
		return
	}
	writeJSON(w, http.StatusOK, jobSummaryToResponse(&js))
}

// ----- GET /api/v1/job/{job_id}/summary-history -----

type jobSummaryHistoryEntryResponse struct {
	JobID      string              `json:"job_id"`
	WorkpoolID string              `json:"workpool_id"`
	Timestamp  time.Time           `json:"timestamp"`
	State      string              `json:"state"`
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
			State:      string(h.State),
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

type taskResponse struct {
	TaskID         string                 `json:"task_id"`
	TaskIndex      int                    `json:"task_index"`
	JobID          string                 `json:"job_id"`
	WorkpoolID     string                 `json:"workpool_id"`
	Status         string                 `json:"status"`
	Command        []string               `json:"command"`
	DockerImage    string                 `json:"docker_image"`
	ResultPath     string                 `json:"result_path,omitempty"`
	LogPath        string                 `json:"log_path,omitempty"`
	OwningWorkerID string                 `json:"owning_worker_id,omitempty"`
	FailureReason  string                 `json:"failure_reason,omitempty"`
	Labels         []labelResponse        `json:"labels"`
	ExitCode       *int                   `json:"exit_code,omitempty"`
	ResourceUsage  *resourceUsageResponse `json:"resource_usage,omitempty"`
	VMConsoleURL   string                 `json:"vm_console_url,omitempty"`
}

// instanceNameRe parses the custom "project/<project>/zone/<zone>/instance/<instance>"
// format written into WorkerRecord.InstanceName (see worker.go).
var instanceNameRe = regexp.MustCompile(`^project/([^/]+)/zone/([^/]+)/instance/([^/]+)$`)

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
	labels := make([]labelResponse, len(task.Labels))
	for i, l := range task.Labels {
		labels[i] = labelResponse{Name: l.Name, Value: l.Value}
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
		Labels:         labels,
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
	if task.OwningWorkerID != "" {
		if wsnap, werr := s.fs.Collection("Workers").Doc(task.OwningWorkerID).Get(ctx); werr == nil {
			var wr v100.WorkerRecord
			if derr := wsnap.DataTo(&wr); derr == nil {
				if m := instanceNameRe.FindStringSubmatch(wr.InstanceName); m != nil {
					resp.VMConsoleURL = fmt.Sprintf(
						"https://console.cloud.google.com/compute/instancesDetail/zones/%s/instances/%s?project=%s",
						m[2], m[3], m[1],
					)
				}
			}
		} else if grpcstatus.Code(werr) != codes.NotFound {
			log.Printf("dashboard: GetTask %s: lookup worker %s: %v", taskID, task.OwningWorkerID, werr)
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
	ProcessCount         *int32             `json:"process_count,omitempty"`
	TotalMemory          *int64             `json:"total_memory,omitempty"`
	TotalData            *int64             `json:"total_data,omitempty"`
	TotalShared          *int64             `json:"total_shared,omitempty"`
	TotalResident        *int64             `json:"total_resident,omitempty"`
	CpuUser              *float64           `json:"cpu_user,omitempty"`
	CpuSystem            *float64           `json:"cpu_system,omitempty"`
	CpuIdle              *float64           `json:"cpu_idle,omitempty"`
	CpuIowait            *float64           `json:"cpu_iowait,omitempty"`
	MemTotal             *int64             `json:"mem_total,omitempty"`
	MemAvailable         *int64             `json:"mem_available,omitempty"`
	MemFree              *int64             `json:"mem_free,omitempty"`
	MemPressureSomeAvg10 *int32             `json:"mem_pressure_some_avg10,omitempty"`
	MemPressureFullAvg10 *int32             `json:"mem_pressure_full_avg10,omitempty"`
	Volumes              []v100.VolumeUsage `json:"volumes,omitempty"`
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
			if v, ok := data["cpu_user"].(float64); ok {
				entry.CpuUser = &v
			}
			if v, ok := data["cpu_system"].(float64); ok {
				entry.CpuSystem = &v
			}
			if v, ok := data["cpu_idle"].(float64); ok {
				entry.CpuIdle = &v
			}
			if v, ok := data["cpu_iowait"].(float64); ok {
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
	// StateMessage carries the workpool_state_change message and the
	// batch_failed reason (both stored in EventRecord.StateMessage).
	StateMessage string `json:"state_message,omitempty"`
	// CleanlyTerminated is populated for worker_stopped events.
	CleanlyTerminated bool `json:"cleanly_terminated,omitempty"`
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
	sortDir := firestore.Asc
	descending := q.Get("order") == "desc"
	if descending {
		sortDir = firestore.Desc
	}
	cq = cq.OrderBy("timestamp", sortDir).Limit(limit)

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
			EventID:           ev.EventID,
			Type:              ev.Type,
			Timestamp:         ev.Timestamp,
			Expiry:            ev.Expiry,
			WorkerID:          ev.WorkerID,
			WorkpoolID:        ev.WorkpoolID,
			TaskID:            ev.TaskID,
			JobID:             ev.JobID,
			OldState:          ev.OldState,
			NewState:          ev.NewState,
			StateMessage:      ev.StateMessage,
			CleanlyTerminated: ev.CleanlyTerminated,
		})
	}

	resp := map[string]any{"events": events}
	// next_after is a cursor for incremental forward polling (see
	// EventProvider on the frontend); it only makes sense in ascending
	// order, where the last-iterated event is also the most recent one.
	if !descending && !lastTimestamp.IsZero() {
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
	if s.config.SubscriberSA == "" {
		return "", fmt.Errorf("subscriber service account not configured; set SparklesConfig.SubscriberSA")
	}
	svc, err := iamcredentials.NewService(ctx)
	if err != nil {
		return "", fmt.Errorf("creating IAM credentials service: %w", err)
	}
	name := "projects/-/serviceAccounts/" + s.config.SubscriberSA
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

	config, err := loadSparklesConfig(ctx, fsClient)
	if err != nil {
		return fmt.Errorf("loading sparkles config: %w", err)
	}

	srv := &dashboardServer{
		project: project,
		fs:      fsClient,
		ps:      psClient,
		config:  config,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/workpools", srv.handleListWorkpools)
	mux.HandleFunc("GET /api/v1/workpool/{workpool_id}", srv.handleGetWorkpool)
	mux.HandleFunc("GET /api/v1/workpool/{workpool_id}/batches", srv.handleListBatches)
	mux.HandleFunc("GET /api/v1/workpool/{workpool_id}/workers", srv.handleListWorkers)
	mux.HandleFunc("GET /api/v1/worker/{worker_id}", srv.handleGetWorker)
	mux.HandleFunc("GET /api/v1/batch/{batch_id}", srv.handleGetBatch)
	mux.HandleFunc("GET /api/v1/workpool/{workpool_id}/summary", srv.handleGetWorkpoolSummary)
	mux.HandleFunc("GET /api/v1/workpool/{workpool_id}/summary-history", srv.handleGetWorkpoolSummaryHistory)
	mux.HandleFunc("POST /api/v1/job", srv.handleSubmitJob)
	mux.HandleFunc("GET /api/v1/jobs", srv.handleListJobs)
	mux.HandleFunc("GET /api/v1/job/{job_id}", srv.handleGetJob)
	mux.HandleFunc("GET /api/v1/job/{job_id}/summary", srv.handleGetJobSummary)
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
