package functest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	v100 "github.com/broadinstitute/sparklespray/v100"
	"github.com/broadinstitute/sparklespray/v100/dev"
	"github.com/google/uuid"
)

// TestLocalizeAndCommand uploads two input files to GCS, runs a task that
// localizes them as a.txt / b.txt, concatenates them, and verifies the result
// file out.txt is uploaded back to the result GCS path containing "catdog".
func TestLocalizeAndCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	startFirestoreEmulator(t)
	startPubSubEmulator(t)
	startGCSEmulator(t)

	fsClient := newFirestoreClient(t, ctx)
	psClient := newPubSubClient(t, ctx)
	gcsClient := newGCSClient(t, ctx)
	ensureTopics(t, ctx, psClient)

	bucket := gcsClient.Bucket(gcsBucket)
	writeGCSObject(t, ctx, bucket, "inputs/a.txt", "cat")
	writeGCSObject(t, ctx, bucket, "inputs/b.txt", "dog")

	workpoolID := "pool-localize-" + randomSuffix()
	startWorker(t, ctx, workpoolID)

	jobID := uuid.New().String()
	taskID := uuid.New().String()
	gcsBase := fmt.Sprintf("gs://%s/jobs/%s", gcsBucket, jobID)
	resultPath := fmt.Sprintf("%s/results/%s/", gcsBase, taskID)

	job := v100.Job{
		JobID:      jobID,
		WorkpoolID: workpoolID,
		Resources:  []v100.ResourceEntry{{Name: "slots", Value: 1}},
	}
	if _, err := fsClient.Collection(v100.JobCollection).Doc(jobID).Set(ctx, job); err != nil {
		t.Fatalf("writing job: %v", err)
	}

	task := v100.Task{
		JobID:       jobID,
		TaskID:      taskID,
		TaskIndex:   0,
		WorkpoolID:  workpoolID,
		Status:      v100.StatusPending,
		Command:     []string{"sh", "-c", "cat a.txt b.txt > out.txt"},
		DockerImage: "unused",
		LogPath:     fmt.Sprintf("%s/logs/%s.log", gcsBase, taskID),
		ResultPath:  resultPath,
		FilesToLocalize: []v100.FileToLocalize{
			{Source: fmt.Sprintf("gs://%s/inputs/a.txt", gcsBucket), Destination: "a.txt"},
			{Source: fmt.Sprintf("gs://%s/inputs/b.txt", gcsBucket), Destination: "b.txt"},
		},
	}
	if _, err := fsClient.Collection(v100.TaskCollection).Doc(taskID).Set(ctx, task); err != nil {
		t.Fatalf("writing task: %v", err)
	}

	statuses := waitForAllTasksTerminal(t, ctx, fsClient, jobID, 60*time.Second)
	if statuses[taskID] != v100.StatusSuccess {
		t.Fatalf("task %s: want status=%s, got %s", taskID, v100.StatusSuccess, statuses[taskID])
	}

	// Verify out.txt was uploaded to the result path and contains "catdog".
	objPath := fmt.Sprintf("jobs/%s/results/%s/out.txt", jobID, taskID)
	r, err := bucket.Object(objPath).NewReader(ctx)
	if err != nil {
		t.Fatalf("reading result gs://%s/%s: %v", gcsBucket, objPath, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("reading result data: %v", err)
	}
	if got := string(data); got != "catdog" {
		t.Errorf("out.txt: want %q, got %q", "catdog", got)
	}
}

// TestSubmitAndComplete submits a two-task job, waits for both tasks to run to
// completion via the no-docker worker, and asserts both reach status=success.
func TestSubmitAndComplete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	startFirestoreEmulator(t)
	startPubSubEmulator(t)
	startGCSEmulator(t)

	fsClient := newFirestoreClient(t, ctx)
	psClient := newPubSubClient(t, ctx)
	ensureTopics(t, ctx, psClient)

	workpoolID := "pool-submit-" + randomSuffix()

	startWorker(t, ctx, workpoolID)
	startWorker(t, ctx, workpoolID)

	jobID := submitJob(t, ctx, fsClient, workpoolID, [][]string{
		{"sh", "-c", "echo hello"},
		{"sh", "-c", "echo world"},
	})

	statuses := waitForAllTasksTerminal(t, ctx, fsClient, jobID, 90*time.Second)

	for taskID, status := range statuses {
		if status != v100.StatusSuccess {
			t.Errorf("task %s: want status=%s, got %s", taskID, v100.StatusSuccess, status)
		}
	}
}

// TestKillJob submits a two-task job running long sleeps, waits until at least
// one task is claimed or running, then kills the job and asserts both tasks
// ultimately reach status=killed.
func TestKillJob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	startFirestoreEmulator(t)
	startPubSubEmulator(t)
	startGCSEmulator(t)

	fsClient := newFirestoreClient(t, ctx)
	psClient := newPubSubClient(t, ctx)
	ensureTopics(t, ctx, psClient)

	workpoolID := "pool-kill-" + randomSuffix()

	// one worker, so one task should get marked as killed and one task should get interrupted
	startWorker(t, ctx, workpoolID)

	jobID := submitJob(t, ctx, fsClient, workpoolID, [][]string{
		{"sleep", "100000"},
		{"sleep", "100000"},
	})

	// Wait until at least one task is claimed or running before issuing the kill.
	waitForAnyTaskInStates(t, ctx, fsClient, jobID,
		[]string{v100.StatusClaimed, v100.StatusRunning}, 30*time.Second)

	// With one worker, one task should be running and the other still pending.
	pendingCount := 0
	for _, status := range taskStatuses(t, ctx, fsClient, jobID) {
		if status == v100.StatusPending {
			pendingCount++
		}
	}
	if pendingCount == 0 {
		t.Fatal("expected at least one task to still be in pending state before kill")
	}

	if err := v100.KillJob(ctx, testProject, testDB, jobID); err != nil {
		t.Fatalf("KillJob: %v", err)
	}

	statuses := waitForAllTasksTerminal(t, ctx, fsClient, jobID, 30*time.Second)

	for taskID, status := range statuses {
		if status != v100.StatusKilled {
			t.Errorf("task %s: want status=%s, got %s", taskID, v100.StatusKilled, status)
		}
	}
}

// TestCancelJobViaAPI is TestKillJob's scenario driven through the dashboard
// API's POST /api/v1/job/{job_id}/cancel endpoint instead of calling
// v100.KillJob directly, to verify the HTTP route itself (registration,
// existence check, response shape) on top of the already-covered kill
// mechanism.
func TestCancelJobViaAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	startFirestoreEmulator(t)
	startPubSubEmulator(t)
	startGCSEmulator(t)

	fsClient := newFirestoreClient(t, ctx)
	psClient := newPubSubClient(t, ctx)
	ensureTopics(t, ctx, psClient)

	// NewDashboardHandler requires a SparklesConfig/default doc to exist; an
	// all-zero config is fine since cancelling a job doesn't touch any of
	// its fields.
	if _, err := fsClient.Collection("SparklesConfig").Doc("default").Set(ctx, dev.SparklesConfig{}); err != nil {
		t.Fatalf("writing SparklesConfig/default: %v", err)
	}
	apiKey := writeAPIKey(t, ctx, fsClient, "test-user")

	handler, err := dev.NewDashboardHandler(ctx, testProject, fsClient, psClient, "")
	if err != nil {
		t.Fatalf("NewDashboardHandler: %v", err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	workpoolID := "pool-cancel-" + randomSuffix()

	// one worker, so one task should get marked as killed and one task should get interrupted
	startWorker(t, ctx, workpoolID)

	jobID := submitJob(t, ctx, fsClient, workpoolID, [][]string{
		{"sleep", "100000"},
		{"sleep", "100000"},
	})

	// Wait until at least one task is claimed or running before issuing the cancel.
	waitForAnyTaskInStates(t, ctx, fsClient, jobID,
		[]string{v100.StatusClaimed, v100.StatusRunning}, 30*time.Second)

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/job/%s/cancel", server.URL, jobID), nil)
	if err != nil {
		t.Fatalf("building cancel request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST cancel: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST cancel: want status 200, got %d", resp.StatusCode)
	}

	statuses := waitForAllTasksTerminal(t, ctx, fsClient, jobID, 30*time.Second)

	for taskID, status := range statuses {
		if status != v100.StatusKilled {
			t.Errorf("task %s: want status=%s, got %s", taskID, v100.StatusKilled, status)
		}
	}
}

// TestCancelJob_NotFound verifies the cancel endpoint returns 404 for a job
// that doesn't exist.
func TestCancelJob_NotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startFirestoreEmulator(t)
	startPubSubEmulator(t)

	fsClient := newFirestoreClient(t, ctx)
	psClient := newPubSubClient(t, ctx)
	ensureTopics(t, ctx, psClient)

	if _, err := fsClient.Collection("SparklesConfig").Doc("default").Set(ctx, dev.SparklesConfig{}); err != nil {
		t.Fatalf("writing SparklesConfig/default: %v", err)
	}
	apiKey := writeAPIKey(t, ctx, fsClient, "test-user")

	handler, err := dev.NewDashboardHandler(ctx, testProject, fsClient, psClient, "")
	if err != nil {
		t.Fatalf("NewDashboardHandler: %v", err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/job/%s/cancel", server.URL, "no-such-job"), nil)
	if err != nil {
		t.Fatalf("building cancel request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST cancel: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("POST cancel for nonexistent job: want status 404, got %d", resp.StatusCode)
	}
}

// newWorkpoolAPIServer sets up a dashboard-backend HTTP handler against fresh
// Firestore/PubSub clients, seeding the SparklesConfig doc and an API key it
// needs. Returns the server and a bearer token to use for requests.
func newWorkpoolAPIServer(t *testing.T, ctx context.Context) (*httptest.Server, string) {
	t.Helper()
	fsClient := newFirestoreClient(t, ctx)
	psClient := newPubSubClient(t, ctx)
	ensureTopics(t, ctx, psClient)

	if _, err := fsClient.Collection("SparklesConfig").Doc("default").Set(ctx, dev.SparklesConfig{}); err != nil {
		t.Fatalf("writing SparklesConfig/default: %v", err)
	}
	apiKey := writeAPIKey(t, ctx, fsClient, "test-user")

	handler, err := dev.NewDashboardHandler(ctx, testProject, fsClient, psClient, "")
	if err != nil {
		t.Fatalf("NewDashboardHandler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server, apiKey
}

func patchWorkpool(t *testing.T, server *httptest.Server, apiKey, workpoolID string, body map[string]any) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshalling PATCH body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPatch,
		fmt.Sprintf("%s/api/v1/workpool/%s", server.URL, workpoolID), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("building PATCH request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PATCH workpool: %v", err)
	}
	return resp
}

// TestUpdateWorkpoolViaAPI verifies PATCH /api/v1/workpool/{workpool_id}
// changes only the fields provided, leaving the rest untouched.
func TestUpdateWorkpoolViaAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startFirestoreEmulator(t)
	startPubSubEmulator(t)
	fsClient := newFirestoreClient(t, ctx)

	workpoolID := "pool-patch-" + randomSuffix()
	writeWorkPool(t, ctx, fsClient, &v100.WorkPool{
		WorkpoolID:                   workpoolID,
		MaxWorkerCount:               10,
		MaxPreemptibleWorkerAttempts: 5,
		MaxWorkersPerRequest:         25,
		MaxZombiesBeforeAbort:        3,
		MaxConsecutiveFailedBatches:  2,
	})

	server, apiKey := newWorkpoolAPIServer(t, ctx)

	resp := patchWorkpool(t, server, apiKey, workpoolID, map[string]any{
		"max_worker_count":                20,
		"max_preemptible_worker_attempts": 0,
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PATCH workpool: want status 200, got %d", resp.StatusCode)
	}
	var got struct {
		WorkpoolID                   string `json:"workpool_id"`
		MaxWorkerCount               int    `json:"max_worker_count"`
		MaxPreemptibleWorkerAttempts int    `json:"max_preemptible_worker_attempts"`
		MaxWorkersPerRequest         int    `json:"max_workers_per_request"`
		MaxZombiesBeforeAbort        int    `json:"max_zombies_before_abort"`
		MaxConsecutiveFailedBatches  int    `json:"max_consecutive_failed_batches"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decoding PATCH response: %v", err)
	}
	if got.MaxWorkerCount != 20 {
		t.Errorf("MaxWorkerCount: want 20, got %d", got.MaxWorkerCount)
	}
	if got.MaxPreemptibleWorkerAttempts != 0 {
		t.Errorf("MaxPreemptibleWorkerAttempts: want 0, got %d", got.MaxPreemptibleWorkerAttempts)
	}
	// Untouched fields should retain their original values.
	if got.MaxWorkersPerRequest != 25 {
		t.Errorf("MaxWorkersPerRequest: want unchanged 25, got %d", got.MaxWorkersPerRequest)
	}
	if got.MaxZombiesBeforeAbort != 3 {
		t.Errorf("MaxZombiesBeforeAbort: want unchanged 3, got %d", got.MaxZombiesBeforeAbort)
	}
	if got.MaxConsecutiveFailedBatches != 2 {
		t.Errorf("MaxConsecutiveFailedBatches: want unchanged 2, got %d", got.MaxConsecutiveFailedBatches)
	}

	// Confirm Firestore itself reflects the same partial update.
	snap, err := fsClient.Collection(v100.WorkpoolCollection).Doc(workpoolID).Get(ctx)
	if err != nil {
		t.Fatalf("re-reading workpool: %v", err)
	}
	var wp v100.WorkPool
	if err := snap.DataTo(&wp); err != nil {
		t.Fatalf("parsing workpool: %v", err)
	}
	if wp.MaxWorkerCount != 20 || wp.MaxPreemptibleWorkerAttempts != 0 {
		t.Errorf("Firestore doc: want MaxWorkerCount=20 MaxPreemptibleWorkerAttempts=0, got %+v", wp)
	}
	if wp.MaxWorkersPerRequest != 25 || wp.MaxZombiesBeforeAbort != 3 || wp.MaxConsecutiveFailedBatches != 2 {
		t.Errorf("Firestore doc: untouched fields changed unexpectedly: %+v", wp)
	}
}

// TestUpdateWorkpool_NotFound verifies the update endpoint returns 404 for a
// workpool that doesn't exist.
func TestUpdateWorkpool_NotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startFirestoreEmulator(t)
	startPubSubEmulator(t)

	server, apiKey := newWorkpoolAPIServer(t, ctx)

	resp := patchWorkpool(t, server, apiKey, "no-such-workpool", map[string]any{"max_worker_count": 5})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("PATCH nonexistent workpool: want status 404, got %d", resp.StatusCode)
	}
}

// TestUpdateWorkpool_NegativeValue verifies the update endpoint rejects a
// negative value with 400 and leaves the document unchanged.
func TestUpdateWorkpool_NegativeValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startFirestoreEmulator(t)
	startPubSubEmulator(t)
	fsClient := newFirestoreClient(t, ctx)

	workpoolID := "pool-patch-neg-" + randomSuffix()
	writeWorkPool(t, ctx, fsClient, &v100.WorkPool{
		WorkpoolID:     workpoolID,
		MaxWorkerCount: 10,
	})

	server, apiKey := newWorkpoolAPIServer(t, ctx)

	resp := patchWorkpool(t, server, apiKey, workpoolID, map[string]any{"max_worker_count": -1})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("PATCH with negative value: want status 400, got %d", resp.StatusCode)
	}

	snap, err := fsClient.Collection(v100.WorkpoolCollection).Doc(workpoolID).Get(ctx)
	if err != nil {
		t.Fatalf("re-reading workpool: %v", err)
	}
	var wp v100.WorkPool
	if err := snap.DataTo(&wp); err != nil {
		t.Fatalf("parsing workpool: %v", err)
	}
	if wp.MaxWorkerCount != 10 {
		t.Errorf("MaxWorkerCount: want unchanged 10, got %d", wp.MaxWorkerCount)
	}
}
