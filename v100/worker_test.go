package v100

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// --- mockTaskQueue ---

type failedRecord struct {
	taskID   string
	reason   string
	oldState string
}

type mockTaskQueue struct {
	tasks []*Task
	jobs  map[string]*Job

	failedTasks []failedRecord
}

func (q *mockTaskQueue) taskByID(taskID string) *Task {
	for _, t := range q.tasks {
		if t.TaskID == taskID {
			return t
		}
	}
	return nil
}

func copyTask(t *Task) *Task {
	cp := *t
	return &cp
}

func (q *mockTaskQueue) GetFirstPendingTask(_ context.Context, workpoolID string) (*Task, error) {
	for _, t := range q.tasks {
		if t.Status == StatusPending && t.WorkpoolID == workpoolID {
			return copyTask(t), nil
		}
	}
	return nil, nil
}

func (q *mockTaskQueue) GetJob(_ context.Context, jobID string) (*Job, error) {
	job, ok := q.jobs[jobID]
	if !ok {
		return nil, fmt.Errorf("job %s not found", jobID)
	}
	return job, nil
}

func (q *mockTaskQueue) ClaimTask(_ context.Context, jobID string, workerID string) (*Task, error) {
	for _, t := range q.tasks {
		if t.JobID == jobID && t.Status == StatusPending {
			t.Status = StatusClaimed
			t.OwningWorkerID = workerID
			return copyTask(t), nil
		}
	}
	return nil, nil
}

func (q *mockTaskQueue) UpdateState(_ context.Context, taskID string, _ string, newState string) error {
	t := q.taskByID(taskID)
	if t == nil {
		return fmt.Errorf("task %s not found", taskID)
	}
	t.Status = newState
	if !IsActiveStatus(newState) {
		t.OwningWorkerID = ""
	}
	return nil
}

func (q *mockTaskQueue) RecordFailed(_ context.Context, taskID string, reason string, oldState string) error {
	t := q.taskByID(taskID)
	if t != nil {
		t.Status = StatusFailed
		t.OwningWorkerID = ""
	}
	q.failedTasks = append(q.failedTasks, failedRecord{taskID: taskID, reason: reason, oldState: oldState})
	return nil
}

func (q *mockTaskQueue) RecordError(_ context.Context, _ string, _ int) error {
	return nil
}

func (q *mockTaskQueue) RecordKilled(_ context.Context, taskID string, _ bool) error {
	t := q.taskByID(taskID)
	if t != nil {
		t.Status = StatusKilled
		t.OwningWorkerID = ""
	}
	return nil
}

// --- recordingDockerCommand ---

// recordingDockerCommand writes the invocation as "image extraArgs... command" to
// logPath so tests can verify execution by reading the uploaded log via mockTransferClient.
type recordingDockerCommand struct {
	err    error        // returned after writing to logPath; nil means success
	blockC chan struct{} // if non-nil, blocks until closed
}

func (r *recordingDockerCommand) run(_ context.Context, imageName string, command []string, workDir string, extraDockerArgs []string, tel *TaskEventLog) error {
	parts := append([]string{imageName}, extraDockerArgs...)
	parts = append(parts, command...)
	if err := tel.WriteOutput(strings.Join(parts, " ")); err != nil {
		return fmt.Errorf("recordingDockerCommand: writing log: %w", err)
	}

	if r.blockC != nil {
		<-r.blockC
	}

	return r.err
}

// --- mockTransferClient ---

type mockTransferClient struct {
	files map[string][]byte
}

func newMockTransferClient() *mockTransferClient {
	return &mockTransferClient{files: make(map[string][]byte)}
}

func (m *mockTransferClient) uploadFile(_ context.Context, localPath, destPath string) error {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return fmt.Errorf("mockTransferClient upload: %w", err)
	}
	m.files[destPath] = data
	return nil
}

func (m *mockTransferClient) downloadFile(_ context.Context, srcPath, localPath string) error {
	data, ok := m.files[srcPath]
	if !ok {
		return fmt.Errorf("mockTransferClient download: %q not found", srcPath)
	}
	if err := os.WriteFile(localPath, data, 0644); err != nil {
		return fmt.Errorf("mockTransferClient download: %w", err)
	}
	return nil
}

// --- helpers ---

func makeResources(slots float64) *Resources {
	r := NewResources()
	r.Set("slots", slots)
	return &r
}

func makeTask(taskID, jobID, workpoolID string) *Task {
	return &Task{
		TaskID:      taskID,
		JobID:       jobID,
		WorkpoolID:  workpoolID,
		Status:      StatusPending,
		Command:     []string{"echo", "hello"},
		DockerImage: "ubuntu:latest",
		LogPath:     "gs://test-bucket/logs/" + taskID,
	}
}

func makeJob(jobID string, slots float64) *Job {
	return &Job{
		JobID:      jobID,
		WorkpoolID: "pool1",
		Resources:  []ResourceEntry{{Name: "slots", Value: slots}},
	}
}

func makeConfig(t *testing.T, q TaskQueue, tc TransferClient, docker *recordingDockerCommand) *WorkerLoopConfig {
	return &WorkerLoopConfig{
		WorkpoolID:           "pool1",
		WorkerID:             "worker1",
		Queue:                q,
		Resources:            makeResources(4),
		TransferClient:       tc,
		WorkDirParent:        t.TempDir(),
		Registry:             &taskRegistry{entries: make(map[string]*registeredTask)},
		FSClient:             nil,
		ExecuteDockerCommand: docker.run,
	}
}

// --- tests ---

func TestWorkerMainLoop_EmptyQueue(t *testing.T) {
	q := &mockTaskQueue{jobs: map[string]*Job{}}
	tc := newMockTransferClient()
	cfg := makeConfig(t, q, tc, &recordingDockerCommand{})

	err := workerMainLoop(context.Background(), cfg)

	require.NoError(t, err)
	require.Empty(t, q.failedTasks)
	require.Empty(t, tc.files)
}

func TestWorkerMainLoop_SingleTaskSuccess(t *testing.T) {
	q := &mockTaskQueue{
		tasks: []*Task{makeTask("t1", "j1", "pool1")},
		jobs:  map[string]*Job{"j1": makeJob("j1", 1)},
	}
	tc := newMockTransferClient()
	cfg := makeConfig(t, q, tc, &recordingDockerCommand{})

	err := workerMainLoop(context.Background(), cfg)

	require.NoError(t, err)
	require.Empty(t, q.failedTasks)
	require.Equal(t, StatusSuccess, q.taskByID("t1").Status)
	log := string(tc.files["gs://test-bucket/logs/t1"])
	require.Contains(t, log, "ubuntu:latest")
	require.Contains(t, log, "echo hello")
}

func TestWorkerMainLoop_TaskExecutionFailure(t *testing.T) {
	q := &mockTaskQueue{
		tasks: []*Task{makeTask("t1", "j1", "pool1")},
		jobs:  map[string]*Job{"j1": makeJob("j1", 1)},
	}
	tc := newMockTransferClient()
	cfg := makeConfig(t, q, tc, &recordingDockerCommand{err: fmt.Errorf("container OOM")})

	err := workerMainLoop(context.Background(), cfg)

	require.NoError(t, err) // task failure is not propagated to caller
	require.Equal(t, StatusFailed, q.taskByID("t1").Status)
	require.Len(t, q.failedTasks, 1)
	require.Equal(t, "t1", q.failedTasks[0].taskID)
	// docker did run — log was written and uploaded before returning the error
	log := string(tc.files["gs://test-bucket/logs/t1"])
	require.Contains(t, log, "ubuntu:latest")
}

func TestWorkerMainLoop_JobExceedsWorkerResources(t *testing.T) {
	q := &mockTaskQueue{
		tasks: []*Task{
			makeTask("t1", "j1", "pool1"),
			makeTask("t2", "j1", "pool1"),
		},
		jobs: map[string]*Job{"j1": makeJob("j1", 8)}, // requires 8 slots, worker has 4
	}
	tc := newMockTransferClient()
	cfg := makeConfig(t, q, tc, &recordingDockerCommand{})

	err := workerMainLoop(context.Background(), cfg)

	require.NoError(t, err)
	require.Empty(t, tc.files) // docker never ran
	require.Len(t, q.failedTasks, 2)
	for _, f := range q.failedTasks {
		require.Contains(t, f.reason, "more resources than allowed")
	}
	require.Equal(t, StatusFailed, q.taskByID("t1").Status)
	require.Equal(t, StatusFailed, q.taskByID("t2").Status)
}

func TestWorkerMainLoop_TwoTasksResourceBackpressure(t *testing.T) {
	// Worker has exactly 4 slots; each task requires 4 — only one can run at a time.
	q := &mockTaskQueue{
		tasks: []*Task{
			makeTask("t1", "j1", "pool1"),
			makeTask("t2", "j1", "pool1"),
		},
		jobs: map[string]*Job{"j1": makeJob("j1", 4)},
	}
	tc := newMockTransferClient()
	cfg := makeConfig(t, q, tc, &recordingDockerCommand{})
	cfg.Resources = makeResources(4)

	err := workerMainLoop(context.Background(), cfg)

	require.NoError(t, err)
	require.Empty(t, q.failedTasks)
	require.Equal(t, StatusSuccess, q.taskByID("t1").Status)
	require.Equal(t, StatusSuccess, q.taskByID("t2").Status)
	require.Contains(t, string(tc.files["gs://test-bucket/logs/t1"]), "ubuntu:latest")
	require.Contains(t, string(tc.files["gs://test-bucket/logs/t2"]), "ubuntu:latest")
}

func TestWorkerMainLoop_FilesToLocalizeManifest(t *testing.T) {
	tc := newMockTransferClient()
	tc.files["gs://test-bucket/inputs/file1.txt"] = []byte("content of file1")
	tc.files["gs://test-bucket/inputs/file2.txt"] = []byte("content of file2")

	manifest := []FileToLocalize{
		{Source: "gs://test-bucket/inputs/file1.txt", Destination: "file1.txt"},
		{Source: "gs://test-bucket/inputs/file2.txt", Destination: "file2.txt"},
	}
	manifestJSON, err := json.Marshal(manifest)
	require.NoError(t, err)
	tc.files["gs://test-bucket/manifests/t1"] = manifestJSON

	task := makeTask("t1", "j1", "pool1")
	task.FilesToLocalizeManifest = "gs://test-bucket/manifests/t1"

	q := &mockTaskQueue{
		tasks: []*Task{task},
		jobs:  map[string]*Job{"j1": makeJob("j1", 1)},
	}

	type fileRead struct {
		content string
		err     error
	}
	var gotFile1, gotFile2 fileRead

	verifyingDocker := func(_ context.Context, _ string, _ []string, workDir string, _ []string, tel *TaskEventLog) error {
		data1, err := os.ReadFile(filepath.Join(workDir, "file1.txt"))
		gotFile1 = fileRead{string(data1), err}

		data2, err := os.ReadFile(filepath.Join(workDir, "file2.txt"))
		gotFile2 = fileRead{string(data2), err}

		return tel.WriteOutput("ok")
	}

	cfg := makeConfig(t, q, tc, &recordingDockerCommand{})
	cfg.ExecuteDockerCommand = verifyingDocker

	err = workerMainLoop(context.Background(), cfg)
	require.NoError(t, err)
	require.Empty(t, q.failedTasks)
	require.Equal(t, StatusSuccess, q.taskByID("t1").Status)

	require.NoError(t, gotFile1.err, "file1.txt not found in work dir")
	require.Equal(t, "content of file1", gotFile1.content)
	require.NoError(t, gotFile2.err, "file2.txt not found in work dir")
	require.Equal(t, "content of file2", gotFile2.content)
}

func TestWorkerMainLoop_ContextCancelledDuringDrain(t *testing.T) {
	t.Skip("TODO: goroutine outlives test; need WaitGroup in recordingDockerCommand to synchronize cleanup")
	q := &mockTaskQueue{
		tasks: []*Task{makeTask("t1", "j1", "pool1")},
		jobs:  map[string]*Job{"j1": makeJob("j1", 1)},
	}

	blockC := make(chan struct{})
	tc := newMockTransferClient()
	cfg := makeConfig(t, q, tc, &recordingDockerCommand{blockC: blockC})

	ctx, cancel := context.WithCancel(context.Background())

	loopErr := make(chan error, 1)
	go func() {
		loopErr <- workerMainLoop(ctx, cfg)
	}()

	cancel()
	close(blockC)

	err := <-loopErr
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}
