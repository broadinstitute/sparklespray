package autoscaler

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/broadinstitute/sparklespray/v100/scheduler"
)

// epoch is the fixed base time used across all autoscaler tests.
var epoch = time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

// ---- FakeBatchAPIClient ----

type fakeJob struct {
	jobID       string
	batchID     string
	workpoolID  string
	preemptible bool
	status      BatchJobStatus
	activeVMs   map[string]bool // instance names currently "running" (in RUNNING state)
}

type FakeCreatedJob struct {
	JobID       string
	BatchID     string
	WorkpoolID  string
	VMCount     int
	Preemptible bool
}

type FakeBatchAPIClient struct {
	mu        sync.Mutex
	jobs      map[string]*fakeJob // jobID → job
	nextJobID int

	CreatedJobs    []FakeCreatedJob
	TerminatedVMs  []string
	TerminatedJobs []string
}

func newFakeBatchAPIClient() *FakeBatchAPIClient {
	return &FakeBatchAPIClient{
		jobs: make(map[string]*fakeJob),
	}
}

// AddJob pre-configures a job in the fake. If status is RUNNING, VMs are pre-populated.
func (f *FakeBatchAPIClient) AddJob(jobID, batchID, workpoolID string, vmCount int, status BatchJobStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()

	activeVMs := make(map[string]bool)
	if status == BatchJobStatusRunning {
		for i := 0; i < vmCount; i++ {
			activeVMs[vmName(batchID, i)] = true
		}
	}
	f.jobs[jobID] = &fakeJob{
		jobID:      jobID,
		batchID:    batchID,
		workpoolID: workpoolID,
		status:     status,
		activeVMs:  activeVMs,
	}
}

// SetJobStatus transitions a job's Batch API status.
// Clears VMs for SUCCEEDED/FAILED (they're gone when the job ends).
func (f *FakeBatchAPIClient) SetJobStatus(jobID string, status BatchJobStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()

	j := f.jobs[jobID]
	if j == nil {
		return
	}
	j.status = status
	if status == BatchJobStatusSucceeded || status == BatchJobStatusFailed {
		j.activeVMs = make(map[string]bool)
	}
}

// RemoveVM removes one VM from a running job without touching job status.
// Simulates a VM that crashed or fast-failed.
func (f *FakeBatchAPIClient) RemoveVM(instanceName string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, j := range f.jobs {
		delete(j.activeVMs, instanceName)
	}
}

func (f *FakeBatchAPIClient) CreateJob(ctx context.Context, spec *WorkerJobSpec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.nextJobID++
	jobID := fmt.Sprintf("job-%d", f.nextJobID)

	activeVMs := make(map[string]bool)
	for i := 0; i < spec.VMCount; i++ {
		activeVMs[vmName(spec.BatchID, i)] = true
	}

	f.jobs[jobID] = &fakeJob{
		jobID:       jobID,
		batchID:     spec.BatchID,
		workpoolID:  spec.WorkpoolID,
		preemptible: spec.Preemptible,
		status:      BatchJobStatusQueued,
		activeVMs:   activeVMs,
	}

	f.CreatedJobs = append(f.CreatedJobs, FakeCreatedJob{
		JobID:       jobID,
		BatchID:     spec.BatchID,
		WorkpoolID:  spec.WorkpoolID,
		VMCount:     spec.VMCount,
		Preemptible: spec.Preemptible,
	})

	return jobID, nil
}

func (f *FakeBatchAPIClient) GetJobStatus(ctx context.Context, jobID string) (BatchJobStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	j := f.jobs[jobID]
	if j == nil {
		return BatchJobStatusFailed, fmt.Errorf("job %s not found", jobID)
	}
	return j.status, nil
}

func (f *FakeBatchAPIClient) ListRunningVMs(ctx context.Context, filterLabelName, filterLabelValue string, zones []string) (map[string]VMInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	result := make(map[string]VMInfo)
	for _, j := range f.jobs {
		if j.status != BatchJobStatusRunning {
			continue
		}
		switch filterLabelName {
		case "sparkles-worker-batch":
			if j.batchID != filterLabelValue {
				continue
			}
		case "sparkles-worker-workpool":
			if j.workpoolID != filterLabelValue {
				continue
			}
		default:
			continue
		}
		for name := range j.activeVMs {
			result[name] = VMInfo{InstanceName: name, Zone: "fake-zone"}
		}
	}
	return result, nil
}

func (f *FakeBatchAPIClient) TerminateVM(ctx context.Context, zone, instanceName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, j := range f.jobs {
		delete(j.activeVMs, instanceName)
	}
	f.TerminatedVMs = append(f.TerminatedVMs, instanceName)
	return nil
}

func (f *FakeBatchAPIClient) TerminateJob(ctx context.Context, jobID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	j := f.jobs[jobID]
	if j != nil {
		j.status = BatchJobStatusFailed
		j.activeVMs = make(map[string]bool)
	}
	f.TerminatedJobs = append(f.TerminatedJobs, jobID)
	return nil
}

// ActiveVMCount returns the number of VMs currently tracked for a job.
func (f *FakeBatchAPIClient) ActiveVMCount(jobID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	j := f.jobs[jobID]
	if j == nil {
		return 0
	}
	return len(j.activeVMs)
}

func vmName(batchID string, n int) string {
	return fmt.Sprintf("vm-%s-%d", batchID, n)
}

// ---- FakeWorkPoolStore ----

type FakeWorkPoolStore struct {
	mu    sync.Mutex
	pools map[string]*WorkPool
}

func newFakeWorkPoolStore() *FakeWorkPoolStore {
	return &FakeWorkPoolStore{pools: make(map[string]*WorkPool)}
}

func (s *FakeWorkPoolStore) Add(pool *WorkPool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := copyPool(pool)
	s.pools[pool.WorkpoolID] = cp
}

func (s *FakeWorkPoolStore) MustGet(workpoolID string) *WorkPool {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.pools[workpoolID]
	if p == nil {
		panic(fmt.Sprintf("workpool %s not found in fake store", workpoolID))
	}
	return copyPool(p)
}

func (s *FakeWorkPoolStore) ListAll(ctx context.Context) ([]*WorkPool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []*WorkPool
	for _, p := range s.pools {
		result = append(result, copyPool(p))
	}
	return result, nil
}

func (s *FakeWorkPoolStore) Get(ctx context.Context, workpoolID string) (*WorkPool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.pools[workpoolID]
	if p == nil {
		return nil, fmt.Errorf("workpool %s not found", workpoolID)
	}
	return copyPool(p), nil
}

func (s *FakeWorkPoolStore) Save(ctx context.Context, pool *WorkPool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pools[pool.WorkpoolID] = copyPool(pool)
	return nil
}

func copyPool(p *WorkPool) *WorkPool {
	cp := *p
	return &cp
}

// ---- FakeBatchRequestStore ----

type FakeBatchRequestStore struct {
	mu      sync.Mutex
	batches map[string]*BatchAPIRequest
}

func newFakeBatchRequestStore() *FakeBatchRequestStore {
	return &FakeBatchRequestStore{batches: make(map[string]*BatchAPIRequest)}
}

func (s *FakeBatchRequestStore) Add(batch *BatchAPIRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches[batch.BatchID] = copyBatch(batch)
}

func (s *FakeBatchRequestStore) MustGet(batchID string) *BatchAPIRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.batches[batchID]
	if b == nil {
		panic(fmt.Sprintf("batch %s not found in fake store", batchID))
	}
	return copyBatch(b)
}

func (s *FakeBatchRequestStore) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.batches)
}

func (s *FakeBatchRequestStore) Create(ctx context.Context, batch *BatchAPIRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches[batch.BatchID] = copyBatch(batch)
	return nil
}

func (s *FakeBatchRequestStore) Get(ctx context.Context, batchID string) (*BatchAPIRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.batches[batchID]
	if b == nil {
		return nil, fmt.Errorf("batch %s not found", batchID)
	}
	return copyBatch(b), nil
}

func (s *FakeBatchRequestStore) Save(ctx context.Context, batch *BatchAPIRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches[batch.BatchID] = copyBatch(batch)
	return nil
}

func (s *FakeBatchRequestStore) ListByWorkpool(ctx context.Context, workpoolID string, statuses []BatchStatus) ([]*BatchAPIRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	statusSet := make(map[BatchStatus]bool, len(statuses))
	for _, st := range statuses {
		statusSet[st] = true
	}

	var result []*BatchAPIRequest
	for _, b := range s.batches {
		if b.WorkpoolID == workpoolID && statusSet[b.Status] {
			result = append(result, copyBatch(b))
		}
	}
	// DESC by SubmittedAt
	sort.Slice(result, func(i, j int) bool {
		return result[i].SubmittedAt.After(result[j].SubmittedAt)
	})
	return result, nil
}

func (s *FakeBatchRequestStore) GetByJobID(ctx context.Context, jobID string) (*BatchAPIRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, b := range s.batches {
		if b.JobID == jobID {
			return copyBatch(b), nil
		}
	}
	return nil, nil
}

func (s *FakeBatchRequestStore) SumPreemptibleVMCount(ctx context.Context, workpoolID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	total := 0
	for _, b := range s.batches {
		if b.WorkpoolID == workpoolID && b.Preemptible {
			total += b.ExpectedVMCount
		}
	}
	return total, nil
}

func copyBatch(b *BatchAPIRequest) *BatchAPIRequest {
	cp := *b
	if b.RunningSince != nil {
		t := *b.RunningSince
		cp.RunningSince = &t
	}
	return &cp
}

// ---- FakeWorkerStore ----

type FakeWorkerStore struct {
	mu      sync.Mutex
	workers map[string]*Worker
}

func newFakeWorkerStore() *FakeWorkerStore {
	return &FakeWorkerStore{workers: make(map[string]*Worker)}
}

func (s *FakeWorkerStore) Add(w *Worker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *w
	s.workers[w.WorkerID] = &cp
}

func (s *FakeWorkerStore) ListExpired(ctx context.Context, now time.Time) ([]*Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []*Worker
	for _, w := range s.workers {
		if w.HeartbeatExpiry.Before(now) {
			cp := *w
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (s *FakeWorkerStore) ListByBatch(ctx context.Context, batchID string) ([]*Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []*Worker
	for _, w := range s.workers {
		if w.BatchID == batchID {
			cp := *w
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (s *FakeWorkerStore) CountActive(ctx context.Context, workpoolID string, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for _, w := range s.workers {
		if w.WorkpoolID == workpoolID && w.HeartbeatExpiry.After(now) {
			count++
		}
	}
	return count, nil
}

// ---- FakeTaskStore ----

type FakeTaskStore struct {
	mu    sync.Mutex
	tasks map[string]*Task
}

func newFakeTaskStore() *FakeTaskStore {
	return &FakeTaskStore{tasks: make(map[string]*Task)}
}

func (s *FakeTaskStore) Add(t *Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *t
	s.tasks[t.TaskID] = &cp
}

func (s *FakeTaskStore) MustGet(taskID string) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tasks[taskID]
	if t == nil {
		panic(fmt.Sprintf("task %s not found in fake store", taskID))
	}
	cp := *t
	return &cp
}

func (s *FakeTaskStore) ListByWorker(ctx context.Context, workerID string, statuses []TaskStatus) ([]*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	statusSet := make(map[TaskStatus]bool, len(statuses))
	for _, st := range statuses {
		statusSet[st] = true
	}

	var result []*Task
	for _, t := range s.tasks {
		if t.OwningWorkerID == workerID && statusSet[t.Status] {
			cp := *t
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (s *FakeTaskStore) CountPending(ctx context.Context, workpoolID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for _, t := range s.tasks {
		if t.WorkpoolID == workpoolID && t.Status == TaskStatusPending {
			count++
		}
	}
	return count, nil
}

func (s *FakeTaskStore) ResetToPending(ctx context.Context, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	t := s.tasks[taskID]
	if t == nil {
		return fmt.Errorf("task %s not found", taskID)
	}
	t.Status = TaskStatusPending
	t.OwningWorkerID = ""
	return nil
}

// ---- FakePubSubReceiver ----

type FakePubSubReceiver struct {
	ch chan Notification
}

func newFakePubSubReceiver() *FakePubSubReceiver {
	return &FakePubSubReceiver{ch: make(chan Notification, 16)}
}

func (f *FakePubSubReceiver) Notifications() <-chan Notification {
	return f.ch
}

func (f *FakePubSubReceiver) Deliver(batchID string) {
	f.ch <- Notification{BatchID: batchID}
}

// ---- World: test fixture builder ----

type World struct {
	Clock    *scheduler.FakeClock
	BatchAPI *FakeBatchAPIClient
	Pools    *FakeWorkPoolStore
	Batches  *FakeBatchRequestStore
	Workers  *FakeWorkerStore
	Tasks    *FakeTaskStore
	PubSub   *FakePubSubReceiver
	A        *Autoscaler
}

func newWorld() *World {
	clock := scheduler.NewFakeClock(epoch)
	batchAPI := newFakeBatchAPIClient()
	pools := newFakeWorkPoolStore()
	batches := newFakeBatchRequestStore()
	workers := newFakeWorkerStore()
	tasks := newFakeTaskStore()
	pubsub := newFakePubSubReceiver()

	a := New(clock, batchAPI, pools, batches, workers, tasks, pubsub)

	return &World{
		Clock:    clock,
		BatchAPI: batchAPI,
		Pools:    pools,
		Batches:  batches,
		Workers:  workers,
		Tasks:    tasks,
		PubSub:   pubsub,
		A:        a,
	}
}

// defaultPool returns a WorkPool with sensible defaults for tests.
// Fields that should be non-zero in tests get set; zero fields use package defaults.
func defaultPool(workpoolID string) *WorkPool {
	return &WorkPool{
		WorkpoolID:                   workpoolID,
		MaxWorkerCount:               100,
		MaxPreemptibleWorkerAttempts: 50,
		Status:                       WorkPoolStatusOK,
	}
}
