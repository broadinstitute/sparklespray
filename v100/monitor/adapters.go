package monitor

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

const batchRequestCollection = "BatchAPIRequests"
const workpoolCollection = "WorkPools"
const workerCollection = "Workers"
const jobCollection = "Jobs"
const taskCollection = "Tasks"
const eventsCollection = "Events"
const taskLogCollection = "TaskLog"

// Exported for use by functional tests.
const (
	CollectionWorkPools              = workpoolCollection
	CollectionBatches                = batchRequestCollection
	CollectionWorkers                = workerCollection
	CollectionJobs                   = jobCollection
	CollectionTasks                  = taskCollection
	CollectionJobSummary             = jobSummaryCollection
	CollectionEvents                 = eventsCollection
	CollectionTaskLog                = taskLogCollection
)

// firestoreWorkPool is the Firestore representation of a workpool document.
// This is immutable — written once at creation and never updated by the monitor.
// Duration fields are stored as seconds (int) to be JSON/Firestore friendly.
// Mutable state (state, state_message, last_incident_at, incident_count) lives
// in WorkPoolSummary, not here.
type firestoreWorkPool struct {
	WorkpoolID            string          `firestore:"workpool_id"`
	MachineType           string          `firestore:"machine_type"`
	Region                string          `firestore:"region"`
	Zones                 []string        `firestore:"zones"`
	Expiry                time.Time       `firestore:"expiry"`
	RootDir               string          `firestore:"root_dir"`
	SparklesWorkerGCSPath string          `firestore:"sparkles_worker_gcs_path"`
	EmptyVolumes          []EmptyVolume   `firestore:"empty_volumes"`
	Resources             []ResourceEntry `firestore:"resources"`
	ServiceAccount        string          `firestore:"service_account"`
	Labels                []Label         `firestore:"labels"`

	MaxWorkerCount               int `firestore:"max_worker_count"`
	MaxPreemptibleWorkerAttempts int `firestore:"max_preemptible_worker_attempts"`
	MaxWorkersPerRequest         int `firestore:"max_workers_per_request"`

	VMShutdownGracePeriodSec    int `firestore:"vm_shutdown_grace_period_sec"`
	MaxZombiesBeforeAbort       int `firestore:"max_zombies_before_abort"`
	MaxConsecutiveFailedBatches int `firestore:"max_consecutive_failed_batches"`

	LingerTimeSec int `firestore:"linger_time_sec"`
}

func secToDur(secs int, defaultDur time.Duration) time.Duration {
	if secs == 0 {
		return defaultDur
	}
	return time.Duration(secs) * time.Second
}

func toWorkPool(f *firestoreWorkPool) *WorkPool {
	return &WorkPool{
		WorkpoolID:                   f.WorkpoolID,
		Region:                       f.Region,
		Zones:                        f.Zones,
		MachineType:                  f.MachineType,
		RootDir:                      f.RootDir,
		SparklesWorkerGCSPath:        f.SparklesWorkerGCSPath,
		EmptyVolumes:                 f.EmptyVolumes,
		Resources:                    f.Resources,
		ServiceAccount:               f.ServiceAccount,
		Labels:                       f.Labels,
		MaxWorkerCount:               f.MaxWorkerCount,
		MaxPreemptibleWorkerAttempts: f.MaxPreemptibleWorkerAttempts,
		MaxWorkersPerRequest:         f.MaxWorkersPerRequest,
		VMShutdownGracePeriod:        secToDur(f.VMShutdownGracePeriodSec, defaultVMShutdownGracePeriod),
		MaxZombiesBeforeAbort:        f.MaxZombiesBeforeAbort,
		MaxConsecutiveFailedBatches:  f.MaxConsecutiveFailedBatches,
		LingerTime:                   time.Duration(f.LingerTimeSec) * time.Second,
	}
}

func toWorkPoolState(workpoolID string, summary *WorkPoolSummary) *WorkPoolState {
	return &WorkPoolState{
		WorkpoolID: workpoolID,
		State:      summary.State,
	}
}


// ----- FirestoreWorkPoolStore -----

type FirestoreWorkPoolStore struct {
	fs *firestore.Client
}

func NewFirestoreWorkPoolStore(fs *firestore.Client) *FirestoreWorkPoolStore {
	return &FirestoreWorkPoolStore{fs: fs}
}

func (s *FirestoreWorkPoolStore) ListAll(ctx context.Context) ([]*WorkPoolWithState, error) {
	iter := s.fs.Collection(workpoolCollection).Documents(ctx)
	var pools []*WorkPool
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var f firestoreWorkPool
		if err := snap.DataTo(&f); err != nil {
			return nil, err
		}
		pools = append(pools, toWorkPool(&f))
	}

	summaries, err := s.loadAllSummaryStates(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]*WorkPoolWithState, 0, len(pools))
	for _, pool := range pools {
		var state *WorkPoolState
		if summary, ok := summaries[pool.WorkpoolID]; ok {
			state = toWorkPoolState(pool.WorkpoolID, summary)
		} else {
			state = &WorkPoolState{WorkpoolID: pool.WorkpoolID}
		}
		result = append(result, &WorkPoolWithState{Pool: pool, State: state})
	}
	return result, nil
}

func (s *FirestoreWorkPoolStore) Get(ctx context.Context, workpoolID string) (*WorkPoolWithState, error) {
	snap, err := s.fs.Collection(workpoolCollection).Doc(workpoolID).Get(ctx)
	if err != nil {
		return nil, err
	}
	var f firestoreWorkPool
	if err := snap.DataTo(&f); err != nil {
		return nil, err
	}
	pool := toWorkPool(&f)

	var state *WorkPoolState
	summSnap, err := s.fs.Collection(workPoolSummaryCollection).Doc(workpoolID).Get(ctx)
	if err == nil {
		var summary WorkPoolSummary
		if err := summSnap.DataTo(&summary); err == nil {
			state = toWorkPoolState(workpoolID, &summary)
		}
	}
	if state == nil {
		state = &WorkPoolState{WorkpoolID: workpoolID}
	}
	return &WorkPoolWithState{Pool: pool, State: state}, nil
}

// SaveState writes the mutable state field into WorkPoolSummary using a
// partial merge so that the computed metrics fields (including
// state_message/last_incident_at/incident_count, which the WorkPool summary
// poll derives from the Events log) are not overwritten.
func (s *FirestoreWorkPoolStore) SaveState(ctx context.Context, state *WorkPoolState) error {
	data := map[string]any{
		"state": string(state.State),
	}
	_, err := s.fs.Collection(workPoolSummaryCollection).Doc(state.WorkpoolID).Set(ctx, data, firestore.MergeAll)
	return err
}

// loadAllSummaryStates fetches all WorkPoolSummary documents and returns them
// keyed by workpool_id. Used by ListAll to bulk-load state.
func (s *FirestoreWorkPoolStore) loadAllSummaryStates(ctx context.Context) (map[string]*WorkPoolSummary, error) {
	iter := s.fs.Collection(workPoolSummaryCollection).Documents(ctx)
	result := make(map[string]*WorkPoolSummary)
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var summary WorkPoolSummary
		if err := snap.DataTo(&summary); err != nil {
			return nil, err
		}
		cp := summary
		result[summary.WorkpoolID] = &cp
	}
	return result, nil
}

// ----- firestoreBatchRequest -----

type firestoreBatchRequest struct {
	BatchID               string     `firestore:"batch_id"`
	JobID                 string     `firestore:"job_id"`
	WorkpoolID            string     `firestore:"workpool_id"`
	ExpectedVMCount       int        `firestore:"expected_vm_count"`
	Preemptible           bool       `firestore:"preemptible"`
	SubmittedAt           time.Time  `firestore:"submitted_at"`
	Expiry                time.Time  `firestore:"expiry"`
	RunningSince          *time.Time `firestore:"running_since"`
	RegisteredWorkerCount int        `firestore:"registered_worker_count"`
	Status                string     `firestore:"status"`
	Unhealthy             bool       `firestore:"unhealthy"`
	TerminationReason     string     `firestore:"termination_reason"`
	Labels                []Label    `firestore:"labels"`
}

func toBatchRequest(f *firestoreBatchRequest) *BatchAPIRequest {
	return &BatchAPIRequest{
		BatchID:               f.BatchID,
		JobID:                 f.JobID,
		WorkpoolID:            f.WorkpoolID,
		ExpectedVMCount:       f.ExpectedVMCount,
		Preemptible:           f.Preemptible,
		SubmittedAt:           f.SubmittedAt,
		Expiry:                f.Expiry,
		RunningSince:          f.RunningSince,
		RegisteredWorkerCount: f.RegisteredWorkerCount,
		Status:                BatchStatus(f.Status),
		Unhealthy:             f.Unhealthy,
		TerminationReason:     f.TerminationReason,
		Labels:                f.Labels,
	}
}

func fromBatchRequest(b *BatchAPIRequest) *firestoreBatchRequest {
	return &firestoreBatchRequest{
		BatchID:               b.BatchID,
		JobID:                 b.JobID,
		WorkpoolID:            b.WorkpoolID,
		ExpectedVMCount:       b.ExpectedVMCount,
		Preemptible:           b.Preemptible,
		SubmittedAt:           b.SubmittedAt,
		Expiry:                b.Expiry,
		RunningSince:          b.RunningSince,
		RegisteredWorkerCount: b.RegisteredWorkerCount,
		Status:                string(b.Status),
		Unhealthy:             b.Unhealthy,
		TerminationReason:     b.TerminationReason,
		Labels:                b.Labels,
	}
}

// ----- FirestoreBatchRequestStore -----

type FirestoreBatchRequestStore struct {
	fs *firestore.Client
}

func NewFirestoreBatchRequestStore(fs *firestore.Client) *FirestoreBatchRequestStore {
	return &FirestoreBatchRequestStore{fs: fs}
}

func (s *FirestoreBatchRequestStore) Create(ctx context.Context, batch *BatchAPIRequest) error {
	_, err := s.fs.Collection(batchRequestCollection).Doc(batch.BatchID).Set(ctx, fromBatchRequest(batch))
	return err
}

func (s *FirestoreBatchRequestStore) Get(ctx context.Context, batchID string) (*BatchAPIRequest, error) {
	snap, err := s.fs.Collection(batchRequestCollection).Doc(batchID).Get(ctx)
	if err != nil {
		return nil, err
	}
	var f firestoreBatchRequest
	if err := snap.DataTo(&f); err != nil {
		return nil, err
	}
	return toBatchRequest(&f), nil
}

func (s *FirestoreBatchRequestStore) Save(ctx context.Context, batch *BatchAPIRequest) error {
	_, err := s.fs.Collection(batchRequestCollection).Doc(batch.BatchID).Set(ctx, fromBatchRequest(batch))
	return err
}

func (s *FirestoreBatchRequestStore) GetByJobID(ctx context.Context, jobID string) (*BatchAPIRequest, error) {
	iter := s.fs.Collection(batchRequestCollection).
		Where("job_id", "==", jobID).
		Limit(1).
		Documents(ctx)
	snap, err := iter.Next()
	if err == iterator.Done {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var f firestoreBatchRequest
	if err := snap.DataTo(&f); err != nil {
		return nil, err
	}
	return toBatchRequest(&f), nil
}

func (s *FirestoreBatchRequestStore) ListByWorkpool(ctx context.Context, workpoolID string, statuses []BatchStatus) ([]*BatchAPIRequest, error) {
	strStatuses := make([]interface{}, len(statuses))
	for i, st := range statuses {
		strStatuses[i] = string(st)
	}
	iter := s.fs.Collection(batchRequestCollection).
		Where("workpool_id", "==", workpoolID).
		Where("status", "in", strStatuses).
		OrderBy("submitted_at", firestore.Desc).
		Documents(ctx)

	var batches []*BatchAPIRequest
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var f firestoreBatchRequest
		if err := snap.DataTo(&f); err != nil {
			return nil, err
		}
		batches = append(batches, toBatchRequest(&f))
	}
	return batches, nil
}

func (s *FirestoreBatchRequestStore) ListAllByWorkpool(ctx context.Context, workpoolID string) ([]*BatchAPIRequest, error) {
	iter := s.fs.Collection(batchRequestCollection).
		Where("workpool_id", "==", workpoolID).
		Documents(ctx)

	var batches []*BatchAPIRequest
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var f firestoreBatchRequest
		if err := snap.DataTo(&f); err != nil {
			return nil, err
		}
		batches = append(batches, toBatchRequest(&f))
	}
	return batches, nil
}

func (s *FirestoreBatchRequestStore) SumPreemptibleVMCount(ctx context.Context, workpoolID string) (int, error) {
	iter := s.fs.Collection(batchRequestCollection).
		Where("workpool_id", "==", workpoolID).
		Where("preemptible", "==", true).
		Documents(ctx)

	total := 0
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		var f firestoreBatchRequest
		if err := snap.DataTo(&f); err != nil {
			return 0, err
		}
		total += f.ExpectedVMCount
	}
	return total, nil
}

// ----- FirestoreWorkerStore -----

type firestoreWorker struct {
	WorkerID        string    `firestore:"worker_id"`
	WorkpoolID      string    `firestore:"workpool_id"`
	BatchID         string    `firestore:"batch_id"`
	InstanceName    string    `firestore:"instance_name"`
	Status          string    `firestore:"status"`
	HeartbeatExpiry time.Time `firestore:"heartbeat_expiry"`
}

type FirestoreWorkerStore struct {
	fs *firestore.Client
}

func NewFirestoreWorkerStore(fs *firestore.Client) *FirestoreWorkerStore {
	return &FirestoreWorkerStore{fs: fs}
}

func (s *FirestoreWorkerStore) ListExpired(ctx context.Context, now time.Time) ([]*Worker, error) {
	iter := s.fs.Collection(workerCollection).
		Where("status", "==", "started").
		Where("heartbeat_expiry", "<", now).
		Documents(ctx)
	return collectWorkers(iter)
}

func (s *FirestoreWorkerStore) ListByBatch(ctx context.Context, batchID string) ([]*Worker, error) {
	iter := s.fs.Collection(workerCollection).
		Where("batch_id", "==", batchID).
		Documents(ctx)
	return collectWorkers(iter)
}

func (s *FirestoreWorkerStore) CountActive(ctx context.Context, workpoolID string, now time.Time) (int, error) {
	iter := s.fs.Collection(workerCollection).
		Where("workpool_id", "==", workpoolID).
		Where("heartbeat_expiry", ">", now).
		Documents(ctx)

	count := 0
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func (s *FirestoreWorkerStore) CountByStatusForWorkpool(ctx context.Context, workpoolID string) (map[string]int, error) {
	iter := s.fs.Collection(workerCollection).
		Where("workpool_id", "==", workpoolID).
		Documents(ctx)

	counts := make(map[string]int)
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var f firestoreWorker
		if err := snap.DataTo(&f); err != nil {
			return nil, err
		}
		counts[f.Status]++
	}
	return counts, nil
}

func collectWorkers(iter *firestore.DocumentIterator) ([]*Worker, error) {
	var workers []*Worker
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var f firestoreWorker
		if err := snap.DataTo(&f); err != nil {
			return nil, err
		}
		workers = append(workers, &Worker{
			WorkerID:        f.WorkerID,
			WorkpoolID:      f.WorkpoolID,
			BatchID:         f.BatchID,
			InstanceName:    f.InstanceName,
			Status:          f.Status,
			HeartbeatExpiry: f.HeartbeatExpiry,
		})
	}
	return workers, nil
}

func (s *FirestoreWorkerStore) ListAllForWorkpool(ctx context.Context, workpoolID string) ([]*Worker, error) {
	iter := s.fs.Collection(workerCollection).
		Where("workpool_id", "==", workpoolID).
		Documents(ctx)
	return collectWorkers(iter)
}

func (s *FirestoreWorkerStore) MarkZombie(ctx context.Context, workerID string) error {
	_, err := s.fs.Collection(workerCollection).Doc(workerID).Update(ctx, []firestore.Update{
		{Path: "status", Value: "zombie"},
	})
	return err
}

// ----- FirestoreTaskStore -----

type firestoreTask struct {
	TaskID         string `firestore:"task_id"`
	JobID          string `firestore:"job_id"`
	WorkpoolID     string `firestore:"workpool_id"`
	Status         string `firestore:"status"`
	OwningWorkerID string `firestore:"owning_worker_id"`
}

type FirestoreTaskStore struct {
	fs        *firestore.Client
	publisher TaskStatePublisher
}

func NewFirestoreTaskStore(fs *firestore.Client, publisher TaskStatePublisher) *FirestoreTaskStore {
	return &FirestoreTaskStore{fs: fs, publisher: publisher}
}

// SetPublisher wires a TaskStatePublisher after construction. Useful when the
// publisher is created after the store (e.g. because both depend on the same
// Firestore client that must be initialised first).
func (s *FirestoreTaskStore) SetPublisher(p TaskStatePublisher) { s.publisher = p }

func (s *FirestoreTaskStore) ListByWorker(ctx context.Context, workerID string, statuses []TaskStatus) ([]*Task, error) {
	strStatuses := make([]interface{}, len(statuses))
	for i, st := range statuses {
		strStatuses[i] = string(st)
	}
	iter := s.fs.Collection(taskCollection).
		Where("owning_worker_id", "==", workerID).
		Where("status", "in", strStatuses).
		Documents(ctx)

	var tasks []*Task
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var f firestoreTask
		if err := snap.DataTo(&f); err != nil {
			return nil, err
		}
		tasks = append(tasks, &Task{
			TaskID:         f.TaskID,
			JobID:          f.JobID,
			WorkpoolID:     f.WorkpoolID,
			Status:         TaskStatus(f.Status),
			OwningWorkerID: f.OwningWorkerID,
		})
	}
	return tasks, nil
}

func (s *FirestoreTaskStore) CountPending(ctx context.Context, workpoolID string) (int, error) {
	iter := s.fs.Collection(taskCollection).
		Where("workpool_id", "==", workpoolID).
		Where("status", "==", string(TaskStatusPending)).
		Documents(ctx)

	count := 0
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func (s *FirestoreTaskStore) ResetToPending(ctx context.Context, taskID, jobID string, oldStatus TaskStatus) error {
	if _, err := s.fs.Collection(taskCollection).Doc(taskID).Update(ctx, []firestore.Update{
		{Path: "status", Value: string(TaskStatusPending)},
		{Path: "owning_worker_id", Value: ""},
	}); err != nil {
		return err
	}
	if s.publisher != nil {
		return s.publisher.PublishTaskStateUpdate(ctx, taskID, jobID, string(oldStatus), string(TaskStatusPending))
	}
	return nil
}

func (s *FirestoreTaskStore) CountByJob(ctx context.Context, jobID string) (map[string]int, error) {
	iter := s.fs.Collection(taskCollection).
		Where("job_id", "==", jobID).
		Documents(ctx)

	counts := make(map[string]int)
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var f firestoreTask
		if err := snap.DataTo(&f); err != nil {
			return nil, err
		}
		counts[f.Status]++
	}
	return counts, nil
}

func (s *FirestoreTaskStore) CountByWorkpool(ctx context.Context, workpoolID string) (map[string]int, error) {
	iter := s.fs.Collection(taskCollection).
		Where("workpool_id", "==", workpoolID).
		Documents(ctx)

	counts := make(map[string]int)
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var f firestoreTask
		if err := snap.DataTo(&f); err != nil {
			return nil, err
		}
		counts[f.Status]++
	}
	return counts, nil
}

// ----- FirestoreJobSummaryStore -----

const jobSummaryCollection = "JobSummary"
const jobSummaryHistoryCollection = "JobSummaryHistory"

type FirestoreJobSummaryStore struct {
	fs *firestore.Client
}

func NewFirestoreJobSummaryStore(fs *firestore.Client) *FirestoreJobSummaryStore {
	return &FirestoreJobSummaryStore{fs: fs}
}

func (s *FirestoreJobSummaryStore) Create(ctx context.Context, summary *JobSummary) error {
	_, err := s.fs.Collection(jobSummaryCollection).Doc(summary.JobID).Set(ctx, summary)
	return err
}

func (s *FirestoreJobSummaryStore) ListNonTerminal(ctx context.Context) ([]*JobSummary, error) {
	nonTerminal := []interface{}{
		string(JobStatusPending),
		string(JobStatusInProgress),
		string(JobStatusInProgressWithError),
		string(JobStatusInProgressWithFailure),
	}
	iter := s.fs.Collection(jobSummaryCollection).
		Where("state", "in", nonTerminal).
		Documents(ctx)

	var summaries []*JobSummary
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var doc JobSummary
		if err := snap.DataTo(&doc); err != nil {
			return nil, err
		}
		cp := doc
		summaries = append(summaries, &cp)
	}
	return summaries, nil
}

func (s *FirestoreJobSummaryStore) Save(ctx context.Context, summary *JobSummary) error {
	_, err := s.fs.Collection(jobSummaryCollection).Doc(summary.JobID).Set(ctx, summary)
	return err
}

func (s *FirestoreJobSummaryStore) SaveHistory(ctx context.Context, history *JobSummaryHistory) error {
	_, _, err := s.fs.Collection(jobSummaryHistoryCollection).Add(ctx, history)
	return err
}

// ----- FirestoreWorkPoolSummaryStore -----

const workPoolSummaryCollection = "WorkPoolSummary"
const workPoolSummaryHistoryCollection = "WorkPoolSummaryHistory"

// Exported for use by functional tests and the dashboard backend.
const (
	CollectionWorkPoolSummary        = workPoolSummaryCollection
	CollectionWorkPoolSummaryHistory = workPoolSummaryHistoryCollection
)

type FirestoreWorkPoolSummaryStore struct {
	fs *firestore.Client
}

func NewFirestoreWorkPoolSummaryStore(fs *firestore.Client) *FirestoreWorkPoolSummaryStore {
	return &FirestoreWorkPoolSummaryStore{fs: fs}
}

func (s *FirestoreWorkPoolSummaryStore) Save(ctx context.Context, summary *WorkPoolSummary) error {
	_, err := s.fs.Collection(workPoolSummaryCollection).Doc(summary.WorkpoolID).Set(ctx, summary)
	return err
}

func (s *FirestoreWorkPoolSummaryStore) SaveHistory(ctx context.Context, history *WorkPoolSummaryHistory) error {
	_, _, err := s.fs.Collection(workPoolSummaryHistoryCollection).Add(ctx, history)
	return err
}

func (s *FirestoreWorkPoolSummaryStore) ListIdle(ctx context.Context) ([]*WorkPoolSummary, error) {
	iter := s.fs.Collection(workPoolSummaryCollection).
		Where("state", "==", string(WorkPoolStatusIdle)).
		Documents(ctx)

	var summaries []*WorkPoolSummary
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var doc WorkPoolSummary
		if err := snap.DataTo(&doc); err != nil {
			return nil, err
		}
		cp := doc
		summaries = append(summaries, &cp)
	}
	return summaries, nil
}

func (s *FirestoreWorkPoolSummaryStore) Delete(ctx context.Context, workpoolID string) error {
	_, err := s.fs.Collection(workPoolSummaryCollection).Doc(workpoolID).Delete(ctx)
	return err
}

// ----- FirestoreEventStore -----

type FirestoreEventStore struct {
	fs *firestore.Client
}

func NewFirestoreEventStore(fs *firestore.Client) *FirestoreEventStore {
	return &FirestoreEventStore{fs: fs}
}

type firestoreEventRecord struct {
	Type         string    `firestore:"type"`
	Timestamp    time.Time `firestore:"timestamp"`
	WorkpoolID   string    `firestore:"workpool_id"`
	StateMessage string    `firestore:"state_message"`
}

func (s *FirestoreEventStore) ListJobCreatedSince(ctx context.Context, since time.Time) ([]JobCreatedRecord, error) {
	q := s.fs.Collection(eventsCollection).Where("type", "==", "job_created")
	if !since.IsZero() {
		q = q.Where("timestamp", ">", since)
	}
	iter := q.Documents(ctx)
	defer iter.Stop()

	var results []JobCreatedRecord
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var rec firestoreEventRecord
		if err := snap.DataTo(&rec); err != nil {
			continue
		}
		results = append(results, JobCreatedRecord{
			WorkpoolID: rec.WorkpoolID,
			Timestamp:  rec.Timestamp,
		})
	}
	return results, nil
}

func (s *FirestoreEventStore) ListRecentBatchOutcomes(ctx context.Context, workpoolID string, since time.Time) ([]BatchOutcome, error) {
	q := s.fs.Collection(eventsCollection).
		Where("workpool_id", "==", workpoolID).
		Where("type", "in", []string{"batch_failed", "batch_succeeded"}).
		Where("timestamp", ">", since).
		OrderBy("timestamp", firestore.Desc)
	iter := q.Documents(ctx)
	defer iter.Stop()

	var results []BatchOutcome
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var rec firestoreEventRecord
		if err := snap.DataTo(&rec); err != nil {
			continue
		}
		results = append(results, BatchOutcome{
			Failed:    rec.Type == "batch_failed",
			Timestamp: rec.Timestamp,
		})
	}
	return results, nil
}

func (s *FirestoreEventStore) ListRecentWorkpoolIncidents(ctx context.Context, workpoolID string, since time.Time) ([]WorkpoolIncident, error) {
	q := s.fs.Collection(eventsCollection).
		Where("workpool_id", "==", workpoolID).
		Where("type", "==", "workpool_incident").
		Where("timestamp", ">", since).
		OrderBy("timestamp", firestore.Desc)
	iter := q.Documents(ctx)
	defer iter.Stop()

	var results []WorkpoolIncident
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var rec firestoreEventRecord
		if err := snap.DataTo(&rec); err != nil {
			continue
		}
		results = append(results, WorkpoolIncident{
			Message:   rec.StateMessage,
			Timestamp: rec.Timestamp,
		})
	}
	return results, nil
}

// ----- FirestoreExpiryStore -----

type FirestoreExpiryStore struct {
	fs *firestore.Client
}

func NewFirestoreExpiryStore(fs *firestore.Client) *FirestoreExpiryStore {
	return &FirestoreExpiryStore{fs: fs}
}

// DeleteExpired deletes all documents in collection where expiry < now.
// Documents are deleted in batches of 500 (Firestore batch write limit).
// Returns the total count of deleted documents.
func (s *FirestoreExpiryStore) DeleteExpired(ctx context.Context, collection string, now time.Time) (int, error) {
	iter := s.fs.Collection(collection).
		Where("expiry", "<", now).
		Documents(ctx)
	defer iter.Stop()

	const maxBatch = 500
	total := 0
	for {
		var refs []*firestore.DocumentRef
		done := false
		for len(refs) < maxBatch {
			snap, err := iter.Next()
			if err == iterator.Done {
				done = true
				break
			}
			if err != nil {
				return total, fmt.Errorf("iterate %s: %w", collection, err)
			}
			refs = append(refs, snap.Ref)
		}
		if len(refs) > 0 {
			bw := s.fs.Batch()
			for _, ref := range refs {
				bw.Delete(ref)
			}
			if _, err := bw.Commit(ctx); err != nil {
				return total, fmt.Errorf("batch delete %s: %w", collection, err)
			}
			total += len(refs)
		}
		if done {
			break
		}
	}
	return total, nil
}
