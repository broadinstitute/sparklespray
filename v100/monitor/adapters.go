package monitor

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

const batchRequestCollection = "BatchAPIRequests"
const workpoolCollection = "WorkPools"
const workerCollection = "Workers"
const taskCollection = "Tasks"

// Exported for use by functional tests.
const (
	CollectionWorkPools   = workpoolCollection
	CollectionBatches     = batchRequestCollection
	CollectionWorkers     = workerCollection
	CollectionTasks       = taskCollection
	CollectionJobSummary  = jobSummaryCollection
)

// firestoreWorkPool is the Firestore representation of a workpool document.
// Duration fields are stored as seconds (int) to be JSON/Firestore friendly.
type firestoreWorkPool struct {
	WorkpoolID            string        `firestore:"workpool_id"`
	MachineType           string        `firestore:"machine_type"`
	Region                string        `firestore:"region"`
	Zones                 []string      `firestore:"zones"`
	Expiry                time.Time     `firestore:"expiry"`
	RootDir               string        `firestore:"root_dir"`
	SparklesWorkerGCSPath string        `firestore:"sparkles_worker_gcs_path"`
	EmptyVolumes          []EmptyVolume   `firestore:"empty_volumes"`
	Resources             []ResourceEntry `firestore:"resources"`
	ServiceAccount        string          `firestore:"service_account"`

	MaxWorkerCount               int `firestore:"max_worker_count"`
	MaxPreemptibleWorkerAttempts int `firestore:"max_preemptible_worker_attempts"`
	MaxWorkersPerRequest         int `firestore:"max_workers_per_request"`

	MinTimeBetweenPollsSec      int `firestore:"min_time_between_polls_sec"`
	MaxTimeBetweenPollsSec      int `firestore:"max_time_between_polls_sec"`
	MaxTimeToStartWorkerSec     int `firestore:"max_time_to_start_worker_sec"`
	MaxTimeInQueueSec           int `firestore:"max_time_in_queue_sec"`
	VMShutdownGracePeriodSec    int `firestore:"vm_shutdown_grace_period_sec"`
	MaxZombiesBeforeAbort       int `firestore:"max_zombies_before_abort"`
	MaxConsecutiveFailedBatches int `firestore:"max_consecutive_failed_batches"`

	Status         string    `firestore:"status"`
	StatusMessage  string    `firestore:"status_message"`
	LastIncidentAt time.Time `firestore:"last_incident_at"`
	IncidentCount  int       `firestore:"incident_count"`
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
		MaxWorkerCount:               f.MaxWorkerCount,
		MaxPreemptibleWorkerAttempts: f.MaxPreemptibleWorkerAttempts,
		MaxWorkersPerRequest:         f.MaxWorkersPerRequest,
		MinTimeBetweenPolls:          secToDur(f.MinTimeBetweenPollsSec, defaultMinTimeBetweenPolls),
		MaxTimeBetweenPolls:          secToDur(f.MaxTimeBetweenPollsSec, defaultMaxTimeBetweenPolls),
		MaxTimeToStartWorker:         secToDur(f.MaxTimeToStartWorkerSec, defaultMaxTimeToStartWorker),
		MaxTimeInQueue:               secToDur(f.MaxTimeInQueueSec, defaultMaxTimeInQueue),
		VMShutdownGracePeriod:        secToDur(f.VMShutdownGracePeriodSec, defaultVMShutdownGracePeriod),
		MaxZombiesBeforeAbort:        f.MaxZombiesBeforeAbort,
		MaxConsecutiveFailedBatches:  f.MaxConsecutiveFailedBatches,
		Status:                       WorkPoolStatus(f.Status),
		StatusMessage:                f.StatusMessage,
		LastIncidentAt:               f.LastIncidentAt,
		IncidentCount:                f.IncidentCount,
	}
}

func toFirestoreWorkPoolUpdates(p *WorkPool) []firestore.Update {
	return []firestore.Update{
		{Path: "status", Value: string(p.Status)},
		{Path: "status_message", Value: p.StatusMessage},
		{Path: "last_incident_at", Value: p.LastIncidentAt},
		{Path: "incident_count", Value: p.IncidentCount},
	}
}

// ----- FirestoreWorkPoolStore -----

type FirestoreWorkPoolStore struct {
	fs *firestore.Client
}

func NewFirestoreWorkPoolStore(fs *firestore.Client) *FirestoreWorkPoolStore {
	return &FirestoreWorkPoolStore{fs: fs}
}

func (s *FirestoreWorkPoolStore) ListAll(ctx context.Context) ([]*WorkPool, error) {
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
	return pools, nil
}

func (s *FirestoreWorkPoolStore) Get(ctx context.Context, workpoolID string) (*WorkPool, error) {
	snap, err := s.fs.Collection(workpoolCollection).Doc(workpoolID).Get(ctx)
	if err != nil {
		return nil, err
	}
	var f firestoreWorkPool
	if err := snap.DataTo(&f); err != nil {
		return nil, err
	}
	return toWorkPool(&f), nil
}

func (s *FirestoreWorkPoolStore) Save(ctx context.Context, pool *WorkPool) error {
	_, err := s.fs.Collection(workpoolCollection).Doc(pool.WorkpoolID).Update(ctx, toFirestoreWorkPoolUpdates(pool))
	return err
}

// ----- firestoreBatchRequest -----

type firestoreBatchRequest struct {
	BatchID               string     `firestore:"batch_id"`
	JobID                 string     `firestore:"job_id"`
	WorkpoolID            string     `firestore:"workpool_id"`
	ExpectedVMCount       int        `firestore:"expected_vm_count"`
	Preemptible           bool       `firestore:"preemptible"`
	SubmittedAt           time.Time  `firestore:"submitted_at"`
	RunningSince          *time.Time `firestore:"running_since"`
	RegisteredWorkerCount int        `firestore:"registered_worker_count"`
	Status                string     `firestore:"status"`
	Unhealthy             bool       `firestore:"unhealthy"`
}

func toBatchRequest(f *firestoreBatchRequest) *BatchAPIRequest {
	return &BatchAPIRequest{
		BatchID:               f.BatchID,
		JobID:                 f.JobID,
		WorkpoolID:            f.WorkpoolID,
		ExpectedVMCount:       f.ExpectedVMCount,
		Preemptible:           f.Preemptible,
		SubmittedAt:           f.SubmittedAt,
		RunningSince:          f.RunningSince,
		RegisteredWorkerCount: f.RegisteredWorkerCount,
		Status:                BatchStatus(f.Status),
		Unhealthy:             f.Unhealthy,
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
		RunningSince:          b.RunningSince,
		RegisteredWorkerCount: b.RegisteredWorkerCount,
		Status:                string(b.Status),
		Unhealthy:             b.Unhealthy,
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

// ----- FirestoreTaskStore -----

type firestoreTask struct {
	TaskID         string `firestore:"task_id"`
	WorkpoolID     string `firestore:"workpool_id"`
	Status         string `firestore:"status"`
	OwningWorkerID string `firestore:"owning_worker_id"`
}

type FirestoreTaskStore struct {
	fs *firestore.Client
}

func NewFirestoreTaskStore(fs *firestore.Client) *FirestoreTaskStore {
	return &FirestoreTaskStore{fs: fs}
}

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

func (s *FirestoreTaskStore) ResetToPending(ctx context.Context, taskID string) error {
	_, err := s.fs.Collection(taskCollection).Doc(taskID).Update(ctx, []firestore.Update{
		{Path: "status", Value: string(TaskStatusPending)},
		{Path: "owning_worker_id", Value: ""},
	})
	return err
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
		Where("status", "in", nonTerminal).
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
