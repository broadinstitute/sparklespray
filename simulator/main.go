package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const EventCollection = "SparklesV5Event"
const ClusterCollection = "SparklesV5Cluster"
const JobCollection = "SparklesV5Job"
const TaskCollection = "SparklesV5Task"
const EventExpiry = 7 * 24 * time.Hour

var failureReasons = []string{
	"out of memory",
	"disk quota exceeded",
	"network timeout",
	"worker node preempted",
	"unhandled exception in task",
	"SIGKILL received",
	"disk full",
}

type Config struct {
	ProjectID       string
	ClusterID       string
	MachineType     string
	MaxJobs         int
	TasksPerJob     int
	NumWorkers      int
	WorkerFailRate  float64
	TaskFailRate    float64
	TaskDuration    time.Duration
	StagingDuration time.Duration
	UploadDuration  time.Duration
	JobInterval     time.Duration
	RequeueDelay       time.Duration
	WorkerStartDelay   time.Duration
	DryRun             bool
}

type Cluster struct {
	ClusterID   string    `datastore:"cluster_id"`
	MachineType string    `datastore:"machine_type"`
	CreatedAt   time.Time `datastore:"created_at"`
}

type TaskHistory struct {
	Timestamp     float64 `datastore:"timestamp,noindex"`
	Status        string  `datastore:"status,noindex"`
	FailureReason string  `datastore:"failure_reason,noindex,omitempty"`
	Owner         string  `datastore:"owner,noindex,omitempty"`
}

type Task struct {
	TaskID           string         `datastore:"task_id"`
	TaskIndex        int64          `datastore:"task_index"`
	JobID            string         `datastore:"job_id"`
	Status           string         `datastore:"status"`
	Owner            string         `datastore:"owner"`
	Args             string         `datastore:"args,noindex"`
	History          []*TaskHistory `datastore:"history,noindex"`
	CommandResultURL string         `datastore:"command_result_url,noindex"`
	FailureReason    string         `datastore:"failure_reason,omitempty"`
	Version          int32          `datastore:"version"`
	ExitCode         string         `datastore:"exit_code"`
	ClusterID        string         `datastore:"cluster_id"`
	MonitorAddress   string         `datastore:"monitor_address,noindex"`
	LogURL           string         `datastore:"log_url,noindex"`
	LastUpdated      time.Time      `datastore:"last_updated"`
}

type Job struct {
	JobID                  string   `datastore:"job_id"`
	Tasks                  []string `datastore:"tasks,noindex"`
	KubeJobSpec            string   `datastore:"kube_job_spec,noindex"`
	Metadata               string   `datastore:"metadata,noindex"`
	ClusterID              string   `datastore:"cluster_id"`
	Status                 string   `datastore:"status"`
	SubmitTime             time.Time `datastore:"submit_time"`
	TaskCount              int32    `datastore:"task_count"`
	MaxPreemptableAttempts int32    `datastore:"max_preemptable_attempts"`
	TargetNodeCount        int32    `datastore:"target_node_count"`
}

type pendingTask struct {
	taskID string
	jobID  string
	task   *Task
	onDone func()
}

var (
	dsClient    *datastore.Client
	runningJobs atomic.Int64
)

func writeEvent(ctx context.Context, cfg *Config, props datastore.PropertyList) {
	eventID := uuid.New().String()
	now := time.Now().UTC()
	props = append(props,
		datastore.Property{Name: "event_id", Value: eventID, NoIndex: false},
		datastore.Property{Name: "timestamp", Value: now, NoIndex: false},
		datastore.Property{Name: "expiry", Value: now.Add(EventExpiry), NoIndex: true},
	)

	if cfg.DryRun {
		for _, p := range props {
			if p.Name == "type" || p.Name == "task_id" || p.Name == "job_id" {
				fmt.Printf("%s=%v ", p.Name, p.Value)
			}
		}
		fmt.Printf("ts=%s\n", now.Format(time.RFC3339))
		return
	}

	key := datastore.NameKey(EventCollection, eventID, nil)
	_, err := dsClient.Put(ctx, key, &props)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.PermissionDenied {
			log.Fatalf("Permission denied writing to Datastore: %v", err)
		}
		log.Printf("ERROR writing event %v: %v", props, err)
	}
}

func deleteAllFromCollection(ctx context.Context, collection string) {
	keys, err := dsClient.GetAll(ctx, datastore.NewQuery(collection).KeysOnly(), nil)
	if err != nil {
		log.Fatalf("Failed to query %s for reset: %v", collection, err)
	}
	const batchSize = 500
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		if err := dsClient.DeleteMulti(ctx, keys[i:end]); err != nil {
			log.Fatalf("Failed to delete from %s: %v", collection, err)
		}
	}
	log.Printf("Deleted %d documents from %s", len(keys), collection)
}

func resetCollections(ctx context.Context) {
	for _, c := range []string{EventCollection, ClusterCollection, JobCollection, TaskCollection} {
		deleteAllFromCollection(ctx, c)
	}
}

func writeCluster(ctx context.Context, cfg *Config) {
	if cfg.DryRun {
		return
	}
	c := &Cluster{
		ClusterID:   cfg.ClusterID,
		MachineType: cfg.MachineType,
		CreatedAt:   time.Now().UTC(),
	}
	key := datastore.NameKey(ClusterCollection, cfg.ClusterID, nil)
	if _, err := dsClient.Put(ctx, key, c); err != nil {
		log.Fatalf("Failed to write cluster: %v", err)
	}
}

func writeJob(ctx context.Context, cfg *Config, jobID string) {
	if cfg.DryRun {
		return
	}
	j := &Job{
		JobID:      jobID,
		ClusterID:  cfg.ClusterID,
		Status:     "pending",
		SubmitTime: time.Now().UTC(),
		TaskCount:  int32(cfg.TasksPerJob),
	}
	key := datastore.NameKey(JobCollection, jobID, nil)
	if _, err := dsClient.Put(ctx, key, j); err != nil {
		log.Fatalf("Failed to write job %s: %v", jobID, err)
	}
}

func upsertTask(ctx context.Context, cfg *Config, t *Task) {
	if cfg.DryRun {
		return
	}
	key := datastore.NameKey(TaskCollection, t.TaskID, nil)
	if _, err := dsClient.Put(ctx, key, t); err != nil {
		log.Fatalf("Failed to write task %s: %v", t.TaskID, err)
	}
}

func setTaskStatus(ctx context.Context, cfg *Config, t *Task, newStatus, owner, failureReason string) {
	now := time.Now().UTC()
	t.Status = newStatus
	t.Owner = owner
	t.LastUpdated = now
	t.FailureReason = failureReason
	t.History = append(t.History, &TaskHistory{
		Timestamp:     float64(now.UnixNano()) / 1e9,
		Status:        newStatus,
		Owner:         owner,
		FailureReason: failureReason,
	})
	upsertTask(ctx, cfg, t)
}

func baseTaskProps(eventType, taskID, jobID, clusterID string) datastore.PropertyList {
	return datastore.PropertyList{
		{Name: "type", Value: eventType, NoIndex: false},
		{Name: "cluster_id", Value: clusterID, NoIndex: false},
		{Name: "job_id", Value: jobID, NoIndex: false},
		{Name: "task_id", Value: taskID, NoIndex: false},
	}
}

func jitter(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.5 + rand.Float64()))
}

func runWorker(ctx context.Context, cfg *Config, taskQueue <-chan pendingTask, wg *sync.WaitGroup) {
	defer wg.Done()

	time.Sleep(jitter(cfg.WorkerStartDelay))

	workerID := "worker-" + uuid.New().String()[:8]
	writeEvent(ctx, cfg, datastore.PropertyList{
		{Name: "type", Value: "worker_started"},
		{Name: "cluster_id", Value: cfg.ClusterID},
		{Name: "worker_id", Value: workerID},
	})
	log.Printf("Worker %s started", workerID)

	for task := range taskQueue {
		log.Printf("Worker %s claiming task %s", workerID, task.taskID)
		writeEvent(ctx, cfg, baseTaskProps("task_claimed", task.taskID, task.jobID, cfg.ClusterID))
		setTaskStatus(ctx, cfg, task.task, "claimed", workerID, "")

		// Staging phase
		time.Sleep(jitter(cfg.StagingDuration))

		if rand.Float64() < cfg.WorkerFailRate {
			log.Printf("Worker %s failed during staging of %s", workerID, task.taskID)
			writeEvent(ctx, cfg, baseTaskProps("task_orphaned", task.taskID, task.jobID, cfg.ClusterID))
			setTaskStatus(ctx, cfg, task.task, "pending", "", "")
			writeEvent(ctx, cfg, datastore.PropertyList{
				{Name: "type", Value: "worker_stopped"},
				{Name: "cluster_id", Value: cfg.ClusterID},
				{Name: "worker_id", Value: workerID},
			})
			// Return task to queue by spawning re-queuer (can't send to read-only chan)
			go func(t pendingTask) {
				// small delay before re-queue to simulate re-scheduling
				time.Sleep(jitter(cfg.RequeueDelay))
				requeue(t)
			}(task)
			workerID = "worker-" + uuid.New().String()[:8]
			writeEvent(ctx, cfg, datastore.PropertyList{
				{Name: "type", Value: "worker_started"},
				{Name: "cluster_id", Value: cfg.ClusterID},
				{Name: "worker_id", Value: workerID},
			})
			continue
		}
		if rand.Float64() < cfg.TaskFailRate {
			reason := failureReasons[rand.Intn(len(failureReasons))]
			writeEvent(ctx, cfg, append(baseTaskProps("task_failed", task.taskID, task.jobID, cfg.ClusterID),
				datastore.Property{Name: "failure_reason", Value: reason, NoIndex: true},
			))
			setTaskStatus(ctx, cfg, task.task, "failed", workerID, reason)
			task.onDone()
			continue
		}

		writeEvent(ctx, cfg, baseTaskProps("task_exec_started", task.taskID, task.jobID, cfg.ClusterID))

		// Execution phase
		time.Sleep(jitter(cfg.TaskDuration))

		if rand.Float64() < cfg.WorkerFailRate {
			log.Printf("Worker %s failed during exec of %s", workerID, task.taskID)
			writeEvent(ctx, cfg, baseTaskProps("task_orphaned", task.taskID, task.jobID, cfg.ClusterID))
			setTaskStatus(ctx, cfg, task.task, "pending", "", "")
			writeEvent(ctx, cfg, datastore.PropertyList{
				{Name: "type", Value: "worker_stopped"},
				{Name: "cluster_id", Value: cfg.ClusterID},
				{Name: "worker_id", Value: workerID},
			})
			go func(t pendingTask) {
				time.Sleep(jitter(cfg.RequeueDelay))
				requeue(t)
			}(task)
			workerID = "worker-" + uuid.New().String()[:8]
			writeEvent(ctx, cfg, datastore.PropertyList{
				{Name: "type", Value: "worker_started"},
				{Name: "cluster_id", Value: cfg.ClusterID},
				{Name: "worker_id", Value: workerID},
			})
			continue
		}
		if rand.Float64() < cfg.TaskFailRate {
			reason := failureReasons[rand.Intn(len(failureReasons))]
			writeEvent(ctx, cfg, append(baseTaskProps("task_failed", task.taskID, task.jobID, cfg.ClusterID),
				datastore.Property{Name: "failure_reason", Value: reason, NoIndex: true},
			))
			setTaskStatus(ctx, cfg, task.task, "failed", workerID, reason)
			task.onDone()
			continue
		}

		writeEvent(ctx, cfg, baseTaskProps("task_exec_complete", task.taskID, task.jobID, cfg.ClusterID))

		// Upload phase
		time.Sleep(jitter(cfg.UploadDuration))

		if rand.Float64() < cfg.WorkerFailRate {
			log.Printf("Worker %s failed during upload of %s", workerID, task.taskID)
			writeEvent(ctx, cfg, baseTaskProps("task_orphaned", task.taskID, task.jobID, cfg.ClusterID))
			setTaskStatus(ctx, cfg, task.task, "pending", "", "")
			writeEvent(ctx, cfg, datastore.PropertyList{
				{Name: "type", Value: "worker_stopped"},
				{Name: "cluster_id", Value: cfg.ClusterID},
				{Name: "worker_id", Value: workerID},
			})
			go func(t pendingTask) {
				time.Sleep(jitter(cfg.RequeueDelay))
				requeue(t)
			}(task)
			workerID = "worker-" + uuid.New().String()[:8]
			writeEvent(ctx, cfg, datastore.PropertyList{
				{Name: "type", Value: "worker_started"},
				{Name: "cluster_id", Value: cfg.ClusterID},
				{Name: "worker_id", Value: workerID},
			})
			continue
		}

		maxMemGB := 0.5 + rand.Float64()*7.5
		maxMemBytes := int64(maxMemGB * 1024 * 1024 * 1024)
		writeEvent(ctx, cfg, datastore.PropertyList{
			{Name: "type", Value: "task_complete", NoIndex: false},
			{Name: "cluster_id", Value: cfg.ClusterID, NoIndex: false},
			{Name: "job_id", Value: task.jobID, NoIndex: false},
			{Name: "task_id", Value: task.taskID, NoIndex: false},
			{Name: "exit_code", Value: int64(0), NoIndex: true},
			{Name: "download_bytes", Value: int64(rand.Intn(500*1024*1024) + 1024*1024), NoIndex: true},
			{Name: "upload_bytes", Value: int64(rand.Intn(100*1024*1024) + 512*1024), NoIndex: true},
			{Name: "max_mem_in_gb", Value: maxMemGB, NoIndex: true},
			{Name: "max_memory_bytes", Value: maxMemBytes, NoIndex: true},
			{Name: "shared_memory_bytes", Value: int64(rand.Intn(50 * 1024 * 1024)), NoIndex: true},
			{Name: "unshared_memory_bytes", Value: maxMemBytes - int64(rand.Intn(50*1024*1024)), NoIndex: true},
			{Name: "user_cpu_sec", Value: rand.Float64() * 300.0, NoIndex: true},
			{Name: "system_cpu_sec", Value: rand.Float64() * 30.0, NoIndex: true},
			{Name: "block_input_ops", Value: int64(rand.Intn(10000)), NoIndex: true},
			{Name: "block_output_ops", Value: int64(rand.Intn(5000)), NoIndex: true},
		})
		setTaskStatus(ctx, cfg, task.task, "complete", workerID, "")
		log.Printf("Task %s completed", task.taskID)
		task.onDone()
	}

	writeEvent(ctx, cfg, datastore.PropertyList{
		{Name: "type", Value: "worker_stopped"},
		{Name: "cluster_id", Value: cfg.ClusterID},
		{Name: "worker_id", Value: workerID},
	})
}

// globalQueue is used by workers spawned outside the main queue fan-out
var globalQueue chan pendingTask

func requeue(t pendingTask) {
	globalQueue <- t
}

func spawnJob(ctx context.Context, cfg *Config) {
	jobID := fmt.Sprintf("job-%s-%s", time.Now().UTC().Format("20060102-150405"), uuid.New().String()[:6])
	log.Printf("Starting job %s with %d tasks", jobID, cfg.TasksPerJob)

	writeJob(ctx, cfg, jobID)
	writeEvent(ctx, cfg, datastore.PropertyList{
		{Name: "type", Value: "job_started"},
		{Name: "cluster_id", Value: cfg.ClusterID},
		{Name: "job_id", Value: jobID},
		{Name: "task_count", Value: int64(cfg.TasksPerJob)},
	})

	var doneTasks atomic.Int64
	onDone := func() {
		n := doneTasks.Add(1)
		if int(n) >= cfg.TasksPerJob {
			log.Printf("Job %s completed all %d tasks", jobID, cfg.TasksPerJob)
			runningJobs.Add(-1)
		}
	}

	for i := 0; i < cfg.TasksPerJob; i++ {
		taskID := fmt.Sprintf("%s.%04d", jobID, i)
		t := &Task{
			TaskID:      taskID,
			TaskIndex:   int64(i),
			JobID:       jobID,
			ClusterID:   cfg.ClusterID,
			Status:      "pending",
			LastUpdated: time.Now().UTC(),
		}
		upsertTask(ctx, cfg, t)
		globalQueue <- pendingTask{
			taskID: taskID,
			jobID:  jobID,
			task:   t,
			onDone: onDone,
		}
	}
}

func runJobSpawner(ctx context.Context, cfg *Config) {
	// Brief startup delay
	time.Sleep(2 * time.Second)

	ticker := time.NewTicker(cfg.JobInterval)
	defer ticker.Stop()

	// Try to fill up to maxJobs immediately
	for runningJobs.Load() < int64(cfg.MaxJobs) {
		runningJobs.Add(1)
		go spawnJob(ctx, cfg)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for runningJobs.Load() < int64(cfg.MaxJobs) {
				runningJobs.Add(1)
				go spawnJob(ctx, cfg)
			}
		}
	}
}

func main() {
	projectID := flag.String("project", "", "GCP project ID (required unless --dry-run)")
	clusterID := flag.String("cluster-id", "", "Cluster ID (default: random UUID)")
	machineType := flag.String("machine-type", "n1-standard-4", "Machine type label for the cluster")
	maxJobs := flag.Int("max-jobs", 3, "Max concurrent jobs")
	tasksPerJob := flag.Int("tasks-per-job", 100, "Tasks per job")
	numWorkers := flag.Int("workers", 100, "Number of worker goroutines")
	workerFailRate := flag.Float64("worker-fail-rate", 0.05, "Probability worker fails per stage (causes task orphan)")
	taskFailRate := flag.Float64("task-fail-rate", 0.02, "Probability task permanently fails per stage")
	taskDuration := flag.Duration("task-duration", 5*time.Minute, "Mean task execution duration")
	stagingDuration := flag.Duration("staging-duration", 30*time.Second, "Mean staging/localisation duration")
	uploadDuration := flag.Duration("upload-duration", 15*time.Second, "Mean upload duration")
	jobInterval := flag.Duration("job-interval", 2*time.Minute, "How often to check for new jobs to start")
	dryRun := flag.Bool("dry-run", false, "Print events to stdout instead of writing to Datastore")
	reset := flag.Bool("reset", false, "Delete all events from the collection before starting")
	speed := flag.Float64("speed", 1.0, "Scale factor for all durations (2 = twice as fast)")
	flag.Parse()

	if *clusterID == "" {
		*clusterID = "cluster-" + uuid.New().String()[:8]
	}

	scale := func(d time.Duration) time.Duration {
		return time.Duration(float64(d) / *speed)
	}

	cfg := &Config{
		ProjectID:       *projectID,
		ClusterID:       *clusterID,
		MachineType:     *machineType,
		MaxJobs:         *maxJobs,
		TasksPerJob:     *tasksPerJob,
		NumWorkers:      *numWorkers,
		WorkerFailRate:  *workerFailRate,
		TaskFailRate:    *taskFailRate,
		TaskDuration:    scale(*taskDuration),
		StagingDuration: scale(*stagingDuration),
		UploadDuration:  scale(*uploadDuration),
		JobInterval:     scale(*jobInterval),
		RequeueDelay:     scale(10 * time.Second),
		WorkerStartDelay: scale(10 * time.Second),
		DryRun:          *dryRun,
	}

	if !cfg.DryRun && cfg.ProjectID == "" {
		log.Fatal("--project is required (or use --dry-run)")
	}

	ctx := context.Background()

	if !cfg.DryRun {
		var err error
		dsClient, err = datastore.NewClient(ctx, cfg.ProjectID)
		if err != nil {
			log.Fatalf("Failed to create Datastore client: %v", err)
		}
		defer dsClient.Close()
		if *reset {
			resetCollections(ctx)
		}
	}

	log.Printf("Simulator starting: cluster=%s max-jobs=%d tasks-per-job=%d workers=%d",
		cfg.ClusterID, cfg.MaxJobs, cfg.TasksPerJob, cfg.NumWorkers)

	// Queue sized to hold all tasks from all concurrent jobs plus headroom
	globalQueue = make(chan pendingTask, cfg.MaxJobs*cfg.TasksPerJob*2)

	writeCluster(ctx, cfg)
	writeEvent(ctx, cfg, datastore.PropertyList{
		{Name: "type", Value: "cluster_started"},
		{Name: "cluster_id", Value: cfg.ClusterID},
	})

	var workerWg sync.WaitGroup
	for i := 0; i < cfg.NumWorkers; i++ {
		workerWg.Add(1)
		go runWorker(ctx, cfg, globalQueue, &workerWg)
	}

	go runJobSpawner(ctx, cfg)

	// Block forever (Ctrl+C to stop)
	select {}
}
