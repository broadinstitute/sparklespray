package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub/v2"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

const EventCollection = "SparklesV6Event"
const ClusterCollection = "SparklesV6Cluster"
const JobCollection = "SparklesV6Job"
const TaskCollection = "SparklesV6Task"
const EventExpiry = 7 * 24 * time.Hour

const TopicLifecycle = "sparkles-v6-events"
const TopicTaskOut = "sparkles-v6-task-out"
const TopicTaskIn = "sparkles-v6-task-in"

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
	ProjectID        string
	ClusterID        string
	MachineType      string
	MaxJobs          int
	TasksPerJob      int
	NumWorkers       int
	WorkerFailRate   float64
	TaskFailRate     float64
	TaskDuration     time.Duration
	StagingDuration  time.Duration
	UploadDuration   time.Duration
	JobInterval      time.Duration
	RequeueDelay     time.Duration
	WorkerStartDelay time.Duration
	DryRun           bool
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
	JobID                  string    `datastore:"job_id"`
	Tasks                  []string  `datastore:"tasks,noindex"`
	KubeJobSpec            string    `datastore:"kube_job_spec,noindex"`
	Metadata               string    `datastore:"metadata,noindex"`
	ClusterID              string    `datastore:"cluster_id"`
	Status                 string    `datastore:"status"`
	SubmitTime             time.Time `datastore:"submit_time"`
	TaskCount              int32     `datastore:"task_count"`
	MaxPreemptableAttempts int32     `datastore:"max_preemptable_attempts"`
	TargetNodeCount        int32     `datastore:"target_node_count"`
}

type VolumeUsage struct {
	Location string  `json:"location"`
	TotalGB  float64 `json:"total_gb"`
	UsedGB   float64 `json:"used_gb"`
}

type ResourceUsageUpdate struct {
	Type                 string        `json:"type"`
	ReqID                string        `json:"req_id"` // todo: remove this
	TaskID               string        `json:"task_id"`
	Timestamp            time.Time     `json:"timestamp"`
	ProcessCount         int32         `json:"process_count"`
	Volumes              []VolumeUsage `json:"volumes,omitempty"`
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

type LogStreamUpdate struct {
	Type      string    `json:"type"`
	ReqID     string    `json:"req_id"` // todo: remove this
	Timestamp time.Time `json:"timestamp"`
	TaskID    string    `json:"task_id"`
	Content   string    `json:"content"`
}

type StartPublishing struct {
	Type   string `json:"type"`
	ReqID  string `json:"req_id"`
	TaskID string `json:"task_id"`
}

type pendingTask struct {
	taskID string
	jobID  string
	task   *Task
	onDone func()
}

var (
	dsClient    *datastore.Client
	psClient    *pubsub.Client
	runningJobs atomic.Int64
)

// activePublishers maps task_id -> channel that receives req_id when start_publishing is requested
var (
	activePublishersMu sync.Mutex
	activePublishers   = map[string]chan string{}
)

func registerPublisher(taskID string) chan string {
	ch := make(chan string, 1)
	activePublishersMu.Lock()
	activePublishers[taskID] = ch
	activePublishersMu.Unlock()
	return ch
}

func unregisterPublisher(taskID string) {
	activePublishersMu.Lock()
	delete(activePublishers, taskID)
	activePublishersMu.Unlock()
}

func notifyPublisher(taskID, reqID string) {
	activePublishersMu.Lock()
	ch, ok := activePublishers[taskID]
	activePublishersMu.Unlock()
	if ok {
		select {
		case ch <- reqID:
		default:
		}
	}
}

func publishToTopic(ctx context.Context, topicName string, data []byte, attrs map[string]string) {
	if psClient == nil {
		return
	}
	publisher := psClient.Publisher(topicName)
	defer publisher.Stop()
	msg := &pubsub.Message{Attributes: attrs}
	if data != nil {
		msg.Data = data
	}
	result := publisher.Publish(ctx, msg)
	if _, err := result.Get(ctx); err != nil {
		log.Printf("ERROR publishing to %s: %v", topicName, err)
	}
}

func writeEvent(ctx context.Context, cfg *Config, props datastore.PropertyList) {
	eventID := uuid.New().String()
	now := time.Now().UTC()
	props = append(props,
		datastore.Property{Name: "event_id", Value: eventID, NoIndex: false},
		datastore.Property{Name: "timestamp", Value: now, NoIndex: false},
		datastore.Property{Name: "expiry", Value: now.Add(EventExpiry), NoIndex: false},
	)

	var eventType string
	for _, p := range props {
		if p.Name == "type" {
			eventType, _ = p.Value.(string)
		}
	}

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

	if eventType != "" {
		go publishToTopic(ctx, TopicLifecycle, nil, map[string]string{"type": eventType})
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

// publishTaskMetrics publishes metric_update and log_update messages to sparkles-task-out
// until ctx is cancelled. reqID is used in all messages.
func publishTaskMetrics(ctx context.Context, taskID string) {
	metricTicker := time.NewTicker(10 * time.Second)
	logTicker := time.NewTicker(time.Duration(3+rand.Intn(5)) * time.Second)
	defer metricTicker.Stop()
	defer logTicker.Stop()

	lastLog := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-metricTicker.C:
			totalMem := int64(1+rand.Intn(8)) * 1024 * 1024 * 1024
			free := totalMem / int64(2+rand.Intn(4))
			update := ResourceUsageUpdate{
				Type:      "metric_update",
				TaskID:    taskID,
				Timestamp: time.Now().UTC(),
				Volumes: []VolumeUsage{
					{Location: "/mnt/disk1", TotalGB: 100, UsedGB: 10 + float64(rand.Intn(80))},
					{Location: "/", TotalGB: 50, UsedGB: 5 + float64(rand.Intn(40))},
				},
				ProcessCount: int32(1 + rand.Intn(8)),
				TotalMemory:          totalMem,
				TotalData:            totalMem - free - int64(rand.Intn(100*1024*1024)),
				TotalShared:          int64(rand.Intn(50 * 1024 * 1024)),
				TotalResident:        totalMem - free,
				CpuUser:              int64(rand.Intn(90)),
				CpuSystem:            int64(rand.Intn(10)),
				CpuIdle:              int64(rand.Intn(30)),
				CpuIowait:            int64(rand.Intn(5)),
				MemTotal:             totalMem,
				MemAvailable:         free,
				MemFree:              free / 2,
				MemPressureSomeAvg10: int32(rand.Intn(30)),
				MemPressureFullAvg10: int32(rand.Intn(5)),
			}
			data, _ := json.Marshal(update)
			log.Printf("Writing task %s metrics to topic", taskID)
			go publishToTopic(ctx, TopicTaskOut, data, map[string]string{"type": "metric_update", "task_id": taskID})

		case t := <-logTicker.C:
			elapsed := int(t.Sub(lastLog).Seconds())
			lastLog = t
			content := fmt.Sprintf("%d seconds since last update to the log.\nThe time is now %s\n",
				elapsed, t.UTC().Format(time.RFC3339))
			update := LogStreamUpdate{
				Type:      "log_update",
				Timestamp: time.Now().UTC(),
				TaskID:    taskID,
				Content:   content,
			}
			data, _ := json.Marshal(update)
			log.Printf("Writing log %s metrics to topic", taskID)
			go publishToTopic(ctx, TopicTaskOut, data, map[string]string{"type": "log_update", "task_id": taskID})

			logTicker.Reset(time.Duration(3+rand.Intn(5)) * time.Second)
		}
	}
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

		writeEvent(ctx, cfg, baseTaskProps("task_exec_started", task.taskID, task.jobID, cfg.ClusterID))

		// Register for start_publishing control messages during execution
		publishCh := registerPublisher(task.taskID)
		publishCtx, cancelPublish := context.WithCancel(ctx)

		// Execution phase
		execDone := make(chan struct{})
		go func() {
			time.Sleep(jitter(cfg.TaskDuration))
			close(execDone)
		}()

	execLoop:
		for {
			select {
			case reqID := <-publishCh:
				// Ack the control message
				ack := map[string]interface{}{"type": "command_ack", "req_id": reqID, "task_id": task.taskID}
				data, _ := json.Marshal(ack)
				go publishToTopic(ctx, TopicTaskOut, data, map[string]string{"type": "command_ack", "req_id": reqID})
				// Start publishing metrics/logs; cancel previous publisher if any
				go publishTaskMetrics(publishCtx, task.taskID)
			case <-execDone:
				break execLoop
			}
		}

		cancelPublish()
		unregisterPublisher(task.taskID)

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
	time.Sleep(2 * time.Second)

	ticker := time.NewTicker(cfg.JobInterval)
	defer ticker.Stop()

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

func ensureTopics(ctx context.Context) {
	for _, name := range []string{TopicLifecycle, TopicTaskOut, TopicTaskIn} {
		fullName := fmt.Sprintf("projects/%s/topics/%s", psClient.Project(), name)
		_, err := psClient.TopicAdminClient.GetTopic(ctx, &pb.GetTopicRequest{Topic: fullName})
		if err != nil {
			if status.Code(err) != codes.NotFound {
				log.Fatalf("Failed to check topic %s: %v", name, err)
			}
			if _, err := psClient.TopicAdminClient.CreateTopic(ctx, &pb.Topic{Name: fullName}); err != nil {
				log.Fatalf("Failed to create topic %s: %v", name, err)
			}
			log.Printf("Created topic %s", name)
		} else {
			log.Printf("Topic %s already exists", name)
		}
	}
}

// listenForControlMessages pulls from an ephemeral subscription on sparklespray-task-in
// and dispatches start_publishing messages to the appropriate task goroutine.
func listenForControlMessages(ctx context.Context, projectID string) {
	subName := "sparkles-simulator-" + uuid.New().String()[:8]
	fullSubName := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subName)
	fullTopicName := fmt.Sprintf("projects/%s/topics/%s", projectID, TopicTaskIn)
	_, err := psClient.SubscriptionAdminClient.CreateSubscription(ctx, &pb.Subscription{
		Name:                  fullSubName,
		Topic:                 fullTopicName,
		AckDeadlineSeconds:    10,
		ExpirationPolicy:      &pb.ExpirationPolicy{Ttl: durationpb.New(24 * time.Hour)},
		EnableMessageOrdering: false,
	})
	if err != nil {
		log.Printf("WARNING: could not create control subscription: %v (control messages disabled)", err)
		return
	}
	log.Printf("Listening for control messages on subscription %s", subName)

	defer func() {
		delCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := psClient.SubscriptionAdminClient.DeleteSubscription(delCtx, &pb.DeleteSubscriptionRequest{Subscription: fullSubName}); err != nil {
			log.Printf("WARNING: failed to delete control subscription %s: %v", subName, err)
		}
	}()

	sub := psClient.Subscriber(fullSubName)
	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		var cmd StartPublishing
		if err := json.Unmarshal(msg.Data, &cmd); err != nil {
			msg.Ack()
			return
		}
		if cmd.Type != "start_publishing" {
			msg.Ack()
			return
		}
		log.Printf("Control message: start_publishing task=%s req_id=%s", cmd.TaskID, cmd.ReqID)
		notifyPublisher(cmd.TaskID, cmd.ReqID)
		msg.Ack()
	})
	if err != nil && ctx.Err() == nil {
		log.Printf("ERROR receiving control messages: %v", err)
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
		ProjectID:        *projectID,
		ClusterID:        *clusterID,
		MachineType:      *machineType,
		MaxJobs:          *maxJobs,
		TasksPerJob:      *tasksPerJob,
		NumWorkers:       *numWorkers,
		WorkerFailRate:   *workerFailRate,
		TaskFailRate:     *taskFailRate,
		TaskDuration:     scale(*taskDuration),
		StagingDuration:  scale(*stagingDuration),
		UploadDuration:   scale(*uploadDuration),
		JobInterval:      scale(*jobInterval),
		RequeueDelay:     scale(10 * time.Second),
		WorkerStartDelay: scale(10 * time.Second),
		DryRun:           *dryRun,
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

		psClient, err = pubsub.NewClient(ctx, cfg.ProjectID)
		if err != nil {
			log.Fatalf("Failed to create Pub/Sub client: %v", err)
		}
		defer psClient.Close()

		ensureTopics(ctx)

		if *reset {
			resetCollections(ctx)
		}

		go listenForControlMessages(ctx, cfg.ProjectID)
	}

	log.Printf("Simulator starting: cluster=%s max-jobs=%d tasks-per-job=%d workers=%d",
		cfg.ClusterID, cfg.MaxJobs, cfg.TasksPerJob, cfg.NumWorkers)

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

	select {}
}
