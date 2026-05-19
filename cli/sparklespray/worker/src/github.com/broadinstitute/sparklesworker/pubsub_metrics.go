package sparklesworker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	pubsub "cloud.google.com/go/pubsub/v2"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/durationpb"
)

const TopicTaskOut = "sparkles-v6-task-out"
const TopicTaskIn = "sparkles-v6-task-in"

type VolumeUsage struct {
	Location string  `json:"location"`
	TotalGB  float64 `json:"total_gb"`
	UsedGB   float64 `json:"used_gb"`
}

type ResourceUsageUpdate struct {
	Type                 string        `json:"type"`
	UUID                 string        `json:"uuid"`
	TaskID               string        `json:"task_id"`
	Timestamp            time.Time     `json:"timestamp"`
	ProcessCount         int32         `json:"process_count"`
	Volumes              []VolumeUsage `json:"volumes,omitempty"`
	TotalMemory          int64         `json:"total_memory"`
	TotalData            int64         `json:"total_data"`
	TotalShared          int64         `json:"total_shared"`
	TotalResident        int64         `json:"total_resident"`
	CpuUser              int64         `json:"cpu_user"`
	CpuSystem            int64         `json:"cpu_system"`
	CpuIdle              int64         `json:"cpu_idle"`
	CpuIowait            int64         `json:"cpu_iowait"`
	MemTotal             int64         `json:"mem_total"`
	MemAvailable         int64         `json:"mem_available"`
	MemFree              int64         `json:"mem_free"`
	MemPressureSomeAvg10 int32         `json:"mem_pressure_some_avg10"`
	MemPressureFullAvg10 int32         `json:"mem_pressure_full_avg10"`
}

type LogStreamUpdate struct {
	Type      string    `json:"type"`
	UUID      string    `json:"uuid"`
	Timestamp time.Time `json:"timestamp"`
	TaskID    string    `json:"task_id"`
	Content   string    `json:"content"`
}

type commandAck struct {
	Type   string `json:"type"`
	ReqID  string `json:"req_id"`
	TaskID string `json:"task_id"`
}

type startPublishing struct {
	Type   string `json:"type"`
	ReqID  string `json:"req_id"`
	TaskID string `json:"task_id"`
}

type killJob struct {
	Type  string `json:"type"`
	JobID string `json:"job_id"`
}

type taskEntry struct {
	logPath        string
	notifyCh       chan string // receives req_id from start_publishing control messages
	cancel         context.CancelFunc
	jobID          string             // extracted from taskID at registration time
	executorCancel context.CancelFunc // cancels the running subprocess
}

// PubSubPublisher manages metric/log streaming for running tasks via Pub/Sub.
// It subscribes to sparkles-task-in for start_publishing commands and publishes
// metric_update and log_update messages to sparkles-task-out.
type PubSubPublisher struct {
	psClient  *pubsub.Client
	projectID string
	tasksDir  string // used for disk volume reporting
	monitor   *Monitor

	mu    sync.Mutex
	tasks map[string]*taskEntry
}

func NewPubSubPublisher(psClient *pubsub.Client, projectID, tasksDir string, monitor *Monitor) *PubSubPublisher {
	return &PubSubPublisher{
		psClient:  psClient,
		projectID: projectID,
		tasksDir:  tasksDir,
		monitor:   monitor,
		tasks:     make(map[string]*taskEntry),
	}
}

// RegisterTask registers a task for metric/log streaming. Call before execution begins.
// executorCancel, when called, will kill the running subprocess for this task.
func (p *PubSubPublisher) RegisterTask(ctx context.Context, taskID, logPath string, executorCancel context.CancelFunc) context.Context {
	jobID := strings.Split(taskID, ".")[0]
	p.mu.Lock()
	defer p.mu.Unlock()

	publisherCtx, cancelFn := context.WithCancel(ctx)

	entry := &taskEntry{
		logPath:        logPath,
		notifyCh:       make(chan string, 1),
		jobID:          jobID,
		executorCancel: executorCancel,
		cancel:         cancelFn,
	}
	p.tasks[taskID] = entry

	return publisherCtx
}

// UnregisterTask removes the task and cancels any active metric publisher for it.
func (p *PubSubPublisher) UnregisterTask(taskID string) {
	p.mu.Lock()
	entry, ok := p.tasks[taskID]
	if ok {
		delete(p.tasks, taskID)
	}
	p.mu.Unlock()

	if ok && entry.cancel != nil {
		entry.cancel()
	}
}

// ListenForControlMessages subscribes to sparkles-task-in and dispatches
// start_publishing commands to the appropriate task goroutine.
// Blocks until ctx is cancelled; creates an ephemeral subscription that is
// deleted on exit.
func (p *PubSubPublisher) ListenForControlMessages(ctx context.Context) {
	subName := fmt.Sprintf("sparkles-worker-%s", uuid.New().String()[:8])
	fullSubName := fmt.Sprintf("projects/%s/subscriptions/%s", p.projectID, subName)
	fullTopicName := fmt.Sprintf("projects/%s/topics/%s", p.projectID, TopicTaskIn)

	_, err := p.psClient.SubscriptionAdminClient.CreateSubscription(ctx, &pb.Subscription{
		Name:               fullSubName,
		Topic:              fullTopicName,
		AckDeadlineSeconds: 10,
		ExpirationPolicy:   &pb.ExpirationPolicy{Ttl: durationpb.New(24 * time.Hour)},
	})
	if err != nil {
		log.Printf("WARNING: could not create control subscription on %s: %v (metric streaming disabled)", TopicTaskIn, err)
		return
	}
	log.Printf("Listening for control messages on subscription %s", subName)

	defer func() {
		delCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := p.psClient.SubscriptionAdminClient.DeleteSubscription(delCtx, &pb.DeleteSubscriptionRequest{Subscription: fullSubName}); err != nil {
			log.Printf("WARNING: failed to delete control subscription %s: %v", subName, err)
		}
	}()

	sub := p.psClient.Subscriber(fullSubName)
	if err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(msg.Data, &envelope); err != nil {
			msg.Ack()
			return
		}
		msg.Ack()

		switch envelope.Type {
		case "start_publishing":
			var cmd startPublishing
			if err := json.Unmarshal(msg.Data, &cmd); err != nil {
				return
			}
			p.mu.Lock()
			entry, ok := p.tasks[cmd.TaskID]
			p.mu.Unlock()
			if !ok {
				log.Printf("Received start_publishing for unknown task %s, ignoring", cmd.TaskID)
				return
			}
			select {
			case entry.notifyCh <- cmd.ReqID:
			default:
			}

		case "kill_job":
			var cmd killJob
			if err := json.Unmarshal(msg.Data, &cmd); err != nil {
				return
			}
			log.Printf("Received kill_job for job %s, cancelling running task", cmd.JobID)
			p.mu.Lock()
			for _, entry := range p.tasks {
				if entry.jobID == cmd.JobID && entry.executorCancel != nil {
					entry.executorCancel()
				}
			}
			p.mu.Unlock()
		}
	}); err != nil && ctx.Err() == nil {
		log.Printf("Control message subscription error: %v", err)
	}
}

const maxHistory = 10 * 60 / 15 // 10 minute history, assuming update every 15 seconds

// waitForStartPublishing blocks until a start_publishing message arrives for taskID,
// then starts metric/log goroutines. Runs for the duration of the task.
func (p *PubSubPublisher) waitForStartPublishing(ctx context.Context, taskID string) {
	p.mu.Lock()
	entry, ok := p.tasks[taskID]
	p.mu.Unlock()
	if !ok {
		return
	}

	isPublishingMetrics := false
	metricHistory := make([]*ResourceUsageUpdate, 0, maxHistory)

	metricUpdateTicker := time.NewTicker(15 * time.Second)
	defer metricUpdateTicker.Stop()

	logUpdateTicker := time.NewTicker(1 * time.Second)
	defer logUpdateTicker.Stop()

	var offset int64

	for {
		select {
		case <-logUpdateTicker.C:
			update := p.pollLog(taskID, entry.logPath, &offset)
			if update != nil && isPublishingMetrics {
				p.publishLogUpdate(ctx, update)
			}
		case <-metricUpdateTicker.C:
			update := p.pollMetrics(taskID)
			metricHistory = append(metricHistory, update)
			if len(metricHistory) > maxHistory {
				copy(metricHistory[:len(metricHistory)-1], metricHistory[1:])
				metricHistory = metricHistory[:len(metricHistory)-1]
			}
			if isPublishingMetrics {
				p.publishMetricUpdate(ctx, update)
			}
		case <-ctx.Done():
			return
		case reqID := <-entry.notifyCh:
			// Acknowledge the command
			ack, _ := json.Marshal(commandAck{Type: "command_ack", ReqID: reqID, TaskID: taskID})
			p.publish(ctx, TopicTaskOut, ack, map[string]string{"type": "command_ack", "task_id": taskID})

			isPublishingMetrics = true
			for _, update := range metricHistory {
				p.publishMetricUpdate(ctx, update)
			}
			offset = 0
		}
	}
}

func (p *PubSubPublisher) publish(ctx context.Context, topicName string, data []byte, attrs map[string]string) {
	publisher := p.psClient.Publisher(topicName)
	defer publisher.Stop()
	result := publisher.Publish(ctx, &pubsub.Message{Data: data, Attributes: attrs})
	if _, err := result.Get(ctx); err != nil && ctx.Err() == nil {
		log.Printf("ERROR publishing to %s: %v", topicName, err)
	}
}

func getVolumeUsage(paths ...string) []VolumeUsage {
	seen := make(map[string]bool)
	var volumes []VolumeUsage
	for _, p := range paths {
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		var stat syscall.Statfs_t
		if err := syscall.Statfs(p, &stat); err != nil {
			continue
		}
		total := float64(stat.Blocks*uint64(stat.Bsize)) / (1024 * 1024 * 1024)
		free := float64(stat.Bfree*uint64(stat.Bsize)) / (1024 * 1024 * 1024)
		volumes = append(volumes, VolumeUsage{Location: p, TotalGB: total, UsedGB: total - free})
	}
	return volumes
}

func (p *PubSubPublisher) pollLog(taskID string, logPath string, offset *int64) *LogStreamUpdate {
	f, err := os.Open(logPath)
	if err != nil {
		return nil
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil
	}
	size := info.Size()
	if size <= *offset {
		f.Close()
		return nil
	}
	buf := make([]byte, size-*offset)
	n, err := f.ReadAt(buf, *offset)
	f.Close()
	if n == 0 {
		return nil
	}
	*offset += int64(n)

	update := LogStreamUpdate{
		Type:      "log_update",
		UUID:      fmt.Sprintf("%s-%d", taskID, *offset),
		Timestamp: time.Now().UTC(),
		TaskID:    taskID,
		Content:   string(buf[:n]),
	}
	return &update
}

func (p *PubSubPublisher) pollMetrics(taskID string) *ResourceUsageUpdate {
	mem, _ := getMemoryUsage()
	sysMem, _ := getSystemMemory()
	cpu, _ := getCPUStats()
	pressure := getMemoryPressure()

	update := ResourceUsageUpdate{
		Type:      "metric_update",
		UUID:      uuid.New().String(),
		TaskID:    taskID,
		Timestamp: time.Now().UTC(),
		Volumes:   getVolumeUsage("/", p.tasksDir),
	}
	if mem != nil {
		update.ProcessCount = int32(mem.procCount)
		update.TotalMemory = mem.totalSize * PAGE_SIZE
		update.TotalData = mem.totalData * PAGE_SIZE
		update.TotalShared = mem.totalShared * PAGE_SIZE
		update.TotalResident = mem.totalResident * PAGE_SIZE
	}
	if cpu != nil {
		update.CpuUser = cpu.User
		update.CpuSystem = cpu.System
		update.CpuIdle = cpu.Idle
		update.CpuIowait = cpu.Iowait
	}
	if sysMem != nil {
		update.MemTotal = sysMem.Total
		update.MemAvailable = sysMem.Available
		update.MemFree = sysMem.Free
	}
	if pressure != nil {
		update.MemPressureSomeAvg10 = pressure.SomeAvg10
		update.MemPressureFullAvg10 = pressure.FullAvg10
	}
	return &update

}
func (p *PubSubPublisher) publishMetricUpdate(ctx context.Context, update *ResourceUsageUpdate) {
	data, err := json.Marshal(update)
	if err != nil {
		panic("Could not marshal update")
	}
	p.publish(ctx, TopicTaskOut, data, map[string]string{"type": "metric_update", "task_id": update.TaskID})
}

func (p *PubSubPublisher) publishLogUpdate(ctx context.Context, update *LogStreamUpdate) {
	data, err := json.Marshal(update)
	if err != nil {
		panic("Could not marshal update")
	}
	p.publish(ctx, TopicTaskOut, data, map[string]string{"type": "log_update", "task_id": update.TaskID})
}
