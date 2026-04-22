package sparklesworker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
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

type taskEntry struct {
	logPath  string
	notifyCh chan string // receives req_id from start_publishing control messages
	cancel   context.CancelFunc
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
func (p *PubSubPublisher) RegisterTask(taskID, logPath string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tasks[taskID] = &taskEntry{
		logPath:  logPath,
		notifyCh: make(chan string, 1),
	}
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
		var cmd startPublishing
		if err := json.Unmarshal(msg.Data, &cmd); err != nil || cmd.Type != "start_publishing" {
			msg.Ack()
			return
		}
		msg.Ack()

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
	}); err != nil && ctx.Err() == nil {
		log.Printf("Control message subscription error: %v", err)
	}
}

// waitForStartPublishing blocks until a start_publishing message arrives for taskID,
// then starts metric/log goroutines. Runs for the duration of the task.
func (p *PubSubPublisher) waitForStartPublishing(ctx context.Context, taskID string) {
	p.mu.Lock()
	entry, ok := p.tasks[taskID]
	p.mu.Unlock()
	if !ok {
		return
	}

	select {
	case <-ctx.Done():
		return
	case reqID := <-entry.notifyCh:
		// Acknowledge the command
		ack, _ := json.Marshal(commandAck{Type: "command_ack", ReqID: reqID, TaskID: taskID})
		p.publish(ctx, TopicTaskOut, ack, map[string]string{"type": "command_ack", "task_id": taskID})

		pubCtx, cancel := context.WithCancel(ctx)
		p.mu.Lock()
		if e, ok := p.tasks[taskID]; ok {
			e.cancel = cancel
		} else {
			cancel()
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()

		go p.publishMetrics(pubCtx, taskID)
		go p.publishLogs(pubCtx, taskID, entry.logPath)
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

func (p *PubSubPublisher) publishMetrics(ctx context.Context, taskID string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mem, _ := getMemoryUsage()
			sysMem, _ := getSystemMemory()
			cpu, _ := getCPUStats()
			pressure := getMemoryPressure()

			update := ResourceUsageUpdate{
				Type:      "metric_update",
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

			data, err := json.Marshal(update)
			if err != nil {
				continue
			}
			go p.publish(ctx, TopicTaskOut, data, map[string]string{"type": "metric_update", "task_id": taskID})
		}
	}
}

func (p *PubSubPublisher) publishLogs(ctx context.Context, taskID, logPath string) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var offset int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f, err := os.Open(logPath)
			if err != nil {
				continue
			}
			info, err := f.Stat()
			if err != nil {
				f.Close()
				continue
			}
			size := info.Size()
			if size <= offset {
				f.Close()
				continue
			}
			buf := make([]byte, size-offset)
			n, err := f.ReadAt(buf, offset)
			f.Close()
			if n == 0 {
				continue
			}
			offset += int64(n)

			update := LogStreamUpdate{
				Type:      "log_update",
				Timestamp: time.Now().UTC(),
				TaskID:    taskID,
				Content:   string(buf[:n]),
			}
			data, err := json.Marshal(update)
			if err != nil {
				continue
			}
			go p.publish(ctx, TopicTaskOut, data, map[string]string{"type": "log_update", "task_id": taskID})

			if err != nil && err.Error() != "EOF" {
				log.Printf("Error reading log for task %s: %v", taskID, err)
			}
		}
	}
}
