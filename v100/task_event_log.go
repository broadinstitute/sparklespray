package v100

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
)

const taskLogCollection = "TaskLog"
const taskEventLogTTL = 7 * 24 * time.Hour
const metricsInterval = 1 * time.Minute

// taskEventLogRegistry maps live task IDs to their TaskEventLog so the
// subscription handler can look them up by task_id.
type taskEventLogRegistry struct {
	mu   sync.Mutex
	logs map[string]*TaskEventLog
}

func (r *taskEventLogRegistry) register(taskID string, tel *TaskEventLog) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logs[taskID] = tel
}

func (r *taskEventLogRegistry) unregister(taskID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.logs, taskID)
}

func (r *taskEventLogRegistry) get(taskID string) *TaskEventLog {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.logs[taskID]
}

// workerControlMessage is the JSON shape of messages received on workerInTopic.
type workerControlMessage struct {
	Type   string `json:"type"`
	TaskID string `json:"task_id"`
}

// OutputTaskEvent is one entry in the events file and in the TaskLog collection.
type OutputTaskEvent struct {
	TaskID    string    `firestore:"task_id" json:"task_id"`
	Type      string    `firestore:"type" json:"type"`
	Timestamp time.Time `firestore:"timestamp" json:"timestamp"`
	Expiry    time.Time `firestore:"expiry" json:"expiry"`
	Content   string    `firestore:"content" json:"content"`
}

// TaskEventLog captures docker output for a single task. It writes structured
// JSON events to an events file and raw text to a plain log file.
// When streaming is activated via StartStreaming, new events are written
// directly to the Firestore TaskLog collection instead of the events file.
type TaskEventLog struct {
	taskID         string
	workDir        string
	eventsFilename string
	file           *os.File
	logFile        *os.File
	streaming      bool
	mu             sync.Mutex
	fsClient       *firestore.Client
	cancelPoll     context.CancelFunc
	pollDone       chan struct{}
}

// OpenTaskEventLog creates a TaskEventLog that writes to filename (raw log)
// and filename+".events.log" (structured JSON events). A background goroutine
// polls resource metrics every minute until Close is called.
func OpenTaskEventLog(ctx context.Context, filename string, taskID string, workDir string, fsClient *firestore.Client) (*TaskEventLog, error) {
	logFile, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("creating log file: %w", err)
	}

	eventsFilename := filename + ".events.log"
	file, err := os.Create(eventsFilename)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("creating task event log %s: %w", eventsFilename, err)
	}

	pollCtx, cancelPoll := context.WithCancel(ctx)
	t := &TaskEventLog{
		taskID:         taskID,
		workDir:        workDir,
		eventsFilename: eventsFilename,
		file:           file,
		logFile:        logFile,
		fsClient:       fsClient,
		cancelPoll:     cancelPoll,
		pollDone:       make(chan struct{}),
	}

	go func() {
		defer close(t.pollDone)
		ticker := time.NewTicker(metricsInterval)
		defer ticker.Stop()
		for {
			select {
			case <-pollCtx.Done():
				return
			case <-ticker.C:
				event := collectMetrics(t.taskID, t.workDir)
				if err := t.WriteMetric(event); err != nil {
					log.Printf("writing metric for task %s: %v", t.taskID, err)
				}
			}
		}
	}()

	return t, nil
}

// WriteMetric records a resource usage sample. Before streaming is activated
// it writes a JSON event to the events file; after activation it writes
// directly to Firestore.
func (t *TaskEventLog) WriteMetric(event *ResourceUsageEvent) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.streaming {
		if _, _, err := t.fsClient.Collection(taskLogCollection).Add(context.Background(), event); err != nil {
			return fmt.Errorf("writing metric to firestore: %w", err)
		}
	} else {
		data, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshalling metric event: %w", err)
		}
		if _, err := t.file.Write(append(data, '\n')); err != nil {
			return fmt.Errorf("writing metric event: %w", err)
		}
	}
	return nil
}

// WriteOutput records a chunk of docker output. Before streaming is activated
// it writes a JSON event to the events file; after activation it writes
// directly to Firestore. Raw content is always appended to the plain log file.
func (t *TaskEventLog) WriteOutput(content string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	event := &OutputTaskEvent{
		TaskID:    t.taskID,
		Type:      "log_update",
		Timestamp: time.Now(),
		Expiry:    time.Now().Add(taskEventLogTTL),
		Content:   content,
	}

	if t.streaming {
		if _, _, err := t.fsClient.Collection(taskLogCollection).Add(context.Background(), event); err != nil {
			return fmt.Errorf("writing task log to firestore: %w", err)
		}
	} else {
		data, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshalling task event: %w", err)
		}
		if _, err := t.file.Write(append(data, '\n')); err != nil {
			return fmt.Errorf("writing task event: %w", err)
		}
	}

	if _, err := t.logFile.WriteString(content); err != nil {
		return fmt.Errorf("writing task log: %w", err)
	}
	return nil
}

// StartStreaming flushes the buffered events file to the Firestore TaskLog
// collection, then sets the streaming flag so future WriteOutput calls write
// directly to Firestore instead of the local file.
func (t *TaskEventLog) StartStreaming() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.streaming {
		return nil
	}

	if err := t.file.Close(); err != nil {
		return fmt.Errorf("closing events file: %w", err)
	}
	t.file = nil

	f, err := os.Open(t.eventsFilename)
	if err != nil {
		return fmt.Errorf("reopening events file: %w", err)
	}
	defer f.Close()

	ctx := context.Background()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var base struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(line, &base); err != nil {
			log.Printf("stream_task_updates: skipping malformed line: %v", err)
			continue
		}
		var doc interface{}
		switch base.Type {
		case "log_update":
			var event OutputTaskEvent
			if err := json.Unmarshal(line, &event); err != nil {
				log.Printf("stream_task_updates: skipping malformed log_update: %v", err)
				continue
			}
			doc = &event
		case "metric_update":
			var event ResourceUsageEvent
			if err := json.Unmarshal(line, &event); err != nil {
				log.Printf("stream_task_updates: skipping malformed metric_update: %v", err)
				continue
			}
			doc = &event
		default:
			log.Printf("stream_task_updates: unknown event type %q, skipping", base.Type)
			continue
		}
		if _, _, err := t.fsClient.Collection(taskLogCollection).Add(ctx, doc); err != nil {
			return fmt.Errorf("writing task log to firestore: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading events file: %w", err)
	}

	t.streaming = true
	return nil
}

// Flush syncs all pending writes to disk. When streaming, only the plain log
// file is synced (the events file is already closed).
func (t *TaskEventLog) Flush() error {
	t.mu.Lock()
	streaming := t.streaming
	defer t.mu.Unlock()

	if !streaming {
		if err := t.file.Sync(); err != nil {
			return fmt.Errorf("flushing event file: %w", err)
		}
	}
	if err := t.logFile.Sync(); err != nil {
		return fmt.Errorf("flushing log file: %w", err)
	}
	return nil
}

// Close stops the metrics polling goroutine, then closes the plain log file
// and, if not streaming, the events file.
func (t *TaskEventLog) Close() error {
	t.cancelPoll()
	<-t.pollDone

	t.mu.Lock()
	streaming := t.streaming
	defer t.mu.Unlock()

	if err := t.logFile.Close(); err != nil {
		return err
	}
	if !streaming {
		return t.file.Close()
	}
	return nil
}
