package monitor

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/broadinstitute/sparklesworker/backend"
)

type Monitor struct {
	ctx              context.Context
	cancel           context.CancelFunc
	cancelPolling    context.CancelFunc
	channel          backend.MessageBus
	topicName        string
	stdoutPollFreq   time.Duration
	resourcePollFreq time.Duration

	lock           sync.RWMutex
	taskID         string
	listeningReqID string

	wg sync.WaitGroup
}

func NewMonitor(ctx context.Context, channel backend.MessageBus, topicName string, stdoutPollFreq time.Duration, resourcePollFreq time.Duration) *Monitor {
	ctx, cancel := context.WithCancel(ctx)
	return &Monitor{
		ctx:              ctx,
		cancel:           cancel,
		channel:          channel,
		topicName:        topicName,
		stdoutPollFreq:   stdoutPollFreq,
		resourcePollFreq: resourcePollFreq,
	}
}

func (m *Monitor) getListeningReqID() string {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.listeningReqID
}

func (m *Monitor) SetListeningReqIDIfTaskID(taskID string, reqID string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.taskID == taskID {
		m.listeningReqID = reqID
	}
}

func (m *Monitor) publish(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return m.channel.Publish(m.ctx, m.topicName, data)
}

func (m *Monitor) pollStdout(stdoutFile *os.File, taskID string, reqID string) error {
	buf := make([]byte, 100*1024)
	n, err := stdoutFile.Read(buf)
	if err != nil && err != io.EOF {
		log.Printf("Monitor.poll: read error: %v", err)
		return err
	}
	if n > 0 {
		msg := backend.LogStreamUpdate{Type: backend.RespLogStreamUpdate, TaskID: taskID, ReqID: reqID, Timestamp: time.Now(), Content: string(buf[:n])}
		if pubErr := m.publish(msg); pubErr != nil {
			log.Printf("Monitor.poll: publish error: %v", pubErr)
			return pubErr
		}
	}
	return nil
}

func (m *Monitor) pollResources(taskID string, reqID string) {
	usage, err := GetResourceUsage(taskID, reqID)
	if err != nil {
		log.Printf("Monitor.pollResources: %v", err)
		return
	}
	if pubErr := m.publish(usage); pubErr != nil {
		log.Printf("Monitor.pollResources: publish error: %v", pubErr)
	}
}

func callPeriodically(ctx context.Context, freq time.Duration, fn func()) {
	ticker := time.NewTicker(freq)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fn()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Monitor) Started(taskID string, stdoutPath string) {
	m.lock.Lock()
	m.taskID = taskID
	m.lock.Unlock()

	stdoutFile, err := os.Open(stdoutPath)
	if err != nil {
		log.Printf("Monitor.Started: failed to open %s: %v", stdoutPath, err)
		return
	}

	var periodicCtx context.Context
	periodicCtx, m.cancelPolling = context.WithCancel(m.ctx)

	m.wg.Go(func() {
		callPeriodically(periodicCtx, m.resourcePollFreq, func() {
			reqID := m.getListeningReqID()
			if reqID != "" {
				m.pollResources(taskID, reqID)
			}
		})
	})

	m.wg.Go(func() {
		callPeriodically(periodicCtx, m.stdoutPollFreq, func() {
			reqID := m.getListeningReqID()
			if reqID != "" {
				m.pollStdout(stdoutFile, taskID, reqID)
			}
		})

		// do one final poll in case we've been canceled because the task completed
		// and some more was written after our last poll
		reqID := m.getListeningReqID()
		if reqID != "" {
			m.pollStdout(stdoutFile, taskID, reqID)
		}

		stdoutFile.Close()
	})
}

// finished is only ever called after Started(). Also, Finished must be called *after* Started has completed.
func (m *Monitor) Finished() {
	// stop the two goroutines which should be polling
	m.cancelPolling()

	// now wait for them to really complete
	m.wg.Wait()

	// clear the listening request ID
	m.lock.Lock()
	m.taskID = ""
	m.listeningReqID = ""
	m.lock.Unlock()
}
