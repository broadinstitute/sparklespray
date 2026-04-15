package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/broadinstitute/sparklesworker/backend"
)

type Monitor struct {
	ctx              context.Context
	cancelPolling    context.CancelFunc
	channel          backend.MessageBus
	topicName        string
	stdoutPollFreq   time.Duration
	resourcePollFreq time.Duration

	lock            sync.Mutex
	taskID          string
	logUpdates      []*backend.LogStreamUpdate
	resourceUpdates []*backend.ResourceUsageUpdate
	isPublishing    bool

	wg sync.WaitGroup
}

func NewMonitor(ctx context.Context, channel backend.MessageBus, topicName string, stdoutPollFreq time.Duration, resourcePollFreq time.Duration) *Monitor {
	return &Monitor{
		ctx:              ctx,
		channel:          channel,
		topicName:        topicName,
		stdoutPollFreq:   stdoutPollFreq,
		resourcePollFreq: resourcePollFreq,
		logUpdates:       make([]*backend.LogStreamUpdate, 0, 100),
		resourceUpdates:  make([]*backend.ResourceUsageUpdate, 0, 100),
	}
}

func (m *Monitor) flushUpdates() error {
	m.lock.Lock()
	var logUpdates []*backend.LogStreamUpdate
	var resourceUpdates []*backend.ResourceUsageUpdate
	if m.isPublishing {
		logUpdates = make([]*backend.LogStreamUpdate, len(m.logUpdates))
		resourceUpdates = make([]*backend.ResourceUsageUpdate, len(m.resourceUpdates))
		copy(logUpdates, m.logUpdates)
		copy(resourceUpdates, m.resourceUpdates)
		m.logUpdates = m.logUpdates[:0]
		m.resourceUpdates = m.resourceUpdates[:0]
	}
	m.lock.Unlock()

	for _, msg := range logUpdates {
		err := m.publish(msg)
		if err != nil {
			return fmt.Errorf("Could not publish message: %s", err)
		}
	}

	for _, msg := range resourceUpdates {
		err := m.publish(msg)
		if err != nil {
			return fmt.Errorf("Could not publish message: %s", err)
		}
	}

	return nil
}

// not allowed to be called after Finished()
func (m *Monitor) StartListeningIfTaskID(taskID string) error {
	m.lock.Lock()

	if m.taskID == taskID {
		m.isPublishing = true
	}

	m.lock.Unlock()

	err := m.flushUpdates()
	if err != nil {
		return err
	}

	return nil
}

// not allowed to be called after Finished()
func (m *Monitor) StopListeningIfTaskID(taskID string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.taskID == taskID {
		m.isPublishing = false
	}
}

func (m *Monitor) publish(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return m.channel.Publish(m.ctx, m.topicName, data)
}

const MaxUpdatesToKeep = 100

func (m *Monitor) pollStdout(stdoutFile *os.File, taskID string) error {
	buf := make([]byte, 100*1024)
	n, err := stdoutFile.Read(buf)
	if err != nil && err != io.EOF {
		log.Printf("Monitor.poll: read error: %v", err)
		return err
	}
	if n > 0 {
		msg := backend.LogStreamUpdate{Type: backend.RespLogStreamUpdate, TaskID: taskID, Timestamp: time.Now(), Content: string(buf[:n])}

		m.lock.Lock()
		m.logUpdates = append(m.logUpdates, &msg)
		if len(m.logUpdates) > MaxUpdatesToKeep {
			copy(m.logUpdates, m.logUpdates[len(m.logUpdates)-MaxUpdatesToKeep:])
			m.logUpdates = m.logUpdates[:MaxUpdatesToKeep]
		}
		m.lock.Unlock()

		err = m.flushUpdates()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Monitor) pollResources(taskID string) error {
	msg, err := GetResourceUsage(taskID)
	if err != nil {
		log.Printf("Monitor.pollResources: %v", err)
		return err
	}

	m.lock.Lock()
	m.resourceUpdates = append(m.resourceUpdates, msg)
	if len(m.resourceUpdates) > MaxUpdatesToKeep {
		copy(m.resourceUpdates, m.resourceUpdates[len(m.resourceUpdates)-MaxUpdatesToKeep:])
		m.resourceUpdates = m.resourceUpdates[:MaxUpdatesToKeep]
	}
	m.lock.Unlock()

	err = m.flushUpdates()
	if err != nil {
		return err
	}
	return nil
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
	stdoutFile, err := os.Open(stdoutPath)
	if err != nil {
		log.Printf("Monitor.Started: failed to open %s: %v", stdoutPath, err)
		return
	}

	m.lock.Lock()
	m.taskID = taskID
	m.lock.Unlock()

	var periodicCtx context.Context
	periodicCtx, m.cancelPolling = context.WithCancel(m.ctx)

	m.wg.Go(func() {
		callPeriodically(periodicCtx, m.resourcePollFreq, func() {
			err := m.pollResources(taskID)
			if err != nil {
				log.Printf("Error polling resources: %s", err)
			}
		})
	})

	m.wg.Go(func() {
		callPeriodically(periodicCtx, m.stdoutPollFreq, func() {
			err := m.pollStdout(stdoutFile, taskID)
			if err != nil {
				log.Printf("Error polling log: %s", err)
			}
		})

		// do one final poll in case we've been canceled because the task completed
		// and some more was written after our last poll
		err := m.pollStdout(stdoutFile, taskID)
		if err != nil {
			log.Printf("Error polling log: %s", err)
		}

		stdoutFile.Close()
	})
}

// finished is only ever called after Started(). Also, Finished must be called *after* Started has completed.
func (m *Monitor) Finished() {
	// stop the two goroutines which should be polling
	if m.cancelPolling != nil {
		m.cancelPolling()
	}

	// now wait for them to really complete
	m.wg.Wait()

}
