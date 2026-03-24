package monitor

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"time"

	"github.com/broadinstitute/sparklesworker/ext_channel"
)

type Monitor struct {
	ctx              context.Context
	cancel           context.CancelFunc
	channel          ext_channel.ExtChannel
	topicName        string
	pollSleep        time.Duration
	resourcePollFreq time.Duration
	stdoutFile       *os.File
	done             chan struct{}
}

func NewMonitor(ctx context.Context, channel ext_channel.ExtChannel, topicName string, pollSleep time.Duration, resourcePollFreqSecs int) *Monitor {
	ctx, cancel := context.WithCancel(ctx)
	return &Monitor{
		ctx:              ctx,
		cancel:           cancel,
		channel:          channel,
		topicName:        topicName,
		pollSleep:        pollSleep,
		resourcePollFreq: time.Duration(resourcePollFreqSecs) * time.Second,
	}
}

func (m *Monitor) publish(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return m.channel.Publish(m.ctx, m.topicName, data)
}

func (m *Monitor) pollStdout() error {
	buf := make([]byte, 100*1024)
	n, err := m.stdoutFile.Read(buf)
	if err != nil && err != io.EOF {
		log.Printf("Monitor.poll: read error: %v", err)
		return err
	}
	if n > 0 {
		msg := StdoutUpdate{Type: "stdout", Content: buf[:n], Timestamp: time.Now()}
		if pubErr := m.publish(msg); pubErr != nil {
			log.Printf("Monitor.poll: publish error: %v", pubErr)
			return pubErr
		}
	}
	return nil
}

func (m *Monitor) pollResources() {
	usage, err := GetResourceUsage()
	if err != nil {
		log.Printf("Monitor.pollResources: %v", err)
		return
	}
	if pubErr := m.publish(usage); pubErr != nil {
		log.Printf("Monitor.pollResources: publish error: %v", pubErr)
	}
}

func (m *Monitor) Started(stdoutPath string) {
	m.done = make(chan struct{})
	go func() {
		defer close(m.done)

		f, err := os.Open(stdoutPath)
		if err != nil {
			log.Printf("Monitor.Started: failed to open %s: %v", stdoutPath, err)
			return
		}
		m.stdoutFile = f

		lastResourcePoll := time.Now()
		for {
			if err := m.pollStdout(); err != nil {
				return
			}
			if time.Since(lastResourcePoll) >= m.resourcePollFreq {
				m.pollResources()
				lastResourcePoll = time.Now()
			}
			select {
			case <-m.ctx.Done():
				return
			case <-time.After(m.pollSleep):
			}
		}
	}()
}

func (m *Monitor) Finished() {
	m.cancel()
	if m.done != nil {
		<-m.done
	}
	if m.stdoutFile != nil {
		// do one final poll to get the last bytes written before the process stopped
		m.pollStdout()
		m.stdoutFile.Close()
		m.stdoutFile = nil
	}
}
