package monitor

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- fake ExtChannel --------------------------------------------------------

type captureChannel struct {
	mu       sync.Mutex
	messages [][]byte
}

func (c *captureChannel) Publish(_ context.Context, _ string, msg []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([]byte, len(msg))
	copy(cp, msg)
	c.messages = append(c.messages, cp)
	return nil
}

func (c *captureChannel) Subscribe(ctx context.Context, _ string, _ func([]byte)) error {
	<-ctx.Done()
	return nil
}

func (c *captureChannel) all() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, len(c.messages))
	copy(out, c.messages)
	return out
}

// ---- helpers ----------------------------------------------------------------

func newTestMonitor(ch *captureChannel) *Monitor {
	return NewMonitor(context.Background(), ch, "test-topic", 10*time.Millisecond, 3600)
}

// writeAndClose creates a temp file, writes content, and closes it so the
// monitor can open and read it independently.
func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "stdout-")
	require.NoError(t, err)
	_, err = f.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

// ---- tests ------------------------------------------------------------------

func TestMonitor_PublishesStdout(t *testing.T) {
	ch := &captureChannel{}
	m := newTestMonitor(ch)

	path := writeTempFile(t, "hello from task\n")

	m.Started(path)
	// Give the goroutine time to poll once.
	time.Sleep(50 * time.Millisecond)
	m.Finished()

	// At least one StdoutUpdate message should have been published containing
	// the written content.
	var found bool
	for _, raw := range ch.all() {
		var msg StdoutUpdate
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		if msg.Type == "stdout" && string(msg.Content) == "hello from task\n" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected a StdoutUpdate with the written content; got %d messages", len(ch.all()))
}

func TestMonitor_FinalPollOnFinished(t *testing.T) {
	// Write nothing initially; write content after Started, then call Finished.
	// The final poll in Finished should capture it.
	ch := &captureChannel{}
	m := newTestMonitor(ch)

	f, err := os.CreateTemp(t.TempDir(), "stdout-")
	require.NoError(t, err)

	m.Started(f.Name())
	// Let the goroutine start, but use a very long pollSleep so it won't
	// naturally poll again before Finished.
	m.pollSleep = 10 * time.Second

	_, err = f.WriteString("late output\n")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	m.Finished()

	var found bool
	for _, raw := range ch.all() {
		var msg StdoutUpdate
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		if msg.Type == "stdout" && string(msg.Content) == "late output\n" {
			found = true
			break
		}
	}
	assert.True(t, found, "Finished should do a final poll and capture late output")
}

func TestMonitor_FinishedBeforeStarted(t *testing.T) {
	// Calling Finished without Started should not panic.
	ch := &captureChannel{}
	m := newTestMonitor(ch)
	assert.NotPanics(t, func() { m.Finished() })
}

func TestMonitor_StartedWithMissingFile(t *testing.T) {
	// Started should not panic when the file doesn't exist — it logs and exits
	// the goroutine cleanly.
	ch := &captureChannel{}
	m := newTestMonitor(ch)
	m.Started("/nonexistent/path/stdout.txt")
	time.Sleep(30 * time.Millisecond)
	assert.NotPanics(t, func() { m.Finished() })
}

func TestMonitor_StdoutUpdateMessageShape(t *testing.T) {
	ch := &captureChannel{}
	m := newTestMonitor(ch)

	path := writeTempFile(t, "abc")
	m.Started(path)
	time.Sleep(50 * time.Millisecond)
	m.Finished()

	var msg StdoutUpdate
	for _, raw := range ch.all() {
		if err := json.Unmarshal(raw, &msg); err == nil && msg.Type == "stdout" {
			break
		}
	}
	require.Equal(t, "stdout", msg.Type)
	assert.Equal(t, "abc", string(msg.Content))
	assert.False(t, msg.Timestamp.IsZero())

	// Verify JSON field names.
	for _, raw := range ch.all() {
		var shape map[string]any
		if json.Unmarshal(raw, &shape) == nil {
			if shape["type"] == "stdout" {
				assert.Contains(t, shape, "content")
				assert.Contains(t, shape, "timestamp")
				break
			}
		}
	}
}

func TestMonitor_MultipleStartStopCycles(t *testing.T) {
	ch := &captureChannel{}
	m := newTestMonitor(ch)

	for i := 0; i < 3; i++ {
		path := writeTempFile(t, "cycle\n")
		m.Started(path)
		time.Sleep(30 * time.Millisecond)
		m.Finished()
		// Re-create the monitor's context for subsequent cycles.
		m = NewMonitor(context.Background(), ch, "test-topic", 10*time.Millisecond, 3600)
	}
	// Should have published at least one stdout message per cycle.
	count := 0
	for _, raw := range ch.all() {
		var msg StdoutUpdate
		if json.Unmarshal(raw, &msg) == nil && msg.Type == "stdout" {
			count++
		}
	}
	assert.GreaterOrEqual(t, count, 3)
}
