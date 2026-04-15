package monitor

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- fake MessageBus --------------------------------------------------------

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
	return NewMonitor(context.Background(), ch, "test-topic", 10*time.Millisecond, 10*time.Millisecond)
}

// writeTempFile creates a temp file, writes content, and closes it so the
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

// logStreamMessages returns all LogStreamUpdate messages captured by ch.
func logStreamMessages(ch *captureChannel) []backend.LogStreamUpdate {
	var out []backend.LogStreamUpdate
	for _, raw := range ch.all() {
		var msg backend.LogStreamUpdate
		if err := json.Unmarshal(raw, &msg); err == nil && msg.Type == backend.RespLogStreamUpdate {
			out = append(out, msg)
		}
	}
	return out
}

// ---- tests ------------------------------------------------------------------

// TestMonitor_NoPublishWithoutListening verifies that the monitor does not
// publish any stdout messages when StartListeningIfTaskID has not been called.
func TestMonitor_NoPublishWithoutListening(t *testing.T) {
	ch := &captureChannel{}
	m := newTestMonitor(ch)

	path := writeTempFile(t, "hello\n")
	m.Started("task-1", path)
	time.Sleep(50 * time.Millisecond)
	m.Finished()

	assert.Empty(t, logStreamMessages(ch), "should not publish without StartListeningIfTaskID")
}

// TestMonitor_PublishesStdoutWhenListening verifies that stdout content is
// published as LogStreamUpdate messages once StartListeningIfTaskID is called
// with the matching task ID.
func TestMonitor_PublishesStdoutWhenListening(t *testing.T) {
	ch := &captureChannel{}
	m := newTestMonitor(ch)

	path := writeTempFile(t, "hello from task\n")
	m.Started("task-1", path)
	require.NoError(t, m.StartListeningIfTaskID("task-1"))
	time.Sleep(50 * time.Millisecond)
	m.Finished()

	msgs := logStreamMessages(ch)
	require.NotEmpty(t, msgs, "expected at least one LogStreamUpdate")

	var found bool
	for _, msg := range msgs {
		if msg.Content == "hello from task\n" {
			found = true
			assert.Equal(t, "task-1", msg.TaskID)
			assert.Equal(t, backend.RespLogStreamUpdate, msg.Type)
			assert.False(t, msg.Timestamp.IsZero())
		}
	}
	assert.True(t, found, "expected a LogStreamUpdate containing the written content")
}

// TestMonitor_WrongTaskIDDoesNotPublish verifies that StartListeningIfTaskID
// with a non-matching task ID does not enable publishing.
func TestMonitor_WrongTaskIDDoesNotPublish(t *testing.T) {
	ch := &captureChannel{}
	m := newTestMonitor(ch)

	path := writeTempFile(t, "hello\n")
	m.Started("task-1", path)
	require.NoError(t, m.StartListeningIfTaskID("task-other"))
	time.Sleep(50 * time.Millisecond)
	m.Finished()

	assert.Empty(t, logStreamMessages(ch), "should not publish when task ID does not match")
}

// TestMonitor_StopListeningIfTaskID verifies that StopListeningIfTaskID
// disables publishing for the matching task.
func TestMonitor_StopListeningIfTaskID(t *testing.T) {
	ch := &captureChannel{}
	// Use a very long poll interval so we control exactly when publishing happens.
	m := NewMonitor(context.Background(), ch, "test-topic", 10*time.Second, 10*time.Second)

	path := writeTempFile(t, "before stop\n")
	m.Started("task-1", path)
	require.NoError(t, m.StartListeningIfTaskID("task-1"))
	m.StopListeningIfTaskID("task-1")

	// The final poll in Finished runs after cancelPolling, but isPublishing is
	// now false so nothing should be sent.
	m.Finished()

	assert.Empty(t, logStreamMessages(ch), "should not publish after StopListeningIfTaskID")
}

// TestMonitor_FinalPollOnFinished verifies that the stdout goroutine performs
// one final read after context cancellation, capturing output written just
// before Finished was called.
func TestMonitor_FinalPollOnFinished(t *testing.T) {
	ch := &captureChannel{}
	// Use a very long poll freq so the periodic ticker never fires — only the
	// post-cancel final poll should capture the content.
	m := NewMonitor(context.Background(), ch, "test-topic", 10*time.Second, 10*time.Second)

	f, err := os.CreateTemp(t.TempDir(), "stdout-")
	require.NoError(t, err)

	m.Started("task-final", f.Name())
	require.NoError(t, m.StartListeningIfTaskID("task-final"))

	_, err = f.WriteString("late output\n")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	m.Finished()

	var found bool
	for _, msg := range logStreamMessages(ch) {
		if msg.Content == "late output\n" {
			found = true
			break
		}
	}
	assert.True(t, found, "Finished should trigger a final poll capturing late output")
}

// TestMonitor_StartedWithMissingFile verifies that Started does not panic when
// the stdout file does not exist — it logs and returns without starting
// goroutines or setting cancelPolling. Finished handles the nil cancelPolling
// gracefully via its nil guard.
func TestMonitor_StartedWithMissingFile(t *testing.T) {
	ch := &captureChannel{}
	m := newTestMonitor(ch)
	assert.NotPanics(t, func() {
		m.Started("task-1", "/nonexistent/path/stdout.txt")
		m.Finished()
	})
}

// TestMonitor_LogStreamUpdateJSONShape verifies the wire format field names of
// a published LogStreamUpdate.
func TestMonitor_LogStreamUpdateJSONShape(t *testing.T) {
	ch := &captureChannel{}
	m := newTestMonitor(ch)

	path := writeTempFile(t, "abc")
	m.Started("task-shape", path)
	require.NoError(t, m.StartListeningIfTaskID("task-shape"))
	time.Sleep(50 * time.Millisecond)
	m.Finished()

	var shape map[string]any
	for _, raw := range ch.all() {
		if json.Unmarshal(raw, &shape) == nil && shape["type"] == backend.RespLogStreamUpdate {
			break
		}
	}
	require.Equal(t, backend.RespLogStreamUpdate, shape["type"])
	assert.Contains(t, shape, "task_id")
	assert.Contains(t, shape, "content")
	assert.Contains(t, shape, "timestamp")
}

// TestMonitor_MultipleStartStopCycles verifies that separate Monitor instances
// work correctly across multiple task cycles, each publishing messages tagged
// with the correct task ID.
func TestMonitor_MultipleStartStopCycles(t *testing.T) {
	taskIDs := []string{"task-a", "task-b", "task-c"}
	ch := &captureChannel{}

	for _, taskID := range taskIDs {
		m := newTestMonitor(ch)
		path := writeTempFile(t, "output for "+taskID+"\n")
		m.Started(taskID, path)
		require.NoError(t, m.StartListeningIfTaskID(taskID))
		time.Sleep(30 * time.Millisecond)
		m.Finished()
	}

	msgs := logStreamMessages(ch)
	assert.GreaterOrEqual(t, len(msgs), len(taskIDs), "expected at least one message per cycle")

	seen := map[string]bool{}
	for _, msg := range msgs {
		seen[msg.TaskID] = true
	}
	for _, taskID := range taskIDs {
		assert.True(t, seen[taskID], "expected messages for task %s", taskID)
	}
}
