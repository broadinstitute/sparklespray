package v100

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNextMetricsDelay(t *testing.T) {
	// The adaptive ladder: fine-grained early so short tasks get real
	// resolution, flattening out so long tasks cost little.
	want := []time.Duration{
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		32 * time.Second,
		60 * time.Second,
		60 * time.Second,
		60 * time.Second,
	}
	got := MetricsFirstDelay
	for i, w := range want {
		got = nextMetricsDelay(got)
		if got != w {
			t.Fatalf("step %d: got %s, want %s", i, got, w)
		}
	}
}

// openTestEventLog opens a TaskEventLog in a temp dir with no Firestore
// client, so metrics land in the local events file.
func openTestEventLog(t *testing.T) (*TaskEventLog, string) {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "task.log")
	tel, err := OpenTaskEventLog(context.Background(), logPath, "task-1", t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenTaskEventLog: %v", err)
	}
	return tel, logPath + ".events.log"
}

// readSamples parses the metric samples written to an events file.
func readSamples(t *testing.T, path string) []MetricSample {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening events file: %v", err)
	}
	defer f.Close()

	var out []MetricSample
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var s MetricSample
		if err := json.Unmarshal(line, &s); err != nil {
			t.Fatalf("unmarshalling %q: %v", line, err)
		}
		if s.Type == MetricUpdateEventType {
			out = append(out, s)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanning events file: %v", err)
	}
	return out
}

func TestTriggerFinalSampleBeforeAnyTick(t *testing.T) {
	// The whole point of the redesign: a task that finishes before the first
	// periodic sample still produces exactly one sample. Under the old fixed
	// one-minute ticker this produced nothing at all.
	tel, eventsPath := openTestEventLog(t)

	final := tel.TriggerFinalSample()
	if final == nil {
		t.Fatal("TriggerFinalSample returned nil")
	}
	if err := tel.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	samples := readSamples(t, eventsPath)
	if len(samples) != 1 {
		t.Fatalf("got %d samples, want exactly 1", len(samples))
	}
	s := samples[0]
	if !s.Final {
		t.Error("sample is not flagged final")
	}
	if s.Seq != 0 {
		t.Errorf("Seq = %d, want 0", s.Seq)
	}
	if s.Type != MetricUpdateEventType {
		t.Errorf("Type = %q, want %q", s.Type, MetricUpdateEventType)
	}
	if s.MetricSchema != MetricSchemaCurrent {
		t.Errorf("MetricSchema = %d, want %d", s.MetricSchema, MetricSchemaCurrent)
	}
	// expiry is what lets the expiry cleaner garbage-collect TaskLog; without
	// it metric documents would accumulate forever.
	if wantExpiry := s.Timestamp.Add(taskEventLogTTL); !s.Expiry.Equal(wantExpiry) {
		t.Errorf("Expiry = %s, want %s", s.Expiry, wantExpiry)
	}
}

func TestTriggerFinalSampleIsIdempotent(t *testing.T) {
	tel, eventsPath := openTestEventLog(t)

	first := tel.TriggerFinalSample()
	second := tel.TriggerFinalSample()
	if first != second {
		t.Error("repeated TriggerFinalSample returned different samples")
	}
	if err := tel.Close(); err != nil {
		t.Fatalf("Close after TriggerFinalSample: %v", err)
	}
	if n := len(readSamples(t, eventsPath)); n != 1 {
		t.Errorf("got %d samples, want 1 (second trigger must not sample again)", n)
	}
}

func TestCloseWithoutFinalSample(t *testing.T) {
	// The executeCommandDirect shape: no container, no final sample, just a
	// clean shutdown. Must not hang.
	tel, _ := openTestEventLog(t)
	done := make(chan error, 1)
	go func() { done <- tel.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung")
	}
}

func TestSampleWithoutContainerIsUnavailable(t *testing.T) {
	// With no container registered, container metrics must read as
	// unavailable rather than as a container that consumed nothing.
	tel, _ := openTestEventLog(t)
	s := tel.TriggerFinalSample()
	if err := tel.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	checks := map[string]*int64{
		"ContainerMemoryCurrentBytes":  s.ContainerMemoryCurrentBytes,
		"ContainerMemoryPeakBytes":     s.ContainerMemoryPeakBytes,
		"ContainerCPUUsageUSec":        s.ContainerCPUUsageUSec,
		"ContainerCPUUserUSec":         s.ContainerCPUUserUSec,
		"ContainerCPUSystemUSec":       s.ContainerCPUSystemUSec,
		"ContainerCPUStallSomeUSec":    s.ContainerCPUStallSomeUSec,
		"ContainerMemoryStallSomeUSec": s.ContainerMemoryStallSomeUSec,
		"ContainerIOStallSomeUSec":     s.ContainerIOStallSomeUSec,
		"ContainerIOReadBytes":         s.ContainerIOReadBytes,
		"ContainerIOWriteOps":          s.ContainerIOWriteOps,
		"ContainerPidsPeak":            s.ContainerPidsPeak,
	}
	for name, got := range checks {
		if got != nil {
			t.Errorf("%s = %d, want nil (unavailable)", name, *got)
		}
	}
}

func TestAdaptiveSamplingProducesEarlySamples(t *testing.T) {
	// A task lasting a couple of seconds must produce several samples. The
	// old one-minute ticker produced none.
	tel, eventsPath := openTestEventLog(t)
	time.Sleep(3500 * time.Millisecond) // expect samples at 1s, 3s (1+2)
	tel.TriggerFinalSample()
	if err := tel.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	samples := readSamples(t, eventsPath)
	if len(samples) < 3 {
		t.Fatalf("got %d samples in 3.5s, want at least 3 (two periodic plus the final)", len(samples))
	}

	// Sequence numbers must be gap-free so a consumer can detect loss.
	for i, s := range samples {
		if s.Seq != int32(i) {
			t.Errorf("sample %d has Seq %d, want %d", i, s.Seq, i)
		}
	}
	if last := samples[len(samples)-1]; !last.Final {
		t.Error("last sample is not flagged final")
	}
	for _, s := range samples[:len(samples)-1] {
		if s.Final {
			t.Errorf("periodic sample seq=%d is wrongly flagged final", s.Seq)
		}
	}
}
