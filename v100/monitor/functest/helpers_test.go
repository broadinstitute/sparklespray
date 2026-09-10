package functest_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os/exec"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	monitor "github.com/broadinstitute/sparklespray/v100/monitor"
	"github.com/broadinstitute/sparklespray/v100/scheduler"
	"github.com/google/uuid"
)

const (
	testProject = "test-project"
	testDB      = "sparkles"
)

// ----- Emulator lifecycle -----

func freePort() int {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(fmt.Sprintf("finding free port: %v", err))
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitForPort(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s", addr)
}

func startFirestoreEmulator(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("gcloud"); err != nil {
		t.Skip("gcloud not found on PATH; skipping functional test")
	}
	port := freePort()
	addr := fmt.Sprintf("localhost:%d", port)
	cmd := exec.Command("gcloud", "beta", "emulators", "firestore", "start",
		"--host-port="+addr, "--database-mode=firestore-native")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting Firestore emulator: %v", err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() }) //nolint:errcheck
	if err := waitForPort(addr, 30*time.Second); err != nil {
		t.Fatalf("Firestore emulator did not start: %v", err)
	}
	t.Setenv("FIRESTORE_EMULATOR_HOST", addr)
	log.Printf("Firestore emulator on %s", addr)
}

func newFirestoreClient(t *testing.T, ctx context.Context) *firestore.Client {
	t.Helper()
	client, err := firestore.NewClientWithDatabase(ctx, testProject, testDB)
	if err != nil {
		t.Fatalf("creating Firestore client: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// ----- Stub types -----

// noopBatchAPIClient satisfies BatchAPIClient with all operations returning nil.
type noopBatchAPIClient struct{}

func (n *noopBatchAPIClient) CreateJob(_ context.Context, _ *monitor.WorkerJobSpec) (string, error) {
	return "", nil
}
func (n *noopBatchAPIClient) GetJobStatus(_ context.Context, _ string) (monitor.BatchJobStatus, error) {
	return "", nil
}
func (n *noopBatchAPIClient) ListRunningVMs(_ context.Context, _, _, _ string, _ []string) (map[string]monitor.VMInfo, error) {
	return nil, nil
}
func (n *noopBatchAPIClient) TerminateVM(_ context.Context, _, _, _ string) error            { return nil }
func (n *noopBatchAPIClient) TerminateJob(_ context.Context, _ string) error                 { return nil }
func (n *noopBatchAPIClient) PrintBatchDebuggingInfo(_ context.Context, _, _ string) error   { return nil }

// noopPubSubReceiver returns a channel that never fires.
type noopPubSubReceiver struct {
	ch chan monitor.Notification
}

func newNoopPubSubReceiver() *noopPubSubReceiver {
	return &noopPubSubReceiver{ch: make(chan monitor.Notification)}
}

func (n *noopPubSubReceiver) Notifications() <-chan monitor.Notification { return n.ch }

// capturingJobTerminatedPublisher records all job_terminated events for assertions.
type capturingJobTerminatedPublisher struct {
	mu     sync.Mutex
	events []string // jobIDs in order received
}

func (c *capturingJobTerminatedPublisher) PublishJobTerminated(_ context.Context, jobID, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, jobID)
	return nil
}

func (c *capturingJobTerminatedPublisher) Events() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([]string, len(c.events))
	copy(cp, c.events)
	return cp
}

// ----- Monitor construction -----

// newMonitorWithStores constructs a Monitor backed by the provided Firestore stores
// and a fake clock set to the given time. Uses noop BatchAPI and PubSub.
func newMonitorWithStores(
	clock scheduler.Clock,
	pools monitor.WorkPoolStore,
	batches monitor.BatchRequestStore,
	workers monitor.WorkerStore,
	tasks monitor.TaskStore,
) (*monitor.Monitor, *capturingJobTerminatedPublisher) {
	pub := &capturingJobTerminatedPublisher{}
	m := monitor.New(clock, &noopBatchAPIClient{}, pools, batches, workers, tasks, newNoopPubSubReceiver(), "test-db")
	m.SetJobTerminatedPublisher(pub)
	return m, pub
}

// ----- Firestore document write helpers -----
// These mirror the unexported structs in monitor/adapters.go.

// fsWorkPoolDoc mirrors the immutable firestoreWorkPool struct (config only; no state fields).
type fsWorkPoolDoc struct {
	WorkpoolID  string `firestore:"workpool_id"`
	MachineType string `firestore:"machine_type"`
	Region      string `firestore:"region"`
}

type fsWorkerDoc struct {
	WorkerID        string    `firestore:"worker_id"`
	WorkpoolID      string    `firestore:"workpool_id"`
	BatchID         string    `firestore:"batch_id"`
	InstanceName    string    `firestore:"instance_name"`
	Status          string    `firestore:"status"`
	HeartbeatExpiry time.Time `firestore:"heartbeat_expiry"`
}

// fsTaskDoc includes job_id which the monitor's firestoreTask subset omits but
// CountByJob queries on.
type fsTaskDoc struct {
	TaskID         string `firestore:"task_id"`
	JobID          string `firestore:"job_id"`
	WorkpoolID     string `firestore:"workpool_id"`
	Status         string `firestore:"status"`
	OwningWorkerID string `firestore:"owning_worker_id"`
}

func writeWorkPool(t *testing.T, ctx context.Context, fs *firestore.Client, doc *fsWorkPoolDoc) {
	t.Helper()
	if _, err := fs.Collection(monitor.CollectionWorkPools).Doc(doc.WorkpoolID).Set(ctx, doc); err != nil {
		t.Fatalf("writing workpool %s: %v", doc.WorkpoolID, err)
	}
}

func writeWorker(t *testing.T, ctx context.Context, fs *firestore.Client, doc *fsWorkerDoc) {
	t.Helper()
	if _, err := fs.Collection(monitor.CollectionWorkers).Doc(doc.WorkerID).Set(ctx, doc); err != nil {
		t.Fatalf("writing worker %s: %v", doc.WorkerID, err)
	}
}

func writeTask(t *testing.T, ctx context.Context, fs *firestore.Client, doc *fsTaskDoc) {
	t.Helper()
	if _, err := fs.Collection(monitor.CollectionTasks).Doc(doc.TaskID).Set(ctx, doc); err != nil {
		t.Fatalf("writing task %s: %v", doc.TaskID, err)
	}
}

// fsWorkpoolIncidentDoc mirrors the subset of firestoreEventRecord relevant to
// workpool_incident events.
type fsWorkpoolIncidentDoc struct {
	Type         string    `firestore:"type"`
	Timestamp    time.Time `firestore:"timestamp"`
	WorkpoolID   string    `firestore:"workpool_id"`
	StateMessage string    `firestore:"state_message"`
	IncidentType string    `firestore:"incident_type"`
}

func writeWorkpoolIncident(t *testing.T, ctx context.Context, fs *firestore.Client, doc *fsWorkpoolIncidentDoc) {
	t.Helper()
	doc.Type = "workpool_incident"
	if _, err := fs.Collection(monitor.CollectionEvents).Doc(randomID()).Set(ctx, doc); err != nil {
		t.Fatalf("writing workpool_incident event for %s: %v", doc.WorkpoolID, err)
	}
}

func randomID() string { return uuid.New().String()[:8] }
