package functest_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/storage"
	v100 "github.com/broadinstitute/sparklespray/v100"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

const (
	testProject = "test-project"
	testDB      = "sparkles"
	gcsBucket   = "functest-bucket"
)

// randomSuffix returns an 8-character hex string suitable for unique test IDs.
func randomSuffix() string {
	return uuid.New().String()[:8]
}

// freePort returns an available TCP port on localhost.
func freePort() int {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(fmt.Sprintf("finding free port: %v", err))
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// dialOnce reports whether addr currently accepts TCP connections.
func dialOnce(addr string) bool {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// runEmulator starts cmd with its combined stdout/stderr captured, waits for
// addr to accept connections, and registers a cleanup that kills the
// process. If the process exits before addr comes up, or the timeout
// expires first, it fails the test with the captured output attached so
// CI failures are debuggable without reproducing locally.
func runEmulator(t *testing.T, name string, cmd *exec.Cmd, addr string, timeout time.Duration) {
	t.Helper()
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting %s (%s): %v", name, cmd.Path, err)
	}

	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill() //nolint:errcheck
		}
		<-waitDone
	})

	deadline := time.Now().Add(timeout)
	for {
		if dialOnce(addr) {
			return
		}
		select {
		case err := <-waitDone:
			waitDone <- err // let Cleanup's <-waitDone still observe it
			t.Fatalf("%s (%s) exited before accepting connections on %s: %v\n--- output ---\n%s",
				name, cmd.Path, addr, err, output.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s (%s) to accept connections on %s\n--- output so far ---\n%s",
				name, cmd.Path, addr, output.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// startFirestoreEmulator starts the gcloud Firestore emulator, sets
// FIRESTORE_EMULATOR_HOST, and registers cleanup. Skips the test if gcloud
// is not on PATH.
func startFirestoreEmulator(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("gcloud"); err != nil {
		t.Skip("gcloud not found on PATH; skipping functional test (install Google Cloud SDK)")
	}
	port := freePort()
	addr := fmt.Sprintf("localhost:%d", port)
	cmd := exec.Command("gcloud", "beta", "emulators", "firestore", "start",
		"--host-port="+addr, "--database-mode=firestore-native")
	runEmulator(t, "Firestore emulator", cmd, addr, 30*time.Second)
	t.Setenv("FIRESTORE_EMULATOR_HOST", addr)
	log.Printf("Firestore emulator listening on %s", addr)
}

// startPubSubEmulator starts the gcloud PubSub emulator, sets
// PUBSUB_EMULATOR_HOST, and registers cleanup.
func startPubSubEmulator(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("gcloud"); err != nil {
		t.Skip("gcloud not found on PATH; skipping functional test (install Google Cloud SDK)")
	}
	port := freePort()
	addr := fmt.Sprintf("localhost:%d", port)
	cmd := exec.Command("gcloud", "beta", "emulators", "pubsub", "start",
		"--host-port="+addr, "--project="+testProject)
	runEmulator(t, "PubSub emulator", cmd, addr, 30*time.Second)
	t.Setenv("PUBSUB_EMULATOR_HOST", addr)
	log.Printf("PubSub emulator listening on %s", addr)
}

// startGCSEmulator starts fake-gcs-server, sets GCS_EMULATOR_ENDPOINT, creates
// the test bucket, and registers cleanup. Skips the test if fake-gcs-server is
// not on PATH (install: go install github.com/fsouza/fake-gcs-server@latest).
func startGCSEmulator(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("fake-gcs-server"); err != nil {
		t.Skip("fake-gcs-server not found on PATH; install with: go install github.com/fsouza/fake-gcs-server@latest")
	}
	port := freePort()
	addr := fmt.Sprintf("localhost:%d", port)
	endpoint := fmt.Sprintf("http://%s/storage/v1/", addr)
	cmd := exec.Command("fake-gcs-server",
		"-scheme", "http",
		"-port", strconv.Itoa(port),
		"-backend", "memory",
		"-public-host", addr)
	runEmulator(t, "fake-gcs-server", cmd, addr, 15*time.Second)
	t.Setenv("GCS_EMULATOR_ENDPOINT", endpoint)
	log.Printf("fake-gcs-server listening on %s", addr)

	ctx := context.Background()
	gcsClient, err := storage.NewClient(ctx,
		option.WithEndpoint(endpoint),
		option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("creating GCS client for bucket setup: %v", err)
	}
	defer gcsClient.Close()
	if err := gcsClient.Bucket(gcsBucket).Create(ctx, testProject, nil); err != nil {
		t.Fatalf("creating test bucket %q: %v", gcsBucket, err)
	}
}

func newGCSClient(t *testing.T, ctx context.Context) *storage.Client {
	t.Helper()
	endpoint := os.Getenv("GCS_EMULATOR_ENDPOINT")
	if endpoint == "" {
		t.Fatal("GCS_EMULATOR_ENDPOINT not set; call startGCSEmulator first")
	}
	client, err := storage.NewClient(ctx,
		option.WithEndpoint(endpoint),
		option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("creating GCS client: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// writeGCSObject writes content to an object in the test bucket.
func writeGCSObject(t *testing.T, ctx context.Context, bucket *storage.BucketHandle, name, content string) {
	t.Helper()
	w := bucket.Object(name).NewWriter(ctx)
	if _, err := w.Write([]byte(content)); err != nil {
		t.Fatalf("writing GCS object %s: %v", name, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing GCS object %s: %v", name, err)
	}
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

func newPubSubClient(t *testing.T, ctx context.Context) *pubsub.Client {
	t.Helper()
	client, err := pubsub.NewClient(ctx, testProject)
	if err != nil {
		t.Fatalf("creating PubSub client: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// ensureTopics creates the PubSub topics required by the worker and event
// publisher. Safe to call multiple times (ignores AlreadyExists).
func ensureTopics(t *testing.T, ctx context.Context, psClient *pubsub.Client) {
	t.Helper()
	for _, name := range []string{"sparkles-events", "sparkles-worker-in"} {
		topicName := fmt.Sprintf("projects/%s/topics/%s", testProject, name)
		_, err := psClient.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName})
		if err != nil && grpcstatus.Code(err) != codes.AlreadyExists {
			t.Fatalf("creating PubSub topic %s: %v", name, err)
		}
	}
}

// startWorker launches a worker in a background goroutine using --no-docker
// and --no-gcp. The worker stops when ctx is cancelled.
func startWorker(t *testing.T, ctx context.Context, workpoolID string) {
	t.Helper()
	workerID := "worker-" + uuid.New().String()
	go func() {
		err := v100.RunWorker(ctx, v100.WorkerRunConfig{
			Project:    testProject,
			DB:         testDB,
			WorkerID:   workerID,
			WorkpoolID: workpoolID,
			NoGCP:      true,
			NoDocker:   true,
		})
		if err != nil && ctx.Err() == nil {
			t.Errorf("worker %s exited with unexpected error: %v", workerID, err)
		}
	}()
}

// submitJob writes a job and its tasks to Firestore. Each task gets GCS paths
// under gs://functest-bucket/jobs/<jobID>/. Returns the job ID.
func submitJob(t *testing.T, ctx context.Context, fsClient *firestore.Client, workpoolID string, commands [][]string) string {
	t.Helper()
	jobID := uuid.New().String()
	gcsBase := fmt.Sprintf("gs://%s/jobs/%s", gcsBucket, jobID)

	job := v100.Job{
		JobID:      jobID,
		WorkpoolID: workpoolID,
		Resources:  []v100.ResourceEntry{{Name: "slots", Value: 1}},
	}
	if _, err := fsClient.Collection(v100.JobCollection).Doc(jobID).Set(ctx, job); err != nil {
		t.Fatalf("writing job %s: %v", jobID, err)
	}

	for i, cmd := range commands {
		taskID := uuid.New().String()
		task := v100.Task{
			JobID:       jobID,
			TaskID:      taskID,
			TaskIndex:   i,
			WorkpoolID:  workpoolID,
			Status:      v100.StatusPending,
			Command:     cmd,
			DockerImage: "unused-in-no-docker-mode",
			LogPath:     fmt.Sprintf("%s/logs/%s.log", gcsBase, taskID),
			ResultPath:  fmt.Sprintf("%s/results/%s/", gcsBase, taskID),
		}
		if _, err := fsClient.Collection(v100.TaskCollection).Doc(taskID).Set(ctx, task); err != nil {
			t.Fatalf("writing task %d for job %s: %v", i, jobID, err)
		}
	}
	log.Printf("Submitted job %s with %d task(s) in workpool %s", jobID, len(commands), workpoolID)
	return jobID
}

// waitForAllTasksTerminal polls Firestore until every task in the job has
// reached a terminal state (success/error/failed/killed) or timeout expires.
func waitForAllTasksTerminal(t *testing.T, ctx context.Context, fsClient *firestore.Client, jobID string, timeout time.Duration) map[string]string {
	t.Helper()
	terminalStatuses := map[string]bool{
		v100.StatusSuccess: true,
		v100.StatusError:   true,
		v100.StatusFailed:  true,
		v100.StatusKilled:  true,
	}
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for all tasks of job %s to reach a terminal state", timeout, jobID)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled while waiting for tasks of job %s", jobID)
		case <-time.After(500 * time.Millisecond):
		}
		statuses := taskStatuses(t, ctx, fsClient, jobID)
		allTerminal := len(statuses) > 0
		for _, s := range statuses {
			if !terminalStatuses[s] {
				allTerminal = false
				break
			}
		}
		if allTerminal {
			log.Printf("All %d task(s) of job %s are terminal", len(statuses), jobID)
			return statuses
		}
	}
}

// waitForAnyTaskInStates blocks until at least one task for jobID is in one of
// the given states, or timeout expires.
func waitForAnyTaskInStates(t *testing.T, ctx context.Context, fsClient *firestore.Client, jobID string, states []string, timeout time.Duration) {
	t.Helper()
	stateSet := make(map[string]bool, len(states))
	for _, s := range states {
		stateSet[s] = true
	}
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for any task of job %s to be in states %v", timeout, jobID, states)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled while waiting for tasks of job %s", jobID)
		case <-time.After(250 * time.Millisecond):
		}
		for _, s := range taskStatuses(t, ctx, fsClient, jobID) {
			if stateSet[s] {
				return
			}
		}
	}
}

func taskStatuses(t *testing.T, ctx context.Context, fsClient *firestore.Client, jobID string) map[string]string {
	t.Helper()
	iter := fsClient.Collection(v100.TaskCollection).
		Where("job_id", "==", jobID).
		Documents(ctx)
	defer iter.Stop()
	result := make(map[string]string)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("querying tasks for job %s: %v", jobID, err)
		}
		var task v100.Task
		if err := doc.DataTo(&task); err != nil {
			t.Fatalf("decoding task document: %v", err)
		}
		result[task.TaskID] = task.Status
	}
	return result
}
