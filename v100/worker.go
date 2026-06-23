package v100

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/urfave/cli"
	"google.golang.org/api/option"
)

const heartbeatPeriod = 1 * time.Minute

const workerOutTopic = "sparkles-events"
const workerInTopic = "sparkles-worker-in"
const workerCollection = "Workers"

type WorkerRecord struct {
	WorkerID        string    `firestore:"worker_id"`
	WorkpoolID      string    `firestore:"workpool_id"`
	BatchID         string    `firestore:"batch_id"`
	InstanceName    string    `firestore:"instance_name"`
	Status          string    `firestore:"status"`
	Expiry          time.Time `firestore:"expiry"`
	HeartbeatExpiry time.Time `firestore:"heartbeat_expiry"`
}

func runHeartbeat(ctx context.Context, doc *firestore.DocumentRef) {
	ticker := time.NewTicker(heartbeatPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := doc.Update(ctx, []firestore.Update{
				{Path: "heartbeat_expiry", Value: time.Now().Add(heartbeatPeriod)},
			})
			if err != nil {
				log.Printf("heartbeat update failed: %v", err)
			} else {
				log.Printf("heartbeat updated")
			}
		}
	}
}

func parseResources(s string) (*Resources, error) {
	r := NewResources()
	if s == "" {
		s = "slots=1"
	}
	for part := range strings.SplitSeq(s, ",") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid resource %q: expected name=value", part)
		}
		v, err := strconv.ParseFloat(kv[1], 64)
		if err != nil {
			return nil, fmt.Errorf("invalid resource value %q: %w", kv[1], err)
		}
		r.Set(kv[0], v)
	}
	return &r, nil
}

func runWorker(c *cli.Context) error {
	project := c.String("project")
	db := c.String("db")
	workpoolID := c.String("workpool")
	noGCP := c.Bool("no-gcp")
	noDocker := c.Bool("no-docker")

	if project == "" {
		return fmt.Errorf("--project is required")
	}
	if workpoolID == "" {
		return fmt.Errorf("--workpool is required")
	}

	resources, err := parseResources(c.String("resources"))
	if err != nil {
		return fmt.Errorf("--resources: %w", err)
	}

	workerID := uuid.New().String()
	log.Printf("Starting worker %s in workpool %s", workerID, workpoolID)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	ws, err := startWorker(ctx, project, db, workerID, workpoolID, noGCP, noDocker)
	if err != nil {
		return err
	}
	defer ws.cleanup()

	// Main poll loop
	if err := ws.mainLoop(ctx, resources); err != nil {
		panic(fmt.Sprintf("Internal error in mainLoop: %v", err))
	}

	log.Printf("Shutting down worker %s", workerID)
	cancel()

	ws.shutdown()

	log.Printf("Worker %s stopped", ws.workerID)
	return nil
}

type taskCompletion struct {
	taskID    string
	resources Resources
	err       error
}

func executeTask(task *Task, resources Resources, completions chan<- taskCompletion, executeTaskCallback func(task *Task) error) {
	log.Printf("Stub: executing: %v", task)
	go func() {
		err := executeTaskCallback(task)
		completions <- taskCompletion{taskID: task.TaskID, resources: resources, err: err}
	}()
}

func executeDockerCommand(ctx context.Context, imageName string, command []string, workDir string, extraDockerArgs []string, tel *TaskEventLog) error {
	args := append([]string{"run", "--rm", "-w", workDir}, extraDockerArgs...)
	args = append(args, imageName)
	args = append(args, command...)

	cmd := exec.CommandContext(ctx, "docker", args...)

	pr, pw := io.Pipe()
	cmd.Stdout = pw
	cmd.Stderr = pw

	pipeErrCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 4096*10)
		for {
			n, err := pr.Read(buf)
			if n > 0 {
				if writeErr := tel.WriteOutput(string(buf[:n])); writeErr != nil {
					pr.CloseWithError(writeErr)
					pipeErrCh <- writeErr
					return
				}
			}
			if err == io.EOF {
				pipeErrCh <- nil
				return
			}
			if err != nil {
				pipeErrCh <- err
				return
			}
		}
	}()

	runErr := cmd.Run()
	pw.Close()
	pipeErr := <-pipeErrCh

	if runErr != nil {
		return fmt.Errorf("could not run docker command (%s): %w", strings.Join(args, " "), runErr)
	}
	return pipeErr
}

type WorkerLoopConfig struct {
	WorkpoolID           string
	WorkerID             string
	Queue                TaskQueue
	Resources            *Resources
	TransferClient       TransferClient
	WorkDirParent        string
	BindMounts           []string
	Registry             *taskRegistry
	FSClient             *firestore.Client
	ExecuteDockerCommand func(ctx context.Context, imageName string, command []string, workDir string, extraDockerArgs []string, tel *TaskEventLog) error
}

// ErrTaskKilled is returned by the task callback when a task was cancelled via a kill_job message.
var ErrTaskKilled = errors.New("task killed")

func workerMainLoop(ctx context.Context, cfg *WorkerLoopConfig) error {
	completions := make(chan taskCompletion, 100)
	runningCount := 0
	curResources := cfg.Resources

	// curResources is the single-goroutine mutable running total of available capacity.
	// Only waitForCompletion and the claim path touch it, both on this goroutine.
	waitForCompletion := func() error {
		select {
		case c := <-completions:
			curResources = curResources.Add(c.resources)
			runningCount--
			if errors.Is(c.err, ErrTaskKilled) {
				if err := cfg.Queue.RecordKilled(ctx, c.taskID, false); err != nil {
					log.Printf("recording task %s as killed: %v", c.taskID, err)
					return err
				}
			} else if c.err != nil {
				log.Printf("task %s failed: %v", c.taskID, c.err)
				var exitErr *exec.ExitError
				if errors.As(c.err, &exitErr) {
					if err := cfg.Queue.RecordError(ctx, c.taskID, exitErr.ExitCode()); err != nil {
						log.Printf("recording task %s error (exit %d): %v", c.taskID, exitErr.ExitCode(), err)
						return err
					}
				} else if err := cfg.Queue.RecordFailed(ctx, c.taskID, c.err.Error(), StatusClaimed); err != nil {
					log.Printf("recording task %s as failed: %v", c.taskID, err)
					return err
				}
			} else {
				if err := cfg.Queue.UpdateState(ctx, c.taskID, StatusWriting, StatusSuccess); err != nil {
					log.Printf("recording task %s as success: %v", c.taskID, err)
					return err
				}
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// loop until we're out of jobs with tasks
	for {
		// first, find a job which has at least one task
		pendingTask, err := cfg.Queue.GetFirstPendingTask(ctx, cfg.WorkpoolID)
		if err != nil {
			return fmt.Errorf("getting first pending task: %w", err)
		}
		if pendingTask == nil {
			break
		}

		job, err := cfg.Queue.GetJob(ctx, pendingTask.JobID)
		if err != nil {
			return fmt.Errorf("getting job %s: %w", pendingTask.JobID, err)
		}

		jobResources := NewResources()
		for _, entry := range job.Resources {
			jobResources.Set(entry.Name, entry.Value)
		}

		// check to make sure that this job can run at least one task if we used our full allocation of resources
		if !cfg.Resources.Sub(jobResources).IsValid() {
			log.Printf("Job %s requires more resources than this worker can provide; failing all pending tasks", job.JobID)
			for {
				task, err := cfg.Queue.ClaimTask(ctx, job.JobID, cfg.WorkerID)
				if err != nil {
					return fmt.Errorf("claiming task to fail for job %s: %w", job.JobID, err)
				}
				if task == nil {
					break
				}
				if err := cfg.Queue.RecordFailed(ctx, task.TaskID, "Task requires more resources than allowed by worker pool", StatusClaimed); err != nil {
					return fmt.Errorf("recording task %s as failed: %w", task.TaskID, err)
				}
			}
			// upon reaching here, all the tasks associated with the jobID are no longer marked pending,
			// so when we call queue.GetFirstPendingTask, we're guarenteed to get a different job.
			continue
		}

		for {
			// drain any completions that have already arrived
			for len(completions) > 0 {
				if err := waitForCompletion(); err != nil {
					return err
				}
			}

			remaining := curResources.Sub(jobResources)
			if !remaining.IsValid() {
				// wait for a running task to free up resources
				if err := waitForCompletion(); err != nil {
					return err
				}
				continue
			}

			task, err := cfg.Queue.ClaimTask(ctx, job.JobID, cfg.WorkerID)
			if err != nil {
				return fmt.Errorf("claiming task: %w", err)
			}
			if task == nil {
				// job's tasks were exhausted, so we're done with this job
				break
			}

			curResources = remaining
			runningCount++
			executeTask(task, jobResources, completions, func(t *Task) error {
				taskCtx, cancel := context.WithCancel(ctx)
				defer cancel()

				files, err := resolveFilesToLocalize(taskCtx, cfg.TransferClient, t)
				if err != nil {
					return fmt.Errorf("resolving files to localize: %w", err)
				}
				paths, err := prepareWorkDir(taskCtx, cfg.TransferClient, cfg.WorkDirParent, files)
				if err != nil {
					return err
				}

				extraDockerArgs := buildDockerArgs(cfg.BindMounts, t)
				tel, err := OpenTaskEventLog(ctx, paths.logPath, t.TaskID, paths.taskWorkDir, cfg.FSClient)
				if err != nil {
					return fmt.Errorf("opening task event log for %s: %w", t.TaskID, err)
				}
				cfg.Registry.register(t.TaskID, t.JobID, tel, cancel)
				defer cfg.Registry.unregister(t.TaskID)
				if err := cfg.Queue.UpdateState(ctx, t.TaskID, StatusClaimed, StatusRunning); err != nil {
					tel.Close()
					return fmt.Errorf("marking task %s running: %w", t.TaskID, err)
				}
				dockerExecErr := cfg.ExecuteDockerCommand(taskCtx, t.DockerImage, t.Command, paths.taskWorkDir, extraDockerArgs, tel)
				killed := cfg.Registry.wasKilled(t.TaskID)
				flushErr := tel.Flush()
				var uploadResultsErr error
				if !killed {
					if err := cfg.Queue.UpdateState(ctx, t.TaskID, StatusRunning, StatusWriting); err != nil {
						tel.Close()
						return fmt.Errorf("marking task %s writing: %w", t.TaskID, err)
					}
					uploadResultsErr = uploadResults(ctx, cfg.TransferClient, paths, t.ResultPath, t.LogPath)
				}
				closeErr := tel.Close()
				cleanupErr := cleanupWorkDir(paths)
				if killed {
					_ = mergeErrors(flushErr, closeErr, cleanupErr)
					return ErrTaskKilled
				}
				return mergeErrors(dockerExecErr, flushErr, uploadResultsErr, closeErr, cleanupErr)
			})
		}
	}

	// drain all remaining running tasks
	for runningCount > 0 {
		if err := waitForCompletion(); err != nil {
			return err
		}
	}

	return nil
}

func (ws *workerState) mainLoop(ctx context.Context, resources *Resources) error {
	execFn := executeDockerCommand
	if ws.noDocker {
		execFn = executeCommandDirect
	}
	queue := NewFirestoreTaskQueue(ws.fsClient, ws.publisher)
	return workerMainLoop(ctx, &WorkerLoopConfig{
		WorkpoolID:           ws.workpoolID,
		WorkerID:             ws.workerID,
		Queue:                queue,
		Resources:            resources,
		TransferClient:       ws.transferClient,
		WorkDirParent:        ws.workDirParent,
		BindMounts:           ws.bindMounts,
		Registry:             ws.registry,
		FSClient:             ws.fsClient,
		ExecuteDockerCommand: execFn,
	})
}

// executeCommandDirect runs a command directly without Docker.
// The image name and extra docker args are ignored.
func executeCommandDirect(ctx context.Context, _ string, command []string, workDir string, _ []string, tel *TaskEventLog) error {
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	cmd.Dir = workDir

	pr, pw := io.Pipe()
	cmd.Stdout = pw
	cmd.Stderr = pw

	pipeErrCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 4096*10)
		for {
			n, err := pr.Read(buf)
			if n > 0 {
				if writeErr := tel.WriteOutput(string(buf[:n])); writeErr != nil {
					pr.CloseWithError(writeErr)
					pipeErrCh <- writeErr
					return
				}
			}
			if err == io.EOF {
				pipeErrCh <- nil
				return
			}
			if err != nil {
				pipeErrCh <- err
				return
			}
		}
	}()

	runErr := cmd.Run()
	pw.Close()
	pipeErr := <-pipeErrCh

	if runErr != nil {
		return fmt.Errorf("could not run command (%s): %w", strings.Join(command, " "), runErr)
	}
	return pipeErr
}

// WorkerRunConfig holds all parameters for running a worker programmatically.
type WorkerRunConfig struct {
	Project    string
	DB         string
	WorkerID   string
	WorkpoolID string
	// Resources is a comma-separated list of name=value pairs (e.g. "slots=1").
	// Defaults to "slots=1" if empty.
	Resources string
	NoGCP     bool
	NoDocker  bool
}

// RunWorker starts a worker and blocks until ctx is cancelled or all pending
// tasks in the workpool are complete. Intended for use by functional tests and
// integration tooling.
func RunWorker(ctx context.Context, cfg WorkerRunConfig) error {
	resources, err := parseResources(cfg.Resources)
	if err != nil {
		return fmt.Errorf("parsing resources: %w", err)
	}

	log.Printf("Starting worker %s in workpool %s", cfg.WorkerID, cfg.WorkpoolID)
	ws, err := startWorker(ctx, cfg.Project, cfg.DB, cfg.WorkerID, cfg.WorkpoolID, cfg.NoGCP, cfg.NoDocker)
	if err != nil {
		return fmt.Errorf("starting worker: %w", err)
	}
	defer ws.cleanup()

	mainLoopErr := ws.mainLoop(ctx, resources)
	ws.shutdown()

	if mainLoopErr != nil && ctx.Err() == nil {
		return fmt.Errorf("worker main loop: %w", mainLoopErr)
	}
	return nil
}

type TaskPaths struct {
	workDir        string
	taskWorkDir    string
	logPath        string
	localizedFiles []string
}

func uploadResults(ctx context.Context, tc TransferClient, paths *TaskPaths, resultPath, logPath string) error {
	if err := tc.uploadFile(ctx, paths.logPath, logPath); err != nil {
		return fmt.Errorf("uploading log: %w", err)
	}

	localized := make(map[string]struct{}, len(paths.localizedFiles))
	for _, p := range paths.localizedFiles {
		localized[p] = struct{}{}
	}

	return filepath.WalkDir(paths.taskWorkDir, func(localPath string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if _, skip := localized[localPath]; skip {
			return nil
		}
		relPath, err := filepath.Rel(paths.taskWorkDir, localPath)
		if err != nil {
			return err
		}
		destGCSPath := strings.TrimSuffix(resultPath, "/") + "/" + filepath.ToSlash(relPath)
		return tc.uploadFile(ctx, localPath, destGCSPath)
	})
}

func mergeErrors(errs ...error) error {
	var nonNil []error
	for _, err := range errs {
		if err != nil {
			nonNil = append(nonNil, err)
		}
	}
	if len(nonNil) == 0 {
		return nil
	}
	if len(nonNil) == 1 {
		return nonNil[0]
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%d error(s) occurred:\n", len(nonNil))
	for i, err := range nonNil {
		fmt.Fprintf(&b, "%d. %s\n", i+1, err.Error())
	}
	return fmt.Errorf("%s", b.String())
}

func resolveFilesToLocalize(ctx context.Context, tc TransferClient, t *Task) ([]FileToLocalize, error) {
	if t.FilesToLocalizeManifest == "" {
		return t.FilesToLocalize, nil
	}

	tmp, err := os.CreateTemp("", "manifest-*.json")
	if err != nil {
		return nil, fmt.Errorf("creating temp file for manifest: %w", err)
	}
	tmpPath := tmp.Name()
	tmp.Close()
	defer os.Remove(tmpPath)

	if err := tc.downloadFile(ctx, t.FilesToLocalizeManifest, tmpPath); err != nil {
		return nil, fmt.Errorf("downloading files_to_localize manifest %s: %w", t.FilesToLocalizeManifest, err)
	}

	data, err := os.ReadFile(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("reading manifest: %w", err)
	}

	var manifest []FileToLocalize
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parsing manifest: %w", err)
	}

	return append(t.FilesToLocalize, manifest...), nil
}

func prepareWorkDir(ctx context.Context, tc TransferClient, workDirParent string, filesToLocalize []FileToLocalize) (*TaskPaths, error) {
	workDir, err := os.MkdirTemp(workDirParent, "task-*")
	if err != nil {
		return nil, fmt.Errorf("creating task dir: %w", err)
	}

	taskWorkDir := filepath.Join(workDir, "work")
	if err := os.Mkdir(taskWorkDir, 0755); err != nil {
		os.RemoveAll(workDir)
		return nil, fmt.Errorf("creating working dir: %w", err)
	}

	logPath := filepath.Join(workDir, "output.log")

	var localizedFiles []string
	for _, f := range filesToLocalize {
		destPath := filepath.Join(taskWorkDir, f.Destination)
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			os.RemoveAll(workDir)
			return nil, fmt.Errorf("creating parent dirs for %s: %w", f.Destination, err)
		}
		if err := tc.downloadFile(ctx, f.Source, destPath); err != nil {
			os.RemoveAll(workDir)
			return nil, fmt.Errorf("localizing %s: %w", f.Source, err)
		}
		if f.IsExecutable {
			if err := os.Chmod(destPath, 0755); err != nil {
				os.RemoveAll(workDir)
				return nil, fmt.Errorf("making %s executable: %w", destPath, err)
			}
		}
		localizedFiles = append(localizedFiles, destPath)
	}

	return &TaskPaths{
		workDir:        workDir,
		taskWorkDir:    taskWorkDir,
		logPath:        logPath,
		localizedFiles: localizedFiles,
	}, nil
}

func cleanupWorkDir(paths *TaskPaths) error {
	return os.RemoveAll(paths.workDir)
}

func buildDockerArgs(bindMounts []string, _ *Task) []string {
	var args []string
	for _, bindMount := range bindMounts {
		args = append(args, "-v", bindMount)
	}
	return args
}

type workerState struct {
	fsClient       *firestore.Client
	psClient       *pubsub.Client
	gcsClient      *storage.Client // held for Close() only
	transferClient TransferClient
	publisher      *EventPublisher
	workerDoc      *firestore.DocumentRef
	registry       *taskRegistry
	subName        string
	project        string
	workerID       string
	workpoolID     string
	bindMounts     []string
	workDirParent  string
	noDocker       bool
}

func (ws *workerState) cleanup() {
	ws.publisher.Stop()
	ws.gcsClient.Close()
	ws.psClient.Close()
	ws.fsClient.Close()
}

func startWorker(ctx context.Context, project, db, workerID, workpoolID string, noGCP, noDocker bool) (*workerState, error) {
	var fsClient *firestore.Client
	var err error
	if db != "" {
		fsClient, err = firestore.NewClientWithDatabase(ctx, project, db)
	} else {
		fsClient, err = firestore.NewClient(ctx, project)
	}
	if err != nil {
		return nil, fmt.Errorf("creating firestore client: %w", err)
	}

	psClient, err := pubsub.NewClient(ctx, project)
	if err != nil {
		fsClient.Close()
		return nil, fmt.Errorf("creating pubsub client: %w", err)
	}

	var gcsOpts []option.ClientOption
	if endpoint := os.Getenv("GCS_EMULATOR_ENDPOINT"); endpoint != "" {
		gcsOpts = append(gcsOpts,
			option.WithEndpoint(endpoint),
			option.WithoutAuthentication(),
		)
	}
	gcsClient, err := storage.NewClient(ctx, gcsOpts...)
	if err != nil {
		psClient.Close()
		fsClient.Close()
		return nil, fmt.Errorf("creating storage client: %w", err)
	}

	var instanceName, batchID string
	if !noGCP {
		instanceName, err = metadata.InstanceNameWithContext(ctx)
		if err != nil {
			psClient.Close()
			fsClient.Close()
			gcsClient.Close()
			return nil, fmt.Errorf("reading instance name from metadata server: %w", err)
		}
		batchID, err = metadata.GetWithContext(ctx, "instance/labels/sparkles-worker-batch")
		if err != nil {
			psClient.Close()
			fsClient.Close()
			gcsClient.Close()
			return nil, fmt.Errorf("reading batch ID from instance labels: %w", err)
		}
	}

	now := time.Now()
	workerDoc := fsClient.Collection(workerCollection).Doc(workerID)
	_, err = workerDoc.Set(ctx, WorkerRecord{
		WorkerID:        workerID,
		WorkpoolID:      workpoolID,
		BatchID:         batchID,
		InstanceName:    instanceName,
		Status:          "started",
		Expiry:          now.Add(7 * 24 * time.Hour),
		HeartbeatExpiry: now.Add(heartbeatPeriod),
	})
	if err != nil {
		psClient.Close()
		fsClient.Close()
		return nil, fmt.Errorf("registering worker in firestore: %w", err)
	}
	log.Printf("Registered worker %s in Firestore", workerID)

	publisher := NewEventPublisher(psClient.Publisher(workerOutTopic), fsClient)

	if err := publisher.PublishWorkerEvent(ctx, WorkerEvent{
		Type:       "worker_started",
		WorkerID:   workerID,
		WorkpoolID: workpoolID,
	}); err != nil {
		publisher.Stop()
		psClient.Close()
		fsClient.Close()
		return nil, fmt.Errorf("publishing worker_started: %w", err)
	}
	log.Printf("Published worker_started event")

	subName := fmt.Sprintf("%s-%s", workerInTopic, workerID)
	subResourceName := fmt.Sprintf("projects/%s/subscriptions/%s", project, subName)
	topicResourceName := fmt.Sprintf("projects/%s/topics/%s", project, workerInTopic)
	if _, err = psClient.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  subResourceName,
		Topic: topicResourceName,
	}); err != nil {
		publisher.Stop()
		psClient.Close()
		fsClient.Close()
		return nil, fmt.Errorf("creating subscription %s: %w", subName, err)
	}
	log.Printf("Created subscription %s", subName)

	registry := &taskRegistry{entries: make(map[string]*registeredTask)}

	go runHeartbeat(ctx, workerDoc)

	go func() {
		recvErr := psClient.Subscriber(subName).Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			msg.Ack()
			var m workerControlMessage
			if err := json.Unmarshal(msg.Data, &m); err != nil {
				log.Printf("worker-in: failed to parse message: %v", err)
				return
			}
			switch m.Type {
			case "stream_task_updates":
				if m.TaskID == "" {
					log.Printf("worker-in: stream_task_updates missing task_id")
					return
				}
				tel := registry.getLog(m.TaskID)
				if tel == nil {
					log.Printf("worker-in: no active TaskEventLog for task %s", m.TaskID)
					return
				}
				if err := tel.StartStreaming(); err != nil {
					log.Printf("worker-in: start streaming for task %s: %v", m.TaskID, err)
				}
			case "kill_job":
				if m.JobID == "" {
					log.Printf("worker-in: kill_job missing job_id")
					return
				}
				log.Printf("worker-in: killing tasks for job %s", m.JobID)
				registry.killJob(m.JobID)
			default:
				log.Printf("worker-in: unknown message type %q", m.Type)
			}
		})
		if recvErr != nil && ctx.Err() == nil {
			log.Printf("subscription receive error: %v", recvErr)
		}
	}()

	return &workerState{
		fsClient:       fsClient,
		psClient:       psClient,
		gcsClient:      gcsClient,
		transferClient: &GCSTransferClient{gcsClient: gcsClient},
		publisher:      publisher,
		workerDoc:      workerDoc,
		registry:       registry,
		subName:        subName,
		project:        project,
		workerID:       workerID,
		workpoolID:     workpoolID,
		noDocker:       noDocker,
	}, nil
}

func (ws *workerState) shutdown() {
	// Use a fresh context for shutdown operations since the main context is cancelled.
	ctx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	shutdownNow := time.Now()
	_, err := ws.workerDoc.Update(ctx, []firestore.Update{
		{Path: "expiry", Value: shutdownNow},
		{Path: "heartbeat_expiry", Value: shutdownNow},
		{Path: "status", Value: "stopped"},
	})
	if err != nil {
		log.Printf("Failed to update worker record on shutdown: %v", err)
	}

	if err := ws.publisher.PublishWorkerEvent(ctx, WorkerEvent{
		Type:       "worker_stopped",
		WorkerID:   ws.workerID,
		WorkpoolID: ws.workpoolID,
	}); err != nil {
		log.Printf("Failed to publish worker_stopped: %v", err)
	}

	subResourceName := fmt.Sprintf("projects/%s/subscriptions/%s", ws.project, ws.subName)
	if err := ws.psClient.SubscriptionAdminClient.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{
		Subscription: subResourceName,
	}); err != nil {
		log.Printf("Failed to delete subscription %s: %v", ws.subName, err)
	}
}
