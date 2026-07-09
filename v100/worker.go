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
const dockerExecutable = "/usr/bin/docker"

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
	bindMounts := c.StringSlice("bind-mount")
	workDirParent := c.String("work-dir")

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

	batchID := c.String("batch")
	lingerTime := time.Duration(c.Int("linger")) * time.Second
	streamLogs := c.Bool("stream")

	ws, err := startWorker(ctx, project, db, workerID, workpoolID, batchID, noGCP, noDocker, streamLogs, bindMounts, workDirParent, lingerTime)
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

func executeDockerCommand(ctx context.Context, imageName string, command []string, workDir string, extraDockerArgs []string, tel *TaskEventLog) (*ResourceUsage, error) {
	containerName := "sparkles-" + uuid.New().String()[:8]

	args := append([]string{"run", "--name", containerName, "-w", workDir}, extraDockerArgs...)
	args = append(args, imageName)
	args = append(args, command...)

	log.Printf("Executing docker: %s %s", dockerExecutable, strings.Join(args, " "))

	cmd := exec.CommandContext(ctx, dockerExecutable, args...)

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

	// Collect resource usage while the container still exists, then remove it.
	ru := collectDockerResourceUsage(containerName)
	if rmOut, rmErr := exec.Command(dockerExecutable, "rm", "-f", containerName).CombinedOutput(); rmErr != nil {
		log.Printf("docker rm %s: %v: %s", containerName, rmErr, rmOut)
	}

	if runErr != nil {
		return ru, fmt.Errorf("could not run docker command (%s): %w", strings.Join(args, " "), runErr)
	}
	return ru, pipeErr
}

type WorkerLoopConfig struct {
	WorkpoolID            string
	WorkerID              string
	Queue                 TaskQueue
	Resources             *Resources
	TransferClient        TransferClient
	WorkDirParent         string
	BindMounts            []string
	Registry              *taskRegistry
	FSClient              *firestore.Client
	ExecuteDockerCommand  func(ctx context.Context, imageName string, command []string, workDir string, extraDockerArgs []string, tel *TaskEventLog) (*ResourceUsage, error)
	LingerTime            time.Duration
	StartStreamingAtStart bool
}

// ErrTaskKilled is returned by the task callback when a task was cancelled via a kill_job message.
var ErrTaskKilled = errors.New("task killed")

const UnknownLeader = 0
const IsLeader = 1
const IsNotLeader = 2

func getPendingTask(ctx context.Context, lingerTime time.Duration, WorkpoolID string, Queue TaskQueue, isLeader func() bool) (*Task, error) {
	lingerDeadline := time.Now().Add(lingerTime)
	leaderStatus := UnknownLeader

	for {
		// first, find a job which has at least one task
		pendingTask, err := Queue.GetFirstPendingTask(ctx, WorkpoolID)
		if err != nil {
			return nil, fmt.Errorf("getting first pending task: %w", err)
		}

		if pendingTask != nil {
			return pendingTask, nil
		}

		// if we're the leader linger, if we're not, exit loop immediately
		if leaderStatus == UnknownLeader {
			if lingerTime == 0 {
				// if the linger time is 0, then it doesn't matter if we're the leader
				// or not. Skip the check. This is really this way to make it make writing
				// a unit test slightly easier
				leaderStatus = IsNotLeader
			} else if isLeader() {
				leaderStatus = IsLeader
			} else {
				leaderStatus = IsNotLeader
			}
		}

		if time.Now().After(lingerDeadline) || leaderStatus == IsNotLeader {
			// if our deadline expired, or we're not the leader, abort loop
			break
		}

		// poll every second if we're the leader until we hit our deadline
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	return nil, nil
}

// checks to see if this worker is one with the lowest ID among the running in this workpool. this is used
// to do a sort of best-effort "leader" election, where the leader is the one node that will hang around
// for any future requests for work. In order to be considered, it must be for this workpoolID and with
// a non-expired heartbeat
func isWorkerWithLowestID(ctx context.Context, fsClient *firestore.Client, workerID string, workpoolID string) bool {
	now := time.Now()
	docs, err := fsClient.Collection(workerCollection).
		Where("workpool_id", "==", workpoolID).
		Where("heartbeat_expiry", ">", now).
		Select("__name__").
		Documents(ctx).GetAll()
	if err != nil {
		log.Printf("isWorkerWithLowestID: querying workers: %v", err)
		return false
	}
	for _, doc := range docs {
		if doc.Ref.ID < workerID {
			return false
		}
	}
	return true
}

type loopState struct {
	completions  chan taskCompletion
	runningCount int
	curResources *Resources
}

func (ls *loopState) waitForCompletion(ctx context.Context, queue TaskQueue) error {
	select {
	case c := <-ls.completions:
		ls.curResources = ls.curResources.Add(c.resources)
		ls.runningCount--
		if errors.Is(c.err, ErrTaskKilled) {
			if err := queue.RecordKilled(ctx, c.taskID, false); err != nil {
				log.Printf("recording task %s as killed: %v", c.taskID, err)
				return err
			}
		} else if c.err != nil {
			log.Printf("task %s failed: %v", c.taskID, c.err)
			var exitErr *exec.ExitError
			if errors.As(c.err, &exitErr) {
				if err := queue.RecordError(ctx, c.taskID, exitErr.ExitCode()); err != nil {
					log.Printf("recording task %s error (exit %d): %v", c.taskID, exitErr.ExitCode(), err)
					return err
				}
			} else if err := queue.RecordFailed(ctx, c.taskID, c.err.Error(), StatusClaimed); err != nil {
				log.Printf("recording task %s as failed: %v", c.taskID, err)
				return err
			}
		} else {
			if err := queue.UpdateState(ctx, c.taskID, StatusWriting, StatusSuccess); err != nil {
				log.Printf("recording task %s as success: %v", c.taskID, err)
				return err
			}
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (ls *loopState) drainCompletions(ctx context.Context, queue TaskQueue) error {
	for len(ls.completions) > 0 {
		if err := ls.waitForCompletion(ctx, queue); err != nil {
			return err
		}
	}
	return nil
}

func loadJobResources(job *Job) Resources {
	jobResources := NewResources()
	for _, entry := range job.Resources {
		jobResources.Set(entry.Name, entry.Value)
	}
	return jobResources
}

func failAllTasksForJob(ctx context.Context, queue TaskQueue, job *Job, workerID string) error {
	for {
		task, err := queue.ClaimTask(ctx, job.JobID, workerID)
		if err != nil {
			return fmt.Errorf("claiming task to fail for job %s: %w", job.JobID, err)
		}
		if task == nil {
			break
		}
		if err := queue.RecordFailed(ctx, task.TaskID, "Task requires more resources than allowed by worker pool", StatusClaimed); err != nil {
			return fmt.Errorf("recording task %s as failed: %w", task.TaskID, err)
		}
	}
	return nil
}

func executeTaskBody(ctx context.Context, cfg *WorkerLoopConfig, t *Task) error {
	log.Printf("Executing task %s", t.TaskID)
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

	log.Printf("Prepared input files and the working dir %s", t.TaskID)
	extraDockerArgs := buildDockerArgs(cfg.BindMounts, t)
	tel, err := OpenTaskEventLog(ctx, paths.logPath, t.TaskID, paths.taskWorkDir, cfg.FSClient)
	if err != nil {
		return fmt.Errorf("opening task event log for %s: %w", t.TaskID, err)
	}
	cfg.Registry.register(t.TaskID, t.JobID, tel, cancel)

	if cfg.StartStreamingAtStart {
		if err := tel.StartStreaming(); err != nil {
			return fmt.Errorf("failed to start streaming %s: %w", t.TaskID, err)
		}
	}

	defer cfg.Registry.unregister(t.TaskID)
	if err := cfg.Queue.UpdateState(ctx, t.TaskID, StatusClaimed, StatusRunning); err != nil {
		tel.Close()
		return fmt.Errorf("marking task %s running: %w", t.TaskID, err)
	}
	log.Printf("Task %s: Executing (%s) %s", t.TaskID, t.DockerImage, strings.Join(t.Command, " "))
	ru, dockerExecErr := cfg.ExecuteDockerCommand(taskCtx, t.DockerImage, t.Command, paths.taskWorkDir, extraDockerArgs, tel)
	if dockerExecErr != nil {
		log.Printf("failed to exec: %s", dockerExecErr)
	}
	log.Printf("Task %s: Docker exec completed with err = %s", t.TaskID, dockerExecErr)
	killed := cfg.Registry.wasKilled(t.TaskID)
	if ru != nil {
		if err := cfg.Queue.RecordResourceUsage(ctx, t.TaskID, ru); err != nil {
			log.Printf("recording resource usage for task %s: %v", t.TaskID, err)
		}
	}
	flushErr := tel.Flush()
	var uploadResultsErr error
	if !killed {
		if err := cfg.Queue.UpdateState(ctx, t.TaskID, StatusRunning, StatusWriting); err != nil {
			tel.Close()
			return fmt.Errorf("marking task %s writing: %w", t.TaskID, err)
		}
		uploadResultsErr = uploadResults(ctx, cfg.TransferClient, paths, t.ResultPath, t.LogPath)
	}
	log.Printf("Task %s: Uploaded results", t.TaskID)
	closeErr := tel.Close()
	cleanupErr := cleanupWorkDir(paths)
	log.Printf("Task %s: Cleaned up workdir %s", t.TaskID, paths.workDir)
	if killed {
		_ = mergeErrors(flushErr, closeErr, cleanupErr)
		return ErrTaskKilled
	}
	finalErr := mergeErrors(dockerExecErr, flushErr, uploadResultsErr, closeErr, cleanupErr)
	log.Printf("Task %s: execution returning err = %s", t.TaskID, finalErr)
	return finalErr
}

func getJobWithPendingTask(ctx context.Context, cfg *WorkerLoopConfig, isLeader func() bool) (*Job, error) {
	pendingTask, err := getPendingTask(ctx, cfg.LingerTime, cfg.WorkpoolID, cfg.Queue, isLeader)
	if err != nil {
		return nil, fmt.Errorf("getting pending task for workpool %s: %w", cfg.WorkpoolID, err)
	}
	if pendingTask == nil {
		return nil, nil
	}
	job, err := cfg.Queue.GetJob(ctx, pendingTask.JobID)
	if err != nil {
		return nil, fmt.Errorf("getting job %s: %w", pendingTask.JobID, err)
	}
	return job, nil
}

func processJob(ctx context.Context, cfg *WorkerLoopConfig, ls *loopState, job *Job) error {
	jobResources := loadJobResources(job)

	for {
		if err := ls.drainCompletions(ctx, cfg.Queue); err != nil {
			return err
		}

		remaining := ls.curResources.Sub(jobResources)
		if !remaining.IsValid() {
			log.Printf("Insufficent resources to start a new task from job %s, waiting for a running task to complete", job.JobID)
			if err := ls.waitForCompletion(ctx, cfg.Queue); err != nil {
				return err
			}
			continue
		}
		log.Printf("Confirmed that we have sufficent resources to start a new task from job %s", job.JobID)

		task, err := cfg.Queue.ClaimTask(ctx, job.JobID, cfg.WorkerID)
		if err != nil {
			return fmt.Errorf("claiming task: %w", err)
		}
		if task == nil {
			log.Printf("No more tasks for job %s", job.JobID)
			break
		}

		ls.curResources = remaining
		ls.runningCount++
		executeTask(task, jobResources, ls.completions, func(t *Task) error {
			return executeTaskBody(ctx, cfg, t)
		})
	}
	return nil
}

func workerMainLoop(ctx context.Context, cfg *WorkerLoopConfig) error {
	ls := &loopState{
		completions:  make(chan taskCompletion, 100),
		curResources: cfg.Resources,
	}
	isLeader := func() bool {
		return isWorkerWithLowestID(ctx, cfg.FSClient, cfg.WorkerID, cfg.WorkpoolID)
	}

	log.Printf("Starting workerMainLoop, querying for tasks...")
	for {
		job, err := getJobWithPendingTask(ctx, cfg, isLeader)
		if err != nil {
			return err
		}
		if job == nil {
			break
		}

		if !cfg.Resources.Sub(loadJobResources(job)).IsValid() {
			log.Printf("Job %s requires more resources than this worker can provide; failing all pending tasks", job.JobID)
			if err := failAllTasksForJob(ctx, cfg.Queue, job, cfg.WorkerID); err != nil {
				return err
			}
			continue
		}

		if err := processJob(ctx, cfg, ls, job); err != nil {
			return err
		}
	}

	log.Printf("No more tasks in queue. Waiting for running tasks to complete...")
	for ls.runningCount > 0 {
		if err := ls.waitForCompletion(ctx, cfg.Queue); err != nil {
			return err
		}
	}
	log.Printf("Worker main loop complete")
	return nil
}

func (ws *workerState) mainLoop(ctx context.Context, resources *Resources) error {
	execFn := executeDockerCommand
	if ws.noDocker {
		execFn = executeCommandDirect
	}
	queue := NewFirestoreTaskQueue(ws.fsClient, ws.publisher)
	return workerMainLoop(ctx, &WorkerLoopConfig{
		WorkpoolID:            ws.workpoolID,
		WorkerID:              ws.workerID,
		Queue:                 queue,
		Resources:             resources,
		TransferClient:        ws.transferClient,
		WorkDirParent:         ws.workDirParent,
		BindMounts:            ws.bindMounts,
		Registry:              ws.registry,
		FSClient:              ws.fsClient,
		ExecuteDockerCommand:  execFn,
		LingerTime:            ws.lingerTime,
		StartStreamingAtStart: ws.streamLogs,
	})
}

// executeCommandDirect runs a command directly without Docker.
// The image name and extra docker args are ignored.
// Resource usage is timing-only (no cgroup data available in this mode).
func executeCommandDirect(ctx context.Context, _ string, command []string, workDir string, _ []string, tel *TaskEventLog) (*ResourceUsage, error) {
	log.Printf("Executing: %s", strings.Join(command, " "))
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

	start := time.Now()
	runErr := cmd.Run()
	end := time.Now()
	pw.Close()
	pipeErr := <-pipeErrCh

	ru := &ResourceUsage{
		StartTime:      start,
		EndTime:        end,
		ElapsedSeconds: end.Sub(start).Seconds(),
	}
	if runErr != nil {
		return ru, fmt.Errorf("could not run command (%s): %w", strings.Join(command, " "), runErr)
	}
	return ru, pipeErr
}

// WorkerRunConfig holds all parameters for running a worker programmatically.
type WorkerRunConfig struct {
	Project    string
	DB         string
	WorkerID   string
	WorkpoolID string
	// Resources is a comma-separated list of name=value pairs (e.g. "slots=1").
	// Defaults to "slots=1" if empty.
	Resources     string
	NoGCP         bool
	NoDocker      bool
	BindMounts    []string
	WorkDirParent string
	BatchID       string
	LingerTime    time.Duration
	StreamLogs    bool
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
	ws, err := startWorker(ctx, cfg.Project, cfg.DB, cfg.WorkerID, cfg.WorkpoolID, cfg.BatchID, cfg.NoGCP, cfg.NoDocker, cfg.StreamLogs, cfg.BindMounts, cfg.WorkDirParent, cfg.LingerTime)
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
		log.Printf("Localizing %s -> %s", f.Source, destPath)
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
	lingerTime     time.Duration
	streamLogs     bool
	noDocker       bool
}

func (ws *workerState) cleanup() {
	ws.publisher.Stop()
	ws.gcsClient.Close()
	ws.psClient.Close()
	ws.fsClient.Close()
}

func startWorker(ctx context.Context, project, db, workerID, workpoolID, batchID string, noGCP, noDocker, streamLogs bool, bindMounts []string, workDirParent string, lingerTime time.Duration) (*workerState, error) {
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

	var instanceName string
	if !noGCP {
		instanceProject, err := metadata.ProjectIDWithContext(ctx)
		if err != nil {
			psClient.Close()
			fsClient.Close()
			gcsClient.Close()
			return nil, fmt.Errorf("reading project ID from metadata server: %w", err)
		}

		zone, err := metadata.ZoneWithContext(ctx)
		if err != nil {
			psClient.Close()
			fsClient.Close()
			gcsClient.Close()
			return nil, fmt.Errorf("reading zone from metadata server: %w", err)
		}

		instance, err := metadata.InstanceNameWithContext(ctx)
		if err != nil {
			psClient.Close()
			fsClient.Close()
			gcsClient.Close()
			return nil, fmt.Errorf("reading instance name from metadata server: %w", err)
		}

		instanceName = fmt.Sprintf("project/%s/zone/%s/instance/%s", instanceProject, zone, instance)
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
		bindMounts:     bindMounts,
		workDirParent:  workDirParent,
		lingerTime:     lingerTime,
		streamLogs:     streamLogs,
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
