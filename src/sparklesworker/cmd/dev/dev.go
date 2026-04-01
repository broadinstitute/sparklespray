package dev

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/google/uuid"
	aetherclient "github.com/pgm/aether/client"

	"github.com/broadinstitute/sparklesworker/backend"
	gcp_backend "github.com/broadinstitute/sparklesworker/backend/gcp"
	redis_backend "github.com/broadinstitute/sparklesworker/backend/redis"
	"github.com/broadinstitute/sparklesworker/cmd/autoscaler"
	"github.com/broadinstitute/sparklesworker/consumer"
	"github.com/broadinstitute/sparklesworker/task_queue"
	"github.com/urfave/cli"
)

type DevClusterConfig struct {
	// MachineType is the GCE machine type used when launching worker nodes.
	MachineType string

	// WorkerDockerImage is the container image run on each worker node.
	WorkerDockerImage string

	// PubSubInTopic is the Pub/Sub topic the monitor publishes control messages to.
	PubSubInTopic string

	// PubSubOutTopic is the Pub/Sub topic workers publish status messages to.
	PubSubOutTopic string

	// Region is the GCP region where Batch jobs are submitted (e.g. "us-central1").
	// Used to construct the Batch API parent path.
	Region string

	// MaxPreemptableAttempts is the total number of preemptable node-attempts
	// allowed per job run. Resets when the queue drains to zero.
	MaxPreemptableAttempts int

	// MaxInstanceCount caps the number of nodes the monitor will request,
	// regardless of queue depth.
	MaxInstanceCount int

	// MaxSuspiciousFailures is the threshold for how many batch jobs may complete
	// without doing any work before the monitor halts node creation and alerts.
	MaxSuspiciousFailures int

	BootDiskType    string
	BootDiskSizeGB  int
	Disks           []backend.Disk           `json:"disks"`
	GCSBucketMounts []backend.GCSBucketMount `json:"gcs_bucket_mounts"`

	// parameters used by autoscaler
	AutoscaleMachineType    string
	AutoscaleBootDiskSizeGB int
	AutoscaleBootDiskType   string
	AutoscalePollInterval   time.Duration
	AutoscaleMaxIdle        time.Duration
}

// DevSubmitRequest encodes all parameters needed to submit and run a job.
// Unlike the top-level submit command, every parameter lives in this JSON
// file so the invocation has no other flags.  This makes it easy to write
// test-harness code that constructs the file and calls "dev submit <path>".
type DevSubmitRequest struct {
	// --- backend ---
	ProjectID string `json:"projectID"`
	Region    string `json:"region"`
	Database  string `json:"database"`
	// If non-empty, use the Redis backend instead of Firestore.
	RedisAddr string `json:"redisAddr"`

	// --- worker directories ---
	Dir      string `json:"dir"`
	CacheDir string `json:"cacheDir"`
	TasksDir string `json:"tasksDir"`

	// --- aether file-staging ---
	AetherRoot            string `json:"aetherRoot"`
	AetherMaxSizeToBundle int64  `json:"aetherMaxSizeToBundle"`
	AetherMaxBundleSize   int64  `json:"aetherMaxBundleSize"`
	AetherWorkers         int    `json:"aetherWorkers"`

	// --- timing / naming ---
	// Duration string accepted by time.ParseDuration, e.g. "24h".  Defaults to "24h".
	Expiry      string `json:"expiry"`
	TopicPrefix string `json:"topicPrefix"`
	// How long RunLoop waits for new tasks after the queue drains before exiting.
	// Also used as the first-poll retry delay. Defaults to 10s.
	RunLoopMaxWait time.Duration `json:"runLoopMaxWait"`

	// --- job spec ---
	Name         string            `json:"name"`
	ClusterID    string            `json:"clusterID"`
	Cluster      *DevClusterConfig `json:"cluster"`
	DockerImage  string            `json:"dockerImage"`
	Command      string            `json:"command"`
	FilesToStage []fileToStage     `json:"filesToStage"`

	// If non-empty, export OutputAetherFSRoot from the completed task to this local directory.
	ExportOutputTo string `json:"exportOutputTo"`
	// If non-empty, export LogAetherFSRoot from the completed task to this local directory.
	ExportLogTo string `json:"exportLogTo"`

	// whether to start autoscaler or not
	SkipAutoscale bool `json:"skipAutoscale"`
}

type fileToStage struct {
	LocalPath string `json:"localPath"`
	Name      string `json:"name"`
}

var DevCmd = cli.Command{
	Name:  "dev",
	Usage: "Developer utilities for testing functionality",
	Subcommands: []cli.Command{
		{
			Name:      "submit",
			Usage:     "Submit a job from a JSON parameter file",
			ArgsUsage: "<params.json>",
			Action:    devSubmit,
		},
	},
}

func devSubmit(c *cli.Context) error {
	if c.NArg() != 1 {
		return fmt.Errorf("usage: dev submit <params.json>")
	}

	data, err := os.ReadFile(c.Args().First())
	if err != nil {
		return fmt.Errorf("reading params file: %w", err)
	}

	var req DevSubmitRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return fmt.Errorf("parsing params file: %w", err)
	}

	_, err = ExecuteSubmit(&req)
	return err
}

const AutoscalerBatchJob = "sparkles-autoscaler"

func isAutoscalarRunning(ext *backend.ExternalServices) (bool, error) {
	batchjob, err := ext.Compute.GetBatchJobByName(AutoscalerBatchJob)
	if err == backend.NoSuchBatchJob {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("Checking for autoscaler failed: %w", err)
	}
	return batchjob.State == backend.Pending || batchjob.State == backend.Running, nil
}

type AutoscaleConfig struct {
	// GCP Specific settings
	project  string
	database string

	// settings for testing/mocking config
	redis string

	// shared settings
	pollInterval time.Duration
	maxIdle      time.Duration

	// batch API parameters
	region         string
	machineType    string
	bootVolumeInGB int
	bootVolumeType string
}

func ensureAutoscalarRunning(ext *backend.ExternalServices, sparklesWorkerDockerImage string, autoscaleConfig *AutoscaleConfig) error {
	cmd := []string{"autoscaler"}
	if autoscaleConfig.project != "" {
		cmd = append(cmd, "--project", autoscaleConfig.project)
	}
	if autoscaleConfig.region != "" {
		cmd = append(cmd, "--region", autoscaleConfig.region)
	}
	if autoscaleConfig.database != "" {
		cmd = append(cmd, "--database", autoscaleConfig.database)
	}
	if autoscaleConfig.redis != "" {
		cmd = append(cmd, "--redis", autoscaleConfig.redis)
	}
	cmd = append(cmd, "--poll-interval", fmt.Sprintf("%ds", autoscaleConfig.pollInterval/time.Second))
	cmd = append(cmd, "--max-idle", fmt.Sprintf("%ds", autoscaleConfig.maxIdle/time.Second))

	return ext.Compute.PutSingletonBatchJob(AutoscalerBatchJob, autoscaleConfig.region, autoscaleConfig.machineType, int64(autoscaleConfig.bootVolumeInGB), autoscaleConfig.bootVolumeType, sparklesWorkerDockerImage, cmd)
}

func ExecuteSubmit(req *DevSubmitRequest) (*task_queue.Task, error) {
	// Apply defaults.
	if req.Dir == "" {
		req.Dir = "./sparklesworker"
	}
	if req.CacheDir == "" {
		req.CacheDir = path.Join(req.Dir, "cache")
	}
	if req.TasksDir == "" {
		req.TasksDir = path.Join(req.Dir, "tasks")
	}
	if req.Expiry == "" {
		req.Expiry = "24h"
	}
	if req.TopicPrefix == "" {
		req.TopicPrefix = "sparkles"
	}
	if req.AetherWorkers == 0 {
		req.AetherWorkers = 1
	}
	if req.RunLoopMaxWait == 0 {
		req.RunLoopMaxWait = 10 * time.Second
	}

	if req.ClusterID == "" && req.Cluster == nil {
		return nil, fmt.Errorf("either clusterID or cluster must be set in the params file")
	}
	if req.ClusterID != "" && req.Cluster != nil {
		return nil, fmt.Errorf("only one of clusterID or cluster may be set, not both")
	}
	if req.Cluster != nil {
		clusterJSON, err := json.Marshal(req.Cluster)
		if err != nil {
			return nil, fmt.Errorf("serializing cluster config: %w", err)
		}
		sum := sha256.Sum256(clusterJSON)
		req.ClusterID = "c-" + hex.EncodeToString(sum[:])[:16]
	}

	expiryDuration, err := time.ParseDuration(req.Expiry)
	if err != nil {
		return nil, fmt.Errorf("invalid expiry %q: %w", req.Expiry, err)
	}

	ctx := context.Background()

	// Stage files via Aether.
	aetherCfg := backend.AetherConfig{
		Root:            req.AetherRoot,
		MaxSizeToBundle: req.AetherMaxSizeToBundle,
		MaxBundleSize:   req.AetherMaxBundleSize,
		Workers:         req.AetherWorkers,
	}

	var files []aetherclient.FileInput
	for _, f := range req.FilesToStage {
		files = append(files, aetherclient.FileInput{Path: f.LocalPath, ManifestName: f.Name})
	}
	mkfsStats, err := aetherclient.MakeFilesystem(ctx, aetherclient.MakeFilesystemOptions{
		Root:            aetherCfg.Root,
		Files:           files,
		MaxSizeToBundle: aetherCfg.MaxSizeToBundle,
		MaxBundleSize:   aetherCfg.MaxBundleSize,
		Workers:         aetherCfg.Workers,
		Expiry:          expiryDuration,
	})
	if err != nil {
		return nil, fmt.Errorf("staging files: %w", err)
	}
	rootID := "sha256:" + mkfsStats.ManifestKey
	log.Printf("Staged %d files, manifest key: %s", mkfsStats.FilesUploaded, rootID)

	// Build job and task records.
	expiryTime := time.Now().Add(expiryDuration)

	job := &task_queue.Job{
		Name:       req.Name,
		ClusterID:  req.ClusterID,
		Status:     task_queue.StatusPending,
		SubmitTime: float64(time.Now().UnixMilli()) / 1000.0,
		TaskCount:  1,
		Expiry:     expiryTime,
	}

	taskSpec := &task_queue.TaskSpec{
		Command:      []string{"/bin/sh", "-c", req.Command},
		DockerImage:  req.DockerImage,
		AetherFSRoot: rootID,
	}

	task := &task_queue.Task{
		TaskID:    req.Name + ".1",
		TaskIndex: 0,
		JobID:     req.Name,
		ClusterID: req.ClusterID,
		Status:    task_queue.StatusPending,
		TaskSpec:  taskSpec,
		Expiry:    expiryTime,
	}

	// Connect to the chosen backend.
	var extServices *backend.ExternalServices
	if req.RedisAddr != "" {
		log.Printf("Using Redis backend at %s", req.RedisAddr)
		extServices, err = redis_backend.CreateMockServices(ctx, req.RedisAddr, false)
	} else {
		extServices, err = gcp_backend.CreateGCPServices(ctx, req.ProjectID, req.Region, req.Database)
	}
	if err != nil {
		return nil, fmt.Errorf("creating backend services: %w", err)
	}
	defer extServices.Close()

	log.Printf("Submitting job %s to cluster %s", job.Name, job.ClusterID)

	if req.Cluster != nil {
		cluster := backend.Cluster{
			UUID:                   uuid.New().String(),
			MachineType:            req.Cluster.MachineType,
			WorkerDockerImage:      req.Cluster.WorkerDockerImage,
			PubSubInTopic:          req.Cluster.PubSubInTopic,
			PubSubOutTopic:         req.Cluster.PubSubOutTopic,
			Region:                 req.Cluster.Region,
			MaxPreemptableAttempts: req.Cluster.MaxPreemptableAttempts,
			MaxInstanceCount:       req.Cluster.MaxInstanceCount,
			MaxSuspiciousFailures:  req.Cluster.MaxSuspiciousFailures,
			BootDiskSizeGB:         req.Cluster.BootDiskSizeGB,
			BootDiskType:           req.Cluster.BootDiskType,
			Disks:                  req.Cluster.Disks,
			GCSBucketMounts:        req.Cluster.GCSBucketMounts,
			AetherConfig: &backend.AetherConfig{
				Root:            req.AetherRoot,
				MaxSizeToBundle: req.AetherMaxSizeToBundle,
				MaxBundleSize:   req.AetherMaxBundleSize,
				Workers:         req.AetherWorkers},
		}
		if err := extServices.Cluster.SetClusterConfig(req.ClusterID, cluster); err != nil {
			return nil, fmt.Errorf("storing cluster config: %w", err)
		}
	}

	useAutoscaler := !req.SkipAutoscale
	// validate cluster ID is valid

	if job.ClusterID == "local" {
		useAutoscaler = false
	} else {
		_, err = extServices.Cluster.GetClusterConfig(job.ClusterID)
		if err != nil {
			return nil, fmt.Errorf("Attempted to submit job to cluster %s but got error fetching its config: %w", job.ClusterID, err)
		}
	}

	queue := extServices.Tasks
	channel := extServices.Channel
	taskCache := extServices.TaskCache

	if err := queue.AddJob(ctx, job, []*task_queue.Task{task}); err != nil {
		return nil, fmt.Errorf("adding job: %w", err)
	}
	log.Printf("Successfully submitted job %s", job.Name)

	topicName := req.TopicPrefix + "-log"
	defer backend.StartLogStream(ctx, channel, topicName)()

	// If running locally, drive the consumer loop ourselves.
	if job.ClusterID == "local" {
		executor := func(taskId string, taskSpec *task_queue.TaskSpec, expiry time.Time) (*consumer.ExecuteTaskResult, error) {
			return consumer.ExecuteTask(ctx, &aetherCfg, taskId, taskSpec, req.Dir, req.CacheDir, req.TasksDir, nil, taskCache, expiry)
		}
		sleepUntilNotify := func(d time.Duration) { time.Sleep(d) }
		if err := consumer.RunLoop(ctx, job.ClusterID, queue, sleepUntilNotify, executor, 1*time.Second, req.RunLoopMaxWait); err != nil {
			return nil, fmt.Errorf("RunLoop failed: %w", err)
		}
	}

	if useAutoscaler {
		if req.Cluster == nil {
			log.Printf("Could not start autoscaler because config is missing")
			useAutoscaler = false
		}
	}

	if useAutoscaler {
		autoscaleConfig := &AutoscaleConfig{
			project:        req.ProjectID,
			database:       req.Database,
			redis:          req.RedisAddr,
			region:         req.Cluster.Region,
			machineType:    req.Cluster.AutoscaleMachineType,
			bootVolumeType: req.Cluster.AutoscaleBootDiskType,
			bootVolumeInGB: req.Cluster.AutoscaleBootDiskSizeGB,
			pollInterval:   req.Cluster.AutoscalePollInterval,
			maxIdle:        req.Cluster.AutoscaleMaxIdle,
		}
		sparklesWorkerDockerImage := req.Cluster.WorkerDockerImage
		isRunning, err := isAutoscalarRunning(extServices)
		if err != nil {
			return nil, fmt.Errorf("Failed to check autoscaler: %w", err)
		}

		if !isRunning {
			log.Printf("Autoscaler isn't running. Doing a manual poll and submitting autoscaler to run")

			// if it'll take a while for the autoscaler to start, so do a round of autoscaling in this process
			// before we start the autoscaler.
			autoscaler.Poll(job.ClusterID, extServices.Compute, extServices.Cluster, extServices.Tasks, extServices.CreateWorkerCommand)

			err := ensureAutoscalarRunning(extServices, sparklesWorkerDockerImage, autoscaleConfig)
			if err != nil {
				return nil, fmt.Errorf("Failed to start autoscaler: %w", err)
			}
		} else {
			log.Printf("Confirmed autoscaler is already running")
		}
	}

	// Poll until the task reaches a terminal state.
	var finalTask *task_queue.Task
	for {
		t, err := queue.GetTask(ctx, task.TaskID)
		if err != nil {
			return nil, fmt.Errorf("polling task %s: %w", task.TaskID, err)
		}
		if t.Status != task_queue.StatusPending && t.Status != task_queue.StatusClaimed {
			log.Printf("Task %s finished with status %s", task.TaskID, t.Status)
			finalTask = t
			break
		}
		log.Printf("Task %s status: %s — waiting...", task.TaskID, t.Status)

		if useAutoscaler {
			// if we're using the autoscaler, confirm its still running/pending. If it fails or disappears, something has
			// done wrong as it is running on a non-preemtable instance
			isRunning, err := isAutoscalarRunning(extServices)
			if err != nil {
				return nil, fmt.Errorf("Failed to check for autoscaler: %w", err)
			}
			if !isRunning {
				return nil, fmt.Errorf("Autoscaler unexpectedly exited")
			}
		}

		time.Sleep(1 * time.Second)
	}

	if req.ExportOutputTo != "" {
		if finalTask.OutputAetherFSRoot == "" {
			return nil, fmt.Errorf("task completed but OutputAetherFSRoot is empty; cannot export")
		}
		exportStats, err := aetherclient.Export(ctx, aetherclient.ExportOptions{
			Root:        aetherCfg.Root,
			ManifestRef: finalTask.OutputAetherFSRoot,
			Dest:        req.ExportOutputTo,
			Workers:     aetherCfg.Workers,
			CacheDir:    req.CacheDir,
		})
		if err != nil {
			return nil, fmt.Errorf("exporting output filesystem to %s: %w", req.ExportOutputTo, err)
		}
		log.Printf("Exported %d files (%d bytes) to %s", exportStats.FilesDownloaded, exportStats.BytesDownloaded, req.ExportOutputTo)
	}

	if req.ExportLogTo != "" {
		if finalTask.LogAetherFSRoot == "" {
			return nil, fmt.Errorf("task completed but LogAetherFSRoot is empty; cannot export")
		}
		exportStats, err := aetherclient.Export(ctx, aetherclient.ExportOptions{
			Root:        aetherCfg.Root,
			ManifestRef: finalTask.LogAetherFSRoot,
			Dest:        req.ExportLogTo,
			Workers:     aetherCfg.Workers,
			CacheDir:    req.CacheDir,
		})
		if err != nil {
			return nil, fmt.Errorf("exporting log filesystem to %s: %w", req.ExportLogTo, err)
		}
		log.Printf("Exported %d files (%d bytes) to %s", exportStats.FilesDownloaded, exportStats.BytesDownloaded, req.ExportLogTo)
	}

	return finalTask, nil
}
