package dev

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	aetherclient "github.com/pgm/aether/client"

	"github.com/broadinstitute/sparklesworker/backend"
	gcp_backend "github.com/broadinstitute/sparklesworker/backend/gcp"
	redis_backend "github.com/broadinstitute/sparklesworker/backend/redis"
	"github.com/broadinstitute/sparklesworker/consumer"
	"github.com/broadinstitute/sparklesworker/ext_channel"
	"github.com/broadinstitute/sparklesworker/task_queue"
	"github.com/urfave/cli"
)

// DevSubmitRequest encodes all parameters needed to submit and run a job.
// Unlike the top-level submit command, every parameter lives in this JSON
// file so the invocation has no other flags.  This makes it easy to write
// test-harness code that constructs the file and calls "dev submit <path>".
type DevSubmitRequest struct {
	// --- backend ---
	ProjectID string `json:"projectID"`
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

	// --- job spec ---
	Name         string        `json:"name"`
	ClusterID    string        `json:"clusterID"`
	DockerImage  string        `json:"dockerImage"`
	Command      string        `json:"command"`
	FilesToStage []fileToStage `json:"filesToStage"`

	// If non-empty, export OutputAetherFSRoot from the completed task to this local directory.
	ExportOutputTo string `json:"exportOutputTo"`
	// If non-empty, export LogAetherFSRoot from the completed task to this local directory.
	ExportLogTo string `json:"exportLogTo"`
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

	return ExecuteSubmit(&req)
}

func ExecuteSubmit(req *DevSubmitRequest) error {
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

	expiryDuration, err := time.ParseDuration(req.Expiry)
	if err != nil {
		return fmt.Errorf("invalid expiry %q: %w", req.Expiry, err)
	}

	ctx := context.Background()

	// Stage files via Aether.
	aetherCfg := consumer.AetherConfig{
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
		return fmt.Errorf("staging files: %w", err)
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

	log.Printf("Submitting job %s to cluster %s", job.Name, job.ClusterID)

	// Connect to the chosen backend.
	var extServices *backend.ExternalServices
	if req.RedisAddr != "" {
		log.Printf("Using Redis backend at %s", req.RedisAddr)
		extServices, err = redis_backend.CreateMockServices(ctx, req.RedisAddr, job.ClusterID, 1*time.Second)
	} else {
		extServices, err = gcp_backend.CreateGCPServices(ctx, req.ProjectID, req.Database, job.ClusterID)
	}
	if err != nil {
		return fmt.Errorf("creating backend services: %w", err)
	}
	defer extServices.Close()

	queue := extServices.Queue
	channel := extServices.Channel

	if err := queue.AddJob(ctx, job, []*task_queue.Task{task}); err != nil {
		return fmt.Errorf("adding job: %w", err)
	}
	log.Printf("Successfully submitted job %s", job.Name)

	topicName := req.TopicPrefix + "-log"
	defer ext_channel.StartLogStream(ctx, channel, topicName)()

	// If running locally, drive the consumer loop ourselves.
	if job.ClusterID == "local" {
		executor := func(taskId string, taskSpec *task_queue.TaskSpec, expiry time.Time) (*consumer.ExecuteTaskResult, error) {
			return consumer.ExecuteTask(ctx, &aetherCfg, taskId, taskSpec, req.Dir, req.CacheDir, req.TasksDir, nil, nil, expiry)
		}
		sleepUntilNotify := func(d time.Duration) { time.Sleep(d) }
		if err := consumer.RunLoop(ctx, queue, sleepUntilNotify, executor, 1*time.Second, 10*time.Second); err != nil {
			return fmt.Errorf("RunLoop failed: %w", err)
		}
	}

	// Poll until the task reaches a terminal state.
	var finalTask *task_queue.Task
	for {
		t, err := queue.GetTask(ctx, task.TaskID)
		if err != nil {
			return fmt.Errorf("polling task %s: %w", task.TaskID, err)
		}
		if t.Status != task_queue.StatusPending && t.Status != task_queue.StatusClaimed {
			log.Printf("Task %s finished with status %s", task.TaskID, t.Status)
			finalTask = t
			break
		}
		log.Printf("Task %s status: %s — waiting...", task.TaskID, t.Status)
		time.Sleep(1 * time.Second)
	}

	if req.ExportOutputTo != "" {
		if finalTask.OutputAetherFSRoot == "" {
			return fmt.Errorf("task completed but OutputAetherFSRoot is empty; cannot export")
		}
		exportStats, err := aetherclient.Export(ctx, aetherclient.ExportOptions{
			Root:        aetherCfg.Root,
			ManifestRef: finalTask.OutputAetherFSRoot,
			Dest:        req.ExportOutputTo,
			Workers:     aetherCfg.Workers,
			CacheDir:    req.CacheDir,
		})
		if err != nil {
			return fmt.Errorf("exporting output filesystem to %s: %w", req.ExportOutputTo, err)
		}
		log.Printf("Exported %d files (%d bytes) to %s", exportStats.FilesDownloaded, exportStats.BytesDownloaded, req.ExportOutputTo)
	}

	if req.ExportLogTo != "" {
		if finalTask.LogAetherFSRoot == "" {
			return fmt.Errorf("task completed but LogAetherFSRoot is empty; cannot export")
		}
		exportStats, err := aetherclient.Export(ctx, aetherclient.ExportOptions{
			Root:        aetherCfg.Root,
			ManifestRef: finalTask.LogAetherFSRoot,
			Dest:        req.ExportLogTo,
			Workers:     aetherCfg.Workers,
			CacheDir:    req.CacheDir,
		})
		if err != nil {
			return fmt.Errorf("exporting log filesystem to %s: %w", req.ExportLogTo, err)
		}
		log.Printf("Exported %d files (%d bytes) to %s", exportStats.FilesDownloaded, exportStats.BytesDownloaded, req.ExportLogTo)
	}

	return nil
}
