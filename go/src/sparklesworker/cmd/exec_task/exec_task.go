package exec_task

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/broadinstitute/sparklesworker/consumer"
	"github.com/broadinstitute/sparklesworker/ext_channel"
	"github.com/broadinstitute/sparklesworker/task_queue"
	aetherclient "github.com/pgm/aether/client"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli"
)

var ExecCmd = cli.Command{
	Name:  "exec",
	Usage: "Execute a single task directly from the command line",
	Flags: []cli.Flag{
		cli.StringFlag{Name: "command", Usage: "Shell command to run (required)"},
		cli.StringFlag{Name: "commandResultURL", Value: "results.json", Usage: "Local path to write result JSON"},
		cli.StringFlag{Name: "aetherRoot", Usage: "Aether store root (gs://bucket/prefix or local path)"},
		cli.StringFlag{Name: "aetherFSRoot", Usage: "Input aether manifest ref for downloads (mutually exclusive with --stageDir)"},
		cli.StringFlag{Name: "stageDir", Usage: "Local directory to upload as the input filesystem root (mutually exclusive with --aetherFSRoot)"},
		cli.Int64Flag{Name: "aetherMaxSizeToBundle", Value: 0, Usage: "Max file size in bytes eligible for bundling (0 = disable bundling)"},
		cli.Int64Flag{Name: "aetherMaxBundleSize", Value: 0, Usage: "Target max bundle size in bytes"},
		cli.IntFlag{Name: "aetherWorkers", Value: 1, Usage: "Parallel upload workers"},
		cli.StringFlag{Name: "dir", Value: "./sparklesworker", Usage: "Base directory for worker data"},
		cli.StringFlag{Name: "cacheDir", Usage: "Cache directory (defaults to dir/cache)"},
		cli.StringFlag{Name: "tasksDir", Usage: "Tasks directory (defaults to dir/tasks)"},
		cli.StringFlag{Name: "taskId", Value: "exec-task", Usage: "Task identifier"},
		cli.StringFlag{Name: "workingDir", Usage: "Working subdirectory within task dir"},
		cli.StringFlag{Name: "dockerImage", Usage: "Docker image for execution"},
		cli.StringSliceFlag{Name: "includePattern", Usage: "Upload include glob pattern (repeatable)"},
		cli.StringSliceFlag{Name: "excludePattern", Usage: "Upload exclude glob pattern (repeatable)"},
		cli.StringSliceFlag{Name: "param", Usage: "Parameter as key=value (repeatable)"},
		cli.StringFlag{Name: "preDownloadScript", Usage: "Script to run before download"},
		cli.StringFlag{Name: "postDownloadScript", Usage: "Script to run after download"},
		cli.StringFlag{Name: "preExecScript", Usage: "Script to run before exec"},
		cli.StringFlag{Name: "postExecScript", Usage: "Script to run after exec"},
		cli.StringFlag{Name: "topicPrefix", Value: "sparkles", Usage: "Prefix for the log topic name (topic will be <topicPrefix>-log)"},
		cli.StringFlag{Name: "redisAddr", Usage: "Redis address for log channel (e.g. localhost:6379); if empty, uses Google Cloud Pub/Sub"},
		cli.StringFlag{Name: "projectId", Usage: "Google Cloud project ID (used for Pub/Sub log channel when redisAddr is not set)"},
	},
	Action: execTask,
}

func execTask(c *cli.Context) error {
	command := c.String("command")
	if command == "" {
		return fmt.Errorf("--command is required")
	}

	commandArray := strings.Split(command, " ")

	ctx := context.Background()

	dir := c.String("dir")
	cacheDir := c.String("cacheDir")
	if cacheDir == "" {
		cacheDir = path.Join(dir, "cache")
	}
	tasksDir := c.String("tasksDir")
	if tasksDir == "" {
		tasksDir = path.Join(dir, "tasks")
	}

	aetherFSRoot := c.String("aetherFSRoot")
	stageDir := c.String("stageDir")
	if (aetherFSRoot == "") == (stageDir == "") {
		return fmt.Errorf("exactly one of --aetherFSRoot or --stageDir is required")
	}

	aetherCfg := consumer.AetherConfig{
		Root:            c.String("aetherRoot"),
		MaxSizeToBundle: c.Int64("aetherMaxSizeToBundle"),
		MaxBundleSize:   c.Int64("aetherMaxBundleSize"),
		Workers:         c.Int("aetherWorkers"),
	}

	if stageDir != "" {
		log.Printf("ResolveUploads")
		stageDir, err := filepath.Abs(stageDir)
		if err != nil {
			return err
		}

		filePaths, err := consumer.ResolveUploads(stageDir, &task_queue.UploadSpec{IncludePatterns: []string{"**/*"}})
		if err != nil {
			return fmt.Errorf("resolving files in stageDir %s: %w", stageDir, err)
		}
		var files []aetherclient.FileInput
		for _, absPath := range filePaths {
			relPath, err := filepath.Rel(stageDir, absPath)
			if err != nil {
				return err
			}
			files = append(files, aetherclient.FileInput{Path: absPath, ManifestName: relPath})
		}
		stats, err := aetherclient.MakeFilesystem(ctx, aetherclient.MakeFilesystemOptions{
			Root:            aetherCfg.Root,
			Files:           files,
			MaxSizeToBundle: aetherCfg.MaxSizeToBundle,
			MaxBundleSize:   aetherCfg.MaxBundleSize,
			Workers:         aetherCfg.Workers,
		})
		if err != nil {
			return fmt.Errorf("staging files from %s: %w", stageDir, err)
		}
		aetherFSRoot = "sha256:" + stats.ManifestKey
		log.Printf("Staged %d files from %s, manifest key: %s", stats.FilesUploaded, stageDir, aetherFSRoot)
	}

	params := map[string]string{}
	for _, p := range c.StringSlice("param") {
		parts := strings.SplitN(p, "=", 2)
		if len(parts) == 2 {
			params[parts[0]] = parts[1]
		}
	}

	taskSpec := &task_queue.TaskSpec{
		Command:      commandArray,
		AetherFSRoot: aetherFSRoot,
		WorkingDir:   c.String("workingDir"),
		DockerImage:  c.String("dockerImage"),
		Parameters:   params,
		Uploads: &task_queue.UploadSpec{
			IncludePatterns: func() []string {
				if p := c.StringSlice("includePattern"); len(p) > 0 {
					return p
				}
				return []string{"**/*"}
			}(),
			ExcludePatterns: c.StringSlice("excludePattern"),
		},
		PreDownloadScript:  c.String("preDownloadScript"),
		PostDownloadScript: c.String("postDownloadScript"),
		PreExecScript:      c.String("preExecScript"),
		PostExecScript:     c.String("postExecScript"),
	}

	taskId := c.String("taskId")

	topicName := c.String("topicPrefix") + "-log"
	redisAddr := c.String("redisAddr")
	projectId := c.String("projectId")
	if (redisAddr == "") == (projectId == "") {
		return fmt.Errorf("exactly one of --redisAddr or --projectId is required")
	}
	var channel ext_channel.ExtChannel
	if redisAddr != "" {
		redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
		channel = ext_channel.NewRedisChannel(redisClient)
	} else {
		channel = ext_channel.NewPubSubChannel(projectId)
	}

	logStreamCtx, logStreamCancel := context.WithCancel(context.Background())
	go channel.Subscribe(logStreamCtx, topicName, func(message []byte) {
		log.Printf("Message: %s", string(message))
	})
	defer logStreamCancel()

	lifeCycle := NewMonitor(ctx, channel, topicName, 1*time.Second)

	result, err := consumer.ExecuteTask(ctx, &aetherCfg, taskId, taskSpec, dir, cacheDir, tasksDir, lifeCycle)
	if err != nil {
		return fmt.Errorf("task failed: %w", err)
	}
	log.Printf("Task completed with exit code: %s", result.RetCode)
	log.Printf("task files: sha256:%s logs: sha256: %s", result.OutputsKey, result.LogsKey)

	return nil
}

type Monitor struct {
	ctx        context.Context
	cancel     context.CancelFunc
	channel    ext_channel.ExtChannel
	topicName  string
	pollSleep  time.Duration
	stdoutFile *os.File
	done       chan struct{}
}

func NewMonitor(ctx context.Context, channel ext_channel.ExtChannel, topicName string, pollSleep time.Duration) *Monitor {
	ctx, cancel := context.WithCancel(ctx)
	return &Monitor{
		ctx:       ctx,
		cancel:    cancel,
		channel:   channel,
		topicName: topicName,
		pollSleep: pollSleep,
	}
}

func (m *Monitor) poll() error {
	buf := make([]byte, 100*1024)
	n, err := m.stdoutFile.Read(buf)
	if err != nil && err != io.EOF {
		log.Printf("Monitor.poll: read error: %v", err)
		return err
	}
	if n > 0 {
		if pubErr := m.channel.Publish(m.ctx, m.topicName, buf[:n]); pubErr != nil {
			log.Printf("Monitor.poll: publish error: %v", pubErr)
			return pubErr
		}
	}
	return nil
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

		for {
			err := m.poll()
			if err != nil {
				return
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
		m.poll()
		m.stdoutFile.Close()
		m.stdoutFile = nil
	}
}
