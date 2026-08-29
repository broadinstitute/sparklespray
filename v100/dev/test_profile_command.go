package dev

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	v100 "github.com/broadinstitute/sparklespray/v100"
	"github.com/google/uuid"
	"github.com/urfave/cli"
)

// runDevTestProfileCommand runs <docker-image> <command...> under Docker and
// drives the worker's periodic resource-metric collection (the same
// v100.CollectMetrics code path polled by OpenTaskEventLog for every real
// task) on the same interval, printing each sample as JSON to stdout instead
// of publishing it to Firestore. Useful for inspecting/debugging what the
// worker's metric_update events actually report.
func runDevTestProfileCommand(c *cli.Context) error {
	args := []string(c.Args())
	if len(args) < 2 {
		return fmt.Errorf("usage: sparkles dev test-profile-command [--interval DURATION] [--] <docker-image> <command...>")
	}
	image := args[0]
	command := args[1:]

	interval := c.Duration("interval")
	if interval <= 0 {
		interval = v100.MetricsInterval
	}

	workDir, err := os.MkdirTemp("", "sparkles-test-profile-*")
	if err != nil {
		return fmt.Errorf("creating work dir: %w", err)
	}
	defer os.RemoveAll(workDir)

	containerName := "sparkles-test-profile-" + uuid.New().String()[:8]

	dockerArgs := append([]string{"run", "--name", containerName, "-w", workDir}, image)
	dockerArgs = append(dockerArgs, command...)

	fmt.Fprintf(os.Stderr, "Running: %s %s\n", v100.DockerExecutable, strings.Join(dockerArgs, " "))
	fmt.Fprintf(os.Stderr, "Sampling metrics every %s\n", interval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, v100.DockerExecutable, dockerArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting docker: %w", err)
	}

	// Same periodic collection loop as OpenTaskEventLog (v100/task_event_log.go),
	// just printed to stdout instead of published to Firestore/a local file.
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		prev := v100.GetCPUStats()
		enc := json.NewEncoder(os.Stdout)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var event *v100.ResourceUsageEvent
				event, prev = v100.CollectMetrics(containerName, workDir, prev)
				if err := enc.Encode(event); err != nil {
					fmt.Fprintf(os.Stderr, "encoding metric event: %v\n", err)
				}
			}
		}
	}()

	runErr := cmd.Wait()
	cancel()
	<-pollDone

	if rmOut, rmErr := exec.Command(v100.DockerExecutable, "rm", "-f", containerName).CombinedOutput(); rmErr != nil {
		fmt.Fprintf(os.Stderr, "docker rm %s: %v: %s\n", containerName, rmErr, rmOut)
	}

	if runErr != nil {
		return fmt.Errorf("docker command failed: %w", runErr)
	}
	return nil
}
