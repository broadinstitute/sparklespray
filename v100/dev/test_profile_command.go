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
// drives the worker's metric collection (the same code path polled by
// OpenTaskEventLog for every real task) against it, printing each sample as
// JSON to stdout instead of publishing it to Firestore.
//
// This is the primary way to inspect what the worker's metric_update events
// actually report on a given host -- in particular whether the container's
// cgroup could be located and which counters that kernel exposes.
//
// With --container, it attaches to an already-running container by name
// instead of starting one, which makes it a fast loop for debugging cgroup
// path resolution against a container you started by hand.
func runDevTestProfileCommand(c *cli.Context) error {
	if existing := c.String("container"); existing != "" {
		return profileExistingContainer(c, existing)
	}

	args := []string(c.Args())
	if len(args) < 2 {
		return fmt.Errorf("usage: sparkles dev test-profile-command [--interval DURATION] [--container NAME] [--docker-arg ARG]... [--] <docker-image> <command...>")
	}
	image := args[0]
	command := args[1:]

	workDir, err := os.MkdirTemp("", "sparkles-test-profile-*")
	if err != nil {
		return fmt.Errorf("creating work dir: %w", err)
	}
	defer os.RemoveAll(workDir)

	containerName := "sparkles-test-profile-" + uuid.New().String()[:8]

	dockerArgs := []string{"run", "--name", containerName, "-w", workDir}
	dockerArgs = append(dockerArgs, splitDockerArgs(c.StringSlice("docker-arg"))...)
	dockerArgs = append(dockerArgs, image)
	dockerArgs = append(dockerArgs, command...)

	fmt.Fprintf(os.Stderr, "Running: %s %s\n", v100.DockerExecutable, strings.Join(dockerArgs, " "))
	describeSchedule(c, os.Stderr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, v100.DockerExecutable, dockerArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting docker: %w", err)
	}

	sampler := v100.NewMetricSampler(containerName, workDir, containerName)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	// Mirrors the poll loop in OpenTaskEventLog: one goroutine owns the
	// sampler, and the final sample is taken by that same goroutine so the
	// CPU baseline is never shared across goroutines.
	pollDone := make(chan struct{})
	finalize := make(chan struct{})
	go func() {
		defer close(pollDone)
		encode := func(final bool) {
			if err := enc.Encode(sampler.Sample(final)); err != nil {
				fmt.Fprintf(os.Stderr, "encoding metric sample: %v\n", err)
			}
		}
		delay := firstDelay(c)
		timer := time.NewTimer(delay)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-finalize:
				encode(true)
				return
			case <-timer.C:
				encode(false)
				delay = nextDelay(c, delay)
				timer.Reset(delay)
			}
		}
	}()

	runErr := cmd.Wait()

	// Take the final sample while the container still exists: "docker rm"
	// destroys its cgroup, and the cumulative counters read here are what
	// close the gap between the last periodic sample and exit.
	close(finalize)
	<-pollDone

	if rmOut, rmErr := exec.Command(v100.DockerExecutable, "rm", "-f", containerName).CombinedOutput(); rmErr != nil {
		fmt.Fprintf(os.Stderr, "docker rm %s: %v: %s\n", containerName, rmErr, rmOut)
	}

	if runErr != nil {
		return fmt.Errorf("docker command failed: %w", runErr)
	}
	return nil
}

// splitDockerArgs lets --docker-arg bundle a flag and its value into one
// shell-quoted string (e.g. --docker-arg='-v /host:/container') instead of
// requiring a separate --docker-arg per token. It only splits on whitespace
// and does not understand quoting, so it can't express an argument that
// itself contains a space.
func splitDockerArgs(args []string) []string {
	var out []string
	for _, arg := range args {
		out = append(out, strings.Fields(arg)...)
	}
	return out
}

// profileExistingContainer samples an already-running container until
// interrupted, without starting or removing anything.
func profileExistingContainer(c *cli.Context, name string) error {
	workDir, err := os.MkdirTemp("", "sparkles-test-profile-*")
	if err != nil {
		return fmt.Errorf("creating work dir: %w", err)
	}
	defer os.RemoveAll(workDir)

	fmt.Fprintf(os.Stderr, "Attaching to container %s (ctrl-c to stop)\n", name)
	describeSchedule(c, os.Stderr)

	sampler := v100.NewMetricSampler(name, workDir, name)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	delay := firstDelay(c)
	for {
		time.Sleep(delay)
		if err := enc.Encode(sampler.Sample(false)); err != nil {
			return fmt.Errorf("encoding metric sample: %w", err)
		}
		delay = nextDelay(c, delay)
	}
}

// firstDelay is the delay before the first sample: the worker's adaptive
// schedule unless --interval pins it to a fixed cadence.
func firstDelay(c *cli.Context) time.Duration {
	if interval := c.Duration("interval"); interval > 0 {
		return interval
	}
	return v100.MetricsFirstDelay
}

// nextDelay advances the schedule, honouring a fixed --interval if given.
func nextDelay(c *cli.Context, cur time.Duration) time.Duration {
	if interval := c.Duration("interval"); interval > 0 {
		return interval
	}
	return v100.NextMetricsDelay(cur)
}

func describeSchedule(c *cli.Context, w *os.File) {
	if interval := c.Duration("interval"); interval > 0 {
		fmt.Fprintf(w, "Sampling metrics every %s\n", interval)
		return
	}
	fmt.Fprintf(w, "Sampling metrics on the adaptive schedule (%s, doubling to %s)\n",
		v100.MetricsFirstDelay, v100.MetricsMaxDelay)
}
