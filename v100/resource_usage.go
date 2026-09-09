package v100

import (
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"time"
)

// buildResourceUsage assembles a task's resource summary from docker's own
// timing/exit report and the metrics poller's final sample.
//
// The two sources are deliberately separate: docker inspect knows when the
// container started and how it exited, while the cgroup counters in the final
// sample cover what it consumed. Splitting them means a container whose cgroup
// has already been torn down still yields correct timing and exit information
// with -1 for the counters, rather than nothing at all.
func buildResourceUsage(containerName string, final *MetricSample) *ResourceUsage {
	ru := &ResourceUsage{}
	setUnavailableContainerUsage(ru)

	if state, err := dockerInspectState(containerName); err != nil {
		// Without docker inspect there is no timing at all, but a final sample
		// may still carry usable counters, so this is not fatal.
		log.Printf("resource_usage: docker inspect %s: %v", containerName, err)
	} else {
		ru.StartTime = state.StartedAt
		ru.EndTime = state.FinishedAt
		ru.ElapsedSeconds = state.FinishedAt.Sub(state.StartedAt).Seconds()
		ru.ExitCode = state.ExitCode
		ru.OOMKilled = state.OOMKilled
	}

	if final == nil {
		log.Printf("resource_usage: no final metric sample for container %s; container counters unavailable", containerName)
		return ru
	}

	ru.ContainerCPUUsageUSec = int64OrUnavailable(final.ContainerCPUUsageUSec)
	ru.ContainerCPUUserUSec = int64OrUnavailable(final.ContainerCPUUserUSec)
	ru.ContainerCPUSystemUSec = int64OrUnavailable(final.ContainerCPUSystemUSec)
	ru.ContainerCPUThrottledUSec = int64OrUnavailable(final.ContainerCPUThrottledUSec)
	ru.ContainerCPUThrottledPeriod = int64OrUnavailable(final.ContainerCPUThrottledPeriods)

	ru.ContainerMemoryPeakBytes = int64OrUnavailable(final.ContainerMemoryPeakBytes)
	ru.ContainerMemoryLimitBytes = int64OrUnavailable(final.ContainerMemoryLimitBytes)
	ru.ContainerMemoryMajorFaults = int64OrUnavailable(final.ContainerMemoryMajorFaults)
	ru.ContainerMemoryWorkingsetRefaults = int64OrUnavailable(final.ContainerMemoryWorkingsetRefaults)
	ru.ContainerOOMKillCount = int64OrUnavailable(final.ContainerMemoryOOMKillCount)

	ru.ContainerCPUStallSomeUSec = int64OrUnavailable(final.ContainerCPUStallSomeUSec)
	ru.ContainerCPUStallFullUSec = int64OrUnavailable(final.ContainerCPUStallFullUSec)
	ru.ContainerMemoryStallSomeUSec = int64OrUnavailable(final.ContainerMemoryStallSomeUSec)
	ru.ContainerMemoryStallFullUSec = int64OrUnavailable(final.ContainerMemoryStallFullUSec)
	ru.ContainerIOStallSomeUSec = int64OrUnavailable(final.ContainerIOStallSomeUSec)
	ru.ContainerIOStallFullUSec = int64OrUnavailable(final.ContainerIOStallFullUSec)

	ru.ContainerIOReadBytes = int64OrUnavailable(final.ContainerIOReadBytes)
	ru.ContainerIOWriteBytes = int64OrUnavailable(final.ContainerIOWriteBytes)
	ru.ContainerIOReadOps = int64OrUnavailable(final.ContainerIOReadOps)
	ru.ContainerIOWriteOps = int64OrUnavailable(final.ContainerIOWriteOps)

	ru.ContainerPidsPeak = int64OrUnavailable(final.ContainerPidsPeak)

	return ru
}

// int64OrUnavailable unwraps a MetricSample counter (nil when the metric
// could not be read) back into ResourceUsage's -1-sentinel convention.
func int64OrUnavailable(p *int64) int64 {
	if p == nil {
		return metricUnavailable
	}
	return *p
}

// setUnavailableContainerUsage marks every container counter as unavailable,
// so a summary we could not populate reports -1 rather than reading as a task
// that consumed nothing.
func setUnavailableContainerUsage(ru *ResourceUsage) {
	ru.ContainerOOMKillCount = metricUnavailable
	ru.ContainerCPUUsageUSec = metricUnavailable
	ru.ContainerCPUUserUSec = metricUnavailable
	ru.ContainerCPUSystemUSec = metricUnavailable
	ru.ContainerCPUThrottledUSec = metricUnavailable
	ru.ContainerCPUThrottledPeriod = metricUnavailable
	ru.ContainerMemoryPeakBytes = metricUnavailable
	ru.ContainerMemoryLimitBytes = metricUnavailable
	ru.ContainerMemoryMajorFaults = metricUnavailable
	ru.ContainerMemoryWorkingsetRefaults = metricUnavailable
	ru.ContainerCPUStallSomeUSec = metricUnavailable
	ru.ContainerCPUStallFullUSec = metricUnavailable
	ru.ContainerMemoryStallSomeUSec = metricUnavailable
	ru.ContainerMemoryStallFullUSec = metricUnavailable
	ru.ContainerIOStallSomeUSec = metricUnavailable
	ru.ContainerIOStallFullUSec = metricUnavailable
	ru.ContainerIOReadBytes = metricUnavailable
	ru.ContainerIOWriteBytes = metricUnavailable
	ru.ContainerIOReadOps = metricUnavailable
	ru.ContainerIOWriteOps = metricUnavailable
	ru.ContainerPidsPeak = metricUnavailable
}

// dockerState is the subset of `docker inspect .State` we care about.
type dockerState struct {
	StartedAt  time.Time `json:"StartedAt"`
	FinishedAt time.Time `json:"FinishedAt"`
	ExitCode   int       `json:"ExitCode"`
	OOMKilled  bool      `json:"OOMKilled"`
}

func dockerInspectState(name string) (*dockerState, error) {
	out, err := exec.Command(dockerExecutable, "inspect", "--format", "{{json .State}}", name).Output()
	if err != nil {
		return nil, fmt.Errorf("docker inspect: %w", err)
	}
	var s dockerState
	if err := json.Unmarshal(out, &s); err != nil {
		return nil, fmt.Errorf("parsing docker inspect output: %w", err)
	}
	return &s, nil
}
