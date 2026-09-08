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

	if final == nil || !final.ContainerPresent {
		if final == nil {
			log.Printf("resource_usage: no final metric sample for container %s; container counters unavailable", containerName)
		}
		return ru
	}

	ru.ContainerCPUUsageUSec = final.ContainerCPUUsageUSec
	ru.ContainerCPUUserUSec = final.ContainerCPUUserUSec
	ru.ContainerCPUSystemUSec = final.ContainerCPUSystemUSec
	ru.ContainerCPUThrottledUSec = final.ContainerCPUThrottledUSec
	ru.ContainerCPUThrottledPeriod = final.ContainerCPUThrottledPeriods

	ru.ContainerMemoryPeakBytes = final.ContainerMemoryPeakBytes
	ru.ContainerMemoryLimitBytes = final.ContainerMemoryLimitBytes
	ru.ContainerMemoryMajorFaults = final.ContainerMemoryMajorFaults
	ru.ContainerMemoryWorkingsetRefaults = final.ContainerMemoryWorkingsetRefaults
	ru.ContainerOOMKillCount = final.ContainerMemoryOOMKillCount

	ru.ContainerCPUStallSomeUSec = final.ContainerCPUStallSomeUSec
	ru.ContainerCPUStallFullUSec = final.ContainerCPUStallFullUSec
	ru.ContainerMemoryStallSomeUSec = final.ContainerMemoryStallSomeUSec
	ru.ContainerMemoryStallFullUSec = final.ContainerMemoryStallFullUSec
	ru.ContainerIOStallSomeUSec = final.ContainerIOStallSomeUSec
	ru.ContainerIOStallFullUSec = final.ContainerIOStallFullUSec

	ru.ContainerIOReadBytes = final.ContainerIOReadBytes
	ru.ContainerIOWriteBytes = final.ContainerIOWriteBytes
	ru.ContainerIOReadOps = final.ContainerIOReadOps
	ru.ContainerIOWriteOps = final.ContainerIOWriteOps

	ru.ContainerPidsPeak = final.ContainerPidsPeak

	return ru
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
