package v100

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// collectDockerResourceUsage gathers resource metrics for a named, stopped (but not yet
// removed) Docker container. It uses docker inspect for timing and cgroup files for
// peak memory / CPU. Missing cgroup files are silently skipped; the caller gets a
// best-effort ResourceUsage rather than an error.
func collectDockerResourceUsage(containerName string) *ResourceUsage {
	state, err := dockerInspectState(containerName)
	if err != nil {
		log.Printf("resource_usage: docker inspect %s: %v", containerName, err)
		return nil
	}

	containerID, err := dockerInspectID(containerName)
	if err != nil {
		log.Printf("resource_usage: getting container ID for %s: %v", containerName, err)
		// Still return timing-only usage.
		ru := &ResourceUsage{
			StartTime:      state.StartedAt,
			EndTime:        state.FinishedAt,
			ElapsedSeconds: state.FinishedAt.Sub(state.StartedAt).Seconds(),
			ExitCode:       state.ExitCode,
			OOMKilled:      state.OOMKilled,
		}
		return ru
	}

	ru := &ResourceUsage{
		StartTime:      state.StartedAt,
		EndTime:        state.FinishedAt,
		ElapsedSeconds: state.FinishedAt.Sub(state.StartedAt).Seconds(),
		ExitCode:       state.ExitCode,
		OOMKilled:      state.OOMKilled,
	}

	if isCgroupV2() {
		fillCgroupV2(containerID, ru)
	} else {
		fillCgroupV1(containerID, ru)
	}

	return ru
}

// dockerState is the subset of `docker inspect .State` we care about.
type dockerState struct {
	StartedAt  time.Time `json:"StartedAt"`
	FinishedAt time.Time `json:"FinishedAt"`
	ExitCode   int       `json:"ExitCode"`
	OOMKilled  bool      `json:"OOMKilled"`
}

func dockerInspectState(name string) (*dockerState, error) {
	out, err := exec.Command("docker", "inspect", "--format", "{{json .State}}", name).Output()
	if err != nil {
		return nil, fmt.Errorf("docker inspect: %w", err)
	}
	var s dockerState
	if err := json.Unmarshal(out, &s); err != nil {
		return nil, fmt.Errorf("parsing docker inspect output: %w", err)
	}
	return &s, nil
}

func dockerInspectID(name string) (string, error) {
	out, err := exec.Command("docker", "inspect", "--format", "{{.Id}}", name).Output()
	if err != nil {
		return "", fmt.Errorf("docker inspect id: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// isCgroupV2 returns true when the host uses cgroup v2 (unified hierarchy).
func isCgroupV2() bool {
	_, err := os.Stat("/sys/fs/cgroup/cgroup.controllers")
	return err == nil
}

// fillCgroupV2 reads peak memory, CPU, and block I/O from cgroup v2 files.
func fillCgroupV2(containerID string, ru *ResourceUsage) {
	base := fmt.Sprintf("/sys/fs/cgroup/system.slice/docker-%s.scope", containerID)
	if _, err := os.Stat(base); os.IsNotExist(err) {
		// Some Docker configurations use a flat path instead.
		base = filepath.Join("/sys/fs/cgroup/docker", containerID)
	}

	if v, err := readInt64File(filepath.Join(base, "memory.peak")); err == nil {
		ru.MaxMemoryBytes = v
	}

	if data, err := os.ReadFile(filepath.Join(base, "cpu.stat")); err == nil {
		ru.CPUUserUSec, ru.CPUSystemUSec = parseCPUStatV2(string(data))
	}

	if data, err := os.ReadFile(filepath.Join(base, "io.stat")); err == nil {
		ru.BlockReadBytes, ru.BlockWriteBytes = parseIOStatV2(string(data))
	}
}

// fillCgroupV1 reads peak memory and CPU from cgroup v1 files.
func fillCgroupV1(containerID string, ru *ResourceUsage) {
	memBase := filepath.Join("/sys/fs/cgroup/memory/docker", containerID)
	cpuBase := filepath.Join("/sys/fs/cgroup/cpuacct/docker", containerID)
	blkBase := filepath.Join("/sys/fs/cgroup/blkio/docker", containerID)

	if v, err := readInt64File(filepath.Join(memBase, "memory.max_usage_in_bytes")); err == nil {
		ru.MaxMemoryBytes = v
	}
	// cgroup v1 cpuacct values are in nanoseconds; convert to microseconds.
	if v, err := readInt64File(filepath.Join(cpuBase, "cpuacct.usage_user")); err == nil {
		ru.CPUUserUSec = v / 1000
	}
	if v, err := readInt64File(filepath.Join(cpuBase, "cpuacct.usage_sys")); err == nil {
		ru.CPUSystemUSec = v / 1000
	}

	if data, err := os.ReadFile(filepath.Join(blkBase, "blkio.throttle.io_service_bytes")); err == nil {
		ru.BlockReadBytes, ru.BlockWriteBytes = parseBlkioV1(string(data))
	}
}

// readInt64File reads a single int64 value from a file.
func readInt64File(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
}

// parseCPUStatV2 extracts user_usec and system_usec from cgroup v2 cpu.stat content.
// Format: "key value\n..." where values are in microseconds.
func parseCPUStatV2(data string) (userUSec, systemUSec int64) {
	for _, line := range strings.Split(data, "\n") {
		parts := strings.Fields(line)
		if len(parts) != 2 {
			continue
		}
		v, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}
		switch parts[0] {
		case "user_usec":
			userUSec = v
		case "system_usec":
			systemUSec = v
		}
	}
	return
}

// parseIOStatV2 sums rbytes and wbytes across all devices from cgroup v2 io.stat.
// Format per line: "major:minor rbytes=N wbytes=N ..."
func parseIOStatV2(data string) (readBytes, writeBytes int64) {
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Fields(line)
		for _, f := range fields[1:] { // skip "major:minor"
			kv := strings.SplitN(f, "=", 2)
			if len(kv) != 2 {
				continue
			}
			v, err := strconv.ParseInt(kv[1], 10, 64)
			if err != nil {
				continue
			}
			switch kv[0] {
			case "rbytes":
				readBytes += v
			case "wbytes":
				writeBytes += v
			}
		}
	}
	return
}

// parseBlkioV1 extracts Read and Write totals from cgroup v1 blkio.throttle.io_service_bytes.
// Format: "major:minor Read N\nmajor:minor Write N\n...\nTotal N"
func parseBlkioV1(data string) (readBytes, writeBytes int64) {
	for _, line := range strings.Split(data, "\n") {
		parts := strings.Fields(line)
		if len(parts) != 3 {
			continue
		}
		v, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			continue
		}
		switch parts[1] {
		case "Read":
			readBytes += v
		case "Write":
			writeBytes += v
		}
	}
	return
}
