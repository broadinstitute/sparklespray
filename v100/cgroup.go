package v100

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// sysfsRoot and procRoot are variables rather than constants so tests can point
// the cgroup and /proc readers at fixture trees.
var (
	sysfsRoot = "/sys/fs/cgroup"
	procRoot  = "/proc"
)

// metricUnavailable is reported for any metric we could not read. It is
// deliberately distinct from a genuine zero: "the container did no I/O" and
// "this kernel does not expose I/O accounting" are different facts, and
// conflating them makes a missing metric look like an idle container.
const metricUnavailable int64 = -1

// cgroupV2WarnOnce ensures we complain about a v1 host at most once per process.
var cgroupV2WarnOnce sync.Once

// cgroupV2Enabled reports whether the host uses the cgroup v2 unified
// hierarchy. cgroup v1 is unsupported: it exposes no PSI at all, which is the
// centerpiece of the metrics we collect, so container metrics are reported as
// unavailable there.
func cgroupV2Enabled() bool {
	_, err := os.Stat(filepath.Join(sysfsRoot, "cgroup.controllers"))
	if err != nil {
		cgroupV2WarnOnce.Do(func() {
			log.Printf("metrics: cgroup v2 not available; container metrics disabled")
		})
		return false
	}
	return true
}

// stallTotals holds the cumulative PSI stall counters from one pressure file.
// Both fields are metricUnavailable when the corresponding line is absent --
// notably many kernels emit no "full" line for cpu.pressure at all, and
// full=0 is a different fact from full=absent.
type stallTotals struct {
	SomeUSec int64
	FullUSec int64
}

func unavailableStallTotals() stallTotals {
	return stallTotals{SomeUSec: metricUnavailable, FullUSec: metricUnavailable}
}

// parsePressure extracts the cumulative total= counters from PSI file content:
//
//	some avg10=0.00 avg60=0.00 avg300=0.00 total=12345
//	full avg10=0.00 avg60=0.00 avg300=0.00 total=678
//
// The avg* fields are deliberately ignored. They are decaying averages with a
// ~10s ramp, so they are meaningless for a task that runs for three seconds,
// whereas total= differences exactly over any interval and composes with the
// post-exit read of the same counter.
func parsePressure(data string) stallTotals {
	st := unavailableStallTotals()
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		var target *int64
		switch fields[0] {
		case "some":
			target = &st.SomeUSec
		case "full":
			target = &st.FullUSec
		default:
			continue
		}
		for _, f := range fields[1:] {
			if !strings.HasPrefix(f, "total=") {
				continue
			}
			if v, err := strconv.ParseInt(strings.TrimPrefix(f, "total="), 10, 64); err == nil {
				*target = v
			}
			break
		}
	}
	return st
}

// readPressureFile reads and parses a PSI file, yielding unavailable sentinels
// when it cannot be read (no PSI support, or a cgroup that has gone away).
func readPressureFile(path string) stallTotals {
	data, err := os.ReadFile(path)
	if err != nil {
		return unavailableStallTotals()
	}
	return parsePressure(string(data))
}

// parseKeyValueFile parses the "key value" line format shared by cpu.stat,
// memory.stat and memory.events. Malformed lines are skipped.
func parseKeyValueFile(data string) map[string]int64 {
	out := make(map[string]int64)
	for _, line := range strings.Split(data, "\n") {
		parts := strings.Fields(line)
		if len(parts) != 2 {
			continue
		}
		v, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}
		out[parts[0]] = v
	}
	return out
}

// lookupInt64 returns the value for key, or metricUnavailable when absent.
func lookupInt64(m map[string]int64, key string) int64 {
	if v, ok := m[key]; ok {
		return v
	}
	return metricUnavailable
}

// sumInt64 adds the values for the given keys, returning metricUnavailable
// only when none of them are present. Used for counters that are split across
// several keys, such as workingset refaults.
func sumInt64(m map[string]int64, keys ...string) int64 {
	total := metricUnavailable
	for _, k := range keys {
		v, ok := m[k]
		if !ok {
			continue
		}
		if total == metricUnavailable {
			total = 0
		}
		total += v
	}
	return total
}

// ioStatV2 aggregates cgroup v2 io.stat across every device.
type ioStatV2 struct {
	ReadBytes  int64
	WriteBytes int64
	ReadOps    int64
	WriteOps   int64
}

func unavailableIOStat() ioStatV2 {
	return ioStatV2{metricUnavailable, metricUnavailable, metricUnavailable, metricUnavailable}
}

// parseIOStatV2 sums the per-device counters in cgroup v2 io.stat. Format per
// line: "major:minor rbytes=N wbytes=N rios=N wios=N ...".
//
// Counters start at zero rather than unavailable: this function is only
// reached once the file has been read, and a readable io.stat with no matching
// keys means the container did no I/O. The unavailable sentinel is reserved
// for the case where the file itself could not be read.
//
// Lines without at least a device and one key=value pair are skipped, which
// includes the trailing blank line these files always end with -- the previous
// implementation indexed fields[1:] unguarded and panicked on exactly that.
func parseIOStatV2(data string) ioStatV2 {
	var s ioStatV2
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
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
				s.ReadBytes += v
			case "wbytes":
				s.WriteBytes += v
			case "rios":
				s.ReadOps += v
			case "wios":
				s.WriteOps += v
			}
		}
	}
	return s
}

// readCgroupInt64 reads a single-value cgroup file. The literal "max" (used by
// limit files to mean unlimited) is reported as unavailable.
func readCgroupInt64(dir, name string) int64 {
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return metricUnavailable
	}
	text := strings.TrimSpace(string(data))
	if text == "max" {
		return metricUnavailable
	}
	v, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return metricUnavailable
	}
	return v
}

// readCgroupKeyValue reads and parses a "key value" cgroup file. The bool
// reports whether the file could be read at all.
func readCgroupKeyValue(dir, name string) (map[string]int64, bool) {
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return nil, false
	}
	return parseKeyValueFile(string(data)), true
}

// containerCounters is one read of a container's cgroup files. Every field is
// metricUnavailable unless it was successfully read, so a missing file or a
// cgroup that has already been torn down is never mistaken for a real zero.
type containerCounters struct {
	MemoryCurrentBytes       int64
	MemoryPeakBytes          int64
	MemoryLimitBytes         int64
	MemoryMajorFaults        int64
	MemoryWorkingsetRefaults int64
	MemoryOOMKillCount       int64

	CPUUsageUSec        int64
	CPUUserUSec         int64
	CPUSystemUSec       int64
	CPUThrottledUSec    int64
	CPUThrottledPeriods int64

	CPUStall    stallTotals
	MemoryStall stallTotals
	IOStall     stallTotals

	IOReadBytes  int64
	IOWriteBytes int64
	IOReadOps    int64
	IOWriteOps   int64

	PidsPeak int64
}

func unavailableContainerCounters() containerCounters {
	io := unavailableIOStat()
	return containerCounters{
		MemoryCurrentBytes:       metricUnavailable,
		MemoryPeakBytes:          metricUnavailable,
		MemoryLimitBytes:         metricUnavailable,
		MemoryMajorFaults:        metricUnavailable,
		MemoryWorkingsetRefaults: metricUnavailable,
		MemoryOOMKillCount:       metricUnavailable,
		CPUUsageUSec:             metricUnavailable,
		CPUUserUSec:              metricUnavailable,
		CPUSystemUSec:            metricUnavailable,
		CPUThrottledUSec:         metricUnavailable,
		CPUThrottledPeriods:      metricUnavailable,
		CPUStall:                 unavailableStallTotals(),
		MemoryStall:              unavailableStallTotals(),
		IOStall:                  unavailableStallTotals(),
		IOReadBytes:              io.ReadBytes,
		IOWriteBytes:             io.WriteBytes,
		IOReadOps:                io.ReadOps,
		IOWriteOps:               io.WriteOps,
		PidsPeak:                 metricUnavailable,
	}
}

// readContainerCounters reads every counter we track from a container's cgroup
// directory. An unreadable or vanished cgroup is not an error: the caller gets
// unavailable sentinels for whatever could not be read. That is deliberate --
// losing container metrics is acceptable, failing the task over it is not.
func readContainerCounters(dir string) containerCounters {
	c := unavailableContainerCounters()
	if dir == "" {
		return c
	}

	c.MemoryCurrentBytes = readCgroupInt64(dir, "memory.current")
	c.MemoryPeakBytes = readCgroupInt64(dir, "memory.peak") // kernel >= 5.19
	c.MemoryLimitBytes = readCgroupInt64(dir, "memory.max") // "max" => unavailable

	// pids.peak needs kernel >= 6.1; pids.current is the best available
	// substitute on older kernels even though it can miss a transient spike.
	c.PidsPeak = readCgroupInt64(dir, "pids.peak")
	if c.PidsPeak == metricUnavailable {
		c.PidsPeak = readCgroupInt64(dir, "pids.current")
	}

	if m, ok := readCgroupKeyValue(dir, "cpu.stat"); ok {
		c.CPUUsageUSec = lookupInt64(m, "usage_usec")
		c.CPUUserUSec = lookupInt64(m, "user_usec")
		c.CPUSystemUSec = lookupInt64(m, "system_usec")
		c.CPUThrottledUSec = lookupInt64(m, "throttled_usec")
		c.CPUThrottledPeriods = lookupInt64(m, "nr_throttled")
	}

	if m, ok := readCgroupKeyValue(dir, "memory.stat"); ok {
		c.MemoryMajorFaults = lookupInt64(m, "pgmajfault")
		c.MemoryWorkingsetRefaults = sumInt64(m, "workingset_refault_file", "workingset_refault_anon")
	}

	// memory.events oom_kill counts OOM-killed children too, which docker's
	// own OOMKilled flag misses because it only reflects the main process.
	if m, ok := readCgroupKeyValue(dir, "memory.events"); ok {
		c.MemoryOOMKillCount = lookupInt64(m, "oom_kill")
	}

	if data, err := os.ReadFile(filepath.Join(dir, "io.stat")); err == nil {
		io := parseIOStatV2(string(data))
		c.IOReadBytes, c.IOWriteBytes = io.ReadBytes, io.WriteBytes
		c.IOReadOps, c.IOWriteOps = io.ReadOps, io.WriteOps
	}

	c.CPUStall = readPressureFile(filepath.Join(dir, "cpu.pressure"))
	c.MemoryStall = readPressureFile(filepath.Join(dir, "memory.pressure"))
	c.IOStall = readPressureFile(filepath.Join(dir, "io.pressure"))

	return c
}

// maxCgroupResolveAttempts bounds how many times we shell out to docker
// inspect for a container whose cgroup we have never managed to locate. A
// sub-second container can exit before the first sample, and there is no point
// retrying forever for a cgroup that no longer exists.
const maxCgroupResolveAttempts = 50

// containerCgroup resolves and caches the cgroup directory for one container.
// Resolution has to be lazy: the metrics poller starts before docker has
// created the container, so early attempts legitimately fail.
type containerCgroup struct {
	name string

	mu       sync.Mutex
	dir      string
	resolved bool
	attempts int
	gaveUp   bool
}

func newContainerCgroup(name string) *containerCgroup {
	return &containerCgroup{name: name}
}

// resolve returns the container's cgroup directory, locating it on first use.
// It returns "" when the cgroup cannot be found, which callers treat as
// "container metrics unavailable" rather than as an error.
func (c *containerCgroup) resolve() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.resolved {
		return c.dir
	}
	if c.gaveUp || !cgroupV2Enabled() {
		return ""
	}

	dir, err := locateContainerCgroup(c.name)
	if err != nil {
		c.attempts++
		if c.attempts >= maxCgroupResolveAttempts {
			// it can take a while for the container to appear because we start polling before we've finished pulling the docker image
			c.gaveUp = true
			log.Printf("metrics: giving up locating cgroup for container %s: %v", c.name, err)
		}
		return ""
	}

	c.dir = dir
	c.resolved = true
	return dir
}

// dockerInspectIDAndPid returns a container's full ID and main PID in a single
// docker inspect call. Both are needed: the ID for the conventional cgroup
// paths, the PID for the authoritative /proc fallback. It is a variable so
// tests can stub out the shell-out.
var dockerInspectIDAndPid = func(name string) (string, int, error) {
	cmd := exec.Command(dockerExecutable, "inspect", "--format", "{{.Id}} {{.State.Pid}}", name)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", 0, fmt.Errorf("docker inspect: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	fields := strings.Fields(string(out))
	if len(fields) != 2 {
		return "", 0, fmt.Errorf("unexpected docker inspect output %q", strings.TrimSpace(string(out)))
	}
	pid, err := strconv.Atoi(fields[1])
	if err != nil {
		return "", 0, fmt.Errorf("parsing container pid %q: %w", fields[1], err)
	}
	return fields[0], pid, nil
}

// locateContainerCgroup finds the cgroup directory for a container.
func locateContainerCgroup(name string) (string, error) {
	id, pid, err := dockerInspectIDAndPid(name)
	if err != nil {
		return "", err
	}

	// The two layouts docker commonly uses, checked directly.
	for _, dir := range []string{
		filepath.Join(sysfsRoot, "system.slice", fmt.Sprintf("docker-%s.scope", id)),
		filepath.Join(sysfsRoot, "docker", id),
	} {
		if isCgroupDir(dir) {
			return dir, nil
		}
	}

	// Authoritative fallback: ask the kernel where the process actually lives.
	// This is driver-agnostic, so it copes with whatever layout the host's
	// docker uses -- which matters because nothing pins the cgroup layout of
	// the stock GCE images the worker runs on.
	if pid > 0 {
		dir, err := cgroupDirForPID(pid)
		if err != nil {
			return "", err
		}
		if isCgroupDir(dir) {
			return dir, nil
		}
		return "", fmt.Errorf("cgroup dir %s for container %s is not readable", dir, name)
	}

	return "", fmt.Errorf("no cgroup directory found for container %s (id %s)", name, id)
}

// isCgroupDir reports whether dir looks like a cgroup v2 directory we can read
// counters from.
func isCgroupDir(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, "cpu.stat"))
	return err == nil
}

// cgroupDirForPID maps a PID to its cgroup directory via /proc/<pid>/cgroup.
// Under cgroup v2 that file holds a single "0::<relpath>" line; a cgroup v1
// file has no such entry and yields an error rather than a wrong path.
func cgroupDirForPID(pid int) (string, error) {
	path := filepath.Join(procRoot, strconv.Itoa(pid), "cgroup")
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", path, err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		rel, ok := strings.CutPrefix(strings.TrimSpace(line), "0::")
		if !ok {
			continue
		}
		return filepath.Join(sysfsRoot, rel), nil
	}
	return "", fmt.Errorf("%s has no cgroup v2 (0::) entry", path)
}
