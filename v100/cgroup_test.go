package v100

import (
	"os"
	"path/filepath"
	"testing"
)

// Fixture content captured from a Linux host running cgroup v2.
const (
	// cpuPressureNoFull is the common shape for cpu.pressure: many kernels
	// emit no "full" line at all.
	cpuPressureNoFull = "some avg10=0.31 avg60=0.12 avg300=0.04 total=182938471\n"

	memoryPressureBoth = "some avg10=2.35 avg60=1.12 avg300=0.60 total=12345\n" +
		"full avg10=0.00 avg60=0.00 avg300=0.00 total=678\n"

	// ioStatMultiDevice has two devices plus the trailing newline every such
	// file carries, and a device line with no counters at all.
	ioStatMultiDevice = "8:0 rbytes=1024 wbytes=2048 rios=10 wios=20 dbytes=0 dios=0\n" +
		"253:0 rbytes=512 wbytes=256 rios=5 wios=3 dbytes=0 dios=0\n" +
		"7:1\n"

	cpuStatFull = "usage_usec 1234567\n" +
		"user_usec 900000\n" +
		"system_usec 334567\n" +
		"nr_periods 12\n" +
		"nr_throttled 3\n" +
		"throttled_usec 45000\n"

	// cpuStatOld lacks the throttling keys, as on older kernels.
	cpuStatOld = "usage_usec 500\nuser_usec 300\nsystem_usec 200\n"

	memoryStatFixture = "anon 1048576\n" +
		"file 2097152\n" +
		"pgfault 9001\n" +
		"pgmajfault 42\n" +
		"workingset_refault_anon 7\n" +
		"workingset_refault_file 11\n"

	memoryEventsFixture = "low 0\nhigh 0\nmax 0\noom 0\noom_kill 2\n"
)

func TestParsePressureNoFullLine(t *testing.T) {
	// A kernel that reports no "full" line must yield unavailable for full,
	// not zero. Reporting zero would make a real host look like it never had
	// a full stall, which is a different and much rosier claim.
	got := parsePressure(cpuPressureNoFull)
	if got.SomeUSec != 182938471 {
		t.Errorf("SomeUSec = %d, want 182938471", got.SomeUSec)
	}
	if got.FullUSec != metricUnavailable {
		t.Errorf("FullUSec = %d, want %d (unavailable)", got.FullUSec, metricUnavailable)
	}
}

func TestParsePressureBothLines(t *testing.T) {
	got := parsePressure(memoryPressureBoth)
	if got.SomeUSec != 12345 {
		t.Errorf("SomeUSec = %d, want 12345", got.SomeUSec)
	}
	if got.FullUSec != 678 {
		t.Errorf("FullUSec = %d, want 678", got.FullUSec)
	}
}

func TestParsePressureGarbage(t *testing.T) {
	for name, data := range map[string]string{
		"empty":     "",
		"truncated": "some avg10=0.31",
		"garbage":   "this is not a pressure file\n\n???\n",
		"no total":  "some avg10=0.31 avg60=0.12\nfull avg10=0.00\n",
	} {
		got := parsePressure(data)
		if got.SomeUSec != metricUnavailable || got.FullUSec != metricUnavailable {
			t.Errorf("%s: got %+v, want both unavailable", name, got)
		}
	}
}

func TestParseIOStatV2(t *testing.T) {
	// Sums across devices, counts ops as well as bytes, and survives both the
	// trailing blank line and a device line with no counters -- the latter two
	// panicked the previous implementation.
	got := parseIOStatV2(ioStatMultiDevice)
	want := ioStatV2{ReadBytes: 1536, WriteBytes: 2304, ReadOps: 15, WriteOps: 23}
	if got != want {
		t.Errorf("parseIOStatV2 = %+v, want %+v", got, want)
	}
}

func TestParseIOStatV2Empty(t *testing.T) {
	// A readable but empty io.stat means the container did no I/O, which is
	// zero rather than unavailable.
	got := parseIOStatV2("")
	if got != (ioStatV2{}) {
		t.Errorf("parseIOStatV2(\"\") = %+v, want all zero", got)
	}
}

func TestParseKeyValueFileAndLookup(t *testing.T) {
	m := parseKeyValueFile(cpuStatFull)
	if v := lookupInt64(m, "usage_usec"); v != 1234567 {
		t.Errorf("usage_usec = %d, want 1234567", v)
	}
	if v := lookupInt64(m, "nr_throttled"); v != 3 {
		t.Errorf("nr_throttled = %d, want 3", v)
	}
	if v := lookupInt64(m, "does_not_exist"); v != metricUnavailable {
		t.Errorf("missing key = %d, want %d (unavailable)", v, metricUnavailable)
	}

	old := parseKeyValueFile(cpuStatOld)
	if v := lookupInt64(old, "throttled_usec"); v != metricUnavailable {
		t.Errorf("throttled_usec on old kernel = %d, want %d (unavailable)", v, metricUnavailable)
	}
}

func TestSumInt64(t *testing.T) {
	m := parseKeyValueFile(memoryStatFixture)
	if v := sumInt64(m, "workingset_refault_file", "workingset_refault_anon"); v != 18 {
		t.Errorf("workingset refaults = %d, want 18", v)
	}
	// Present keys are summed even if some are missing.
	if v := sumInt64(m, "pgmajfault", "nope"); v != 42 {
		t.Errorf("partial sum = %d, want 42", v)
	}
	// None present stays unavailable rather than collapsing to zero.
	if v := sumInt64(m, "nope", "also_nope"); v != metricUnavailable {
		t.Errorf("absent sum = %d, want %d (unavailable)", v, metricUnavailable)
	}
}

// writeCgroupDir builds a fake container cgroup directory from name->content.
func writeCgroupDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
	}
	return dir
}

// completeCgroupFiles is a fully-populated cgroup v2 container directory.
func completeCgroupFiles() map[string]string {
	return map[string]string{
		"memory.current":  "536870912\n",
		"memory.peak":     "1073741824\n",
		"memory.max":      "2147483648\n",
		"memory.stat":     memoryStatFixture,
		"memory.events":   memoryEventsFixture,
		"memory.pressure": memoryPressureBoth,
		"cpu.stat":        cpuStatFull,
		"cpu.pressure":    cpuPressureNoFull,
		"io.stat":         ioStatMultiDevice,
		"io.pressure":     memoryPressureBoth,
		"pids.peak":       "17\n",
		"pids.current":    "4\n",
	}
}

func TestReadContainerCountersComplete(t *testing.T) {
	dir := writeCgroupDir(t, completeCgroupFiles())
	c := readContainerCounters(dir)

	checks := map[string]struct{ got, want int64 }{
		"MemoryCurrentBytes":       {c.MemoryCurrentBytes, 536870912},
		"MemoryPeakBytes":          {c.MemoryPeakBytes, 1073741824},
		"MemoryLimitBytes":         {c.MemoryLimitBytes, 2147483648},
		"MemoryMajorFaults":        {c.MemoryMajorFaults, 42},
		"MemoryWorkingsetRefaults": {c.MemoryWorkingsetRefaults, 18},
		"MemoryOOMKillCount":       {c.MemoryOOMKillCount, 2},
		"CPUUsageUSec":             {c.CPUUsageUSec, 1234567},
		"CPUUserUSec":              {c.CPUUserUSec, 900000},
		"CPUSystemUSec":            {c.CPUSystemUSec, 334567},
		"CPUThrottledUSec":         {c.CPUThrottledUSec, 45000},
		"CPUThrottledPeriods":      {c.CPUThrottledPeriods, 3},
		"IOReadBytes":              {c.IOReadBytes, 1536},
		"IOWriteBytes":             {c.IOWriteBytes, 2304},
		"IOReadOps":                {c.IOReadOps, 15},
		"IOWriteOps":               {c.IOWriteOps, 23},
		// pids.peak wins over pids.current when both exist.
		"PidsPeak": {c.PidsPeak, 17},
	}
	for name, ck := range checks {
		if ck.got != ck.want {
			t.Errorf("%s = %d, want %d", name, ck.got, ck.want)
		}
	}

	if c.MemoryStall.SomeUSec != 12345 || c.MemoryStall.FullUSec != 678 {
		t.Errorf("MemoryStall = %+v, want {12345 678}", c.MemoryStall)
	}
	if c.CPUStall.SomeUSec != 182938471 || c.CPUStall.FullUSec != metricUnavailable {
		t.Errorf("CPUStall = %+v, want some=182938471 full=unavailable", c.CPUStall)
	}
}

func TestReadContainerCountersMissingMemoryPeak(t *testing.T) {
	// memory.peak needs kernel >= 5.19. Its absence must not disturb any other
	// field -- this is the sentinel discipline that the old
	// "if err == nil {}"-leaves-zero code got wrong.
	files := completeCgroupFiles()
	delete(files, "memory.peak")
	c := readContainerCounters(writeCgroupDir(t, files))

	if c.MemoryPeakBytes != metricUnavailable {
		t.Errorf("MemoryPeakBytes = %d, want %d (unavailable)", c.MemoryPeakBytes, metricUnavailable)
	}
	if c.MemoryCurrentBytes != 536870912 {
		t.Errorf("MemoryCurrentBytes = %d, want 536870912", c.MemoryCurrentBytes)
	}
	if c.CPUUserUSec != 900000 {
		t.Errorf("CPUUserUSec = %d, want 900000", c.CPUUserUSec)
	}
}

func TestReadContainerCountersPidsFallback(t *testing.T) {
	// Without pids.peak (kernel < 6.1) we fall back to pids.current.
	files := completeCgroupFiles()
	delete(files, "pids.peak")
	c := readContainerCounters(writeCgroupDir(t, files))
	if c.PidsPeak != 4 {
		t.Errorf("PidsPeak = %d, want 4 (from pids.current)", c.PidsPeak)
	}
}

func TestReadContainerCountersUnlimitedMemory(t *testing.T) {
	// memory.max reads "max" when there is no limit; that is unavailable, not
	// a numeric limit of zero.
	files := completeCgroupFiles()
	files["memory.max"] = "max\n"
	c := readContainerCounters(writeCgroupDir(t, files))
	if c.MemoryLimitBytes != metricUnavailable {
		t.Errorf("MemoryLimitBytes = %d, want %d (unavailable)", c.MemoryLimitBytes, metricUnavailable)
	}
}

func TestReadContainerCountersMissingDir(t *testing.T) {
	// A cgroup that has already been torn down must yield all-unavailable and
	// must not error or panic: losing container metrics is acceptable, failing
	// the task over it is not.
	for name, dir := range map[string]string{
		"nonexistent": filepath.Join(t.TempDir(), "gone"),
		"empty path":  "",
	} {
		c := readContainerCounters(dir)
		want := unavailableContainerCounters()
		if c != want {
			t.Errorf("%s: got %+v, want all unavailable", name, c)
		}
	}
}

func TestCgroupDirForPIDV2(t *testing.T) {
	root := t.TempDir()
	procDir := filepath.Join(root, "proc", "4242")
	if err := os.MkdirAll(procDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(procDir, "cgroup"),
		[]byte("0::/system.slice/docker-abc123.scope\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	withRoots(t, filepath.Join(root, "sys"), filepath.Join(root, "proc"))

	got, err := cgroupDirForPID(4242)
	if err != nil {
		t.Fatalf("cgroupDirForPID: %v", err)
	}
	want := filepath.Join(root, "sys", "system.slice", "docker-abc123.scope")
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestCgroupDirForPIDV1IsAnError(t *testing.T) {
	// A cgroup v1 /proc/<pid>/cgroup has no "0::" entry. We must report an
	// error rather than guessing a path that would read as all zeros.
	root := t.TempDir()
	procDir := filepath.Join(root, "proc", "99")
	if err := os.MkdirAll(procDir, 0o755); err != nil {
		t.Fatal(err)
	}
	v1 := "11:blkio:/docker/abc\n4:cpuacct,cpu:/docker/abc\n1:name=systemd:/docker/abc\n"
	if err := os.WriteFile(filepath.Join(procDir, "cgroup"), []byte(v1), 0o644); err != nil {
		t.Fatal(err)
	}

	withRoots(t, filepath.Join(root, "sys"), filepath.Join(root, "proc"))

	if _, err := cgroupDirForPID(99); err == nil {
		t.Fatal("expected an error for a cgroup v1 file, got nil")
	}
}

// withRoots points the cgroup and proc readers at fixture trees for one test.
func withRoots(t *testing.T, sysfs, proc string) {
	t.Helper()
	oldSysfs, oldProc := sysfsRoot, procRoot
	sysfsRoot, procRoot = sysfs, proc
	t.Cleanup(func() { sysfsRoot, procRoot = oldSysfs, oldProc })
}

func TestLocateContainerCgroupCandidatePaths(t *testing.T) {
	const id = "abc123"
	for name, rel := range map[string]string{
		"systemd scope": filepath.Join("system.slice", "docker-"+id+".scope"),
		"flat docker":   filepath.Join("docker", id),
	} {
		root := t.TempDir()
		sysfs := filepath.Join(root, "sys")
		target := filepath.Join(sysfs, rel)
		if err := os.MkdirAll(target, 0o755); err != nil {
			t.Fatal(err)
		}
		// isCgroupDir looks for cpu.stat to confirm it found a real cgroup.
		if err := os.WriteFile(filepath.Join(target, "cpu.stat"), []byte(cpuStatFull), 0o644); err != nil {
			t.Fatal(err)
		}

		withRoots(t, sysfs, filepath.Join(root, "proc"))
		stubDockerInspect(t, id, 0, nil)

		got, err := locateContainerCgroup("sparkles-test")
		if err != nil {
			t.Errorf("%s: locateContainerCgroup: %v", name, err)
			continue
		}
		if got != target {
			t.Errorf("%s: got %q, want %q", name, got, target)
		}
	}
}

func TestLocateContainerCgroupProcFallback(t *testing.T) {
	// Neither conventional path exists, so resolution must fall back to
	// /proc/<pid>/cgroup. This is the case that matters on stock cloud images,
	// where nothing pins where docker puts container cgroups.
	root := t.TempDir()
	sysfs := filepath.Join(root, "sys")
	target := filepath.Join(sysfs, "some", "unexpected", "layout.scope")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "cpu.stat"), []byte(cpuStatFull), 0o644); err != nil {
		t.Fatal(err)
	}
	procDir := filepath.Join(root, "proc", "777")
	if err := os.MkdirAll(procDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(procDir, "cgroup"),
		[]byte("0::/some/unexpected/layout.scope\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	withRoots(t, sysfs, filepath.Join(root, "proc"))
	stubDockerInspect(t, "deadbeef", 777, nil)

	got, err := locateContainerCgroup("sparkles-test")
	if err != nil {
		t.Fatalf("locateContainerCgroup: %v", err)
	}
	if got != target {
		t.Errorf("got %q, want %q", got, target)
	}
}

func TestContainerCgroupGivesUp(t *testing.T) {
	// A container that never appears (it exited before we ever sampled it)
	// must stop shelling out to docker inspect rather than retrying forever.
	root := t.TempDir()
	sysfs := filepath.Join(root, "sys")
	if err := os.MkdirAll(sysfs, 0o755); err != nil {
		t.Fatal(err)
	}
	// cgroupV2Enabled checks for this marker file.
	if err := os.WriteFile(filepath.Join(sysfs, "cgroup.controllers"), []byte("cpu io memory\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	withRoots(t, sysfs, filepath.Join(root, "proc"))

	calls := 0
	oldInspect := dockerInspectIDAndPid
	dockerInspectIDAndPid = func(string) (string, int, error) {
		calls++
		return "", 0, os.ErrNotExist
	}
	t.Cleanup(func() { dockerInspectIDAndPid = oldInspect })

	cg := newContainerCgroup("sparkles-missing")
	for i := 0; i < maxCgroupResolveAttempts+3; i++ {
		if dir := cg.resolve(); dir != "" {
			t.Fatalf("attempt %d unexpectedly resolved to %q", i, dir)
		}
	}
	if calls != maxCgroupResolveAttempts {
		t.Errorf("docker inspect called %d times, want %d", calls, maxCgroupResolveAttempts)
	}
}

// stubDockerInspect replaces the docker inspect shell-out for one test.
func stubDockerInspect(t *testing.T, id string, pid int, err error) {
	t.Helper()
	old := dockerInspectIDAndPid
	dockerInspectIDAndPid = func(string) (string, int, error) { return id, pid, err }
	t.Cleanup(func() { dockerInspectIDAndPid = old })
}
