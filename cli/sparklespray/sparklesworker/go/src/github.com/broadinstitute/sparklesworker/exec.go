package sparklesworker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/logging"
	"github.com/bmatcuk/doublestar"
)

type TaskDownload struct {
	IsCASKey    bool   `json:"is_cas_key"`
	Executable  bool   `json:"executable"`
	SymlinkSafe bool   `json:"symlink_safe"`
	Dst         string `json:"dst"`
	SrcURL      string `json:"src_url"`
}

type UploadSpec struct {
	IncludePatterns []string `json:"include_patterns"`
	ExcludePatterns []string `json:"exclude_patterns"`
	DstURL          string   `json:"dst_url"`
}

type TaskSpec struct {
	WorkingDir         string            `json:"working_dir,omitempty"`
	PreDownloadScript  string            `json:"pre-download-script,omitempty"`
	PostDownloadScript string            `json:"post-download-script,omitempty"`
	PostExecScript     string            `json:"post-exec-script,omitempty"`
	PreExecScript      string            `json:"pre-exec-script,omitempty"`
	Parameters         map[string]string `json:"parameters,omitempty"`
	Uploads            *UploadSpec       `json:"uploads"`
	Downloads          []*TaskDownload   `json:"downloads"`
	Command            string            `json:"command"`
	CommandResultURL   string            `json:"command_result_url"`
	StdoutURL          string            `json:"stdout_url"`
}

type ResultFile struct {
	Src    string `json:"src"`
	DstURL string `json:"dst_url"`
}

type ResourceUsage struct {
	UserCPUTime        syscall.Timeval `json:"user_cpu_time"`
	SystemCPUTime      syscall.Timeval `json:"system_cpu_time"`
	MaxMemorySize      int64           `json:"max_memory_size"`
	SharedMemorySize   int64           `json:"shared_memory_size"`
	UnsharedMemorySize int64           `json:"unshared_memory_size"`
	BlockInputOps      int64           `json:"block_input_ops"`
	BlockOutputOps     int64           `json:"block_output_ops"`
}

// PerfStats holds whole-machine memory/CPU usage sampled periodically while a task's
// command runs, to capture resource usage that a single process's rusage misses
// (e.g. when the command spawns additional child processes). Memory figures are
// absolute (total machine memory in use at the moment of the sample), not relative to
// whatever else was running when the task started, so MaxMemoryInUse can be compared
// directly against MemoryTotal to see how close the machine came to running out.
type PerfStats struct {
	MachineType               string  `json:"machine_type"`
	MemoryTotal               int64   `json:"memory_total"`
	NumberOfCPUs              int     `json:"number_of_cpus"`
	MaxMemoryInUse            int64   `json:"max_memory_in_use"`
	P95MemoryInUse            int64   `json:"p95_memory_in_use"`
	MedianMemoryInUse         int64   `json:"median_memory_in_use"`
	MaxUserCPUPercentUsage    float64 `json:"max_user_cpu_percent_usage"`
	P95UserCPUPercentUsage    float64 `json:"p95_user_cpu_percent_usage"`
	MedianUserCPUPercentUsage float64 `json:"median_user_cpu_percent_usage"`
}

type ResultStruct struct {
	Command      string            `json:"command"`
	Parameters   map[string]string `json:"parameters,omitempty"`
	ReturnCode   string            `json:"return_code"`
	Files        []*ResultFile     `json:"files"`
	Usage        *ResourceUsage    `json:"resource_usage"`
	MachineStats *PerfStats        `json:"machine_stats"`
}

// The amount of time we're willing to wait for a file we uploaded to appear in cloud storage
// Based on the docs, it sounds like it should be visible immediately, but adding some buffer
// just in case we can't rely on that.
const MaxUploadDelay = 30 * time.Second

type stringset map[string]bool

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}

	_, err = io.Copy(NotifyOnWrite(out), in)
	if err != nil {
		out.Close()
		return err
	}

	return out.Close()
}

func downloadAll(ioc IOClient, workdir string, downloads []*TaskDownload, cacheDir string) (error, stringset) {
	if !path.IsAbs(workdir) {
		panic("bad workdir")
	}
	downloaded := make(stringset)

	for _, dl := range downloads {
		srcURL := dl.SrcURL
		destination := path.Join(workdir, dl.Dst)

		if _, err := os.Stat(destination); err == nil {
			log.Printf("Skipping download of %s -> %s because file already exists.", srcURL, destination)
			continue
		}

		//parentDir := strings.ToLower(path.Base(path.Dir(srcURL)))
		if dl.IsCASKey {
			casKey := path.Base(srcURL)
			if _, err := os.Stat(cacheDir); os.IsNotExist(err) {
				err = os.MkdirAll(cacheDir, 0777)
				if err != nil {
					return fmt.Errorf("Could not create cacheDir %s: %s", cacheDir, err), downloaded
				}
			}

			cacheDest := path.Join(cacheDir, casKey)

			if _, err := os.Stat(cacheDest); os.IsNotExist(err) {
				log.Printf("Downloading %s -> %s", srcURL, destination)
				err = ioc.Download(srcURL, cacheDest)
				if err != nil {
					return err, downloaded
				}
			} else {
				log.Printf("No download, %s already exists", cacheDest)
			}

			dstDir := path.Dir(destination)
			if _, err := os.Stat(dstDir); os.IsNotExist(err) {
				err = os.MkdirAll(dstDir, 0777)
				if err != nil {
					return fmt.Errorf("Could not create directory %s: %s", dstDir, err), downloaded
				}
			}

			if _, err := os.Stat(destination); !os.IsNotExist(err) {
				log.Printf("Warning: Removing existing file %s", destination)
				err = os.Remove(destination)
				if err != nil {
					return fmt.Errorf("Could not remove existing file %s that is the destination of new DL", destination, err), downloaded
				}
			}

			if dl.SymlinkSafe {
				log.Printf("Symlinking %s -> %s", cacheDest, destination)
				err := os.Symlink(cacheDest, destination)
				if err != nil {
					panic(fmt.Sprintf("symlink %s -> %s failed: %s", cacheDest, destination, err))
				}
			} else {
				log.Printf("copying %s -> %s", cacheDest, destination)
				err := copyFile(cacheDest, destination)
				if err != nil {
					// this should not be possible
					panic(fmt.Sprintf("copyFile %s -> %s failed: %s", cacheDest, destination, err))
				}
			}

		} else {
			log.Printf("Downloading %s -> %s", srcURL, destination)
			err := ioc.Download(srcURL, destination)
			if err != nil {
				return err, downloaded
			}
		}
		if dl.Executable {
			err := os.Chmod(destination, 0777)
			if err != nil {
				return err, downloaded
			}
		}
		downloaded[destination] = true
	}

	return nil, downloaded
}

// How often the perfMonitor samples whole-machine memory/CPU usage while a task's command runs.
const perfMetricPollPeriod = 1 * time.Minute

// cpuStat holds the numeric fields of the aggregate "cpu" line in /proc/stat, in jiffies.
type cpuStat struct {
	user, nice, system, idle, iowait, irq, softirq, steal, guest, guestNice int64
}

func (s cpuStat) total() int64 {
	return s.user + s.nice + s.system + s.idle + s.iowait + s.irq + s.softirq + s.steal + s.guest + s.guestNice
}

// readMemInfo parses /proc/meminfo and returns the MemTotal and MemAvailable values, in bytes.
func readMemInfo() (memTotal int64, memAvailable int64, err error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	found := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() && found < 2 {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		var target *int64
		switch fields[0] {
		case "MemTotal:":
			target = &memTotal
		case "MemAvailable:":
			target = &memAvailable
		default:
			continue
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		*target = kb * 1024
		found++
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, err
	}
	if found < 2 {
		return 0, 0, fmt.Errorf("did not find both MemTotal and MemAvailable in /proc/meminfo")
	}

	return memTotal, memAvailable, nil
}

// readMemInUse returns how much memory is currently in use, machine-wide, in bytes
// (MemTotal - MemAvailable, both read fresh). This is an absolute figure comparable
// directly against MemTotal, not a delta relative to some earlier baseline.
func readMemInUse() (int64, error) {
	memTotal, memAvail, err := readMemInfo()
	if err != nil {
		return 0, err
	}
	memInUse := memTotal - memAvail
	if memInUse < 0 {
		memInUse = 0
	}
	return memInUse, nil
}

// readCPUStat parses the aggregate "cpu" line (not the per-core "cpu0", "cpu1", ... lines) from /proc/stat.
func readCPUStat() (cpuStat, error) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return cpuStat{}, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 || fields[0] != "cpu" {
			continue
		}

		values := make([]int64, 10)
		for i := 1; i < len(fields) && i-1 < len(values); i++ {
			v, err := strconv.ParseInt(fields[i], 10, 64)
			if err != nil {
				return cpuStat{}, err
			}
			values[i-1] = v
		}

		return cpuStat{
			user: values[0], nice: values[1], system: values[2], idle: values[3], iowait: values[4],
			irq: values[5], softirq: values[6], steal: values[7], guest: values[8], guestNice: values[9],
		}, nil
	}
	if err := scanner.Err(); err != nil {
		return cpuStat{}, err
	}

	return cpuStat{}, fmt.Errorf("did not find aggregate cpu line in /proc/stat")
}

// cpuUserPercent returns the percentage of aggregate machine CPU capacity spent in user mode
// between two samples. Because the aggregate "cpu" line in /proc/stat already sums ticks across
// every core, a fully-busy multi-core machine yields ~100%, not (numCPUs * 100)%.
func cpuUserPercent(prev, cur cpuStat) float64 {
	deltaTotal := cur.total() - prev.total()
	if deltaTotal <= 0 {
		return 0
	}
	deltaUser := (cur.user + cur.nice) - (prev.user + prev.nice)
	return float64(deltaUser) / float64(deltaTotal) * 100
}

// percentileFloat64 returns the pct-th percentile (0-100) of sortedAscending using linear
// interpolation between ranks. sortedAscending must be sorted ascending and non-empty.
func percentileFloat64(sortedAscending []float64, pct float64) float64 {
	if len(sortedAscending) == 1 {
		return sortedAscending[0]
	}
	rank := pct / 100 * float64(len(sortedAscending)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	if lower == upper {
		return sortedAscending[lower]
	}
	frac := rank - float64(lower)
	return sortedAscending[lower]*(1-frac) + sortedAscending[upper]*frac
}

// perfMonitor periodically samples whole-machine memory/CPU usage in a background goroutine
// for the duration of a running task's command.
type perfMonitor struct {
	shutdownChan chan bool
	resultChan   chan *PerfStats
}

func startPerfMonitor(stdout *os.File) *perfMonitor {
	numCPUs := runtime.NumCPU()

	memTotal, _, err := readMemInfo()
	if err != nil {
		log.Printf("perfMonitor: could not read total memory, will report 0: %v", err)
	}

	machineType, err := GetMachineType()
	if err != nil {
		log.Printf("perfMonitor: could not read machine type, will report empty: %v", err)
	}

	pm := &perfMonitor{shutdownChan: make(chan bool), resultChan: make(chan *PerfStats, 1)}
	go pm.run(stdout, memTotal, numCPUs, machineType)
	return pm
}

// Stop signals the monitor's goroutine to stop sampling and returns the computed stats.
func (pm *perfMonitor) Stop() *PerfStats {
	pm.shutdownChan <- true
	return <-pm.resultChan
}

// Only write another line to stdout.txt once a sample differs from the last-written one by at
// least this much, so a long-running, steady-state task doesn't flood its log with near-duplicates.
const perfLogMemThresholdGB = 0.1
const perfLogCPUPercentThreshold = 5.0

func shouldLogPerfSample(hasLogged bool, lastMemInUse int64, lastCPUPercent float64, memInUse int64, cpuPercent float64) bool {
	if !hasLogged {
		return true
	}
	memDeltaGB := math.Abs(float64(memInUse-lastMemInUse)) / (1024 * 1024 * 1024)
	cpuDelta := math.Abs(cpuPercent - lastCPUPercent)
	return memDeltaGB > perfLogMemThresholdGB || cpuDelta > perfLogCPUPercentThreshold
}

func formatPerfLogLine(memInUse, memTotal int64, cpuPercent float64, numCPUs int) string {
	const bytesPerGB = 1024 * 1024 * 1024
	return fmt.Sprintf(
		"[%s sparkles metrics] Mem in use: %.1f/%.1f GB, User CPU: %.1f%% (%d CPUs)\n",
		time.Now().Format(time.RFC3339),
		float64(memInUse)/bytesPerGB,
		float64(memTotal)/bytesPerGB,
		cpuPercent,
		numCPUs,
	)
}

func (pm *perfMonitor) run(stdout *os.File, memTotal int64, numCPUs int, machineType string) {
	var memSamples []int64
	var cpuPercentSamples []float64
	loggedError := false
	stopSampling := false

	hasLoggedLine := false
	var lastLoggedMemInUse int64
	var lastLoggedCPUPercent float64

	// Take an immediate memory reading so that even a task shorter than perfMetricPollPeriod
	// still gets at least one sample, rather than waiting for the first tick. There's no cpu
	// percentage to pair it with yet (that needs a delta between two readings), so this sample
	// feeds the final result.json stats but doesn't produce a stdout.txt line on its own.
	if memInUse, err := readMemInUse(); err == nil {
		memSamples = append(memSamples, memInUse)
	} else {
		log.Printf("perfMonitor: could not take initial memory reading, will stop sampling: %v", err)
		loggedError = true
		stopSampling = true
	}

	ticker := time.NewTicker(perfMetricPollPeriod)
	defer ticker.Stop()

	prevCPUStat, err := readCPUStat()
	if err != nil {
		if !loggedError {
			log.Printf("perfMonitor: could not read initial cpu stats, will stop sampling: %v", err)
			loggedError = true
		}
		stopSampling = true
	}

	for {
		select {
		case <-pm.shutdownChan:
			pm.resultChan <- computePerfStats(memTotal, numCPUs, machineType, memSamples, cpuPercentSamples)
			return
		case <-ticker.C:
			if stopSampling {
				continue
			}

			memInUse, memErr := readMemInUse()
			curCPUStat, cpuErr := readCPUStat()
			if memErr != nil || cpuErr != nil {
				if !loggedError {
					log.Printf("perfMonitor: error sampling machine stats, will stop sampling: mem=%v cpu=%v", memErr, cpuErr)
					loggedError = true
				}
				stopSampling = true
				continue
			}

			cpuPercent := cpuUserPercent(prevCPUStat, curCPUStat)
			memSamples = append(memSamples, memInUse)
			cpuPercentSamples = append(cpuPercentSamples, cpuPercent)
			prevCPUStat = curCPUStat

			if shouldLogPerfSample(hasLoggedLine, lastLoggedMemInUse, lastLoggedCPUPercent, memInUse, cpuPercent) {
				if _, err := stdout.WriteString(formatPerfLogLine(memInUse, memTotal, cpuPercent, numCPUs)); err != nil {
					log.Printf("perfMonitor: could not write metrics line to stdout: %v", err)
				}
				hasLoggedLine = true
				lastLoggedMemInUse = memInUse
				lastLoggedCPUPercent = cpuPercent
			}
		}
	}
}

func computePerfStats(memTotal int64, numCPUs int, machineType string, memSamples []int64, cpuPercentSamples []float64) *PerfStats {
	stats := &PerfStats{
		MachineType:  machineType,
		MemoryTotal:  memTotal,
		NumberOfCPUs: numCPUs,
	}

	if len(memSamples) > 0 {
		memAsFloat := make([]float64, len(memSamples))
		for i, v := range memSamples {
			memAsFloat[i] = float64(v)
		}
		sort.Float64s(memAsFloat)
		stats.MaxMemoryInUse = int64(memAsFloat[len(memAsFloat)-1])
		stats.P95MemoryInUse = int64(percentileFloat64(memAsFloat, 95))
		stats.MedianMemoryInUse = int64(percentileFloat64(memAsFloat, 50))
	}

	if len(cpuPercentSamples) > 0 {
		sorted := append([]float64(nil), cpuPercentSamples...)
		sort.Float64s(sorted)
		stats.MaxUserCPUPercentUsage = sorted[len(sorted)-1]
		stats.P95UserCPUPercentUsage = percentileFloat64(sorted, 95)
		stats.MedianUserCPUPercentUsage = percentileFloat64(sorted, 50)
	}

	return stats
}

func execCommand(command string, workdir string, stdout *os.File) (*syscall.Rusage, *PerfStats, string, error) {
	attr := &os.ProcAttr{Dir: workdir, Env: nil, Files: []*os.File{nil, stdout, stdout}}
	exePath := "/bin/sh"

	perfMon := startPerfMonitor(stdout)

	proc, err := os.StartProcess(exePath, []string{exePath, "-c", command}, attr)
	if err != nil {
		perfMon.Stop()
		return nil, nil, "", err
	}

	var procState *os.ProcessState
	err = NotifyUntilComplete(func() error {
		var err2 error
		procState, err2 = proc.Wait()
		return err2
	})
	if err != nil {
		// this should not be possible
		panic(fmt.Sprintf("Error calling proc.Wait(): %s", err))
	}

	perfStats := perfMon.Stop()

	rusage := procState.SysUsage().(*syscall.Rusage)
	status := procState.Sys().(syscall.WaitStatus)
	var statusStr string
	if status.Signaled() {
		statusStr = fmt.Sprintf("signaled(%s)", status.Signal())
	} else {
		statusStr = fmt.Sprintf("%d", status.ExitStatus())
	}

	return rusage, perfStats, statusStr, nil
}

type UploadMapping map[string]string

func newUploadMapping() UploadMapping {
	return make(map[string]string)
}

func addUpload(mapping UploadMapping, filename string, destURL string) {
	mapping[filename] = destURL
}

func addFilesToStringSet(workdir string, pattern string, dest stringset) error {
	pathWithGlob := path.Join(workdir, pattern)
	matches, err := doublestar.Glob(pathWithGlob)
	if err != nil {
		return err
	}
	log.Printf("pathWithGlob=%v, matches=%v\n", pathWithGlob, matches)

	for _, match := range matches {
		// skip any directories that match
		fi, err := os.Stat(match)
		if err != nil {
			return err
		}
		if fi.IsDir() {
			continue
		}

		match, err = filepath.Abs(match)
		if err != nil {
			return err
		}

		dest[match] = true
	}

	return nil
}

func resolveUploads(workdir string, uploads *UploadSpec, toExclude stringset) (UploadMapping, error) {
	included := make(stringset)

	for _, pattern := range uploads.IncludePatterns {
		addFilesToStringSet(workdir, pattern, included)
	}
	for _, pattern := range uploads.ExcludePatterns {
		addFilesToStringSet(workdir, pattern, toExclude)
	}

	m := newUploadMapping()
	for match, _ := range included {

		if toExclude[match] {
			continue
		}

		relpath, err := filepath.Rel(workdir, match)
		if err != nil {
			return nil, err
		}
		dest := uploads.DstURL + "/" + relpath
		addUpload(m, match, dest)
	}
	return m, nil
}

func execLifecycleScript(label string, workdir string, script string) {
	if script == "" {
		return
	}

	log.Printf("Executing %s script: %s", label, script)
	cmd := exec.Command("sh", "-c", script)
	cmd.Dir = workdir
	err := cmd.Run()
	if err != nil {
		log.Printf("Command finished with error: %v", err)
	} else {
		log.Printf("Command completed succesfully")
	}
}

// Perhaps should not be hardcoded, and could be determined at compile time somehow.
const PAGE_SIZE = 4 * 1024

func startWatchingLog(loggingClient *logging.Client, taskID string, stdoutPath string) (chan bool, error) {
	log.Printf("Starting watch of logfile: %s", stdoutPath)

	buffer := make([]byte, 50000)
	shutdownChan := make(chan bool)
	labels := make(map[string]string)
	labels["sparkles-job-id"] = strings.Split(taskID, ".")[0]
	labels["sparkles-task-id"] = taskID
	logger := loggingClient.Logger(taskID, logging.CommonLabels(labels))

	stdout, err := os.Open(stdoutPath)
	if err != nil {
		log.Printf("Could not open %s for reading: %v", stdoutPath, err)
		return shutdownChan, err
	}

	poll := func() {
		watching := true
		for watching {
			n, err := stdout.Read(buffer)
			if err != nil && err != io.EOF {
				log.Printf("Got error reading %s: %v", stdoutPath, err)
				break
			}

			if n > 0 {
				logger.Log(logging.Entry{Payload: string(buffer[0:n])})
				//				continue
			}

			select {
			case <-shutdownChan:
				watching = false
			case <-time.After(time.Second):
			}
		}
		stdout.Close()
	}

	go poll()
	return shutdownChan, nil
}

func getModificationTimes(filenames stringset) map[string]time.Time {
	mtimes := make(map[string]time.Time)
	for filename, _ := range filenames {
		fi, err := os.Stat(filename)
		if err != nil {
			log.Printf("Error trying to stat %s to record last modification time, skipping...", err)
			continue
		}
		mtimes[filename] = fi.ModTime()
	}

	return mtimes
}

func getFilesWithMatchingMTimes(a map[string]time.Time, b map[string]time.Time) stringset {
	matching := make(stringset)
	for key, aTime := range a {
		bTime, ok := b[key]
		if !ok {
			log.Printf("While checking if %s was updated, could not find the file. Skipping...")
			continue
		}

		if aTime == bTime {
			matching[key] = true
		}
	}
	return matching
}

func executeTaskInDir(ioc IOClient, workdir string, taskId string, spec *TaskSpec, cachedir string, monitor *Monitor) (string, error) {
	stdoutPath := path.Join(workdir, "stdout.txt")
	execLifecycleScript("PreDownloadScript", workdir, spec.PreDownloadScript)

	stdout, err := os.OpenFile(stdoutPath, os.O_WRONLY|os.O_CREATE, 0766)
	if err != nil {
		return "", err
	}

	if monitor != nil {
		monitor.StartWatchingLog(taskId, stdoutPath)
	}

	if len(spec.Downloads) > 0 {
		stdout.WriteString(fmt.Sprintf("sparkles: Downloading %d files...\n", len(spec.Downloads)))
	}

	err, downloaded := downloadAll(ioc, workdir, spec.Downloads, cachedir)
	if err != nil {
		return "", err
	}

	if len(spec.Downloads) > 0 {
		stdout.WriteString(fmt.Sprintf("sparkles: download complete.\n"))
	}

	downloadedInitialMTimes := getModificationTimes(downloaded)

	execLifecycleScript("PostDownloadScript", workdir, spec.PostDownloadScript)

	commandWorkingDir := spec.WorkingDir
	if commandWorkingDir == "" {
		commandWorkingDir = "."
	}
	if path.IsAbs(commandWorkingDir) {
		panic("bad commandWorkingDir")
	}

	cwdDir := path.Join(workdir, commandWorkingDir)
	log.Printf("Executing (working dir: %s, output written to: %s): %s", cwdDir, stdoutPath, spec.Command)
	resourceUsage, perfStats, retcode, err := execCommand(spec.Command, cwdDir, stdout)
	if err != nil {
		return retcode, err
	}

	execLifecycleScript("PostExecScript", workdir, spec.PostExecScript)

	downloadedFinalMTimes := getModificationTimes(downloaded)

	downloadsToExclude := getFilesWithMatchingMTimes(downloadedInitialMTimes, downloadedFinalMTimes)

	filesToUpload, err := resolveUploads(workdir, spec.Uploads, downloadsToExclude)
	if err != nil {
		return retcode, err
	}

	addUpload(filesToUpload, stdoutPath, spec.StdoutURL)

	err = writeResultFile(ioc, spec.CommandResultURL, retcode, resourceUsage, perfStats, workdir, filesToUpload, spec.Command, spec.Parameters)
	if err != nil {
		return retcode, err
	}

	err = uploadMapped(ioc, filesToUpload)

	return retcode, err
}

func executeTask(ioc IOClient, taskId string, taskSpec *TaskSpec, cacheDir string, tasksDir string, monitor *Monitor) (string, error) {
	//	log.Printf("Job spec (%s) of claimed task: %s", json_url, json.dumps(spec, indent=2))

	mode := os.FileMode(0700)
	err := os.MkdirAll(tasksDir, mode)
	if err != nil {
		return "", err
	}

	taskDir, err := ioutil.TempDir(tasksDir, "task-")
	if err != nil {
		return "", err
	}

	logDir := path.Join(taskDir, "log")
	err = os.Mkdir(logDir, mode)
	if err != nil {
		return "", err
	}

	workDir := path.Join(taskDir, "work")
	err = os.Mkdir(workDir, mode)
	if err != nil {
		return "", err
	}

	workDir, err = filepath.Abs(workDir)
	if err != nil {
		return "", err
	}

	cacheDir, err = filepath.Abs(cacheDir)
	if err != nil {
		return "", err
	}

	retcode, err := executeTaskInDir(ioc, workDir, taskId, taskSpec, cacheDir, monitor)
	if err != nil {
		return retcode, err
	}

	return retcode, nil
}

func uploadMapped(ioc IOClient, files map[string]string) error {
	log.Printf("Uploading %d files", len(files))
	for src, dst := range files {
		err := ioc.Upload(src, dst)
		if err != nil {
			return err
		}
	}

	uploadCompleteTime := time.Now()

	for _, dst := range files {
		warningPrinted := false
		for {
			exists, err := ioc.IsExists(dst)
			if err != nil {
				return err
			}

			if exists {
				if warningPrinted {
					log.Printf("File now exists: %s", dst)
				}
				break
			}

			if time.Now().Sub(uploadCompleteTime) > MaxUploadDelay {
				return fmt.Errorf("Attempted to upload %s but file is missing", dst)
			}

			log.Printf("Uploaded file is missing: %s. Will check again shortly", dst)
			time.Sleep(5)
		}
	}

	log.Printf("Confirmed %d files in cloud storage", len(files))
	return nil
}

func writeResultFile(ioc IOClient,
	CommandResultURL string,
	retcode string,
	resourceUsage *syscall.Rusage,
	perfStats *PerfStats,
	workdir string,
	filesToUpload map[string]string,
	command string,
	parameters map[string]string) error {

	files := make([]*ResultFile, 0, 100)
	for src, dstURL := range filesToUpload {
		rel_src, err := filepath.Rel(workdir, src)
		if err != nil {
			log.Printf("Got error in relpath(%s, %s): %v", workdir, src, err)
			return err
		}
		files = append(files, &ResultFile{Src: rel_src, DstURL: dstURL})
	}

	result := &ResultStruct{
		Command:    command,
		Parameters: parameters,
		ReturnCode: retcode,
		Files:      files,
		Usage: &ResourceUsage{
			UserCPUTime:        resourceUsage.Utime,
			SystemCPUTime:      resourceUsage.Stime,
			MaxMemorySize:      resourceUsage.Maxrss,
			SharedMemorySize:   resourceUsage.Isrss,
			UnsharedMemorySize: resourceUsage.Ixrss,
			BlockInputOps:      resourceUsage.Inblock,
			BlockOutputOps:     resourceUsage.Oublock},
		MachineStats: perfStats}

	resultJson, err := json.Marshal(result)
	if err != nil {
		return err
	}

	return ioc.UploadBytes(CommandResultURL, resultJson)
}

func loadTaskSpec(ioc IOClient, taskURL string) (*TaskSpec, error) {
	var data []byte

	if taskURL[0] == '{' {
		data = []byte(taskURL)
	} else {
		var err error
		data, err = ioc.DownloadAsBytes(taskURL)
		if err != nil {
			return nil, err
		}
	}

	var taskSpec TaskSpec
	err := json.Unmarshal(data, &taskSpec)
	if err != nil {
		return nil, err
	}

	return &taskSpec, nil
}

func ExecuteTaskFromUrl(ioc IOClient, taskId string, taskURL string, cacheDir string, tasksDir string, monitor *Monitor) (string, error) {
	taskSpec, err := loadTaskSpec(ioc, taskURL)
	if err != nil {
		return "", err
	}

	return executeTask(ioc, taskId, taskSpec, cacheDir, tasksDir, monitor)
}
