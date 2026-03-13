package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/logging"
	"github.com/bmatcuk/doublestar"
	aetherclient "github.com/pgm/aether/client"
	"github.com/broadinstitute/sparklesworker/monitor"
	"github.com/broadinstitute/sparklesworker/task_queue"
	"github.com/broadinstitute/sparklesworker/watchdog"
)

type ResourceUsage struct {
	UserCPUTime        float64 `json:"user_cpu_time"`
	SystemCPUTime      float64 `json:"system_cpu_time"`
	MaxMemorySize      int64   `json:"max_memory_size"`
	SharedMemorySize   int64   `json:"shared_memory_size"`
	UnsharedMemorySize int64   `json:"unshared_memory_size"`
	BlockInputOps      int64   `json:"block_input_ops"`
	BlockOutputOps     int64   `json:"block_output_ops"`
	StartTime          float64 `json:"start_time"`
	EndTime            float64 `json:"end_time"`
	ElapsedTime        float64 `json:"elapsed_time"`
	DownloadBytes      int64   `json:"download_bytes"`
	DownloadFileCount  int     `json:"download_file_count"`
	DownloadStartTime  float64 `json:"download_start_time"`
	DownloadEndTime    float64 `json:"download_end_time"`
	DownloadElapsed    float64 `json:"download_elapsed"`
	UploadBytes        int64   `json:"upload_bytes"`
	UploadFileCount    int     `json:"upload_file_count"`
	UploadStartTime    float64 `json:"upload_start_time"`
	UploadEndTime      float64 `json:"upload_end_time"`
	UploadElapsed      float64 `json:"upload_elapsed"`
}

type TransferStats struct {
	Bytes     int64
	FileCount int
	StartTime time.Time
	EndTime   time.Time
}

func toUnixFloat(t time.Time) float64 {
	return float64(t.Unix()) + float64(t.Nanosecond())/1e9
}

func timevalToSeconds(tv syscall.Timeval) float64 {
	return float64(tv.Sec) + float64(tv.Usec)/1e6
}

// AetherConfig holds configuration for the aether content-addressed store.
type AetherConfig struct {
	Root string // aether store root (gs://bucket/prefix or local path)
}

type ResultStruct struct {
	ExePath     string            `json:"exe_path"`
	ExeArgs     []string          `json:"exe_args"`
	Parameters  map[string]string `json:"parameters,omitempty"`
	ReturnCode  string            `json:"return_code"`
	ManifestKey string            `json:"manifest_key,omitempty"`
	Usage       *ResourceUsage    `json:"resource_usage"`
}

type Stringset map[string]bool

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

	_, err = io.Copy(watchdog.NotifyOnWrite(out), in)
	if err != nil {
		out.Close()
		return err
	}

	return out.Close()
}

// ExecResult holds the results of command execution
type ExecResult struct {
	Rusage    *syscall.Rusage
	Status    string
	StartTime time.Time
	EndTime   time.Time
	ExePath   string
	ExeArgs   []string
}

func execCommand(command string, rootdir string, workdir string, stdout *os.File, dockerImage string) (*ExecResult, error) {
	var exePath string
	var args []string

	if dockerImage != "" {
		exePath = "docker"
		args = []string{
			"docker", "run", "--rm",
			"-v", rootdir + ":" + rootdir,
			"-w", workdir,
			dockerImage,
			"/bin/sh", "-c", command,
		}
	} else {
		exePath = "/bin/sh"
		args = []string{exePath, "-c", command}
	}

	attr := &os.ProcAttr{Dir: workdir, Env: nil, Files: []*os.File{nil, stdout, stdout}}

	startTime := time.Now()
	proc, err := os.StartProcess(exePath, args, attr)
	if err != nil {
		return nil, err
	}

	var procState *os.ProcessState
	err = watchdog.NotifyUntilComplete(func() error {
		var err2 error
		procState, err2 = proc.Wait()
		return err2
	})
	endTime := time.Now()

	if err != nil {
		// this should not be possible
		panic(fmt.Sprintf("Error calling proc.Wait(): %s", err))
	}

	rusage := procState.SysUsage().(*syscall.Rusage)
	status := procState.Sys().(syscall.WaitStatus)
	var statusStr string
	if status.Signaled() {
		statusStr = fmt.Sprintf("signaled(%s)", status.Signal())
	} else {
		statusStr = fmt.Sprintf("%d", status.ExitStatus())
	}

	return &ExecResult{
		Rusage:    rusage,
		Status:    statusStr,
		StartTime: startTime,
		EndTime:   endTime,
		ExePath:   exePath,
		ExeArgs:   args,
	}, nil
}

func addFilesToStringSet(workdir string, pattern string, dest Stringset) error {
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

func ResolveUploads(workdir string, uploads *task_queue.UploadSpec) ([]string, error) {
	included := make(Stringset)
	excluded := make(Stringset)

	for _, pattern := range uploads.IncludePatterns {
		addFilesToStringSet(workdir, pattern, included)
	}
	for _, pattern := range uploads.ExcludePatterns {
		addFilesToStringSet(workdir, pattern, excluded)
	}

	var files []string
	for match := range included {
		if excluded[match] {
			continue
		}
		files = append(files, match)
	}
	return files, nil
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

func prepareTaskDirectories(tasksDir string, cacheDir string) (workDir string, absCacheDir string, err error) {
	mode := os.FileMode(0700)
	err = os.MkdirAll(tasksDir, mode)
	if err != nil {
		return "", "", err
	}

	taskDir, err := os.MkdirTemp(tasksDir, "task-")
	if err != nil {
		return "", "", err
	}

	logDir := path.Join(taskDir, "log")
	err = os.Mkdir(logDir, mode)
	if err != nil {
		return "", "", err
	}

	workDir = path.Join(taskDir, "work")
	err = os.Mkdir(workDir, mode)
	if err != nil {
		return "", "", err
	}

	workDir, err = filepath.Abs(workDir)
	if err != nil {
		return "", "", err
	}

	absCacheDir, err = filepath.Abs(cacheDir)
	if err != nil {
		return "", "", err
	}

	return workDir, absCacheDir, nil
}

type DownloadTaskFilesResult struct {
	Stdout     *os.File
	StdoutPath string
	DlStats    *TransferStats
}

func downloadTaskFiles(ctx context.Context, aetherCfg AetherConfig, workDir string, cacheDir string, taskId string, taskSpec *task_queue.TaskSpec, mon *monitor.Monitor) (*DownloadTaskFilesResult, error) {
	stdoutPath := path.Join(workDir, "stdout.txt")
	execLifecycleScript("PreDownloadScript", workDir, taskSpec.PreDownloadScript)

	stdout, err := os.OpenFile(stdoutPath, os.O_WRONLY|os.O_CREATE, 0766)
	if err != nil {
		return nil, err
	}

	if mon != nil {
		mon.StartWatchingLog(taskId, stdoutPath)
	}

	dlStart := time.Now()
	exportStats, err := aetherclient.Export(ctx, aetherclient.ExportOptions{
		Root:        aetherCfg.Root,
		ManifestRef: taskSpec.AetherFSRoot,
		Dest:        workDir,
		CacheDir:    cacheDir,
	})
	dlEnd := time.Now()
	if err != nil {
		return nil, err
	}
	log.Printf("Downloaded %d files (%d bytes) in %s", exportStats.FilesDownloaded, exportStats.BytesDownloaded, exportStats.Duration)

	execLifecycleScript("PostDownloadScript", workDir, taskSpec.PostDownloadScript)

	return &DownloadTaskFilesResult{
		Stdout:     stdout,
		StdoutPath: stdoutPath,
		DlStats: &TransferStats{
			Bytes:     exportStats.BytesDownloaded,
			FileCount: exportStats.FilesDownloaded,
			StartTime: dlStart,
			EndTime:   dlEnd,
		},
	}, nil
}

func uploadTaskResults(ctx context.Context, aetherCfg AetherConfig, workDir string, stdoutPath string, taskSpec *task_queue.TaskSpec) (string, *TransferStats, error) {
	filePaths, err := ResolveUploads(workDir, taskSpec.Uploads)
	if err != nil {
		return "", nil, err
	}
	filePaths = append(filePaths, stdoutPath)

	var fileInputs []aetherclient.FileInput
	for _, absPath := range filePaths {
		relPath, err := filepath.Rel(workDir, absPath)
		if err != nil {
			return "", nil, err
		}
		fileInputs = append(fileInputs, aetherclient.FileInput{Path: absPath, ManifestName: relPath})
	}

	ulStats := &TransferStats{FileCount: len(fileInputs), StartTime: time.Now()}
	for _, f := range fileInputs {
		fi, err := os.Stat(f.Path)
		if err == nil {
			ulStats.Bytes += fi.Size()
		}
	}

	mkfsStats, err := aetherclient.MakeFilesystem(ctx, aetherclient.MakeFilesystemOptions{
		Root:  aetherCfg.Root,
		Files: fileInputs,
	})
	ulStats.EndTime = time.Now()
	if err != nil {
		return "", nil, err
	}
	log.Printf("Uploaded %d files (%d bytes, %d skipped) in %s, manifest key: %s",
		mkfsStats.FilesUploaded, mkfsStats.BytesUploaded, mkfsStats.FilesSkipped, mkfsStats.UploadDuration, mkfsStats.ManifestKey)

	return mkfsStats.ManifestKey, ulStats, nil
}

func determineCwd(workDir string, taskSpec *task_queue.TaskSpec) string {
	commandWorkingDir := taskSpec.WorkingDir
	if commandWorkingDir == "" {
		commandWorkingDir = "."
	}
	if path.IsAbs(commandWorkingDir) {
		panic("bad commandWorkingDir")
	}

	cwdDir := path.Join(workDir, commandWorkingDir)
	return cwdDir
}

func ExecuteTask(ctx context.Context, ioc IOClient, aetherCfg AetherConfig, taskId string, taskSpec *task_queue.TaskSpec, rootDir string, cacheDir string, tasksDir string, mon *monitor.Monitor) (string, error) {
	workDir, cacheDir, err := prepareTaskDirectories(tasksDir, cacheDir)
	if err != nil {
		return "", err
	}

	dl, err := downloadTaskFiles(ctx, aetherCfg, workDir, cacheDir, taskId, taskSpec, mon)
	if err != nil {
		return "", err
	}

	cwdDir := determineCwd(workDir, taskSpec)

	log.Printf("Executing (working dir: %s, output written to: %s): %s", cwdDir, dl.StdoutPath, taskSpec.Command)
	execResult, err := execCommand(taskSpec.Command, rootDir, cwdDir, dl.Stdout, taskSpec.DockerImage)
	if err != nil {
		return "", err
	}
	retcode := execResult.Status

	execLifecycleScript("PostExecScript", workDir, taskSpec.PostExecScript)

	manifestKey, ulStats, err := uploadTaskResults(ctx, aetherCfg, workDir, dl.StdoutPath, taskSpec)
	if err != nil {
		return retcode, err
	}

	err = writeResultFile(ioc, taskSpec.CommandResultURL, retcode, execResult, workDir, manifestKey, taskSpec.Parameters, dl.DlStats, ulStats)
	if err != nil {
		return retcode, err
	}

	return retcode, nil
}

func writeResultFile(ioc IOClient,
	CommandResultURL string,
	retcode string,
	execResult *ExecResult,
	workdir string,
	manifestKey string,
	parameters map[string]string,
	dlStats *TransferStats,
	ulStats *TransferStats) error {

	rusage := execResult.Rusage
	elapsedTime := execResult.EndTime.Sub(execResult.StartTime).Seconds()

	result := &ResultStruct{
		ExePath:     execResult.ExePath,
		ExeArgs:     execResult.ExeArgs,
		Parameters:  parameters,
		ReturnCode:  retcode,
		ManifestKey: manifestKey,
		Usage: &ResourceUsage{
			UserCPUTime:        timevalToSeconds(rusage.Utime),
			SystemCPUTime:      timevalToSeconds(rusage.Stime),
			MaxMemorySize:      rusage.Maxrss,
			SharedMemorySize:   rusage.Isrss,
			UnsharedMemorySize: rusage.Ixrss,
			BlockInputOps:      rusage.Inblock,
			BlockOutputOps:     rusage.Oublock,
			StartTime:          toUnixFloat(execResult.StartTime),
			EndTime:            toUnixFloat(execResult.EndTime),
			ElapsedTime:        elapsedTime,
			DownloadBytes:      dlStats.Bytes,
			DownloadFileCount:  dlStats.FileCount,
			DownloadStartTime:  toUnixFloat(dlStats.StartTime),
			DownloadEndTime:    toUnixFloat(dlStats.EndTime),
			DownloadElapsed:    dlStats.EndTime.Sub(dlStats.StartTime).Seconds(),
			UploadBytes:        ulStats.Bytes,
			UploadFileCount:    ulStats.FileCount,
			UploadStartTime:    toUnixFloat(ulStats.StartTime),
			UploadEndTime:      toUnixFloat(ulStats.EndTime),
			UploadElapsed:      ulStats.EndTime.Sub(ulStats.StartTime).Seconds(),
		}}

	resultJson, err := json.Marshal(result)
	if err != nil {
		return err
	}

	return ioc.UploadBytes(CommandResultURL, resultJson)
}
