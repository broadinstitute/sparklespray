package consumer

import (
	"context"
	"crypto/sha256"
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

	"github.com/bmatcuk/doublestar"
	"github.com/broadinstitute/sparklesworker/task_queue"
	"github.com/broadinstitute/sparklesworker/watchdog"
	aetherclient "github.com/pgm/aether/client"
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
	Root            string // aether store root (gs://bucket/prefix or local path)
	MaxSizeToBundle int64  // max file size eligible for bundling (0 = disable bundling)
	MaxBundleSize   int64  // target max size per bundle
	Workers         int    // parallel upload workers (0 = use aether default of 1)
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

func execCommand(command []string, rootdir string, workdir string, stdout *os.File, dockerImage string) (*ExecResult, error) {
	var exePath string
	var args []string

	joinedCommand := strings.Join(command, " ")

	if dockerImage != "" {
		exePath = "docker"
		args = []string{
			"docker", "run", "--rm",
			"-v", rootdir + ":" + rootdir,
			"-w", workdir,
			dockerImage,
			"/bin/sh", "-c", joinedCommand,
		}
	} else {
		exePath = "/bin/sh"
		args = []string{exePath, "-c", joinedCommand}
	}

	// log.Printf("About to execute: exePath=%s args=%v", exePath, args)
	// for i, arg := range args {
	// 	log.Printf("args[%d]=\"%s\"", i, arg)
	// }
	// log.Printf("stdout=%v", stdout)
	// n, err3 := stdout.Write([]byte("test"))
	// log.Printf("n=%d err=%v", n, err3)

	// exePath = "/bin/echo"
	// // args = []string{"/bin/sh", "-c", "'echo hello'"}
	// args = []string{"/bin/echo", "helllo"}
	// //	args = []string{"/usr/bin/true"}
	// exePath = args[0]
	attr := &os.ProcAttr{Dir: workdir, Env: nil, Files: []*os.File{nil, stdout, stdout}}

	startTime := time.Now()
	proc, err := os.StartProcess(exePath, args, attr)
	if err != nil {
		return nil, err
	}

	var procState *os.ProcessState
	procState, _ = proc.Wait()
	// err = watchdog.NotifyUntilComplete(func() error {
	// 	var err2 error
	// 	procState, err2 = proc.Wait()
	// 	return err2
	// })
	endTime := time.Now()

	if err != nil {
		// this should not be possible
		panic(fmt.Sprintf("Error calling proc.Wait(): %s", err))
	}

	rusage := procState.SysUsage().(*syscall.Rusage)
	status := procState.Sys().(syscall.WaitStatus)
	log.Printf("status=%v", status)
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
	workdir, err := filepath.Abs(workdir)
	if err != nil {
		return nil, err
	}

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

type TaskPaths struct {
	rootDir    string
	logDir     string
	cacheDir   string
	workDir    string
	cwdDir     string
	stdoutPath string
	resultPath string
}

func prepareTaskDirectories(rootDir string, tasksDir string, cacheDir string, taskWorkingDir string) (*TaskPaths, error) {
	mode := os.FileMode(0700)
	err := os.MkdirAll(tasksDir, mode)
	if err != nil {
		return nil, err
	}

	taskDir, err := os.MkdirTemp(tasksDir, "task-")
	if err != nil {
		return nil, err
	}

	logDir := path.Join(taskDir, "log")
	err = os.Mkdir(logDir, mode)
	if err != nil {
		return nil, err
	}

	workDir := path.Join(taskDir, "work")
	err = os.Mkdir(workDir, mode)
	if err != nil {
		return nil, err
	}

	workDir, err = filepath.Abs(workDir)
	if err != nil {
		return nil, err
	}

	cacheDir, err = filepath.Abs(cacheDir)
	if err != nil {
		return nil, err
	}

	if taskWorkingDir == "" {
		taskWorkingDir = "."
	}
	if path.IsAbs(taskWorkingDir) {
		return nil, fmt.Errorf("Only relative paths allowed for working dir in task spec, but was \"%s\"", taskWorkingDir)
	}

	return &TaskPaths{rootDir: rootDir, logDir: logDir,
		cacheDir:   cacheDir,
		workDir:    workDir,
		cwdDir:     path.Join(workDir, taskWorkingDir),
		stdoutPath: path.Join(logDir, "stdout.txt"),
		resultPath: path.Join(logDir, "result.json")}, nil
}

func downloadTaskFiles(ctx context.Context, aetherCfg *AetherConfig, dirs *TaskPaths, taskId string, taskSpec *task_queue.TaskSpec, lifecycle ExecuteLifecycle) (*TransferStats, error) {
	execLifecycleScript("PreDownloadScript", dirs.workDir, taskSpec.PreDownloadScript)

	dlStart := time.Now()
	exportStats, err := aetherclient.Export(ctx, aetherclient.ExportOptions{
		Root:        aetherCfg.Root,
		ManifestRef: taskSpec.AetherFSRoot,
		Dest:        dirs.workDir,
		CacheDir:    dirs.cacheDir,
	})
	dlEnd := time.Now()
	if err != nil {
		return nil, err
	}
	log.Printf("Downloaded %d files (%d bytes) in %s", exportStats.FilesDownloaded, exportStats.BytesDownloaded, exportStats.Duration)

	execLifecycleScript("PostDownloadScript", dirs.workDir, taskSpec.PostDownloadScript)

	return &TransferStats{
		Bytes:     exportStats.BytesDownloaded,
		FileCount: exportStats.FilesDownloaded,
		StartTime: dlStart,
		EndTime:   dlEnd,
	}, nil
}

type UploadTaskResultsResult struct {
	taskFilesManifestKey string
	uploadStats          *TransferStats
}

func collectFileInputs(workDir string, uploadSpec *task_queue.UploadSpec) ([]aetherclient.FileInput, error) {
	filePaths, err := ResolveUploads(workDir, uploadSpec)
	if err != nil {
		return nil, err
	}

	var filesToUpload []aetherclient.FileInput
	for _, absPath := range filePaths {
		relPath, err := filepath.Rel(workDir, absPath)
		if err != nil {
			return nil, fmt.Errorf("error in collectFileInputs: %s", err)
		}
		filesToUpload = append(filesToUpload, aetherclient.FileInput{Path: absPath, ManifestName: relPath})
	}

	return filesToUpload, nil
}

func uploadFilesPerSpec(ctx context.Context, aetherCfg *AetherConfig, dir string,
	uploadSpec *task_queue.UploadSpec) (*UploadTaskResultsResult, error) {
	log.Printf("calling collectFileInputs")
	filesToUpload, err := collectFileInputs(dir, uploadSpec)
	if err != nil {
		return nil, err
	}

	ulStats := &TransferStats{FileCount: len(filesToUpload), StartTime: time.Now()}

	for _, f := range filesToUpload {
		fi, err := os.Stat(f.Path)
		if err == nil {
			ulStats.Bytes += fi.Size()
		}
	}

	log.Printf("calling MakeFilesystem")

	mkfsFilesStats, err := aetherclient.MakeFilesystem(ctx, aetherclient.MakeFilesystemOptions{
		Root:            aetherCfg.Root,
		Files:           filesToUpload,
		MaxSizeToBundle: aetherCfg.MaxSizeToBundle,
		MaxBundleSize:   aetherCfg.MaxBundleSize,
		Workers:         aetherCfg.Workers,
	})

	ulStats.EndTime = time.Now()
	if err != nil {
		return nil, err
	}

	log.Printf("Uploaded %d files and logs from task (%d bytes, %d skipped) in %s, task files key: %s",
		mkfsFilesStats.FilesUploaded,
		mkfsFilesStats.BytesUploaded,
		mkfsFilesStats.FilesSkipped,
		mkfsFilesStats.UploadDuration,
		mkfsFilesStats.ManifestKey)

	return &UploadTaskResultsResult{
		taskFilesManifestKey: mkfsFilesStats.ManifestKey,
		uploadStats:          ulStats,
	}, nil
}

type ExecuteTaskResult struct {
	RetCode                   string
	OutputsKey                string
	LogsKey                   string
	TaskPaths                 *TaskPaths
	UsedCacheResultFromTaskID string
}

func computeCacheKey(taskSpec *task_queue.TaskSpec) (string, error) {
	data, err := json.Marshal([]interface{}{taskSpec.Command, taskSpec.DockerImage, taskSpec.AetherFSRoot})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:]), nil
}

type ExecuteLifecycle interface {
	Started(stdoutPath string)
	Finished()
}

func ExecuteTask(ctx context.Context, aetherCfg *AetherConfig, taskId string, taskSpec *task_queue.TaskSpec, rootDir string, cacheDir string, tasksDir string, lifecycle ExecuteLifecycle, taskCache task_queue.TaskCache, expiry time.Time) (*ExecuteTaskResult, error) {
	if taskCache != nil {
		cacheKey, err := computeCacheKey(taskSpec)
		if err == nil {
			entry, err := taskCache.GetCachedEntry(ctx, cacheKey)
			if err != nil {
				log.Printf("cache lookup failed (ignoring): %v", err)
			} else if entry != nil && time.Now().Before(entry.Expiry) {
				log.Printf("cache hit for task %s (originally computed by task %s)", taskId, entry.TaskID)
				return &ExecuteTaskResult{
					RetCode:                   "0",
					OutputsKey:                entry.OutputAetherFSRoot,
					LogsKey:                   entry.LogAetherFSRoot,
					UsedCacheResultFromTaskID: entry.TaskID,
				}, nil
			}
		}
	}

	dirs, err := prepareTaskDirectories(rootDir, tasksDir, cacheDir, taskSpec.WorkingDir)
	if err != nil {
		return nil, err
	}

	stdout, err := os.OpenFile(dirs.stdoutPath, os.O_WRONLY|os.O_CREATE, 0766)
	if err != nil {
		return nil, err
	}
	defer stdout.Close()

	if lifecycle != nil {
		lifecycle.Started(dirs.stdoutPath)
		defer lifecycle.Finished()
	}

	dlStats, err := downloadTaskFiles(ctx, aetherCfg, dirs, taskId, taskSpec, lifecycle)
	if err != nil {
		return nil, err
	}

	log.Printf("Executing (working dir: %s, output written to: %s): %s", dirs.cwdDir, dirs.stdoutPath, taskSpec.Command)
	execResult, err := execCommand(taskSpec.Command, rootDir, dirs.cwdDir, stdout, taskSpec.DockerImage)
	if err != nil {
		return nil, err
	}
	retcode := execResult.Status

	execLifecycleScript("PostExecScript", dirs.workDir, taskSpec.PostExecScript)

	uploadResult, err := uploadFilesPerSpec(ctx, aetherCfg, dirs.workDir, taskSpec.Uploads)
	if err != nil {
		return nil, err
	}

	err = writeResultFile(dirs.resultPath, retcode, execResult, dirs.workDir, uploadResult.taskFilesManifestKey, taskSpec.Parameters, dlStats, uploadResult.uploadStats)
	if err != nil {
		return nil, err
	}

	logsUpload, err := uploadFilesPerSpec(ctx, aetherCfg, dirs.logDir, &task_queue.UploadSpec{IncludePatterns: []string{"**/*"}})
	if err != nil {
		return nil, err
	}

	outputsKey := "sha256:" + uploadResult.taskFilesManifestKey
	logsKey := "sha256:" + logsUpload.taskFilesManifestKey

	if taskCache != nil && retcode == "0" {
		if cacheKey, err := computeCacheKey(taskSpec); err == nil {
			entry := &task_queue.CachedTaskEntry{
				ID:                 cacheKey,
				TaskID:             taskId,
				OutputAetherFSRoot: outputsKey,
				LogAetherFSRoot:    logsKey,
				Expiry:             expiry,
			}
			if err := taskCache.SetCachedEntry(ctx, entry); err != nil {
				log.Printf("failed to store cache entry (ignoring): %v", err)
			}
		}
	}

	return &ExecuteTaskResult{RetCode: retcode, OutputsKey: outputsKey, LogsKey: logsKey, TaskPaths: dirs}, nil
}

func writeResultFile(resultPath string,
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

	resultFd, err := os.Create(resultPath)
	if err != nil {
		return err
	}
	defer resultFd.Close()

	_, err = resultFd.Write(resultJson)
	if err != nil {
		return err
	}

	return nil
}
