package consumer

import (
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
	"github.com/broadinstitute/sparklesworker/monitor"
	"github.com/broadinstitute/sparklesworker/task_queue"
	"github.com/broadinstitute/sparklesworker/watchdog"
)

type ResultFile struct {
	Src    string `json:"src"`
	DstURL string `json:"dst_url"`
}

type ResourceUsage struct {
	UserCPUTime        float64 `json:"user_cpu_time"`
	SystemCPUTime      float64 `json:"system_cpu_time"`
	MaxMemorySize      int64           `json:"max_memory_size"`
	SharedMemorySize   int64           `json:"shared_memory_size"`
	UnsharedMemorySize int64           `json:"unshared_memory_size"`
	BlockInputOps      int64           `json:"block_input_ops"`
	BlockOutputOps     int64           `json:"block_output_ops"`
	StartTime          float64         `json:"start_time"`
	EndTime            float64         `json:"end_time"`
	ElapsedTime        float64         `json:"elapsed_time"`
	DownloadBytes      int64           `json:"download_bytes"`
	DownloadFileCount  int             `json:"download_file_count"`
	DownloadStartTime  float64         `json:"download_start_time"`
	DownloadEndTime    float64         `json:"download_end_time"`
	DownloadElapsed    float64         `json:"download_elapsed"`
	UploadBytes        int64           `json:"upload_bytes"`
	UploadFileCount    int             `json:"upload_file_count"`
	UploadStartTime    float64         `json:"upload_start_time"`
	UploadEndTime      float64         `json:"upload_end_time"`
	UploadElapsed      float64         `json:"upload_elapsed"`
}

type TransferStats struct {
	Bytes     int64
	FileCount int
	StartTime time.Time
	EndTime   time.Time
}

func sumFileSizes(files Stringset) int64 {
	var total int64
	for f := range files {
		fi, err := os.Stat(f)
		if err == nil {
			total += fi.Size()
		}
	}
	return total
}

func toUnixFloat(t time.Time) float64 {
	return float64(t.Unix()) + float64(t.Nanosecond())/1e9
}

func timevalToSeconds(tv syscall.Timeval) float64 {
	return float64(tv.Sec) + float64(tv.Usec)/1e6
}

type ResultStruct struct {
	ExePath    string            `json:"exe_path"`
	ExeArgs    []string          `json:"exe_args"`
	Parameters map[string]string `json:"parameters,omitempty"`
	ReturnCode string            `json:"return_code"`
	Files      []*ResultFile     `json:"files"`
	Usage      *ResourceUsage    `json:"resource_usage"`
}

// The amount of time we're willing to wait for a file we uploaded to appear in cloud storage
// Based on the docs, it sounds like it should be visible immediately, but adding some buffer
// just in case we can't rely on that.
const MaxUploadDelay = 30 * time.Second

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

func downloadAll(ioc IOClient, workdir string, downloads []*task_queue.TaskDownload, cacheDir string) (error, Stringset) {
	if !path.IsAbs(workdir) {
		panic("bad workdir")
	}
	downloaded := make(Stringset)

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
					return fmt.Errorf("Could not remove existing file %s that is the destination of new DL: %v", destination, err), downloaded
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

type UploadMapping map[string]string

func newUploadMapping() UploadMapping {
	return make(map[string]string)
}

func addUpload(mapping UploadMapping, filename string, destURL string) {
	mapping[filename] = destURL
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

func ResolveUploads(workdir string, uploads *task_queue.UploadSpec, toExclude Stringset) (UploadMapping, error) {
	included := make(Stringset)

	for _, pattern := range uploads.IncludePatterns {
		addFilesToStringSet(workdir, pattern, included)
	}
	for _, pattern := range uploads.ExcludePatterns {
		addFilesToStringSet(workdir, pattern, toExclude)
	}

	m := newUploadMapping()
	for match := range included {

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

func getModificationTimes(filenames Stringset) map[string]time.Time {
	mtimes := make(map[string]time.Time)
	for filename := range filenames {
		fi, err := os.Stat(filename)
		if err != nil {
			log.Printf("Error trying to stat %s to record last modification time, skipping...", err)
			continue
		}
		mtimes[filename] = fi.ModTime()
	}

	return mtimes
}

func getFilesWithMatchingMTimes(a map[string]time.Time, b map[string]time.Time) Stringset {
	matching := make(Stringset)
	for key, aTime := range a {
		bTime, ok := b[key]
		if !ok {
			log.Printf("While checking if %s was updated, could not find the file. Skipping...", key)
			continue
		}

		if aTime == bTime {
			matching[key] = true
		}
	}
	return matching
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
	Stdout       *os.File
	StdoutPath   string
	Downloaded   Stringset
	InitialMTimes map[string]time.Time
	DlStats      *TransferStats
}

func downloadTaskFiles(ioc IOClient, workDir string, cacheDir string, taskId string, taskSpec *task_queue.TaskSpec, mon *monitor.Monitor) (*DownloadTaskFilesResult, error) {
	stdoutPath := path.Join(workDir, "stdout.txt")
	execLifecycleScript("PreDownloadScript", workDir, taskSpec.PreDownloadScript)

	stdout, err := os.OpenFile(stdoutPath, os.O_WRONLY|os.O_CREATE, 0766)
	if err != nil {
		return nil, err
	}

	if mon != nil {
		mon.StartWatchingLog(taskId, stdoutPath)
	}

	if len(taskSpec.Downloads) > 0 {
		stdout.WriteString(fmt.Sprintf("sparkles: Downloading %d files...\n", len(taskSpec.Downloads)))
	}

	dlStart := time.Now()
	err, downloaded := downloadAll(ioc, workDir, taskSpec.Downloads, cacheDir)
	dlEnd := time.Now()
	if err != nil {
		return nil, err
	}

	if len(taskSpec.Downloads) > 0 {
		stdout.WriteString(fmt.Sprintf("sparkles: download complete.\n"))
	}

	initialMTimes := getModificationTimes(downloaded)

	execLifecycleScript("PostDownloadScript", workDir, taskSpec.PostDownloadScript)

	return &DownloadTaskFilesResult{
		Stdout:        stdout,
		StdoutPath:    stdoutPath,
		Downloaded:    downloaded,
		InitialMTimes: initialMTimes,
		DlStats: &TransferStats{
			Bytes:     sumFileSizes(downloaded),
			FileCount: len(downloaded),
			StartTime: dlStart,
			EndTime:   dlEnd,
		},
	}, nil
}

func uploadTaskResults(ioc IOClient, workDir string, stdoutPath string, taskSpec *task_queue.TaskSpec, downloaded Stringset, initialMTimes map[string]time.Time) (UploadMapping, *TransferStats, error) {
	finalMTimes := getModificationTimes(downloaded)
	downloadsToExclude := getFilesWithMatchingMTimes(initialMTimes, finalMTimes)

	filesToUpload, err := ResolveUploads(workDir, taskSpec.Uploads, downloadsToExclude)
	if err != nil {
		return nil, nil, err
	}

	addUpload(filesToUpload, stdoutPath, taskSpec.StdoutURL)

	ulStats := &TransferStats{FileCount: len(filesToUpload), StartTime: time.Now()}
	for src := range filesToUpload {
		fi, err := os.Stat(src)
		if err == nil {
			ulStats.Bytes += fi.Size()
		}
	}

	if err = UploadMapped(ioc, filesToUpload); err != nil {
		return nil, nil, err
	}
	ulStats.EndTime = time.Now()

	return filesToUpload, ulStats, nil
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

func ExecuteTask(ioc IOClient, taskId string, taskSpec *task_queue.TaskSpec, rootDir string, cacheDir string, tasksDir string, mon *monitor.Monitor) (string, error) {
	workDir, cacheDir, err := prepareTaskDirectories(tasksDir, cacheDir)
	if err != nil {
		return "", err
	}

	dl, err := downloadTaskFiles(ioc, workDir, cacheDir, taskId, taskSpec, mon)
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

	filesToUpload, ulStats, err := uploadTaskResults(ioc, workDir, dl.StdoutPath, taskSpec, dl.Downloaded, dl.InitialMTimes)
	if err != nil {
		return retcode, err
	}

	err = writeResultFile(ioc, taskSpec.CommandResultURL, retcode, execResult, workDir, filesToUpload, taskSpec.Parameters, dl.DlStats, ulStats)
	if err != nil {
		return retcode, err
	}

	return retcode, nil
}

func UploadMapped(ioc IOClient, files map[string]string) error {
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
	execResult *ExecResult,
	workdir string,
	filesToUpload map[string]string,
	parameters map[string]string,
	dlStats *TransferStats,
	ulStats *TransferStats) error {

	files := make([]*ResultFile, 0, 100)
	for src, dstURL := range filesToUpload {
		rel_src, err := filepath.Rel(workdir, src)
		if err != nil {
			log.Printf("Got error in relpath(%s, %s): %v", workdir, src, err)
			return err
		}
		files = append(files, &ResultFile{Src: rel_src, DstURL: dstURL})
	}

	rusage := execResult.Rusage
	elapsedTime := execResult.EndTime.Sub(execResult.StartTime).Seconds()

	result := &ResultStruct{
		ExePath:    execResult.ExePath,
		ExeArgs:    execResult.ExeArgs,
		Parameters: parameters,
		ReturnCode: retcode,
		Files:      files,
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

