package kubequeconsume

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
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
)

type TaskDownload struct {
	IsCASKey   bool   `json:"is_cas_key"`
	Executable bool   `json:"executable"`
	Dst        string `json:"dst"`
	SrcURL     string `json:"src_url"`
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

type ResultStruct struct {
	Command    string            `json:"command"`
	Parameters map[string]string `json:"parameters,omitempty"`
	ReturnCode string            `json:"return_code"`
	Files      []*ResultFile     `json:"files"`
	Usage      *ResourceUsage    `json:"resource_usage"`
}

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

		//parentDir := strings.ToLower(path.Base(path.Dir(srcURL)))
		if dl.IsCASKey {
			casKey := path.Base(srcURL)
			cacheDest := path.Join(cacheDir, casKey)

			if _, err := os.Stat(cacheDest); !os.IsNotExist(err) {
				err = ioc.Download(srcURL, cacheDest)
				if err != nil {
					return err, downloaded
				}
			} else {
				log.Printf("No download, %s already exists", cacheDest)
			}

			err := copyFile(cacheDest, destination)
			if err != nil {
				// this should not be possible
				panic(fmt.Sprintf("Error calling proc.Wait(): %s", err))
			}
		} else {
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

func execCommand(command string, workdir string, stdout *os.File) (*syscall.Rusage, string, error) {
	attr := &os.ProcAttr{Dir: workdir, Env: nil, Files: []*os.File{nil, stdout, stdout}}
	exePath := "/bin/sh"
	proc, err := os.StartProcess(exePath, []string{exePath, "-c", command}, attr)
	if err != nil {
		return nil, "", err
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

	rusage := procState.SysUsage().(*syscall.Rusage)
	status := procState.Sys().(syscall.WaitStatus)
	var statusStr string
	if status.Signaled() {
		statusStr = fmt.Sprintf("signaled(%s)", status.Signal())
	} else {
		statusStr = fmt.Sprintf("%d", status.ExitStatus())
	}

	return rusage, statusStr, nil
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

func executeTaskInDir(ioc IOClient, workdir string, taskId string, spec *TaskSpec, cachedir string, loggingClient *logging.Client) (string, error) {
	stdoutPath := path.Join(workdir, "stdout.txt")
	execLifecycleScript("PreDownloadScript", workdir, spec.PreDownloadScript)

	err, downloaded := downloadAll(ioc, workdir, spec.Downloads, cachedir)
	if err != nil {
		return "", err
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

	stdout, err := os.OpenFile(stdoutPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0766)
	if err != nil {
		return "", err
	}

	if loggingClient != nil {
		shutdownChan, err := startWatchingLog(loggingClient, taskId, stdoutPath)
		if err != nil {
			log.Printf("Could not start log watch: %v", err)
		}
		defer close(shutdownChan)
	}

	cwdDir := path.Join(workdir, commandWorkingDir)
	log.Printf("Executing (working dir: %s, output written to: %s): %s", cwdDir, stdoutPath, spec.Command)
	resourceUsage, retcode, err := execCommand(spec.Command, cwdDir, stdout)
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

	err = writeResultFile(ioc, spec.CommandResultURL, retcode, resourceUsage, workdir, filesToUpload, spec.Command, spec.Parameters)
	if err != nil {
		return retcode, err
	}

	err = uploadMapped(ioc, filesToUpload)

	return retcode, err
}

func executeTask(ioc IOClient, taskId string, taskSpec *TaskSpec, cacheDir string, tasksDir string, loggingClient *logging.Client) (string, error) {
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

	retcode, err := executeTaskInDir(ioc, workDir, taskId, taskSpec, cacheDir, loggingClient)
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
	return nil
}

func writeResultFile(ioc IOClient,
	CommandResultURL string,
	retcode string,
	resourceUsage *syscall.Rusage,
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
			BlockOutputOps:     resourceUsage.Oublock}}

	resultJson, err := json.Marshal(result)
	if err != nil {
		return err
	}

	return ioc.UploadBytes(CommandResultURL, resultJson)
}

func loadTaskSpec(ioc IOClient, taskURL string) (*TaskSpec, error) {
	data, err := ioc.DownloadAsBytes(taskURL)
	if err != nil {
		return nil, err
	}

	var taskSpec TaskSpec
	err = json.Unmarshal(data, &taskSpec)
	if err != nil {
		return nil, err
	}

	return &taskSpec, nil
}

func ExecuteTaskFromUrl(ioc IOClient, taskId string, taskURL string, cacheDir string, tasksDir string, loggingClient *logging.Client) (string, error) {
	taskSpec, err := loadTaskSpec(ioc, taskURL)
	if err != nil {
		return "", err
	}

	return executeTask(ioc, taskId, taskSpec, cacheDir, tasksDir, loggingClient)
}
