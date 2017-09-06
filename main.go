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
	"syscall"
	"time"
	//	"cloud.google.com/go/storage"
	//	"golang.org/x/net/context"
)

const STATUS_CLAIMED = "claimed"
const STATUS_PENDING = "pending"

type ClaimedEvent struct {
	timestamp int64
	status    string
	owner     string //optional
}

type FailedEvent struct {
	failure_reason string //optional
}

type Task struct {
	// will be of the form: job_id + task_index
	task_id            string
	task_index         int64
	job_id             string
	status             string
	owner              string
	args               string
	history            []interface{}
	command_result_url string
	failure_reason     string // optional
	version            int32
	exit_code          int32
}

type Job struct {
	job_id        int
	tasks         []Task
	kube_job_spec string
	metadata      string
}

const INITIAL_CLAIM_RETRY_DELAY = 1000

func getTimestamp() int64 {
	return int64(time.Now().Unix())
}

/*
func getTasks(jobId string, state string, maxFetch int) []*Task {

}

func claimTask(ctx context.Context, client datastore.Client, jobId string, newOwner string, minTryTime int, claimTimeout int) (*Task, error) {
	//     "Returns None if no unclaimed ready tasks. Otherwise returns instance of Task"
	claimStart := getTimestamp()
	maxSleepTime := INITIAL_CLAIM_RETRY_DELAY
	for {
		tasks := getTasks(jobId, "pending", 10)
		if len(tasks) == 0 {
			// We might have tasks we can't see yet.
			if getTimestamp()-claimStart < int64(minTryTime) {
				time.Sleep(time.Second)
				continue
			} else {
				return nil, nil
			}
		}

		// pick a random task to avoid contention
		task := tasks[rand.Int31n(int32(len(tasks)))]

		finalTask, err := updateTaskClaimed(ctx, client, task.task_id, newOwner)
		if err == nil {
			maxSleepTime = INITIAL_CLAIM_RETRY_DELAY
			return task, nil
		}

		// failed to claim task.
		claimEnd := getTimestamp()
		if claimEnd-claimStart > int64(claimTimeout) {
			return nil, errors.New("Timed out trying to get task")
		}

		log.Printf("Got error claiming task: %s, will rety", err)

		maxSleepTime *= 2
		time.Sleep(time.Millisecond * rand.Int31n(int32(maxSleepTime)))
	}
}

// def task_completed(self, task_id, was_successful, failure_reason=None, retcode=None):
//     if was_successful:
//         new_status = STATUS_SUCCESS
//         assert failure_reason is None
//     else:
//         new_status = STATUS_FAILED
//     self.updateTaskStatus(task_id, new_status, failure_reason, retcode)

func updateTaskClaimed(ctx context.Context, client datastore.Client, task_id string, newOwner string) (*Task, error) {
	now := getTimestamp()
	event := ClaimedEvent{timestamp: now,
		status: STATUS_CLAIMED,
		owner:  newOwner}

	mutate := func(task *Task) bool {
		if task.status != STATUS_PENDING {
			log.Printf("Expected status to be pending but was '%s'\n", task.status)
			return false
		}

		task.history = append(task.history, &event)
		task.status = STATUS_CLAIMED
		task.owner = newOwner

		return true
	}

	updatedTask, err := atomicUpdateTask(ctx, client, task_id, mutate)
	if err != nil {
		return nil, err
	}

	return updatedTask, nil
}

func updateTaskStatus(ctx context.Context, client datastore.Client, task_id string, newStatus string, failureReason string, retcode int32) (*Task, error) {
	now := getTimestamp()
	taskHistory := TaskHistory{timestamp: now,
		status:         newStatus,
		failure_reason: failureReason}

	mutate := func(task *Task) bool {
		task.history = append(task.history, taskHistory)
		task.status = newStatus
		task.failure_reason = failureReason
		task.exit_code = retcode

		return true
	}

	task, err := atomicUpdateTask(ctx, client, task_id, mutate)
	if err != nil {
		// I suppose this is not technically correct. Could be a simultaneous update of "success" or "failed" and "lost"
		return nil, err
	}

	return task, nil
}

func atomicUpdateTask(ctx context.Context, client datastore.Client, task_id string, mutateTaskCallback func(task *Task) bool) (*Task, error) {
	var task Task

	_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		log.Printf("attempting update of task %s start", task_id)

		taskKey := datastore.NameKey("Task", task_id, nil)
		err := client.Get(ctx, taskKey, task)
		if err != nil {
			return err
		}

		successfulUpdate := mutateTaskCallback(&task)
		if !successfulUpdate {
			return errors.New("Update failed")
		}

		task.version = task.version + 1
		_, err = tx.Put(taskKey, &task)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// task_as_json = json.dumps(attr.asdict(task)).encode("utf8")

	// topic_name = self._job_id_to_topic(task.job_id)
	// topic = self.pubsub.topic(topic_name)
	// topic.publish(task_as_json)

	log.Printf("atomic update of task %s success", task_id)
	return task, nil
}

func mainx() {
	ctx := context.Background()

	// Create a datastore client. In a typical application, you would create
	// a single client which is reused for every datastore operation.
	dsClient, err := datastore.NewClient(ctx, "my-project")
	if err != nil {
		// Handle error.
	}

	k := datastore.NameKey("Entity", "stringID", nil)
	e := new(Entity)
	if err := dsClient.Get(ctx, k, e); err != nil {
		// Handle error.
	}

	old := e.Value
	e.Value = "Hello World!"

	if _, err := dsClient.Put(ctx, k, e); err != nil {
		// Handle error.
	}

	fmt.Printf("Updated value from %q to %q\n", old, e.Value)
}

func main() {
	c := cli.NewCLI("app", "1.0.0")
	c.Args = os.Args[1:]
	// c.Commands = map[string]cli.CommandFactory{
	// 	"foo": fooCommandFactory,
	// 	"bar": barCommandFactory,
	// }

	exitStatus, err := c.Run()
	if err != nil {
		log.Println(err)
	}

	os.Exit(exitStatus)
}

func consumerRunLoop(ctx context.Context, client datastore.Client, jobId string, newOwner string) error {
	for {
		claimed, err := claimTask(jobId, newOwner, 5*1000, 30*1000)
		if err != nil {
			return err
		}
		if claimed == nil {
			break
		}

		log.Printf("Claimed task %s\n", claimed.task_id)

		retcode = executeTask(task_id, claimed.args)
		_, err = updateTaskStatus(ctx, client, claimed.task_id, STATUS_SUCCESS, "", retcode)
		if err != nil {
			log.Printf("Got error executing task %s: %s\n", claimed.task_id, err)
			return err
		}
	}
	log.Printf("No more tasks to claim\n")

	return nil
}
*/

// test: populate lots of tasks.  Spawn a lot of threads which concurrently try to claim tasks and then mark them complete.
// afterwards, reconcile the task histories against the thread's logs of what they executed.

type TaskUpload struct {
	SrcWildcard string `json:"src_wildcard"`
	DstURL      string `json:"dst_url"`
}

type TaskDownload struct {
	IsCASKey   bool   `json:"is_cas_key"`
	Executable bool   `json:"executable"`
	Dst        string `json:"dst"`
	SrcURL     string `json:"src_url"`
}

type TaskSpec struct {
	WorkingDir         string            `json:"working_dir,omitempty"`
	PreDownloadScript  string            `json:"pre-download-script,omitempty"`
	PostDownloadScript string            `json:"post-download-script,omitempty"`
	PostExecScript     string            `json:"post-exec-script,omitempty"`
	PreExecScript      string            `json:"pre-exec-script,omitempty"`
	Parameters         map[string]string `json:"parameters,omitempty"`
	Uploads            []*TaskUpload     `json:"uploads"`
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
	ReturnCode string         `json:"return_code"`
	Files      []*ResultFile  `json:"files"`
	Usage      *ResourceUsage `json:"resource_usage"`
}

type stringset map[interface{}]bool

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

	_, err = io.Copy(out, in)
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
			copyFile(cacheDest, destination)
		} else {
			err := ioc.Download(srcURL, destination)
			if err != nil {
				return err, downloaded
			}
		}
		downloaded[destination] = true
	}

	return nil, downloaded
}

func execCommand(command, workdir, stdoutPath string) (*syscall.Rusage, string, error) {
	stdout, err := os.OpenFile(stdoutPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0766)
	if err != nil {
		return nil, "", err
	}

	attr := &os.ProcAttr{Dir: workdir, Env: nil, Files: []*os.File{nil, stdout, stdout}}
	proc, err := os.StartProcess("/bin/sh", []string{"-c", command}, attr)
	if err != nil {
		return nil, "", err
	}

	procState, err := proc.Wait()
	if err != nil {
		return nil, "", err
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

func resolveUploads(workdir string, uploads []*TaskUpload, downloaded stringset) (UploadMapping, error) {
	m := newUploadMapping()
	for _, upload := range uploads {
		pathWithGlob := path.Join(workdir, upload.SrcWildcard)
		matches, err := filepath.Glob(pathWithGlob)
		if err != nil {
			return nil, err
		}
		log.Printf("pathWithGlob=%v, matches=%v\n", pathWithGlob, matches)

		for _, match := range matches {
			match, err = filepath.Abs(match)
			if err != nil {
				return nil, err
			}
			if downloaded[match] {
				continue
			}

			relpath, err := filepath.Rel(workdir, match)
			if err != nil {
				return nil, err
			}
			dest := upload.DstURL + "/" + relpath
			addUpload(m, match, dest)
		}
	}
	return m, nil
}

func execLifecycleScript(label string, workdir string, script string) {
	if script == "" {
		return
	}

	log.Printf("Executing %s script:", label)
	cmd := exec.Command("sh", "-c", script)
	cmd.Dir = workdir
	err := cmd.Run()
	if err != nil {
		log.Printf("Command finished with error: %v", err)
	}
}

func executeTaskInDir(ioc IOClient, workdir string, spec *TaskSpec, cachedir string) error {
	stdoutPath := path.Join(workdir, "stdout.txt")
	execLifecycleScript("PreDownloadScript", workdir, spec.PreDownloadScript)

	err, downloaded := downloadAll(ioc, workdir, spec.Downloads, cachedir)
	if err != nil {
		return err
	}

	execLifecycleScript("PostDownloadScript", workdir, spec.PostDownloadScript)

	commandWorkingDir := spec.WorkingDir
	if commandWorkingDir == "" {
		commandWorkingDir = "."
	}
	if path.IsAbs(commandWorkingDir) {
		panic("bad commandWorkingDir")
	}

	resourceUsage, retcode, err := execCommand(spec.Command, path.Join(workdir, commandWorkingDir), stdoutPath)
	if err != nil {
		return err
	}

	execLifecycleScript("PostExecScript", workdir, spec.PostExecScript)

	filesToUpload, err := resolveUploads(workdir, spec.Uploads, downloaded)
	if err != nil {
		return err
	}

	addUpload(filesToUpload, stdoutPath, spec.StdoutURL)

	err = writeResultFile(ioc, spec.CommandResultURL, retcode, resourceUsage, workdir, filesToUpload)
	if err != nil {
		return err
	}

	err = uploadMapped(ioc, filesToUpload)

	return err
}

func executeTask(ioc IOClient, taskId string, taskSpec *TaskSpec, cacheDir string) error {
	//	log.Printf("Job spec (%s) of claimed task: %s", json_url, json.dumps(spec, indent=2))

	mode := os.FileMode(0700)
	taskDir, err := ioutil.TempDir(".", "task-")
	err = os.Mkdir(taskDir, mode)
	if err != nil {
		return err
	}

	logDir := path.Join(taskDir, "log")
	err = os.Mkdir(logDir, mode)
	if err != nil {
		return err
	}

	workDir := path.Join(taskDir, "work")
	err = os.Mkdir(workDir, mode)
	if err != nil {
		return err
	}

	err = executeTaskInDir(ioc, workDir, taskSpec, cacheDir)
	if err != nil {
		return err
	}

	return nil
}

func uploadMapped(ioc IOClient, files map[string]string) error {
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
	filesToUpload map[string]string) error {

	files := make([]*ResultFile, 0, 100)
	for src, dstURL := range filesToUpload {
		files = append(files, &ResultFile{Src: src, DstURL: dstURL})
	}

	result := &ResultStruct{
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

func executeTaskFromUrl(ioc IOClient, taskId string, taskURL string, cacheDir string) error {
	taskSpec, err := loadTaskSpec(ioc, taskURL)
	if err != nil {
		return err
	}

	return executeTask(ioc, taskId, taskSpec, cacheDir)
}
