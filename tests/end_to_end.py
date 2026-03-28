import asyncio
import json
import tempfile


class Process:
    def __init__(self, proc: asyncio.subprocess.Process):
        self._proc = proc

    async def wait(self):
        return await self._proc.wait()

    def is_running(self):
        return self._proc.returncode is None

    def kill(self):
        try:
            self._proc.kill()
        except ProcessLookupError:
            pass


class Watcher:
    def __init__(self) -> None:
        self.patterns = []

    def watch_for(self, text):
        event = asyncio.Event()
        self.patterns.append((text, event))
        return event

    def process_line(self, line):
        for text, event in self.patterns:
            if text in line:
                event.set()


async def _stream_output(name: str, stream: asyncio.StreamReader, watcher: Watcher):
    while True:
        line = await stream.readline()
        if not line:
            break
        line = line.decode(errors="replace")
        print(f"[{name}] {line}", end="")
        watcher.process_line(line)
    print(f"[{name}] (done)")


class ProcessGroup:
    def __init__(self):
        self.procs = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        for proc in self.procs:
            # TODO add check to see if still running
            proc.kill()

    async def run_and_stream(self, name: str, command: str, watcher=None) -> Process:
        if watcher is None:
            watcher = Watcher()

        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        assert proc.stdout is not None
        asyncio.create_task(_stream_output(name, proc.stdout, watcher))
        pproc = Process(proc)
        self.procs.append(pproc)
        return pproc


import contextlib
import tempfile
import shutil


@contextlib.contextmanager
def make_temp_dir():
    tmpdir = tempfile.mkdtemp()
    print(f"Created temp directory for test: {tmpdir}")
    try:
        yield tmpdir
    except:
        print(f"Got exception -- leaving {tmpdir} behind for debugging purposes")
        raise
    shutil.rmtree(tmpdir)


async def minimal_end_to_end_test():
    # use ProcessGroup context manager so that we're guarenteed that processes are terminated
    # before returning. Don't want to accidently leave a process behind when an exception is thrown
    with ProcessGroup() as group, make_temp_dir() as tmpDir:
        print("building executables")

        await (
            await group.run_and_stream(
                "build",
                "cd src/sparklesworker/cmd && go build -o ../../../bin/sparkles sparkles/main.go",
            )
        ).wait()
        await (
            await group.run_and_stream(
                "build",
                "cd src/sparklesworker/cmd && go build -o ../../../bin/sparklesworker sparklesworker/main.go",
            )
        ).wait()

        print("Submitting job with autoscaler running (but using simulated Batch API)")

        redis_port = 7779
        redis = await group.run_and_stream(
            "redis", f"redis-server --port {redis_port} --save ''"
        )

        watcher = Watcher()
        submitted_seen = watcher.watch_for("Successfully submitted")

        # Name      string `json:"name"`
        # SizeGB    int64  `json:"size_gb"`
        # Type      string `json:"type"`
        # MountPath string `json:"mount_path"`

        # type DevClusterConfig struct {
        # 	// MachineType is the GCE machine type used when launching worker nodes.
        # 	MachineType string

        # 	// WorkerDockerImage is the container image run on each worker node.
        # 	WorkerDockerImage string

        # 	// PubSubInTopic is the Pub/Sub topic the monitor publishes control messages to.
        # 	PubSubInTopic string

        # 	// PubSubOutTopic is the Pub/Sub topic workers publish status messages to.
        # 	PubSubOutTopic string

        # 	// Region is the GCP region where Batch jobs are submitted (e.g. "us-central1").
        # 	// Used to construct the Batch API parent path.
        # 	Region string

        # 	// MaxPreemptableAttempts is the total number of preemptable node-attempts
        # 	// allowed per job run. Resets when the queue drains to zero.
        # 	MaxPreemptableAttempts int

        # 	// MaxInstanceCount caps the number of nodes the monitor will request,
        # 	// regardless of queue depth.
        # 	MaxInstanceCount int

        # 	// MaxSuspiciousFailures is the threshold for how many batch jobs may complete
        # 	// without doing any work before the monitor halts node creation and alerts.
        # 	MaxSuspiciousFailures int

        # 	BootDisk        backend.Disk             `json:"boot_disk"`
        # 	Disks           []backend.Disk           `json:"disks"`
        # 	GCSBucketMounts []backend.GCSBucketMount `json:"gcs_bucket_mounts"`
        # }

        # type DevSubmitRequest struct {
        # 	// --- backend ---
        # 	ProjectID string `json:"projectID"`
        # 	Database  string `json:"database"`
        # 	// If non-empty, use the Redis backend instead of Firestore.
        # 	RedisAddr string `json:"redisAddr"`

        # 	// --- worker directories ---
        # 	Dir      string `json:"dir"`
        # 	CacheDir string `json:"cacheDir"`
        # 	TasksDir string `json:"tasksDir"`

        # 	// --- aether file-staging ---
        # 	AetherRoot            string `json:"aetherRoot"`
        # 	AetherMaxSizeToBundle int64  `json:"aetherMaxSizeToBundle"`
        # 	AetherMaxBundleSize   int64  `json:"aetherMaxBundleSize"`
        # 	AetherWorkers         int    `json:"aetherWorkers"`

        # 	// --- timing / naming ---
        # 	// Duration string accepted by time.ParseDuration, e.g. "24h".  Defaults to "24h".
        # 	Expiry      string `json:"expiry"`
        # 	TopicPrefix string `json:"topicPrefix"`
        # 	// How long RunLoop waits for new tasks after the queue drains before exiting.
        # 	// Also used as the first-poll retry delay. Defaults to 10s.
        # 	RunLoopMaxWait time.Duration `json:"runLoopMaxWait"`

        # 	// --- job spec ---
        # 	Name         string            `json:"name"`
        # 	ClusterID    string            `json:"clusterID"`
        # 	Cluster      *DevClusterConfig `json:"cluster"`
        # 	DockerImage  string            `json:"dockerImage"`
        # 	Command      string            `json:"command"`
        # 	FilesToStage []fileToStage     `json:"filesToStage"`

        # 	// If non-empty, export OutputAetherFSRoot from the completed task to this local directory.
        # 	ExportOutputTo string `json:"exportOutputTo"`
        # 	// If non-empty, export LogAetherFSRoot from the completed task to this local directory.
        # 	ExportLogTo string `json:"exportLogTo"`
        # }

        submission = {
            "name": "test-end-to-end",
            "cluster": {
                "MachineType": "n2-standard-2",
                "WorkerDockerImage": "invalid-docker-image",
                "PubSubInTopic": "sparkles-in",
                "PubSubOutTopic": "sparkles-out",
                "Region": "us-central1",
                "MaxPreemptableAttempts": 1,
                "MaxInstanceCount": 1,
                "MaxSuspiciousFailures": 1,
                "BootDisk": {"size_gb": 50, "type": "pd-standard"},
            },
            "redisAddr": f"localhost:{redis_port}",
            "aetherRoot": f"{tmpDir}/aether",
            "exportOutputTo": f"{tmpDir}/out",
            "exportLogTo": f"{tmpDir}/log",
            "dockerImage": "",
            "command": "echo hello from sparklespray",
            "filesToStage": [],
            "topicPrefix": "sparkles",
            "runLoopMaxWait": 0,
        }
        submission_path = f"{tmpDir}/submission.json"
        with open(submission_path, "wt") as fd:
            fd.write(json.dumps(submission))

        # submit a task
        submit = await group.run_and_stream(
            "submit",
            f"bin/sparkles dev submit {submission_path}",
            watcher,  # todo: add --skip-provisioning once we have submit do that
        )

        # block until we've successfully seen that the job was submitted
        try:
            await asyncio.wait_for(submitted_seen.wait(), 3)
        except asyncio.TimeoutError:
            raise Exception("Did not see successful submission")

        # but make sure the submit process is still waiting for the job to complete
        assert submit.is_running()

        # At this point there should be a should be sitting in a queue.
        #
        # Now, start the autoscaler, which should submit to the (mock) batch API a
        # request to start a sparklesworker consumer. (The mock batch API runs within
        # the autoscaler and will spawn processes locally for testing purposes)
        #
        # The consumer should then then pick up the job. Eventually the task should be
        # done, and the autoscaler should terminate when it sees there's not remaining work to do
        #
        autoscale = await group.run_and_stream(
            "autoscale",
            f"bin/sparklesworker autoscaler --redis localhost:{redis_port} --poll-interval 100ms",
        )

        # breakpoint()
        # wait for the autoscale proccess to shutdown.
        await asyncio.wait_for(autoscale.wait(), 5)

        # now submit should also be done
        await asyncio.wait_for(submit.wait(), 5)

        # verify the outputs
        with open(f"{tmpDir}/log/stdout.txt", "rt") as fd:
            stdout = fd.read()

        assert "hello from sparklespray" in stdout


if __name__ == "__main__":
    asyncio.run(minimal_end_to_end_test())
