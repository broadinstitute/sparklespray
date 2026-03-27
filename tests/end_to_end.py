import asyncio
import json
import tempfile


class Process:
    def __init__(self, proc: asyncio.subprocess.Process):
        self._proc = proc

    async def wait(self):
        return await self._proc.wait()

    def kill(self):
        self._proc.kill()


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
        asyncio.create_task(_stream_output(name, proc.stdout, watcher))
        pproc = Process(proc)
        self.procs.append(pproc)
        return pproc


async def main():
    print("Submitting job with autoscaler running (but using simulated Batch API)")

    # use ProcessGroup context manager so that we're guarenteed that processes are terminated
    # before returning. Don't want to accidently leave a process behind when an exception is thrown
    with ProcessGroup() as group:
        watcher = Watcher()
        submitted_seen = watcher.watch_for("submitted")

        submission = {
            "Name": "test-job-1",
            "ClusterID": "local",
            "Command": "echo 'hello from sparklespray' && date",
            "TopicPrefix": "sparkles",
            # "FilesToStage": [{ "LocalPath": "test-script", "Name": "test-script" }]
        }
        submission_path = "sample.json"
        with open(submission_path, "wt") as fd:
            fd.write(json.dumps(submission))

        # bash -c 'cd src/sparklesworker/cmd && go build -o ../../../sparkles sparkles/main.go
        # bash -c 'cd src/sparklesworker/cmd && go build -o ../../../sparklesworker sparklesworker/main.go '

        # submit a task
        submit = await group.run_and_stream(
            "submit", f"./sparkles dev submit {submission_path}", watcher
        )
        # block until we've successfully seen that the job was submitted
        await submitted_seen.wait()

        # at this point it should be sitting in a queue
        # start the autoscaler, which should submit to the (mock) batch API a
        # request for a consumer. (The mock batch API runs within the autoscaler and will spawn
        # processes locally for testing purposes)
        #
        # The consumer should then spawn as a child process
        # and then pick up the job. Eventually the task should be done, and the autoscaler
        # should terminate.
        autoscale = await group.run_and_stream(
            "autoscale", "./sparklesworker autoscaler --redisAddr localhost:6379"
        )

        # wait for the autoscale to shutdown.
        await autoscale.wait()

        # now submit should also be done
        await submit.wait()

        # verify the outputs


if __name__ == "__main__":
    asyncio.run(main())
