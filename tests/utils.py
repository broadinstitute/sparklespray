import asyncio
from typing import Optional


class Watcher:
    def __init__(self) -> None:
        # the patterns to look out for, and when seen, signal an event
        self.patterns = []
        # collects the output here
        self.output_lines = []

    def watch_for(self, text):
        event = asyncio.Event()
        self.patterns.append((text, event))
        return event

    def process_line(self, line):
        for text, event in self.patterns:
            if text in line:
                event.set()
        self.output_lines.append(line)


class Process:
    def __init__(self, name: str, proc: asyncio.subprocess.Process, watcher: Watcher):
        self.name = name
        self._proc = proc
        self.watcher = watcher

    async def wait(self, timeout=None):
        if timeout is None:
            return await self._proc.wait()
        else:
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                raise Exception(f"Timeout waiting for {repr(self.name)} to complete")

    def is_running(self):
        return self._proc.returncode is None

    def kill(self):
        try:
            self._proc.kill()
        except ProcessLookupError:
            pass

    def get_output(self):
        return self.watcher.output_lines


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
    # use ProcessGroup context manager so that we're guarenteed that processes are terminated
    # before returning. Don't want to accidently leave a process behind when an exception is thrown
    def __init__(self):
        self.procs = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        for proc in self.procs:
            # TODO add check to see if still running
            proc.kill()

    async def run(self, name: str, command: str, timeout=60):
        proc = await self.run_in_background(name, command)
        try:
            retcode = await asyncio.wait_for(proc.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            raise Exception("Timeout waiting for submission to complete")

        assert retcode == 0

    async def run_in_background(
        self,
        name: str,
        command: str,
        block_until_text: Optional[str] = None,
        timeout=30,
    ) -> Process:
        """
        Runs a command and streams the output to the console prefixed with "[<name>] ..." so we
        can tell which output came from which command.

        Optionally takes a string `block_until_text` which will cause this to block until that string is seen in the output or until the timeout parameter is reached.
        This is to distinguish a command which exits immediately due to an error or one that successfully launches and then keeps running.
        """
        print("running", command)
        watcher = Watcher()
        event_to_wait_for = None
        if block_until_text is not None:
            event_to_wait_for = watcher.watch_for(block_until_text)

        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        assert proc.stdout is not None
        asyncio.create_task(_stream_output(name, proc.stdout, watcher))

        pproc = Process(name, proc, watcher)
        self.procs.append(pproc)

        if event_to_wait_for is not None:
            try:
                await asyncio.wait(
                    [
                        asyncio.create_task(event_to_wait_for.wait()),
                        asyncio.create_task(proc.wait()),
                    ],
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            except asyncio.TimeoutError:
                raise Exception(
                    f"Timeout waiting to see {repr(block_until_text)} in the output"
                )

            if not event_to_wait_for.is_set():
                raise Exception(f"Did not see {repr(block_until_text)} in the output")

        return pproc

    async def wait_all(self):
        for proc in self.procs:
            await proc.wait()
