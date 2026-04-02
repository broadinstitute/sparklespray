import asyncio
import json
import tempfile
import pytest

import contextlib
import shutil
from utils import ProcessGroup, Watcher


async def build_executables(tmpdir: str, proc_group: ProcessGroup):
    print("building executables")

    await proc_group.run(
        "build",
        "cd src/sparklesworker/cmd && go build -o ../../../bin/sparkles sparkles/main.go",
    )

    await proc_group.run(
        "build",
        "cd src/sparklesworker/cmd && go build -o ../../../bin/sparklesworker sparklesworker/main.go",
    )


async def _submit(tmpdir: str, proc_group: ProcessGroup, submission: dict):
    submission_path = f"{tmpdir}/submission.json"

    with open(submission_path, "wt") as fd:
        fd.write(json.dumps(submission))

    # submit a task
    submit = await proc_group.run_in_background(
        "submit",
        f"bin/sparkles dev submit {submission_path}",
        block_until_text="Successfully submitted",  # todo: add --skip-provisioning once we have submit do that
    )

    return submit


async def build_and_push_worker_image(proc_group: ProcessGroup, sparkles_docker_image):
    print("skipping docker build")
    return
    await proc_group.run(
        "build",
        f"cd src/sparklesworker && docker build -t {sparkles_docker_image} .",
        timeout=200,
    )
    await proc_group.run(
        "build",
        f"docker push {sparkles_docker_image}",
    )


async def minimal_gcp_submission_with_autoscale(proc_group: ProcessGroup, tmpdir):
    submit = lambda submission: _submit(
        proc_group=proc_group, tmpdir=tmpdir, submission=submission
    )

    sparkles_docker_image = (
        "us-central1-docker.pkg.dev/test-sparkles-2/docker/sparklesworker:test"
    )
    sparklesworkerDir = "/sparkleswork"

    await build_and_push_worker_image(proc_group, sparkles_docker_image)
    await build_executables(proc_group=proc_group, tmpdir=tmpdir)

    submission = {
        "name": "test-end-to-end",
        "cluster": {
            "MachineType": "n2-standard-2",
            "WorkerDockerImage": sparkles_docker_image,
            "PubSubInTopic": "sparkles-in",
            "PubSubOutTopic": "sparkles-out",
            "Region": "us-central1",
            "MaxPreemptableAttempts": 1,
            "MaxInstanceCount": 1,
            "MaxSuspiciousFailures": 1,
            "BootDisk": {"size_gb": 50, "type": "pd-standard"},
            "MaxLingerSeconds": 1,
        },
        "projectID": "test-sparkles-2",
        "region": "us-central1",
        "database": "sparkles-v6",
        "aetherRoot": f"{tmpdir}/aether",
        "exportOutputTo": f"{tmpdir}/out",
        "exportLogTo": f"{tmpdir}/log",
        "dir": sparklesworkerDir,
        "dockerImage": sparkles_docker_image,
        "command": "echo hello from sparklespray",
        "filesToStage": [],
        "topicPrefix": "sparkles",
        "runLoopMaxWait": 5,
    }

    submit_proc = await submit(submission)

    # but make sure the submit process is still waiting for the job to complete
    assert submit_proc.is_running()

    # At this point there should be a should be sitting in a queue and the autoscaler should be starting. Wait for command to run.
    try:
        await asyncio.wait_for(submit_proc.wait(), 60)
    except asyncio.TimeoutError:
        raise Exception("Timeout waiting for submission to complete")

    # verify the outputs
    with open(f"{tmpdir}/log/stdout.txt", "rt") as fd:
        stdout = fd.read()

    assert "hello from sparklespray" in stdout

    # submit a second job, and this should be almost immediate
    submission["command"] = ("echo second hello from sparklespray",)
    submit_proc = await submit(submission)

    # but make sure the submit process is still waiting for the job to complete
    assert submit_proc.is_running()

    # Since the autoscaler and existing worker should still be running, should start quickly
    try:
        await asyncio.wait_for(submit_proc.wait(), 10)
    except asyncio.TimeoutError:
        raise Exception("Timeout waiting for submission to complete")

    # verify the outputs
    with open(f"{tmpdir}/log/stdout.txt", "rt") as fd:
        stdout = fd.read()

    assert "second hello from sparklespray" in stdout


async def minimal_gcp_with_local_autoscale(proc_group: ProcessGroup, tmpdir: str):
    submit = lambda submission: _submit(
        proc_group=proc_group, tmpdir=tmpdir, submission=submission
    )

    sparkles_docker_image = (
        "us-central1-docker.pkg.dev/test-sparkles-2/docker/sparklesworker:test"
    )
    sparklesworkerDir = "/sparkleswork"

    await build_and_push_worker_image(proc_group, sparkles_docker_image)
    await build_executables(proc_group=proc_group, tmpdir=tmpdir)

    project = "test-sparkles-2"
    region = "us-central1"
    database = "sparkles-v6"

    submission = {
        "name": "test-end-to-end",
        "cluster": {
            "MachineType": "n2-standard-2",
            "WorkerDockerImage": sparkles_docker_image,
            "PubSubInTopic": "sparkles-in",
            "PubSubOutTopic": "sparkles-batchapi-out",
            "Region": "us-central1",
            "MaxPreemptableAttempts": 1,
            "MaxInstanceCount": 1,
            "MaxSuspiciousFailures": 1,
            "BootDisk": {"size_gb": 50, "type": "pd-standard"},
        },
        "projectID": project,
        "region": region,
        "database": database,
        "aetherRoot": f"{tmpdir}/aether",
        "exportOutputTo": f"{tmpdir}/out",
        "exportLogTo": f"{tmpdir}/log",
        "dir": sparklesworkerDir,
        "dockerImage": sparkles_docker_image,
        "command": "echo hello from sparklespray",
        "filesToStage": [],
        "topicPrefix": "sparkles",
        "runLoopMaxWait": 5,
        "skipAutoscale": True,
    }

    submit_proc = await submit(submission)

    # make sure the submit process is still waiting for the job to complete
    assert submit_proc.is_running()

    # At this point there should be a should be sitting in a queue. We in this scenerio we need to start the autoscaler

    await proc_group.run(
        "autoscaler",
        f"bin/sparklesworker autoscaler --project {project} --region {region} --database {database} --poll-interval 0s --max-idle 0s",
        timeout=60 * 10,
    )

    # now that the autoscaler has completed, the submission process should have also completed
    await submit_proc.wait(timeout=600)

    # verify the outputs
    with open(f"{tmpdir}/log/stdout.txt", "rt") as fd:
        stdout = fd.read()

    assert "hello from sparklespray" in stdout

    # submit a second job, and this should be almost immediate
    submission["command"] = ("echo second hello from sparklespray",)
    submit_proc = await submit(submission)

    # but make sure the submit process is still waiting for the job to complete
    assert submit_proc.is_running()

    # Since the autoscaler and existing worker should still be running, should start quickly
    await submit_proc.wait(timeout=10)

    # verify the outputs
    with open(f"{tmpdir}/log/stdout.txt", "rt") as fd:
        stdout = fd.read()

    assert "second hello from sparklespray" in stdout


async def minimal_local_test(
    proc_group, tmpdir, unused_tcp_port_factory, use_sparklesworker_docker_image=False
):

    redis_port = unused_tcp_port_factory()

    if use_sparklesworker_docker_image:
        sparkles_docker_image = "sparklesworker:test"
        sparklesworkerDir = "/sparkleswork"
        await proc_group.run_with_check(
            "build",
            f"cd src/sparklesworker && docker build -t {sparkles_docker_image} .",
            timeout=1000,
        )
    else:
        sparkles_docker_image = ""
        sparklesworkerDir = "sparkleswork"

    print("Submitting job with autoscaler running (but using simulated Batch API)")

    redis = await proc_group.run_and_stream(
        "redis", f"redis-server --port {redis_port} --save ''"
    )

    watcher = Watcher()
    submitted_seen = watcher.watch_for("Successfully submitted")

    submission = {
        "name": "test-end-to-end",
        "cluster": {
            "MachineType": "n2-standard-2",
            "WorkerDockerImage": sparkles_docker_image,
            "PubSubInTopic": "sparkles-in",
            "PubSubOutTopic": "sparkles-out",
            "Region": "us-central1",
            "MaxPreemptableAttempts": 1,
            "MaxInstanceCount": 1,
            "MaxSuspiciousFailures": 1,
            "BootDisk": {"size_gb": 50, "type": "pd-standard"},
        },
        "redisAddr": f"localhost:{redis_port}",
        "aetherRoot": f"{tmpdir}/aether",
        "exportOutputTo": f"{tmpdir}/out",
        "exportLogTo": f"{tmpdir}/log",
        "dir": sparklesworkerDir,
        "dockerImage": "",
        "command": "echo hello from sparklespray",
        "filesToStage": [],
        "topicPrefix": "sparkles",
        "runLoopMaxWait": 0,
    }
    submission_path = f"{tmpdir}/submission.json"
    with open(submission_path, "wt") as fd:
        fd.write(json.dumps(submission))

    # submit a task
    submit = await proc_group.run_and_stream(
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
    autoscale = await proc_group.run_and_stream(
        "autoscale",
        f"bin/sparklesworker autoscaler --redis localhost:{redis_port} --poll-interval 100ms",
    )

    # breakpoint()
    # wait for the autoscale proccess to shutdown.
    await asyncio.wait_for(autoscale.wait(), 5)

    # now submit should also be done
    await asyncio.wait_for(submit.wait(), 5)

    # verify the outputs
    with open(f"{tmpdir}/log/stdout.txt", "rt") as fd:
        stdout = fd.read()

    assert "hello from sparklespray" in stdout

    # redis is the one service that doesn't automatically shut down
    redis.kill()


async def run_test(test_fn):
    tmpdir = tempfile.mkdtemp()
    with ProcessGroup() as proc_group:
        await test_fn(proc_group=proc_group, tmpdir=tmpdir)
    shutil.rmtree(tmpdir)


if __name__ == "__main__":
    test_fn = minimal_gcp_with_local_autoscale
    asyncio.run(run_test(test_fn))
