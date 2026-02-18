"""
Integration tests for sparklespray.

These tests can run in two modes:
1. With emulators (default): Uses local emulators for Pub/Sub, Datastore, and GCS
2. With real GCP: Run with --project=<your-project-id> to test against real GCP services

Run with emulators:
    poetry run pytest tests/sparklespray/integration_test.py -v

Run with real GCP:
    poetry run pytest tests/sparklespray/integration_test.py --project=<your-project-id> -v
"""

import os
import subprocess
import tempfile
import time
import uuid

import pytest


# Path to the sparklesworker Go CLI directory
SPARKLESWORKER_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "go",
        "src",
        "github.com",
        "broadinstitute",
        "sparklesworker",
    )
)


def start_worker(tmpdir, cluster_id, emulator_project, env):
    # Create directories for sparklesworker
    worker_root = os.path.join(tmpdir, "worker_root")
    os.makedirs(worker_root, exist_ok=True)

    # Start sparklesworker in background with emulator environment
    worker_process = subprocess.Popen(
        [
            "go",
            "run",
            "cli/main.go",
            "consume",
            "--localhost",
            "--cluster",
            cluster_id,
            "--projectId",
            emulator_project,
            "--database",
            "sparkles-v6",
            "--timeout",
            "10",
            "--dir",
            worker_root,
            "--shutdownAfter",
            "120",
        ],
        cwd=SPARKLESWORKER_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    return worker_process


@pytest.mark.emulator
def test_sub_and_kill_with_emulators(
    emulators, gcs_bucket, emulator_project, monkeypatch
):
    """
    Integration test that submits a job and verifies it completes successfully.

    This test uses local emulators instead of real GCP services:
    - Pub/Sub emulator for messaging
    - Firestore emulator (Datastore mode) for task coordination
    - fake-gcs-server for Cloud Storage

    This test:
    1. Creates a temporary sparkles config file pointing to emulators
    3. Starts sparklesworker in the background connected to emulators
    4. Submits 'echo hello' job with --nodes=0 (so the background worker handles it)
    5. Waits for job completion
    6. Verifies the output contains 'hello'
    """
    # skip reading credentials
    import sparklespray.config

    monkeypatch.setattr(
        sparklespray.config,
        "_load_service_account_credentials",
        lambda key_path: (None, "fake@sample.com"),
    )
    import sparklespray.commands.submit

    monkeypatch.setattr(
        sparklespray.commands.submit,
        "has_access_to_docker_image",
        lambda *args: (True, None),
    )
    import sparklespray.watch.runner

    monkeypatch.setattr(
        sparklespray.watch.runner,
        "_make_get_node_reqs_callable",
        lambda cluster: lambda: [],
    )
    # Generate unique cluster ID and job ID
    cluster_id = f"test-{uuid.uuid4().hex[:12]}"
    job_id = f"test-job-{uuid.uuid4().hex[:8]}"

    # Get paths
    cli_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create temporary sparkles config pointing to emulator bucket
        config_path = os.path.join(tmpdir, ".sparkles")
        with open(config_path, "w") as f:
            f.write(
                f"""[config]
default_url_prefix=gs://{gcs_bucket.name}/test
project={emulator_project}
default_image=ubuntu
machine_type=n2-standard-2
region=us-central1
account=test@test.com
sparklesworker_image=invalid
database=sparkles-v6
"""
            )

        # Environment for subprocess calls - include emulator hosts
        env = os.environ.copy()
        env["PUBSUB_EMULATOR_HOST"] = emulators["pubsub"]["host"]
        env["DATASTORE_EMULATOR_HOST"] = emulators["firestore"]["host"]
        env["STORAGE_EMULATOR_HOST"] = emulators["gcs"]["host"]

        # Submit the job with --nodes=0 so no GCE nodes are started
        returncode = sparklespray.main.main(
            [
                "-c",
                config_path,
                "sub",
                "--name",
                job_id,
                "--cluster",
                cluster_id,
                "--nodes",
                "0",
                "--no-wait",
                "echo",
                "hello",
            ]
        )
        if returncode != 0:
            pytest.fail(f"sparkles sub failed with code {returncode}")

        import sparklespray.main

        worker_process = start_worker(tmpdir, cluster_id, emulator_project, env)
        try:
            returncode = sparklespray.main.main(
                [
                    "-c",
                    config_path,
                    "watch",
                    job_id,
                    "--nodes",
                    "0",
                ]
            )
            if returncode != 0:
                pytest.fail(f"sparkles sub failed with code {returncode}")

            # Check worker is still running
            if worker_process.poll() is not None:
                stdout, stderr = worker_process.communicate()
                pytest.fail(
                    f"sparklesworker exited prematurely with code {worker_process.returncode}\n"
                    f"stdout: {stdout.decode()}\nstderr: {stderr.decode()}"
                )

        finally:
            # Clean up: terminate the worker process
            worker_process.terminate()
            try:
                worker_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker_process.kill()
                worker_process.wait()


def test_sub_and_kill_with_real_gcp(project):
    """
    Integration test that submits a job and verifies it completes successfully.

    This test requires a real GCP project and runs against actual GCP services.
    Run with: pytest --project=<your-project-id>

    This test:
    1. Creates a temporary sparkles config file
    2. Runs sparkles setup on the project
    3. Starts sparklesworker in the background with a unique cluster ID
    4. Submits 'echo hello' job with --nodes=0 (so the background worker handles it)
    5. Waits for job completion
    6. Verifies the output contains 'hello'
    """
    if project is None:
        pytest.skip("--project not provided, skipping real GCP integration test")

    # Generate unique cluster ID and job ID
    cluster_id = f"test-{uuid.uuid4().hex[:12]}"
    job_id = f"test-job-{uuid.uuid4().hex[:8]}"

    # Get paths
    cli_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create temporary sparkles config
        config_path = os.path.join(tmpdir, ".sparkles")
        with open(config_path, "w") as f:
            f.write(
                f"""[config]
default_url_prefix=gs://{project}-sparkles-test
project={project}
default_image=ubuntu
machine_type=n2-standard-2
region=invalid
sparklesworker_image=invalid
"""
            )

        # Environment for subprocess calls
        env = os.environ.copy()

        # Run sparkles setup
        setup_result = subprocess.run(
            ["poetry", "run", "sparkles", "-c", config_path, "setup"],
            cwd=cli_dir,
            capture_output=True,
            text=True,
            env=env,
        )
        if setup_result.returncode != 0:
            print(f"Setup stdout: {setup_result.stdout}")
            print(f"Setup stderr: {setup_result.stderr}")
            pytest.fail(f"sparkles setup failed with code {setup_result.returncode}")

        # Create directories for sparklesworker
        worker_root = os.path.join(tmpdir, "worker_root")
        os.makedirs(worker_root, exist_ok=True)

        # Start sparklesworker in background
        worker_process = subprocess.Popen(
            [
                "go",
                "run",
                "cli/main.go",
                "consume",
                "--localhost",
                "--cluster",
                cluster_id,
                "--projectId",
                project,
                "--timeout",
                "10",
                "--dir",
                worker_root,
                "--shutdownAfter",
                "120",
            ],
            cwd=SPARKLESWORKER_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )

        try:
            # Give worker a moment to start
            time.sleep(2)

            # Check worker is still running
            if worker_process.poll() is not None:
                stdout, stderr = worker_process.communicate()
                pytest.fail(
                    f"sparklesworker exited prematurely with code {worker_process.returncode}\n"
                    f"stdout: {stdout.decode()}\nstderr: {stderr.decode()}"
                )

            # Submit the job with --nodes=0 so no GCE nodes are started
            submit_result = subprocess.run(
                [
                    "poetry",
                    "run",
                    "sparkles",
                    "-c",
                    config_path,
                    "sub",
                    "--name",
                    job_id,
                    "--cluster",
                    cluster_id,
                    "--nodes",
                    "0",
                    "echo",
                    "hello",
                ],
                cwd=cli_dir,
                capture_output=True,
                text=True,
                env=env,
            )
            if submit_result.returncode != 0:
                print(f"Submit stdout: {submit_result.stdout}")
                print(f"Submit stderr: {submit_result.stderr}")
                pytest.fail(f"sparkles sub failed with code {submit_result.returncode}")

            # Fetch the output and verify it contains 'hello'
            subprocess.run(
                [
                    "poetry",
                    "run",
                    "sparkles",
                    "-c",
                    config_path,
                    "fetch",
                    job_id,
                    "-o",
                    tmpdir,
                ],
                cwd=cli_dir,
                capture_output=True,
                text=True,
                env=env,
            )

            # Read stdout.txt from the fetched output
            stdout_path = os.path.join(tmpdir, "1", "stdout.txt")
            assert os.path.exists(stdout_path)
            with open(stdout_path, "r") as f:
                output = f.read()
            assert "hello" in output, f"Expected 'hello' in output, got: {output}"

        finally:
            # Clean up: terminate the worker process
            worker_process.terminate()
            try:
                worker_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker_process.kill()
                worker_process.wait()

            # Clean up: kill the job to remove any resources
            subprocess.run(
                [
                    "poetry",
                    "run",
                    "sparkles",
                    "-c",
                    config_path,
                    "kill",
                    job_id,
                ],
                cwd=cli_dir,
                capture_output=True,
                env=env,
            )
