"""
Integration tests for sparklespray that require a real GCP project.

Run with: poetry run pytest tests/sparklespray/integration_test.py --project=<your-project-id> -v
"""

import os
import subprocess
import tempfile
import time
import uuid

import pytest


def test_sub_and_kill(project):
    """
    Integration test that submits a job and verifies it completes successfully.

    This test:
    1. Creates a temporary sparkles config file
    2. Runs sparkles setup on the project
    3. Starts sparklesworker in the background with a unique cluster ID
    4. Submits 'echo hello' job with --nodes=0 (so the background worker handles it)
    5. Waits for job completion
    6. Verifies the output contains 'hello'
    """
    if project is None:
        pytest.skip("--project not provided, skipping integration test")

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
                "-C",
                "go/src/github.com/broadinstitute/sparklesworker",
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
