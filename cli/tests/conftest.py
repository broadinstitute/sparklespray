import os
import shutil
import socket
import subprocess
import time
from contextlib import closing

import pytest


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "emulator: mark test as requiring GCP emulators")


def pytest_addoption(parser):
    parser.addoption(
        "--longrun",
        action="store_true",
        dest="longrun",
        default=False,
        help="enable longrundecorated tests",
    )
    parser.addoption(
        "--project",
        action="store",
        dest="project",
        default=None,
        help="GCP project ID for integration tests",
    )


@pytest.fixture
def project(request):
    """Fixture to get the --project option value."""
    return request.config.getoption("--project")


def find_free_port():
    """Find a free port on localhost."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def check_gcloud_available():
    """Check if gcloud CLI is available."""
    return shutil.which("gcloud") is not None


def wait_for_port(host, port, timeout=30):
    """Wait for a port to become available."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                s.connect((host, port))
                return True
        except (socket.error, ConnectionRefusedError):
            time.sleep(0.5)
    return False


@pytest.fixture(scope="session")
def pubsub_emulator():
    """Start the Pub/Sub emulator for the test session.

    This fixture starts the Google Cloud Pub/Sub emulator and sets the
    PUBSUB_EMULATOR_HOST environment variable so that the google-cloud-pubsub
    library connects to the emulator instead of the real service.

    Requires: gcloud SDK with beta components installed
    Install: gcloud components install pubsub-emulator
    """
    if not check_gcloud_available():
        pytest.skip("gcloud CLI is not available")

    port = find_free_port()
    host = "localhost"
    emulator_host = f"{host}:{port}"

    # Start the emulator process
    process = subprocess.Popen(
        [
            "gcloud",
            "beta",
            "emulators",
            "pubsub",
            "start",
            f"--host-port={emulator_host}",
            "--verbosity=error",
        ]
    )

    # Wait for the emulator to be ready
    if not wait_for_port(host, port, timeout=30):
        process.terminate()
        process.wait()
        pytest.fail(f"Pub/Sub emulator failed to start on {emulator_host}")

    # Set environment variable for google-cloud-pubsub library
    old_env = os.environ.get("PUBSUB_EMULATOR_HOST")
    os.environ["PUBSUB_EMULATOR_HOST"] = emulator_host

    yield {
        "host": emulator_host,
        "process": process,
    }

    # Cleanup
    if old_env is not None:
        os.environ["PUBSUB_EMULATOR_HOST"] = old_env
    else:
        os.environ.pop("PUBSUB_EMULATOR_HOST", None)

    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


@pytest.fixture(scope="session")
def firestore_emulator():
    """Start the Firestore emulator in Datastore mode for the test session.

    This fixture starts the Google Cloud Firestore emulator and sets the
    DATASTORE_EMULATOR_HOST environment variable so that the google-cloud-datastore
    library connects to the emulator instead of the real service.

    Requires: gcloud SDK with beta components installed
    Install: gcloud components install cloud-firestore-emulator

    Note: The Firestore emulator in Datastore mode is used because the
    standalone Datastore emulator is deprecated.
    """
    if not check_gcloud_available():
        pytest.skip("gcloud CLI is not available")

    port = find_free_port()
    host = "localhost"
    emulator_host = f"{host}:{port}"

    # Start the emulator process in Datastore mode
    process = subprocess.Popen(
        [
            "gcloud",
            "beta",
            "emulators",
            "firestore",
            "start",
            f"--host-port={emulator_host}",
            "--database-mode=datastore-mode",
        ]
    )

    # Wait for the emulator to be ready
    if not wait_for_port(host, port, timeout=30):
        process.terminate()
        process.wait()
        pytest.fail(f"Firestore emulator failed to start on {emulator_host}")

    # Set environment variable for google-cloud-datastore library
    old_env = os.environ.get("DATASTORE_EMULATOR_HOST")
    os.environ["DATASTORE_EMULATOR_HOST"] = emulator_host

    yield {
        "host": emulator_host,
        "process": process,
    }

    # Cleanup
    if old_env is not None:
        os.environ["DATASTORE_EMULATOR_HOST"] = old_env
    else:
        os.environ.pop("DATASTORE_EMULATOR_HOST", None)

    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


import tempfile


@pytest.fixture(scope="session")
def fake_gcs_server():
    """Start the fake-gcs-server Docker container for the test session.

    This fixture starts the fsouza/fake-gcs-server Docker container and sets the
    STORAGE_EMULATOR_HOST environment variable so that the google-cloud-storage
    library connects to the emulator instead of the real service.

    Requires: Docker installed and running
    Image: fsouza/fake-gcs-server (will be pulled automatically if not present)
    """
    gcs_filesystem = tempfile.mkdtemp()
    port = find_free_port()
    host = "localhost"
    emulator_host = f"http://{host}:{port}"

    # Start the fake-gcs-server container
    # -scheme http: Use HTTP instead of HTTPS (simpler for testing)
    # -public-host: Set the public host for URLs returned by the server
    process = subprocess.Popen(
        [
            "fake-gcs-server",
            "-filesystem-root",
            str(gcs_filesystem),
            "-scheme",
            "http",
            "-host",
            f"{host}",
            "-port",
            str(port),
        ],
    )

    # Wait for the server to be ready
    if not wait_for_port(host, port, timeout=30):
        # Cleanup on failure
        process.kill()
        pytest.fail(f"fake-gcs-server failed to start on {emulator_host}")

    # Set environment variable for google-cloud-storage library
    old_env = os.environ.get("STORAGE_EMULATOR_HOST")
    os.environ["STORAGE_EMULATOR_HOST"] = emulator_host

    try:
        yield {"host": emulator_host, "port": port, "process": process}
    finally:
        # Cleanup
        if old_env is not None:
            os.environ["STORAGE_EMULATOR_HOST"] = old_env
        else:
            os.environ.pop("STORAGE_EMULATOR_HOST", None)

        # Stop and remove the container
        process.kill()
        shutil.rmtree(gcs_filesystem)


@pytest.fixture(scope="session")
def emulators(pubsub_emulator, firestore_emulator, fake_gcs_server):
    """Combined fixture that starts Pub/Sub, Firestore, and GCS emulators.

    Use this fixture for end-to-end tests that need all services.
    """
    return {
        "pubsub": pubsub_emulator,
        "firestore": firestore_emulator,
        "gcs": fake_gcs_server,
    }


@pytest.fixture
def emulator_project():
    """Return a test project ID for use with emulators."""
    return "test-project"


@pytest.fixture
def pubsub_clients(pubsub_emulator, emulator_project):
    """Create Pub/Sub publisher and subscriber clients connected to the emulator.

    Returns a dict with 'publisher' and 'subscriber' clients.
    """
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    yield {
        "publisher": publisher,
        "subscriber": subscriber,
        "project": emulator_project,
    }

    # Cleanup - close the clients
    publisher.transport.close()
    subscriber.close()


@pytest.fixture
def datastore_client(firestore_emulator, emulator_project):
    """Create a Datastore client connected to the Firestore emulator.

    Returns a google.cloud.datastore.Client instance.
    """
    from google.cloud import datastore

    client = datastore.Client(project=emulator_project)

    yield client


@pytest.fixture
def storage_client(fake_gcs_server, emulator_project):
    """Create a Storage client connected to the fake-gcs-server.

    Returns a google.cloud.storage.Client instance.
    """
    from google.cloud import storage

    # When STORAGE_EMULATOR_HOST is set, the client connects to the emulator
    client = storage.Client(project=emulator_project)

    yield client


@pytest.fixture
def gcs_bucket(storage_client):
    """Create a test bucket in the fake GCS server.

    Returns the bucket object. The bucket is named 'test-bucket'.
    """
    import requests

    bucket_name = "test-bucket"

    # fake-gcs-server requires creating buckets via its API
    # The STORAGE_EMULATOR_HOST env var contains the base URL
    emulator_host = os.environ.get("STORAGE_EMULATOR_HOST", "")
    if emulator_host:
        # Create bucket using the fake-gcs-server API
        requests.post(
            f"{emulator_host}/storage/v1/b",
            json={"name": bucket_name},
        )

    bucket = storage_client.bucket(bucket_name)

    yield bucket


@pytest.fixture
def io_helper(fake_gcs_server, gcs_bucket, emulator_project):
    """Create an IO helper connected to the fake GCS server.

    Returns a sparklespray.io_helper.IO instance configured for the emulator.
    """
    from sparklespray.io_helper import IO

    cas_url_prefix = f"gs://{gcs_bucket.name}/cas/"

    # Create IO without credentials (emulator doesn't need them)
    io = IO(
        project=emulator_project,
        cas_url_prefix=cas_url_prefix,
        credentials=None,
    )

    yield io


# Path to the sparklesworker Go CLI directory (relative to the cli directory)
SPARKLESWORKER_CLI_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "go",
        "src",
        "github.com",
        "broadinstitute",
        "sparklesworker",
        "cli",
    )
)


@pytest.fixture(scope="session")
def worker_root_dir(tmp_path_factory):
    """Create a temporary directory for the sparklesworker to use as its root.

    This directory is used by the worker for task execution and file storage.
    """
    root_dir = tmp_path_factory.mktemp("sparklesworker_root")
    yield str(root_dir)


@pytest.fixture(scope="session")
def sparklesworker(
    emulators,
    emulator_project,
    worker_root_dir,
):
    """Start the sparklesworker Go process for end-to-end testing.

    This fixture starts the sparklesworker using `go run` with the emulator
    environment variables set, so it connects to the local emulators instead
    of real GCP services.

    Requires: Go installed and available in PATH

    The worker is started with:
    - --localhost: Run in localhost mode (no GCP metadata server)
    - --cluster test-cluster: Use test cluster name
    - --projectId test-project: Use test project
    - --database sparkles-v6: Use the sparkles database
    - --shutdownAfter 100: Shutdown after 100 seconds of idle time
    """

    if not os.path.isdir(SPARKLESWORKER_CLI_DIR):
        pytest.skip(f"sparklesworker CLI directory not found: {SPARKLESWORKER_CLI_DIR}")

    # Build the environment with emulator hosts
    env = os.environ.copy()
    # The emulator environment variables should already be set by the emulators fixture,
    # but we explicitly include them to ensure they're passed to the subprocess
    env["PUBSUB_EMULATOR_HOST"] = emulators["pubsub"]["host"]
    env["DATASTORE_EMULATOR_HOST"] = emulators["firestore"]["host"]
    env["STORAGE_EMULATOR_HOST"] = emulators["gcs"]["host"]

    # Start the sparklesworker process
    process = subprocess.Popen(
        [
            "go",
            "run",
            "main.go",
            "consume",
            "--dir",
            worker_root_dir,
            "--cluster",
            "test-cluster",
            "--localhost",
            "--projectId",
            emulator_project,
            "--database",
            "sparkles-v6",
            "--shutdownAfter",
            "100",
        ],
        cwd=SPARKLESWORKER_CLI_DIR,
        env=env,
    )

    # Give the worker a moment to start up
    # We can't easily check if it's ready without more infrastructure,
    # so we just wait a bit for Go to compile and start
    time.sleep(3)

    yield {
        "process": process,
        "root_dir": worker_root_dir,
        "cluster": "test-cluster",
        "project": emulator_project,
        "database": "sparkles-v6",
    }

    # Cleanup - terminate the worker process
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


@pytest.fixture
def e2e_test_env(
    emulators, sparklesworker, datastore_client, pubsub_clients, io_helper
):
    """Complete end-to-end test environment with all emulators and worker running.

    This fixture provides everything needed for full integration tests:
    - Pub/Sub emulator
    - Firestore/Datastore emulator
    - Fake GCS server
    - Running sparklesworker process

    Returns a dict with all the components.
    """
    return {
        "emulators": emulators,
        "worker": sparklesworker,
        "datastore_client": datastore_client,
        "pubsub_clients": pubsub_clients,
        "io": io_helper,
        "project": sparklesworker["project"],
        "cluster": sparklesworker["cluster"],
    }
