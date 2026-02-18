# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Sparklespray ("sparkles") is a batch job submission system for Google Compute Engine. The CLI (this repo) submits jobs to Google Cloud's Batch API and Datastore, monitors execution, and manages results. A Go worker component (`sparklesworker` in `../go/`) runs on GCE instances to execute tasks.

## Build and Test Commands

```bash
# Install dependencies
poetry install

# Run tests
poetry run pytest tests/

# Run single test file
poetry run pytest tests/sparklespray/test_submit_cmd.py -v

# Include integration tests
poetry run pytest tests/ --longrun

# Type checking
mypy sparklespray

# Format code
black sparklespray

# Progressive type checking
./pyright-ratchet
```

## CLI Entry Point

The `sparkles` command is defined in `sparklespray/main.py`. Entry point: `sparklespray.main:main` (see pyproject.toml).

## Architecture

### Dependency Injection Pattern

Commands declare parameters they need in their function signature. `config.py:create_func_params()` examines the signature and instantiates required objects (Config, IO, JobQueue, ClusterAPI, etc.) automatically.

### Key Components

- **Config** (`config.py`): Loads `.sparkles` config file, handles parameter overrides
- **JobQueue** (`job_queue.py`): Coordinates jobs and tasks via Datastore
- **TaskStore** (`task_store.py`): Task state and lifecycle management
- **ClusterAPI** (`batch_api.py`): Google Cloud Batch API wrapper
- **IO** (`io_helper.py`): GCS operations and Content-Addressable Storage (CAS)

### Task Lifecycle

States: `pending` → `claimed` → `complete`/`failed`/`killed`

Tasks are claimed atomically via Datastore transactions. TaskHistory records all state transitions.

### Storage Model

- Files uploaded by content hash to GCS CAS: `gs://bucket/cache/<sha256>`
- Task outputs stored per-task: `gs://bucket/job-id/task-index/`
- Results include `results.json` (exit code, resource usage) and `stdout.txt`

### Adding Commands

1. Create `sparklespray/commands/mycommand.py`:

```python
def mycommand(config: Config, jq: JobQueue, io: IO, args):
    """Command handler - parameters injected based on signature"""
    pass

def add_mycommand(subparser):
    cmd = subparser.add_parser('mycommand')
    cmd.set_defaults(func=mycommand)
```

2. Import and call `add_mycommand(subparser)` in `main.py`

### Error Handling

Raise `UserError` for user-facing errors with helpful messages. Main catches and displays these cleanly.

## Key Files by Task

- **Job submission**: `commands/submit.py`, `spec.py`
- **Monitoring**: `watch/runner.py`, `watch/failure_monitor.py`
- **Configuration**: `config.py`, `model.py`
- **GCP integration**: `batch_api.py`, `cluster_service.py`
- **Task coordination**: `task_store.py`, `job_queue.py`

## Testing

Tests use pytest with mocks for GCP services:

- `DatastoreClientSimulator`: In-memory Datastore
- `MockIO`: Mocks GCS operations
- Fixtures in `tests/sparklespray/factories.py`

### Integration Tests with Emulators

For end-to-end testing without connecting to real GCP services, the test suite includes fixtures that start local emulators. These emulators allow testing the full stack locally, including the Go worker.

#### Prerequisites

Install the required emulators:

```bash
# Pub/Sub and Firestore emulators (requires gcloud SDK)
gcloud components install pubsub-emulator
gcloud components install cloud-firestore-emulator

# required by cloud-firestore-emulator
brew install openjdk

# GCS emulator
go install github.com/fsouza/fake-gcs-server@latest

# Go (for running sparklesworker)
# Install from https://go.dev/
```

#### Available Fixtures

The fixtures are defined in `tests/conftest.py`:

| Fixture              | Scope    | Description                                                           |
| -------------------- | -------- | --------------------------------------------------------------------- |
| `pubsub_emulator`    | session  | Starts Pub/Sub emulator, sets `PUBSUB_EMULATOR_HOST`                  |
| `firestore_emulator` | session  | Starts Firestore in Datastore mode, sets `DATASTORE_EMULATOR_HOST`    |
| `fake_gcs_server`    | session  | Starts fake-gcs-server Docker container, sets `STORAGE_EMULATOR_HOST` |
| `emulators`          | session  | Combined fixture starting all three emulators                         |
| `sparklesworker`     | session  | Starts the Go worker process connected to emulators                   |
| `datastore_client`   | function | Datastore client connected to emulator                                |
| `pubsub_clients`     | function | Publisher/subscriber clients connected to emulator                    |
| `storage_client`     | function | GCS client connected to emulator                                      |
| `io_helper`          | function | `IO` instance configured for the GCS emulator                         |
| `e2e_test_env`       | function | Complete test environment with all services                           |

#### How It Works

The Google Cloud client libraries (both Python and Go) automatically detect these environment variables and connect to emulators instead of production services:

- `PUBSUB_EMULATOR_HOST` → Pub/Sub client
- `DATASTORE_EMULATOR_HOST` → Datastore client
- `STORAGE_EMULATOR_HOST` → GCS client

The fixtures:

1. Find a free port on localhost
2. Start the emulator process
3. Set the environment variable
4. Yield control to the test
5. Clean up the emulator process on teardown

#### Example Usage

```python
import pytest

@pytest.mark.emulator
def test_pubsub_messaging(pubsub_clients):
    """Test Pub/Sub messaging with the emulator."""
    publisher = pubsub_clients["publisher"]
    project = pubsub_clients["project"]

    # Create topic and publish message
    topic_path = publisher.topic_path(project, "test-topic")
    publisher.create_topic(request={"name": topic_path})
    publisher.publish(topic_path, b"test message")

@pytest.mark.emulator
def test_datastore_operations(datastore_client):
    """Test Datastore operations with the emulator."""
    key = datastore_client.key("Task", "test-1")
    entity = datastore.Entity(key=key)
    entity["status"] = "pending"
    datastore_client.put(entity)

    retrieved = datastore_client.get(key)
    assert retrieved["status"] == "pending"

@pytest.mark.emulator
def test_full_job_execution(e2e_test_env):
    """End-to-end test with worker processing tasks."""
    datastore = e2e_test_env["datastore_client"]
    io = e2e_test_env["io"]
    worker = e2e_test_env["worker"]

    # Submit a job - the worker will pick it up and execute
    # ...
```

#### Running Emulator Tests

```bash
# Run only emulator tests
poetry run pytest tests/ -m emulator

# Run all tests including emulator tests
poetry run pytest tests/ --longrun
```

Tests will be skipped automatically if prerequisites are not available (gcloud, Docker, or Go not installed).

## Worker Communication

CLI communicates with workers via Google Cloud Pub/Sub. Each cluster has two topics:

- `incoming_topic`: CLI publishes requests, workers subscribe
- `response_topic`: Workers publish responses, CLI subscribes

The Cluster entity in Datastore stores the topic names for each cluster.
