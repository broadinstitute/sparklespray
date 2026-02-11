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

## gRPC Protocol

CLI communicates with workers via gRPC. If protocol changes, regenerate code:

```bash
sh scripts/build-grpc.sh
```
