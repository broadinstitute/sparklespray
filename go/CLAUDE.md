# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the Go worker component (`sparklesworker`) for Sparklespray, a batch job submission system for Google Compute Engine. The worker runs inside Docker containers on GCE instances, consuming tasks from a Google Datastore queue, executing commands, and uploading results to Google Cloud Storage.

## Build Commands

```bash
# Build locally for development (creates ./sparklesworker.local)
./local-build.sh

# Build and push Docker image (updates image tag in build.sh)
./build.sh
```

## Running Tests

```bash
cd src/github.com/broadinstitute/sparklesworker
go test ./...

# Run specific test
go test -run TestSimpleClaimTasks ./...
```

Note: Many tests require a real Google Cloud project (`broad-achilles`) and Datastore access.

## Running Locally

```bash
./sparklesworker.local consume --localhost \
    --cacheDir /tmp/mnt/data/cache \
    --tasksDir /tmp/mnt/data/tasks \
    --cluster <cluster-id> \
    --projectId <project-id> \
    --timeout 10 --shutdownAfter 600
```

## Architecture

### Core Components

- **main.go**: CLI entry point using `urfave/cli`. Defines three commands:

  - `consume`: Main worker loop that claims and executes tasks
  - `copyexe`: Copies the worker binary to a destination path
  - `fetch`: Downloads a file from GCS with MD5 verification

- **consumer.go**: Task consumption loop (`ConsumerRunLoop`). Polls Datastore for pending tasks, claims them atomically, executes via the executor function, and updates task status.

- **datastore_queue.go**: Implements the `Queue` interface for Google Datastore. Handles task claiming with optimistic concurrency via transactions.

- **exec.go**: Task execution logic. Downloads required files, runs the command via shell, captures resource usage, uploads results to GCS.

- **io.go**: `IOClient` interface for GCS operations (upload/download). Also provides GCE metadata service helpers.

- **server.go**: gRPC `Monitor` service for remote log tailing and memory usage reporting.

- **watchdog.go**: Process watchdog that panics if the main loop doesn't check in periodically.

### Data Flow

1. Worker polls Datastore for tasks with `status=pending` in its cluster
2. Claims task by atomically updating status to `claimed` with its owner ID
3. Downloads task spec (JSON) from GCS or inline in task args
4. Downloads input files to working directory (with CAS caching support)
5. Executes command via `/bin/sh -c`
6. Uploads output files matching glob patterns to GCS
7. Writes result JSON with exit code, resource usage, and file manifest
8. Updates task status to `complete` or `failed`

### Key Types

- `Task`: Datastore entity with task_id, status, owner, args, history
- `TaskSpec`: JSON spec defining downloads, command, uploads, working_dir
- `Queue`: Interface for task queue operations (Datastore or file-based)

### Protobuf

The `pb/` directory contains generated protobuf code for the gRPC Monitor service.

## Important Patterns

- Tasks are claimed randomly from available pending tasks to reduce contention
- Atomic updates use Datastore transactions with optimistic concurrency
- Downloaded files can be symlinked from cache if marked `symlink_safe`
- Watchdog requires periodic `NotifyWatchdog()` calls during long operations
- Uses `NotifyOnWrite` wrapper to keep watchdog alive during I/O
