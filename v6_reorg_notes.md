# Sparklespray V6 Reorganization Notes

This document summarizes the changes made during the V6 reorganization of sparklespray.

## Overview

The V6 reorganization involved several major changes:

1. Replacing gRPC with Pub/Sub for worker-CLI communication
2. Removing certificate/key generation code
3. Renaming fields for clarity
4. Versioning Datastore collections
5. Removing dead code

## Datastore Collection Names

All Datastore collections now use a `SparklesV6` prefix:

| Old Name           | New Name                          |
| ------------------ | --------------------------------- |
| `SparklesV5Task`   | `SparklesV6Task`                  |
| `Job`              | `SparklesV6Job`                   |
| `Cluster`          | `SparklesV6Cluster`               |
| `ClusterHeartbeat` | `SparklesV6ClusterWatchHeartbeat` |
| `NodeReq`          | _(removed - was never used)_      |

Constants are defined in:

- `cli/sparklespray/task_store.py`: `TASK_COLLECTION`
- `cli/sparklespray/job_store.py`: `JOB_COLLECTION`
- `cli/sparklespray/cluster_store.py`: `CLUSTER_COLLECTION`
- `cli/sparklespray/cluster_service.py`: `CLUSTER_HEARTBEAT_COLLECTION`
- `go/src/.../datastore_queue.go`: `TaskCollection`, `ClusterCollection`, `JobCollection`

## Field Renames

### Task Entity

- `owner` → `owned_by_worker_id` (tracks which worker owns a task)

### TaskHistory Entity

- `owner` → `owned_by_worker_id`

### Pub/Sub Messages

- `owner` → `worker_id` (in request messages for filtering)

## Pub/Sub Communication

Workers now communicate with the CLI via Google Cloud Pub/Sub instead of gRPC:

- Each cluster has two topics:

  - `sparkles-{cluster_id}-incoming`: CLI publishes requests, workers subscribe
  - `sparkles-{cluster_id}-response`: Workers publish responses, CLI subscribes

- Request types:

  - `PubSubReadOutputRequest`: Request log output from a worker
  - `PubSubGetProcessStatusRequest`: Request process status (memory, CPU)

- Workers filter requests by `worker_id` field and only respond to requests meant for them

## Removed Code

### gRPC Code (replaced by Pub/Sub)

- `go/src/.../pb/` directory
- `cli/sparklespray/pb_pb2.py`
- `cli/sparklespray/pb_pb2_grpc.py`
- `scripts/build-grpc.sh`
- gRPC server code in `server.go` (kept Monitor struct and system stats functions)
- `--port` flag from worker

### Certificate/Key Generation (no longer needed without gRPC TLS)

- `cli/sparklespray/key_store.py`
- `cli/sparklespray/certgen.py`
- `cli/tests/sparklespray/key_store_test.py`
- `pyOpenSSL` dependency

### Unused NodeReq Datastore Storage

- `NODE_REQ_COLLECTION` constant
- `node_req_to_entity()` function
- `entity_to_node_req()` function
- Note: `NodeReq` dataclass is still used in-memory (created from Batch API responses)

### Dead Code Cleanup

- Duplicate imports across multiple files
- `get_credentials()` in `job_queue.py` (returned None)
- `indices_are_good_enough()` in `gcp_setup.py` (unreachable code)
- Commented-out `list_nodes` functionality in `commands/list.py`
- Commented-out `_run_local_command` in `workflow.py`
- Commented-out `_resolve_jobid` in `main.py`
- Various other commented-out code blocks

## Key Files Changed

### Python (CLI)

- `cli/sparklespray/task_store.py` - Task entity, V6 collection
- `cli/sparklespray/job_store.py` - Job entity, V6 collection
- `cli/sparklespray/job_queue.py` - Job/task coordination
- `cli/sparklespray/cluster_store.py` - Cluster config, Pub/Sub topics
- `cli/sparklespray/cluster_service.py` - Cluster management, heartbeat
- `cli/sparklespray/node_req_store.py` - NodeReq (in-memory only now)
- `cli/sparklespray/livelog/pubsub_client.py` - Pub/Sub client
- `cli/sparklespray/livelog/logclient.py` - Log monitoring

### Go (Worker)

- `go/src/.../datastore_queue.go` - V6 collections, field renames
- `go/src/.../consumer.go` - Task struct with owned_by_worker_id
- `go/src/.../pubsub.go` - Pub/Sub handler with worker_id filtering
- `go/src/.../main.go` - Entry point, worker ID handling
- `go/src/.../server.go` - Monitor struct (gRPC removed)

## Breaking Changes

These changes are breaking for existing deployments:

1. Datastore collection names changed - existing data won't be found
2. Task field `owner` renamed to `owned_by_worker_id`
3. gRPC communication replaced with Pub/Sub

A migration would be needed to:

1. Copy entities from old collections to new V6 collections
2. Rename the `owner` field to `owned_by_worker_id` in Task entities
3. Ensure Pub/Sub topics are created for existing clusters

## Commits

1. Remove gRPC dependencies and server code
2. Remove ClusterKeys and certificate generation code
3. Add owner filtering to pub/sub requests
4. Rename owner to worker_id and owned_by_worker_id
5. Update all Datastore collection names to V6 prefix
6. Rename ClusterHeartbeat collection to ClusterWatchHeartbeat
7. Remove unused NodeReq Datastore storage code
8. Remove dead code and duplicate imports
