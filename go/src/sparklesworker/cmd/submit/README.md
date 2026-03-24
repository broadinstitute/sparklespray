# submit command

The `submit` command submits a single-task job to a Sparklespray cluster and waits for it to complete.

It reads a job specification from a JSON file, writes the job and task records to the backend (Firestore or Redis), streams task logs to the terminal, and polls until the task reaches a terminal state (`complete`, `failed`, or `killed`).

## Usage

```
sparklesworker submit --file job.json [options]
```

## Flags

| Flag                      | Default            | Description                                                                              |
| ------------------------- | ------------------ | ---------------------------------------------------------------------------------------- |
| `--file`                  | _(required)_       | Path to a JSON file containing a `JobSpec`                                               |
| `--projectID`             |                    | Google Cloud project ID (required when using Firestore backend)                          |
| `--database`              |                    | Firestore database ID                                                                    |
| `--redisAddr`             |                    | Redis server address (e.g. `localhost:6379`); selects Redis backend instead of Firestore |
| `--dir`                   | `./sparklesworker` | Base directory for worker data (used in local execution mode)                            |
| `--cacheDir`              | `<dir>/cache`      | Cache directory for downloaded files                                                     |
| `--tasksDir`              | `<dir>/tasks`      | Working directory for task execution                                                     |
| `--aetherRoot`            |                    | Aether store root — a GCS path (`gs://bucket/prefix`) or local path                      |
| `--aetherMaxSizeToBundle` | `0`                | Max file size in bytes eligible for bundling (0 disables bundling)                       |
| `--aetherMaxBundleSize`   | `0`                | Target maximum bundle size in bytes                                                      |
| `--aetherWorkers`         | `1`                | Number of parallel upload workers                                                        |
| `--expiry`                | `24h`              | How long job/task data is retained (e.g. `24h`, `168h`)                                  |
| `--topicPrefix`           | `sparkles`         | Prefix for the log topic name; the full topic is `<topicPrefix>-log`                     |

## Job specification file format

The `--file` argument must be a JSON file matching the `JobSpec` structure:

```json
{
  "Name": "my-job",
  "ClusterID": "my-cluster",
  "DockerImage": "ubuntu:22.04",
  "Command": "echo hello world",
  "TopicPrefix": "sparkles"
}
```

Fields:

| Field          | Description                                                                                        |
| -------------- | -------------------------------------------------------------------------------------------------- |
| `Name`         | Job name; must follow Google Cloud ID conventions                                                  |
| `ClusterID`    | ID of an existing cluster to submit to (mutually exclusive with `ClusterSpec`)                     |
| `ClusterSpec`  | Inline cluster definition (mutually exclusive with `ClusterID`; ID is derived by hashing the spec) |
| `DockerImage`  | Docker image to run the task in                                                                    |
| `Command`      | Shell command executed as `/bin/sh -c <Command>`                                                   |
| `FilesToStage` | List of local files to stage (each has `LocalPath` and `Name`)                                     |
| `TopicPrefix`  | Overrides the `--topicPrefix` flag if set                                                          |

### ClusterSpec fields

When providing an inline cluster definition instead of a `ClusterID`:

```json
{
  "ClusterSpec": {
    "machineType": "n2-standard-4",
    "bootVolumeInGB": 50,
    "bootVolumeType": "pd-ssd",
    "MaxPreemptableAttempts": 3,
    "TargetNodeCount": 1
  }
}
```

The cluster ID is derived deterministically by SHA-256 hashing the JSON-serialised spec (first 8 bytes, hex-encoded).

## Backends

### Firestore (production)

The default backend. Requires `--projectID` and optionally `--database`. Task records are stored in Firestore and logs are streamed via Google Cloud Pub/Sub on the topic `<topicPrefix>-log`.

```
sparklesworker submit \
  --file job.json \
  --projectID my-gcp-project \
  --database "(default)"
```

### Redis (local testing)

When `--redisAddr` is provided, the command uses Redis for both the task queue and the log channel instead of Firestore/Pub/Sub. This is intended for local development and integration testing — no Google Cloud credentials are required.

```
# Start a local Redis instance
docker run -d -p 6379:6379 redis

sparklesworker submit \
  --file job.json \
  --redisAddr localhost:6379 \
  --file job.json
```

## Local execution mode

If the job's `ClusterID` is set to `"local"`, the submit command runs the task in-process on the local machine immediately after submitting it, rather than waiting for a remote worker. This is useful for end-to-end testing without a running cluster.

In local mode the command:

1. Submits the job/task to the queue (Firestore or Redis).
2. Starts a `consumer.RunLoop` that claims and executes the task locally using the same `consumer.ExecuteTask` path that a real worker would use.
3. Polls until the task reaches a terminal status.

Example `job.json` for local execution:

```json
{
  "Name": "local-test",
  "ClusterID": "local",
  "DockerImage": "ubuntu:22.04",
  "Command": "echo 'running locally'"
}
```

Combined with the Redis backend, this lets you run and test the full submit → execute → complete flow entirely on your laptop:

```
sparklesworker submit \
  --file job.json \
  --redisAddr localhost:6379 \
  --aetherRoot /tmp/aether
```

## Lifecycle

1. Reads and parses the `JobSpec` JSON file.
2. Derives or validates the cluster ID.
3. Constructs a `Job` and a single `Task` record with status `pending`.
4. Connects to the configured backend (Redis or Firestore).
5. Writes the job and task via `queue.AddJob`.
6. Starts streaming logs from the `<topicPrefix>-log` topic.
7. If `ClusterID == "local"`, runs the consumer loop in-process.
8. Polls `queue.GetTask` every second until the task status is neither `pending` nor `claimed`.
9. Exits with an error if any step fails; exits cleanly on task completion regardless of exit code.
