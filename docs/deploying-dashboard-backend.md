# Deploying the control plane (dashboard-backend + monitor) to a remote server

## Overview

`sparkles` is a single self-contained binary bundling the `worker`, `submit`,
and `kill` commands, the `serve` command, and a `dev` subcommand tree
(`dev set-config`, `dev create-topics`, `dev add-api-key`, ...).

`sparkles serve` is the deployment entry point: it runs the entire control
plane in **one process** — the monitor (autoscaling and watchdog polls) plus
the dashboard-backend, which serves both the REST API at `/api/v1/...` and the
embedded dashboard UI at `/` on the same port. So one binary, one process, one
port on one host; there is nothing separate to install or keep in sync.

The two halves can still be run on their own for debugging —
`sparkles dev monitor` and `sparkles dev dashboard-backend` — but production
deployments should use `serve`.

## Prerequisites

- A Linux x86_64 host to run the binary on (matches `build-server.sh`'s
  `GOOS=linux GOARCH=amd64` build).
- GCP credentials reachable from that host, with access to Firestore,
  Pub/Sub, and GCS in the target project — either a service account key file
  plus `GOOGLE_APPLICATION_CREDENTIALS`, or a service account attached to the
  host (e.g. a GCE instance), i.e. standard Application Default Credentials.
  There is no sparkles-specific credentials flag or env var.
- The one-time cluster setup already done for the target project:
  - `sparkles dev create-topics --project <project>` — creates the Pub/Sub
    topics/subscriptions the monitor and workers depend on.
  - `sparkles dev set-config --project <project> <config.json>` — writes the
    `SparklesConfig/default` Firestore document dashboard-backend reads at
    startup (GCS prefix, service account, region/zones, etc. — see
    `sample-config.json`).
- At least one API key for dashboard/CLI auth:
  `sparkles dev add-api-key --project <project> <user>`. Users of the
  dashboard UI paste this key in on first load (stored in the browser's
  `localStorage`); `sparkles submit` reads it from `SPARKLES_API_KEY`.
- An open inbound port (default `:8080`, or whatever `--addr` is set to) if
  the dashboard needs to be reachable from outside the host. TLS termination
  and access control in front of that port (e.g. nginx, Caddy, or a GCP HTTPS
  load balancer) are up to the operator — not something this binary does
  itself.

## Building

From a machine with Node/npm and Go installed (it does not need to be the
target server):

```
./v100/build-server.sh <version>
```

This builds the frontend (`npm ci && npm run build` in `dashboard/`), copies
the build output into the binary's embedded assets, and cross-compiles a
single static binary at:

```
v100/bin/sparkles-server-linux-amd64-<version>
```

(This is a different artifact from `v100/build-linux-amd64.sh`'s
`sparkles-linux-amd64-<version>`, which is the cheap worker-VM build that
skips the frontend build entirely since worker VMs never serve the
dashboard.)

## Copying the binary to the server

```
gcloud compute scp v100/bin/sparkles-server-linux-amd64-<version> \
  my-host:/opt/sparkles/sparkles-server-<version>
```

(or plain `scp` if not using GCE). A suggested layout on the server:

```
/opt/sparkles/sparkles-server-<version>   # versioned binaries, one per release
/opt/sparkles/sparkles -> sparkles-server-<version>  # symlink to the active one
```

so upgrades are: copy the new binary in, repoint the symlink, restart.

## Running it

```
/opt/sparkles/sparkles serve \
  --project <gcp-project> \
  --db <firestore-db, default "sparkles"> \
  --addr :8080
```

This starts the monitor and the dashboard-backend together. `serve` shuts both
down on `SIGINT`/`SIGTERM`, draining in-flight HTTP requests first, so
`systemctl restart` and `systemctl stop` are clean. If either half fails
fatally the process exits rather than limping along half-alive — let the
service manager restart it (`Restart=on-failure` below).

GCP auth is picked up from the environment (ADC); set
`GOOGLE_APPLICATION_CREDENTIALS` if not using an attached service account.

## Running workloads in another project

By default a job's worker VMs run in the same project the backend and monitor
were started with. A submission can override this per workpool by setting
`workpool.projectID` in the `POST /api/v1/job` body: the GCP Batch job, and
therefore the worker VMs, are created in that project instead.

Only the workload moves. The control plane stays put:

- Firestore, Pub/Sub, and the job/task records remain in the backend's own
  project.
- The worker binary is still launched with `--project <backend project>`, since
  that's where it claims tasks and reports state.
- The Batch job's state-change notifications are still published to the backend
  project's `batch-api-notifications` topic, because that's what the monitor
  subscribes to.

The backend validates only the _format_ of `projectID`; everything else is IAM.
For a workload project `W` and control-plane project `C`, you need:

| Grant                                                                                                                                                | Where                                    | Why                                                                                                                |
| ---------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Backend/monitor SA: `roles/batch.jobsEditor` (or equivalent `batch.jobs.create`/`get`/`cancel`), `compute.instances.list`/`delete`, and Logging read | project `W`                              | Create and reconcile Batch jobs and their VMs; fetch batch logs on failure                                         |
| `W`'s Batch service agent (`service-<W-number>@gcp-sa-batch.iam.gserviceaccount.com`): `roles/pubsub.publisher`                                      | on `C`'s `batch-api-notifications` topic | Without it the monitor never receives Batch state changes and falls back to timer polling — slower, but not broken |
| Worker VM SA (`workpool.serviceAccount`): Firestore, Pub/Sub, and GCS access                                                                         | project `C`                              | Workers claim tasks and stream results against the control plane                                                   |

Note that attaching a service account belonging to `C` to VMs in `W` can be
blocked by the `iam.disableCrossProjectServiceAccountUsage` org policy; the
alternative is an SA in `W` granted the roles above on `C`.

## systemd unit example

`/etc/systemd/system/sparkles-dashboard.service`:

```ini
[Unit]
Description=Sparkles control plane (monitor + dashboard-backend)
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/opt/sparkles/sparkles serve --project my-gcp-project --db sparkles --addr :8080
Environment=GOOGLE_APPLICATION_CREDENTIALS=/opt/sparkles/gcp-sa-key.json
Restart=on-failure
RestartSec=5
User=sparkles
WorkingDirectory=/opt/sparkles

[Install]
WantedBy=multi-user.target
```

```
sudo systemctl daemon-reload
sudo systemctl enable --now sparkles-dashboard
```

## Upgrading / redeploying

Copy the new versioned binary alongside the old one, repoint the
`/opt/sparkles/sparkles` symlink, then `systemctl restart sparkles-dashboard`. There's no migration step: the Firestore schema is
additive, and dashboard-backend itself is stateless apart from an in-memory
API-key cache that just repopulates on restart.

## Verifying

```
curl http://localhost:8080/
# -> dashboard HTML

curl -H "Authorization: Bearer <api-key>" http://localhost:8080/api/v1/jobs
# -> JSON
```

and loading `http://<host>:8080/` in a browser should show the dashboard UI
and (after entering an API key) real data.
