# Deploying the dashboard-backend (with UI) to a remote server

## Overview

`sparkles` is a single self-contained binary bundling the `worker`,
`submit`, `kill`, and `monitor` commands plus a `dev` subcommand tree
(`dev dashboard-backend`, `dev set-config`, `dev create-topics`, `dev add-api-key`, ...). As of this change, `dev dashboard-backend` also serves
the built dashboard frontend directly: the API lives at `/api/v1/...` and the
UI is served from `/` on the same port. That means the whole control plane —
API + UI — is one binary and one process on one host; there is nothing
separate to install or keep in sync.

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
/opt/sparkles/sparkles dev dashboard-backend \
  --project <gcp-project> \
  --db <firestore-db, default "sparkles"> \
  --addr :8080
```

GCP auth is picked up from the environment (ADC); set
`GOOGLE_APPLICATION_CREDENTIALS` if not using an attached service account.

## systemd unit example

`/etc/systemd/system/sparkles-dashboard.service`:

```ini
[Unit]
Description=Sparkles dashboard-backend
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/opt/sparkles/sparkles dev dashboard-backend --project my-gcp-project --db sparkles --addr :8080
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
