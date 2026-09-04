# Sparklespray

Sparklespray, or "sparkles" for short, makes it easy to submit ad-hoc batch
jobs — run the same (or parametrized) command across many tasks, packaged as
a Docker image — on a pool of preemptible-first Google Compute Engine VMs,
without hand-managing VM lifecycle, retries, or preemption.

The implementation lives entirely under [`v100/`](v100): a single static Go
binary (`sparkles`) that acts as the CLI, the worker process that runs on
each VM, and the control plane — `sparkles serve`, which runs the
autoscaling/watchdog monitor and the dashboard-backend (REST API + dashboard
UI) together in one process. It replaced an earlier Python CLI
implementation, which has been removed from the repo.

## Repository layout

| Path                      | What it is                                                                                                                                                                               |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`v100/`](v100)           | The Go implementation: CLI (`submit`/`kill`), `worker`, `serve` (monitor + dashboard-backend), and `dev` subcommands. See `v100/*.go`, `v100/dev/`, `v100/monitor/`.                     |
| [`dashboard/`](dashboard) | The React/Vite dashboard frontend. Built and embedded into the `sparkles` binary (see `v100/build.sh`); run standalone via `npm run dev` for local frontend development.                 |
| [`docs/`](docs)           | All current documentation — architecture (arc42), design docs, and the remote-deployment guide. Start at [`docs/README.md`](docs/README.md).                                             |
| [`examples/`](examples)   | Pre-v100 example job specs. Predate the Go rewrite and aren't verified against it — treat as inspiration, not working examples.                                                          |
| `start-dashboard-emu.sh`  | Runs the stack locally against Firestore/Pub-Sub emulators (dashboard-backend, dashboard dev server, and a synthetic load generator in place of a real monitor) — no GCP project needed. |

## Building

```
./v100/build.sh
```

Produces one self-contained binary — `v100/bin/sparkles-linux-amd64-<version>`
— with `worker`, `submit`, `kill`, `serve`, and `dev ...` subcommands
(`./bin/sparkles --help`), with the dashboard UI embedded. The same binary
bootstraps worker VMs and is deployed as the control plane; needs Node/npm in
addition to Go, since it builds the frontend. `<version>` defaults to
`git describe --tags --always --dirty`, or pass one explicitly:
`./v100/build.sh v1.2.3`.

For quick local iteration on Go code only (skips the frontend build, and
serves a placeholder page instead of the real UI at `/`), plain
`go build -o bin/sparkles ./cmd/sparkles` from `v100/` still works.

See [docs/deploying-dashboard-backend.md](docs/deploying-dashboard-backend.md)
for deploying the built binary, and `v100/upload-worker-binary.sh` for
publishing it to the GCS path worker VMs bootstrap from.

## Running locally

`./start-dashboard-emu.sh` builds the binary and brings up the
dashboard-backend, dashboard dev server, and a synthetic load generator
against local Firestore/Pub-Sub emulators — no GCP project or credentials
required. `v100/start.sh` is the equivalent for a real GCP project: it runs
`sparkles serve` plus the frontend dev server (reads
`v100/sample-config.json`, requires `gcloud` auth).

## Submitting a job against a real GCP project

Once `sparkles serve` is running against a real project
(see [docs/deploying-dashboard-backend.md](docs/deploying-dashboard-backend.md)
for the one-time `sparkles dev create-topics` / `set-config` / `add-api-key`
setup):

```
SPARKLES_API_KEY=<api-key> ./bin/sparkles submit --url http://localhost:8080 job.json
```

See [docs/design/dashboard-api.md](docs/design/dashboard-api.md) for the
full REST API and [docs/design/datamodel.md](docs/design/datamodel.md) for
the job/task/workpool data model.

## Documentation

Start at [docs/README.md](docs/README.md) for the full index: arc42
architecture docs, design docs, and the deployment guide.
