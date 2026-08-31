# 1. Introduction and Goals

## 1.1 Requirements Overview

`sparkles` lets a user submit an "ad-hoc batch job" — run the same (or
parametrized) command across many tasks, packaged as a Docker image — on a
pool of Google Compute Engine VMs, without hand-managing VM lifecycle,
retries, or preemption. A user should be able to:

- Submit a job (`sparkles submit`) consisting of N tasks, each a shell
  command plus input files to localize from GCS and a result/log path to
  upload to.
- Have a pool of worker VMs ("workpool") scale up automatically to meet
  task backlog, scale down when idle, and prefer cheap preemptible VMs over
  on-demand ones.
- Get the job's results back in Google Cloud Storage, with per-task status,
  resource usage, and logs visible through a dashboard.
- Kill a running job (`sparkles kill`) and have in-flight tasks stop
  promptly.
- Not have to think about GCP Batch, Compute Engine, or VM failure modes —
  the system should recover from preemption, VM startup failures, and
  crashed/hung workers on its own, and should stop spending money on a
  workpool that is systematically broken (misconfigured image, bad service
  account, etc.) rather than retrying forever.

v100 is a ground-up Go rewrite of an existing Python implementation (the
rest of this repository) with the same job model, motivated primarily by
operational simplicity (see [Solution Strategy](04-solution-strategy.md)).

## 1.2 Quality Goals

Ranked, most important first:

| #   | Quality goal                     | Motivation                                                                                                                                                                                                                                                                                      |
| --- | -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | **Resilience / self-healing**    | Preemption, VM creation failures, and stuck/zombie workers are the normal case at scale, not exceptions. The monitor must detect and recover from them without operator intervention, and must stop (halt) a workpool that is failing systematically instead of burning budget on retries.      |
| 2   | **Testability without live GCP** | Every GCP integration point is behind a narrow interface with an in-memory fake or local emulator, so unit and functional tests run fast, offline, and deterministically. This is treated as a first-class design goal, not an afterthought (see `autoscaler_testing.md`, `func-test-plan.md`). |
| 3   | **Operational simplicity**       | A worker VM's entire bootstrap is "download one static binary from GCS, chmod +x, run" — no Python runtime, virtualenv, or package install on the VM.                                                                                                                                           |
| 4   | **Observability**                | Every state transition (task, worker, workpool, batch) is mirrored to an append-only `Events` collection/Pub-Sub topic, so the dashboard and post-hoc debugging never rely on inferring history from current state alone.                                                                       |
| 5   | **Cost efficiency**              | Prefer preemptible VMs; scale workers to pending-task backlog; avoid idle capacity.                                                                                                                                                                                                             |

## 1.3 Stakeholders

| Role                                                   | Concern                                                                                                                                 |
| ------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| End users (researchers/engineers submitting jobs)      | Simple CLI to submit/kill jobs and see progress; jobs complete correctly and cheaply.                                                   |
| Sparkles operators/maintainers                         | Keep the control-plane (monitor, dashboard-backend) running reliably; diagnose incidents from Firestore/Events; keep GCP spend bounded. |
| Dashboard frontend (separate project, outside `v100/`) | Stable REST API (`openapi.yaml`) to show job/task/workpool/worker status and logs.                                                      |
