# Plan: capture the final metric snapshot from inside the container

**Status:** an interim mitigation for the bug below has since shipped: the
final sample now backfills any field it couldn't read itself from the last
periodic sample that did (`mergeMetricFields`/`updateLastGoodAccumulator` in
`v100/task_metrics.go`), and `ContainerPresent` was removed as redundant with
the per-field nil convention (it could disagree with the real per-field
availability in exactly this race). That closes the practical impact for any
task that got at least one periodic sample first. It does **not** help a task
whose _first_ sample is also its _final_ one (too short to be sampled
periodically at all) — this proposal's in-container approach remains the
complete fix for that case and is still open.

## Problem

`executeDockerCommand` (`v100/worker.go:156-220`) runs a task's container, and
right after `cmd.Run()` returns — before `docker rm -f` — calls
`tel.TriggerFinalSample()` to take the last `MetricSample` for the task. The
comment there explains the intent: "docker rm destroys its cgroup", so the
final read has to happen while the container still exists.

That protects against the _explicit_ `docker rm`, but not against the cgroup
disappearing on its own. On hosts using the `systemd` cgroup driver (confirmed
on `container-os-test`: `Cgroup Driver: systemd`, `Cgroup Version: 2`, the
common Docker default), each container's cgroup is a transient systemd scope
that systemd garbage-collects as soon as the last process in it exits —
independent of Docker, containerd, or anything sparklespray does. This can
happen in the sub-millisecond-to-millisecond window between `cmd.Run()`
returning and the final-sample goroutine actually reading the cgroup files.

`containerCgroup.resolve()` (`v100/cgroup.go:365-389`) makes this worse by
caching: once it finds the cgroup directory on an earlier periodic sample, it
returns that cached path forever, without re-checking it still exists. So
`containerMetrics()` (`v100/task_metrics.go:243-252`) reports
`ContainerPresent: true` (the cached directory is non-empty), but
`readContainerCounters` (`v100/cgroup.go:283-329`) then fails to open every
individual file, and each counter falls back to `metricUnavailable` — which,
since the `MetricSample` nil/omitempty change (schema 3), shows up as every
`container_*` field simply missing from the final sample.

Net effect: a task's final `MetricSample`/`ResourceUsage` — the numbers used
for cost/resource accounting — can silently lose all container-side counters,
indistinguishable from the normal "container never started" case, for a race
that has nothing to do with task duration or correctness of the task itself.

## Proposed approach

Stop trying to read the container's cgroup from the host after the fact.
Instead, run a small wrapper binary as the container's entrypoint that reads
its own cgroup from the inside, while it is still alive — which is exactly
the process whose exit is what allows the cgroup to be torn down, so there is
no window where "still running" and "cgroup gone" can both be true.

This works because of cgroup namespacing: Docker/containerd give every
container a private cgroup namespace by default on cgroup v2, so a process
inside the container sees `/sys/fs/cgroup` as _its own_ cgroup root rather
than something it has to locate on the host. Confirmed directly:

```
$ docker run --rm alpine cat /proc/self/cgroup
0::/
```

So the wrapper never needs `docker inspect`, never needs to guess which of
`system.slice/docker-<id>.scope` or `docker/<id>` layout the host uses, and
never races against systemd's cleanup — it just reads
`/sys/fs/cgroup/memory.current`, `cpu.stat`, etc., directly.

Scope: **this only replaces how the final sample's container counters are
collected.** Periodic per-task samples keep using today's external
`containerCgroup`/`locateContainerCgroup` machinery unchanged; the wrapper's
job is limited to producing one JSON snapshot, once, at container exit.

## Design

### 1. New binary: `v100/cmd/sparkles-init`

A minimal PID-1 wrapper:

1. Exec the task's real command as a child process (argv/env/cwd/stdio all
   passed through unchanged), tracking its PID separately from anything else.
2. Forward `SIGTERM`/`SIGINT`/etc. it receives to the child (process group),
   so `docker stop`'s existing behavior is unaffected.
3. Reap any orphaned grandchildren reparented to it — required because it is
   now PID 1 inside the container's PID namespace, a responsibility the
   codebase has never needed before (there is no existing `SIGCHLD`/reaper/
   tini-equivalent anywhere in the repo today — this is new ground, not a
   refactor of something already present).
4. On the child's exit, read `/sys/fs/cgroup/*` (see below), write a small
   JSON summary to the path given by an environment variable (e.g.
   `SPARKLES_METRICS_OUT`), and `os.Exit` with the child's exit status
   (translating death-by-signal to the conventional `128+signum`).

An env var rather than an argv flag for the output path, to avoid needing any
`--`-style separator convention between the wrapper's own flags and the
task's command/args — `t.Command` is passed through completely unmodified.

### 2. Share the cgroup-file-parsing logic instead of duplicating it

`v100/cgroup.go` already has all the parsing logic this needs
(`containerCounters`, `readContainerCounters`, `readCgroupInt64`,
`readCgroupKeyValue`/`parseKeyValueFile`, `lookupInt64`/`sumInt64`,
`parseIOStatV2`, `parsePressure`/`readPressureFile`, the `metricUnavailable`
sentinel) — with real nuance already worked out (the `pids.peak` → `pids. current` fallback for older kernels, `"max"` meaning unavailable for
`memory.max`, summing `workingset_refault_file`+`workingset_refault_anon`,
etc.). Forking that logic into a second copy for the wrapper would let the two
silently drift.

Move the pure file-parsing half (everything above) into a new package,
`v100/internal/cgroupstats`, importable by both:

- `v100/cgroup.go`, which keeps the host-side discovery half
  (`containerCgroup`, `locateContainerCgroup`, `cgroupDirForPID`,
  `dockerInspectIDAndPid`) and just calls
  `cgroupstats.ReadContainerCounters(dir)` where it used to call the private
  function directly.
- `v100/cmd/sparkles-init`, which calls
  `cgroupstats.ReadContainerCounters("/sys/fs/cgroup")` directly — no
  discovery step needed at all.

(`internal/` here works because both consumers live under `v100/`, the
directory `internal` is rooted at.)

### 3. Preserving the image's own `ENTRYPOINT`

This is the main compatibility risk and needs to be explicit rather than
glossed over. Today, `executeDockerCommand` runs
`docker run --name ... -w workDir <extra-args> image command...`
(`v100/worker.go:166-168`) with no `--entrypoint` override, so Docker's normal
resolution applies: `command` becomes arguments to the image's own
`ENTRYPOINT` if it has one, or the full argv if it doesn't. Interposing
`sparkles-init` means passing `--entrypoint /sparkles-init`, which _replaces_
the image's entrypoint outright — if the wrapper just execs `t.Command`
directly, any image relying on its own `ENTRYPOINT` (e.g. the common `python`
base-image pattern of `ENTRYPOINT ["python"]` + `CMD`-as-script-name) breaks.

Fix: before starting the container, resolve what the effective argv _would
have been_ under Docker's normal rules, and give the wrapper that instead of
raw `t.Command`:

```go
// one extra docker inspect on the image, cheap and already a pattern used
// elsewhere in this codebase (dockerInspectState, dockerInspectIDAndPid)
entrypoint, imageCmd, err := dockerInspectImageEntrypoint(imageName)
effectiveArgv := entrypoint
if len(t.Command) > 0 {
    effectiveArgv = append(effectiveArgv, t.Command...)
} else {
    effectiveArgv = append(effectiveArgv, imageCmd...)
}
```

`sparkles-init` execs `effectiveArgv` unchanged from what Docker would have
run. Given the added complexity here, ship this behind an opt-in flag (e.g.
`--capture-final-snapshot-via-init`, defaulting off) so it can be validated
against a range of real task images before becoming the default, with a clean
fallback to today's external read if it's off or if the wrapper's output file
is missing/unparseable after exit.

### 4. Getting the file bind-mounted in and the JSON back out

Two bind mounts added to `executeDockerCommand`'s docker args, read-only for
the binary, read-write for the output:

- `-v <extracted-sparkles-init-path>:/sparkles-init:ro`
- `-v <per-task-host-path>/final-metrics.json:/final-metrics.json` (the
  per-task host path can live alongside `paths.taskWorkDir`, which
  `prepareWorkDir` already creates per task — `v100/worker.go:750-791`)
- `--entrypoint /sparkles-init`, `SPARKLES_METRICS_OUT=/final-metrics.json`
  via `-e`

Because this is a bind-mounted host file rather than a cgroup path, the host
side can read it any time after `cmd.Run()` returns — there's no more race to
manage at all, unlike the current cgroup read.

This reuses the existing `-v host:container[:opts]` mechanism
`buildDockerArgs` already builds for operator-configured `--bind-mount`
entries (`v100/worker.go:797-803`) — these two mounts are just added
unconditionally alongside those, not configured by the operator.

### 5. Build/deploy: embed the wrapper binary in `sparkles` itself

Shipping `sparkles-init` as a separate deployed artifact would create a
version-skew risk (a worker running against a stale copy from a previous
deploy). `build.sh` already has a directly analogous pattern for exactly this
problem: it builds the dashboard frontend first, copies the output into
`v100/dev/webui/dist/`, and `v100/dev/webui/webui.go` embeds it with
`//go:embed all:dist` so the `sparkles` binary is self-contained
(`v100/build.sh:44-59`, `v100/dev/webui/webui.go:15-16`).

Do the same for `sparkles-init`:

1. `build.sh` builds `v100/cmd/sparkles-init` as its own static binary first
   (`CGO_ENABLED=0 GOOS=linux GOARCH=amd64`, same flags already used for
   `sparkles` itself at `v100/build.sh:68-72` — static is required here
   regardless, since this binary gets bind-mounted into arbitrary task images
   with unknown/no libc).
2. Copy the resulting binary into a new embed directory, e.g.
   `v100/dockerinit/embed/sparkles-init`.
3. A new small package (`v100/dockerinit`) does `//go:embed sparkles-init`
   and, on first use, writes it out to a temp file with `0755` permissions,
   once per worker process, reusing that extracted copy as the `-v ...:ro`
   source for every task.

### 6. What stays the same

- Periodic (non-final) `MetricSample` collection: unchanged, still uses
  `containerCgroup`/`locateContainerCgroup` from the host, including its
  existing tolerance for "container not created yet."
- `buildResourceUsage`'s use of `docker inspect .State` for
  `StartTime`/`EndTime`/`ExitCode`/`OOMKilled` (`v100/resource_usage.go:19-33`):
  unaffected, since that's containerd/Docker's own bookkeeping, not cgroup
  state.
- `MetricSample`'s wire shape/schema: unchanged. The wrapper produces the same
  `container_*` fields, nil under the same "genuinely unavailable" rule (e.g.
  a controller not delegated) — just sourced differently. No
  `MetricSchemaCurrent` bump needed.
- `ResourceUsage`'s `-1` sentinel convention: unaffected.

### 7. Testing

- Move `cgroup_test.go`'s parsing-function tests to live alongside the new
  `cgroupstats` package (pure refactor, same test cases).
- Integration test: run `simulate_load.py` with a short `--run_time` (exactly
  today's racy case) through the wrapper and assert the final sample's
  `container_*` fields are always populated — this is the regression test for
  the bug this whole proposal exists to fix.
- Entrypoint-resolution test: a task image with its own `ENTRYPOINT` (e.g. a
  `python:...` image with a script `CMD`) runs identically with the flag on
  and off.

### 8. Out of scope / explicitly not doing

- Moving periodic sampling in-container too (would remove
  `containerCgroup`/`locateContainerCgroup` entirely and let the external
  `MetricSampler` shrink to host-only metrics) — a larger, separate change;
  not part of this proposal.
- Anything involving a persistent parent cgroup / `--cgroup-parent` (an
  earlier idea for this same problem) — superseded by this approach, which
  has no dependency on cgroup-driver behavior, systemd unit properties, or
  controller delegation into an ad hoc parent slice at all.
