# Issues to resolve later

Known, deliberately-deferred consequences of the worker metric collection
revamp (adaptive sampling + container-scoped metrics + post-exit final
sample). The Go side is complete; the items below are not.

## 1. The dashboard frontend is broken (deliberate)

That pass was scoped to Go only, so nothing under `dashboard/src/` was
touched. The API now serves a different shape, so the metrics UI is broken
until the frontend is rewritten. Specifically:

### Wire format changes the frontend has not caught up with

- **`GET /api/v1/task/{id}/log` entries are now a discriminated union.** The
  metric payload moved from ~15 flat fields on the entry into a nested
  `metric` object (`taskLogEntry.Metric`, `dev/dashboard_backend.go`). Every
  field was also renamed and re-scoped, e.g. `cpu_user` →
  `host_cpu_user_pct`, `mem_total` → `host_memory_total_bytes`,
  `volumes[].total_gb` → `host_volumes[].total_bytes`.
- **`resource_usage` field names all changed**: `max_memory_bytes` →
  `container_memory_peak_bytes`, `cpu_user_usec` →
  `container_cpu_user_usec`, `block_read_bytes` →
  `container_io_read_bytes`, and so on.
- **Removed with no replacement**: `process_count`, `total_memory`,
  `total_data`, `total_shared`, `total_resident` (these summed
  `/proc/*/statm` across the whole VM, so they never attributed to a task —
  container metrics replace them), `mem_free`, and the
  `mem_pressure_*_avg10` fields (superseded by cumulative `*_stall_*_usec`
  totals).

### Files that need updating

- `dashboard/src/data/useTaskLog.ts` — decodes fields that no longer exist.
- `dashboard/src/types.ts:203-211` — `TaskSummaryRecord.resource_usage`,
  plus **two inline duplicates** of the same type in
  `pages/TaskDetail.tsx:44-51` and `components/TaskProperties.tsx:7-14`.
  Consolidate into one exported `ResourceUsageSummary` while rebuilding.
- `dashboard/src/pages/TaskDetail.tsx:321-405` — five `MultiLineChart`s
  whose `series[].key` string literals no longer resolve.
- `dashboard/src/components/TaskProperties.tsx:718-748` — reads the removed
  summary fields. **Line 742 (`resourceUsage.elapsed_seconds.toFixed(1)`)
  has no error boundary and will crash the task-detail React subtree** if
  that field is ever absent; guard it.
- `dashboard/src/data/jobPerf.ts:54-70` — percentiles over removed field
  names, which yields silently wrong percentiles rather than an error.
  Consumers: `pages/PerfOverview.tsx:249,264,280`,
  `pages/JobDetail.tsx:267-277`.
- `dashboard/src/data/useTaskPubsub.ts` — **dead code** (no importers) and a
  fourth copy of the schema. Delete rather than migrate.

### Guidance for the rebuild

- **Hold raw `MetricSample[]` in state and derive display points in a
  `useMemo`**, rather than converting per-message on arrival the way
  `toResourceDataPoint` does today. Stall and CPU rates require differencing
  against the previous sample, so a late-arriving or backfilled sample
  otherwise produces wrong rates. Put the differencing in a new
  `dashboard/src/data/metricRates.ts` and clamp negative deltas (counter
  reset / container replaced) to `null`.
- **Map `undefined | null | -1` to `null`, never to `NaN` or `0`.** `-1`
  means "unavailable on this kernel", which is different from zero. Render
  with `connectNulls={false}` so it shows as an honest gap. The current code
  silently produces `NaN` and draws blank charts with no indication why.
- **`MultiLineChart` types `data` as `any[]`** (`MultiLineChart.tsx:22`), so
  TypeScript does _not_ catch a stale `series[].key`. Tighten
  `SeriesConfig.key` to `keyof ResourceDataPoint` so future renames become
  compile errors instead of blank charts.
- **Keep the time-scaled `xDomain`** (`MultiLineChart.tsx:28-32`). It is now
  essential: adaptive sampling spaces points 1s apart early and 60s apart
  later, so a categorical axis would badly distort the early part of every
  task — which is the most interesting part.
- Suggested chart set: container CPU (stacked user+system as "% of one
  core", with a `ReferenceLine` at `100 x requested cores`); container stall
  %; host CPU (stacked, keeping iowait so it sums to 100%); host stall %
  — placed beside container stall, the pair distinguishes "my task is
  starved" from "the whole VM is saturated"; memory (container current and
  peak against host total and available); container I/O bytes/s; volumes.
- `build.sh` skips the frontend build when `dashboard/` is unchanged (commit
  7f26b41), and `dashboard/dist` is `//go:embed`ed by
  `v100/dev/webui/webui.go:16-17`, so the stale bundle keeps shipping until
  the frontend is actually rebuilt.

## 2. Firestore single-field index exemptions (ops)

`TaskLog` metric documents now carry ~40 flat fields, and the repo has no
`firestore.indexes.json`, so Firestore will auto-create a single-field index
per field per document. Only `task_id`, `type`, `timestamp` and `expiry` are
ever queried. **Add single-field index exemptions for the metric fields** to
avoid paying for ~36 useless indexes on every sample. Note this is a
write-amplification and cost issue, not a correctness one.

## 3. Post-exit cgroup lifetime (accepted gap)

Nothing proves the container's cgroup directory still exists between
`cmd.Run()` returning and `docker rm`; it depends on containerd teardown
timing. Current behaviour is deliberate and safe: if the cgroup has gone,
every container counter reports `-1` and the task continues normally, with
timing and exit code still populated from `docker inspect`. Losing final
container metrics is acceptable; failing a task over it is not.

If final metrics turn out to be missing often in practice, the fix is
`--cgroup-parent` with a pre-created parent cgroup, whose hierarchical
counters retain a dead child's CPU / peak memory / I/O / PSI. That is a
producer-side change confined to `v100/cgroup.go` plus two lines of
`executeDockerCommand`, with no schema impact. Caveat: a stock GCE image
likely uses the systemd cgroup driver, which rejects any `--cgroup-parent`
that is not a systemd-managed `*.slice`, so this needs a pre-created slice.

## 4. Network I/O is not collected

Time blocked on an HTTP request is invisible to both `iowait` and
`io.pressure` — both track block I/O (and, for PSI, memory reclaim), whereas
a socket wait is a _voluntary_ sleep that simply shows up as host idle.

It is therefore inferred rather than measured, via **unaccounted wait**:

    elapsed - container_cpu_usage - cpu_stall - memory_stall - io_stall

A container that burned 0.2s of CPU over 60s wall with all stall totals near
zero spent the rest voluntarily sleeping, which for a data-mover task means
network. This is a display-time derivation and is not stored.

If that proves too coarse, add container network counters from
`/sys/class/net/*/statistics/{rx,tx}_bytes` inside the container's network
namespace (or `/proc/<pid>/net/dev`). Note that gives _volume_, not blocked
time.

## 5. cgroup v1 is unsupported

Support was dropped rather than maintained: v1 exposes no PSI at all, and
PSI totals are the centerpiece of the new metric set, so a v1 path would
report `-1` for much of the schema anyway. On a v1 host every container
metric is `-1` and one warning is logged per process
(`cgroupV2Enabled`, `v100/cgroup.go`).

## 6. Pre-existing test-harness data race (unrelated, found in passing)

`v100/functest/helpers_test.go:73-79` races on `cmd.ProcessState`: a
goroutine calls `cmd.Wait()` (which writes it) while `t.Cleanup` reads
`cmd.ProcessState == nil` without synchronisation. This makes
`go test -race ./functest/` fail (`TestKillJob`, `TestSubmitAndComplete`,
`TestLocalizeAndCommand`) on clean `HEAD` as well — verified against commit
1472d8e, so it predates the metrics work. The tests pass without `-race`.

Fix: have the cleanup select on the existing `waitDone` channel instead of
inspecting `cmd.ProcessState`. Worth doing, because it currently masks any
_real_ race the functests might otherwise catch.
