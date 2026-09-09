# Issues to resolve later

Known, deliberately-deferred consequences of the worker metric collection
revamp (adaptive sampling + container-scoped metrics + post-exit final
sample). The Go side is complete; the items below are not.

## 1. The dashboard frontend is broken (deliberate) — partially resolved

**Resolved**: the periodic-metrics tab (`GET /api/v1/task/{id}/log`'s
`metric_update` entries). `useTaskLog.ts` now decodes the real nested
`entry.metric` shape via a new metadata-driven pipeline: `GET /api/v1/metrics`
(`v100/metric_metadata.go`, `MetricMetadataTable`) describes every metric
(`name`/`description`/`units`/`type`/`default_position`); `useMetricMetadata.ts`
fetches it; `metricSeries.ts` turns a metric's raw samples into a display
series (a `"counter"`-typed metric becomes a rate, a `"gauge"` is plotted
as-is); `MetricsPanel.tsx` replaces `TaskDetail.tsx`'s old hardcoded
`MultiLineChart` block with one chart per metric the user has checked
(seeded from `default_position`: `host_cpu_user_pct`, then
`container_memory_current_bytes`). `useTaskPubsub.ts` (dead code, no
importers) was deleted rather than migrated, as this doc already suggested.
`connectNulls` is still left at its default `false` on `MultiLineChart`, so
a metric absent from a sample (server-side nil) still renders as an honest
gap, not a dip to zero — the guidance below about not mapping missing values
to `0`/`NaN` was already satisfied by that default, just needed the data
layer to stop reading fields that don't exist so gaps are the only way a
missing value shows up.

**Still unresolved** — a different data path (`ResourceUsage`, the one-shot
post-exit summary on `Task`, not the periodic `MetricSample` stream above)
was out of scope for that pass:

- `dashboard/src/types.ts:203-211` — `TaskSummaryRecord.resource_usage`,
  plus **two inline duplicates** of the same type in
  `pages/TaskDetail.tsx:44-51` and `components/TaskProperties.tsx:7-14`, all
  three still using the old `resource_usage` field names (`max_memory_bytes`,
  `cpu_user_usec`, `block_read_bytes`, ...) instead of the current
  `container_memory_peak_bytes`/`container_cpu_user_usec`/
  `container_io_read_bytes`/etc. Consolidate into one exported
  `ResourceUsageSummary` while fixing.
- `dashboard/src/components/TaskProperties.tsx:718-748` — reads the removed
  summary fields. **Line 742 (`resourceUsage.elapsed_seconds.toFixed(1)`)
  has no error boundary and will crash the task-detail React subtree** if
  that field is ever absent; guard it. This is a real crash risk, not just a
  stale-data issue — worth prioritizing over the rest of this section.
- `dashboard/src/data/jobPerf.ts:54-70` — percentiles over removed field
  names, which yields silently wrong percentiles rather than an error.
  Consumers: `pages/PerfOverview.tsx:249,264,280`,
  `pages/JobDetail.tsx:267-277`.

`build.sh` skips the frontend build when `dashboard/` is unchanged (commit
7f26b41), and `dashboard/dist` is `//go:embed`ed by
`v100/dev/webui/webui.go:16-17` — remember to rebuild before deploying so
this fix (and any future one covering the items above) actually ships.

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
