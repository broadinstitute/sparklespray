This document describes what was implemented and I've added sections marked **Additional changes requested** with additions that should be made based on my review.

## Implemented

- **Facet / "Filter by" bar**: popover-based label-key dropdowns appear below the
  search bar whenever jobs with labels are visible. Clicking a chip on a job row
  also toggles the corresponding facet. Within a key = OR, across keys = AND.

- **Worker Pools sidebar**: polls `GET /api/v1/cluster_statuses` and
  `GET /api/v1/clusters` (both newly added) every 10 s. Shows a card per cluster
  with running tasks, workers in use/idle, two proportion bars
  (in-use/idle + preemptable/non-preemptable), orphaned-task strip, worker-request
  breakdown, and a short-fail alert banner when applicable.

- **Worker Pool column** in the jobs table (between Identifier and Tasks/OK/Fail),
  using a color swatch + cluster ID in a distinct visual style from label chips.

**Additional changes requested**

Change `GET /api/v1/cluster_statuses` to `GET /api/v1/clusters/summary` to be more consistent with job summaries

Instead of polling every 10s, please poll every 30s. (clusters don't update frequently)

### `ClusterStatus.lastUpdate` staleness

If the cluster health monitor stops running or can't reach GCP, `lastUpdate` will
drift. The card displays `updated Xs ago` but there is currently no threshold at
which the card would visually flag itself as stale.

**Question**: At what age should a cluster status card be considered stale and
shown with a warning?

**Additional changes requested**

If lastUpdate > 5 minutes, highlight the `updated Xs ago` in red

### 4. Worker Pools sidebar only appears when data is present

The sidebar is hidden until at least one `ClusterStatus` record is returned by the
API. On first load (before the first 10 s poll completes) the layout is
single-column. A loading skeleton or placeholder would improve the experience on
wider screens.

**Additional changes requested**

Put a placeholder in the worker pools section saying something like "No worker pools.

Worker pools will be automatically created when jobs need workers to run tasks."
