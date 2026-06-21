# Datamodel Documentation vs Code Discrepancies

Each item notes what the doc says, what the code does, and whether the doc or the code likely needs updating.

---

## 1. Topic name inconsistency: "sparkles-worker-out" vs "sparkles-events"

**Doc:** Lines in the Events collection intro refer to "sparkles-worker-out"; elsewhere (job events, monitor sections) the doc uses "sparkles-events".

**Code:** `worker.go` defines `const workerOutTopic = "sparkles-events"`. All publisher/subscriber code uses "sparkles-events".

**Verdict:** Fix the doc. The correct topic name is `sparkles-events`; remove all references to `sparkles-worker-out`.

---

## 2. Task `command` field documented as `string`, implemented as `[]string`

**Doc:** Tasks table lists `"command"` with type `string`.

**Code:** `task_queue.go` — `Command []string \`firestore:"command"\``

**Verdict:** Fix the doc. The field is an array of strings (command + args).

---

## 3. Workers collection — undocumented fields `batch_id` and `instance_name`

**Doc:** Workers table lists only `worker_id`, `workpool_id`, `status`, `expiry`, `heartbeat_expiry`.

**Code:** `worker.go` `WorkerRecord` also stores:

- `batch_id string` — which batch request spawned this worker
- `instance_name string` — GCP VM instance name (used for surgical VM termination)

**Verdict:** Fix the doc. Add both fields to the Workers table.

---

## 4. WorkPools collection — undocumented fields

**Doc:** WorkPools table has no mention of status or incident fields.

**Code:** `task_queue.go` `WorkPool` struct also stores four monitor-written fields:

- `status string`
- `status_message string`
- `last_incident_at timestamp`
- `incident_count int`

And two config fields also absent from the doc:

- `region string`
- `zones []string`

**Verdict:** Fix the doc. Add all six fields to the WorkPools table (region/zones as config, the four status fields as monitor-managed).

---

## 5. JobSummary and JobSummaryHistory — undocumented `workpool_id` field

**Doc:** JobSummary table lists `job_id`, `expiry`, `status`, `tasks`. JobSummaryHistory lists the same plus `timestamp`.

**Code:** Both structs in `monitor/interfaces.go` include `WorkpoolID string \`firestore:"workpool_id"\``.

**Verdict:** Fix the doc. Add `workpool_id` to both collection tables.

---

## 6. BatchAPIRequests collection — exists in code, not documented

**Doc:** Collection is only mentioned in passing in a prose paragraph ("The monitor reverse-looks up the internal `batch_id` from the `BatchAPIRequests` Firestore collection"). No field table exists.

**Code:** `monitor/adapters.go` implements a full store with fields: `batch_id`, `job_id`, `workpool_id`, `expected_vm_count`, `preemptible`, `submitted_at`, `running_since`, `registered_worker_count`, `status`, `unhealthy`.

**Verdict:** Fix the doc. Add a formal `BatchAPIRequests` collection section with the full field list. Possible status values for `status`: `pending`, `started`, `completed`, `failed`.

---

## 7. TaskLog collection — documented but not implemented

**Doc:** A full `TaskLog` collection section describes `task_id`, `type`, `timestamp`, `expiry` fields and metric/log update entries.

**Code:** No corresponding struct, collection constant, or Firestore operations exist anywhere under `v100/`.

**Verdict:** Needs a decision. Either remove the section from the doc (not yet implemented and no near-term plan) or mark it clearly as "planned / not yet implemented". Do not leave it appearing as current.

---

## 8. Task state machine — `running` and `writing` states never entered

**Doc:** Documents the full state machine: `claimed → running` (worker starts process) and `running → writing` (process exits, uploads begin).

**Code:** Worker stub skips these transitions. Tasks go directly from `claimed` to `success` or `failed`; `running` and `writing` are never written to Firestore.

**Verdict:** Needs a decision. If the stub is temporary and these states will be implemented, leave the doc as-is and note this in code. If the states will never be used, simplify the documented state machine to `claimed → success/error/failed`.

---

## 9. `error` task status — documented but never set

**Doc:** `writing → error` transition: task process exits with non-zero code; `exit_code` is recorded.

**Code:** `worker.go` records only `success` or `failed`; there is no call path that sets status to `error`. `RecordError` is defined in `task_queue.go` but never called.

**Verdict:** Needs a decision. If `error` (process exited non-zero) should be distinguished from `failed` (infrastructure failure), implement the `RecordError` call. If not, remove the `error` status from the doc and simplify to `success` / `failed`.

---

## 10. `killed` task status — defined but never set

**Doc:** `killed` is listed as a terminal task status meaning the task was administratively terminated.

**Code:** `StatusKilled = "killed"` constant is defined but there is no code path that ever sets a task to this status.

**Verdict:** Needs a decision. Either implement the kill path or mark this status as planned/unimplemented in the doc.

---

## 11. Events collection — `job_terminated` event fields not documented in the Events section

**Doc:** The `job_terminated` event is described in the `sparkles-events` topic section, but the Events Firestore collection section does not list which `EventRecord` fields are populated for `job_created` or `job_terminated` events.

**Code:** `events.go` populates `job_id` and `workpool_id` for both event types.

**Verdict:** Fix the doc. Add per-event-type field annotations to the Events collection section, matching the pattern already used for `worker_started`/`worker_stopped` and `task_state_update`.

---

## 12. Orphaned task recovery (watchdog) — documented but partially unimplemented

**Doc:** Documents that the watchdog resets orphaned tasks from active states back to `pending`.

**Code:** `monitor/requeue_orphaned.go` implements resetting tasks from `claimed`, `running`, and `writing` back to `pending`. However `running` and `writing` are never actually entered (see issue 8), so in practice only `claimed` tasks are ever recovered.

**Verdict:** No doc change needed now; this resolves itself once issue 8 is resolved.
