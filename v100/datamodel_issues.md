# Datamodel Documentation vs Code Discrepancies

Each item notes what the doc says, what the code does, and whether the doc or the code likely needs updating.

---

## 7. TaskLog collection — documented but not implemented

**Doc:** A full `TaskLog` collection section describes `task_id`, `type`, `timestamp`, `expiry` fields and metric/log update entries.

**Code:** No corresponding struct, collection constant, or Firestore operations exist anywhere under `v100/`.

**Verdict:** Needs a decision. Either remove the section from the doc (not yet implemented and no near-term plan) or mark it clearly as "planned / not yet implemented". Do not leave it appearing as current.

---

## 10. `killed` task status — defined but never set

**Doc:** `killed` is listed as a terminal task status meaning the task was administratively terminated.

**Code:** `StatusKilled = "killed"` constant is defined but there is no code path that ever sets a task to this status.

**Verdict:** Needs a decision. Either implement the kill path or mark this status as planned/unimplemented in the doc.
