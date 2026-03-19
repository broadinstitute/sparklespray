# Firestore Migration: Remaining Steps

## What Has Been Done

All core source files have been migrated from `google.cloud.datastore` to `google.cloud.firestore`:

1. **`cli/pyproject.toml`** ✅ — replaced `google-cloud-datastore` with `google-cloud-firestore = "^2.11.0"`
2. **`cli/poetry.lock`** ✅ — regenerated (`poetry lock --no-update && poetry install`)
3. **`cli/sparklespray/datastore_batch.py`** ✅ — rewrote `Batch` and `ImmediateBatch` for Firestore (`doc_ref.set()`, `client.batch()`, etc.)
4. **`cli/sparklespray/task_store.py`** ✅ — replaced `task_to_entity`/`entity_to_task` with `task_to_dict`/`snapshot_to_task`; updated all `TaskStore` methods to use Firestore collection/document API
5. **`cli/sparklespray/job_store.py`** ✅ — same pattern; replaced `job_to_entity`/`entity_to_job` with `job_to_dict`/`snapshot_to_job`; updated `get_last_job` to use `order_by(...).limit(1).stream()`
6. **`cli/sparklespray/cluster_service.py`** ✅ — rewrote `heartbeat()` and `clear_heartbeat()` using `@firestore.transactional` decorator; updated `get_heartbeat()` to use `doc_ref.get()`
7. **`cli/sparklespray/job_queue.py`** ✅ — updated import and `delete_job()` to use Firestore document API
8. **`cli/sparklespray/config.py`** ✅ — replaced `datastore.Client(...)` with `firestore.Client(project=..., ...)` (kept the key name `datastore_client` to avoid touching all command files)
9. **`cli/sparklespray/gcp_setup.py`** ✅ — replaced `datastore.googleapis.com` with `firestore.googleapis.com`; renamed `can_reach_datastore_api` → `can_reach_firestore_api`; replaced `deploy_datastore_indexes` with `deploy_firestore_indexes` (uses `gcloud firestore indexes composite create`)
10. **`cli/sparklespray/commands/submit.py`** ✅ — updated import and type annotation from `datastore.Client` to `firestore.Client`
11. **`cli/tests/sparklespray/factories.py`** ✅ — rewrote `DatastoreClientSimulator` as `FirestoreClientSimulator` with `SimulatedCollection`, `SimulatedDocumentRef`, `SimulatedDocumentSnapshot`, `SimulatedQuery`, `SimulatedWriteBatch`, `SimulatedTransaction`; kept `DatastoreClientSimulator` as alias for backward compatibility
12. **`cli/tests/conftest.py`** ✅ — removed `--database-mode=datastore-mode` from emulator startup; changed env var from `DATASTORE_EMULATOR_HOST` to `FIRESTORE_EMULATOR_HOST`; renamed `datastore_client` fixture to `firestore_client` using `firestore.Client`; updated `e2e_test_env` to use `firestore_client` key; updated `sparklesworker` fixture to pass `FIRESTORE_EMULATOR_HOST`

## Current Test Status

Running `poetry run pytest tests/sparklespray/ -v`:

- **54 passing**, 3 skipped ✅
- **7 failing**:
  - `test_watch_basic`, `test_watch_no_nodes`, `test_watch_keyboard_interrupt` — `SimulatedTransaction` needs more attributes for `@firestore.transactional` compatibility (needs `_commit()` method — partially fixed, need to rerun)
  - `test_submit_cmd_basic`, `test_submit_cmd_with_seq_parameter`, `test_submit_cmd_complex_args` — `config.service_account_email` is `None`; **likely pre-existing** (not caused by this migration; needs verification)
  - `test_sub_and_kill_with_emulators` — emulator integration test, likely pre-existing

## Remaining Work

### 1. Fix `SimulatedTransaction` (in progress)

The `@firestore.transactional` decorator in `cluster_service.py` uses the Firestore Python library's internal transaction machinery. The `SimulatedTransaction` in `factories.py` needs to satisfy the interface that `google.cloud.firestore_v1.transaction.run_in_transaction` expects.

**Already added:**

- `_read_only = False`
- `_max_attempts = 1`
- `_id = None`
- `_begin(retry_id=None)`
- `_clean_up()`
- `_rollback()`
- `_commit()` (private, returns `[]`)
- `commit()` (public, calls `_commit()`)

**Likely still needed** (keep running tests to find each missing attribute):

- Look at what `self._pre_commit(transaction, ...)` inside the Firestore `transactional.__call__` expects from the transaction. Check the Firestore library source at:
  `/Users/pmontgom/Library/Caches/pypoetry/virtualenvs/sparklespray-nkyf_5y2-py3.12/lib/python3.12/site-packages/google/cloud/firestore_v1/transaction.py`

  Add any missing attributes/methods to `SimulatedTransaction`.

**Alternative approach** (if the above becomes too painful): In `cluster_service.py`, instead of `@firestore.transactional`, manually implement the transaction using a try/except that works with both real Firestore and the simulator:

```python
def heartbeat(self, watch_run_uuid, expected_uuid=None):
    doc_ref = self.client.collection(CLUSTER_HEARTBEAT_COLLECTION).document(self.cluster_id)
    took_over_stale = False

    def do_heartbeat(transaction):
        nonlocal took_over_stale
        snapshot = doc_ref.get(transaction=transaction)
        now = time.time()
        if expected_uuid is not None and snapshot.exists:
            data = snapshot.to_dict()
            current_uuid = data.get("watch_run_uuid")
            if current_uuid != expected_uuid:
                current_timestamp = data.get("timestamp", 0)
                if (now - current_timestamp) > SECONDS_UNTIL_STALE_HEARTBEAT:
                    took_over_stale = True
                else:
                    return False
        transaction.set(doc_ref, {"watch_run_uuid": watch_run_uuid, "timestamp": now})
        return True

    transaction = self.client.transaction()
    result = transaction.run(do_heartbeat)  # or similar
    ...
```

However, `transaction.run()` is not a real Firestore API either. The `@firestore.transactional` decorator is the official API.

**Recommended**: Just keep adding the missing attributes to `SimulatedTransaction` iteratively by running the tests.

### 2. Verify `test_submit_cmd` failures are pre-existing

Run the following to confirm the submit tests were already failing before the migration:

```bash
git stash -q && poetry run pytest tests/sparklespray/test_submit_cmd.py -v --no-header -q 2>&1 | tail -10; git stash pop -q
```

If they were already failing, no action needed. If they were passing before, investigate what changed — the submit command's Firestore client usage is minimal (just passes it through).

### 3. Check `index.yaml` in `pyproject.toml`

The `pyproject.toml` still has:

```toml
include = ["sparklespray/bin/sparklesworker", "sparklespray/index.yaml"]
```

The `index.yaml` was used for Datastore index definitions. Now that we use Firestore native mode, this file is no longer needed. You can:

- Remove `"sparklespray/index.yaml"` from the `include` list, OR
- Keep it (harmless if the file doesn't exist)

### 4. CLAUDE.md is outdated

The `cli/CLAUDE.md` still references Datastore extensively. Once everything is working, update it to reflect Firestore. Key things to update:

- Architecture section: "Datastore" → "Firestore"
- Integration test fixtures table: rename `datastore_client` → `firestore_client`, update env var
- The emulator test examples that use `datastore.Entity(key=key)` etc.
- The docstring about `DatastoreClientSimulator` → `FirestoreClientSimulator`

### 5. Remove `index.yaml` from the sparklespray package (optional cleanup)

If `cli/sparklespray/index.yaml` exists, it can be deleted since it was for Datastore composite indexes. Firestore indexes are created via `gcloud firestore indexes composite create` instead.

## Key Design Decisions Made

- **Kept `datastore_client` as variable/parameter name** throughout command files and `config.py` LazyInit. This avoids touching ~6 command files. The name is just a historical artifact — it's actually a `firestore.Client` at runtime.
- **Used `@firestore.transactional` decorator** in `cluster_service.py` as specified in the plan, requiring `SimulatedTransaction` to implement the Firestore library's internal transaction interface.
- **`DatastoreClientSimulator` kept as alias** for `FirestoreClientSimulator` — all existing test imports still work without changes.
