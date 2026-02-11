from typing import Optional

from sparklespray.watch.runner_types import IncrementalTaskFetcher
from sparklespray.task_store import Task, STATUS_PENDING, STATUS_COMPLETE
from unittest.mock import Mock, call
import time


def make_task(
    task_id: str, status: str = STATUS_PENDING, last_updated: Optional[float] = None
):
    """Helper to create a Task with minimal required fields."""
    return Task(
        task_id=task_id,
        task_index=int(task_id.split(".")[-1]) if "." in task_id else 1,
        job_id="test-job",
        status=status,
        owner=None,
        monitor_address=None,
        args="{}",
        history=[],
        command_result_url="",
        cluster="test-cluster",
        log_url="",
        last_updated=last_updated,
    )


def test_first_call_fetches_all_tasks():
    """First call should fetch all tasks using get_tasks()."""
    task_store = Mock()
    tasks = [make_task("test-job.1", last_updated=100.0)]
    task_store.get_tasks.return_value = tasks

    fetcher = IncrementalTaskFetcher(task_store, "test-job", min_delay=0)
    result = fetcher()

    task_store.get_tasks.assert_called_once_with(job_id="test-job")
    task_store.get_tasks_updated_since.assert_not_called()
    assert len(result) == 1
    assert result[0].task_id == "test-job.1"


def test_subsequent_calls_fetch_incrementally():
    """After first call, should use get_tasks_updated_since()."""
    task_store = Mock()
    initial_tasks = [
        make_task("test-job.1", last_updated=100.0),
        make_task("test-job.2", last_updated=100.0),
    ]
    task_store.get_tasks.return_value = initial_tasks
    task_store.get_tasks_updated_since.return_value = []

    fetcher = IncrementalTaskFetcher(task_store, "test-job", min_delay=0)

    # First call
    fetcher()
    assert task_store.get_tasks.call_count == 1

    # Second call should use incremental fetch
    result = fetcher()
    task_store.get_tasks_updated_since.assert_called_once()
    assert len(result) == 2


def test_incremental_fetch_merges_changed_tasks():
    """Changed tasks should be merged into the cache."""
    task_store = Mock()
    initial_tasks = [
        make_task("test-job.1", status=STATUS_PENDING, last_updated=100.0),
        make_task("test-job.2", status=STATUS_PENDING, last_updated=100.0),
    ]
    task_store.get_tasks.return_value = initial_tasks

    # Task 1 has been updated to complete
    updated_task = make_task("test-job.1", status=STATUS_COMPLETE, last_updated=200.0)
    task_store.get_tasks_updated_since.return_value = [updated_task]

    fetcher = IncrementalTaskFetcher(task_store, "test-job", min_delay=0)

    # First call
    fetcher()

    # Second call - should merge the updated task
    result = fetcher()
    result_dict = {t.task_id: t for t in result}

    assert len(result) == 2
    assert result_dict["test-job.1"].status == STATUS_COMPLETE
    assert result_dict["test-job.2"].status == STATUS_PENDING


def test_rate_limiting_returns_cached_results():
    """Calls within min_delay should return cached results without fetching."""
    task_store = Mock()
    tasks = [make_task("test-job.1", last_updated=100.0)]
    task_store.get_tasks.return_value = tasks

    fetcher = IncrementalTaskFetcher(task_store, "test-job", min_delay=10.0)

    # First call
    fetcher()
    assert task_store.get_tasks.call_count == 1

    # Immediate second call should return cached (within min_delay)
    result = fetcher()
    assert task_store.get_tasks.call_count == 1  # No additional call
    assert len(result) == 1


def test_watermark_updated_from_tasks():
    """Watermark should be updated to max last_updated from fetched tasks."""
    task_store = Mock()
    initial_tasks = [
        make_task("test-job.1", last_updated=100.0),
        make_task("test-job.2", last_updated=150.0),
        make_task("test-job.3", last_updated=120.0),
    ]
    task_store.get_tasks.return_value = initial_tasks
    task_store.get_tasks_updated_since.return_value = []

    fetcher = IncrementalTaskFetcher(task_store, "test-job", min_delay=0)
    fetcher()

    # Watermark should be 150.0 (max of all tasks)
    assert fetcher.last_updated_watermark == 150.0

    # Second call should query with watermark - padding
    fetcher()
    expected_since = 150.0 - IncrementalTaskFetcher.INDEX_CONSISTENCY_PADDING
    task_store.get_tasks_updated_since.assert_called_with("test-job", expected_since)


def test_padding_applied_for_eventual_consistency():
    """Query should use watermark - padding to handle eventual consistency."""
    task_store = Mock()
    initial_tasks = [make_task("test-job.1", last_updated=1000.0)]
    task_store.get_tasks.return_value = initial_tasks
    task_store.get_tasks_updated_since.return_value = []

    fetcher = IncrementalTaskFetcher(task_store, "test-job", min_delay=0)
    fetcher()
    fetcher()

    # Should query with padding subtracted
    call_args = task_store.get_tasks_updated_since.call_args
    query_since = call_args[0][1]
    expected = 1000.0 - IncrementalTaskFetcher.INDEX_CONSISTENCY_PADDING
    assert query_since == expected


def test_full_refresh_after_max_time(monkeypatch):
    """Should do full fetch after MAX_TIME_UNTIL_FULL_REFRESH elapses."""
    task_store = Mock()
    initial_tasks = [make_task("test-job.1", last_updated=100.0)]
    task_store.get_tasks.return_value = initial_tasks
    task_store.get_tasks_updated_since.return_value = []

    fetcher = IncrementalTaskFetcher(task_store, "test-job", min_delay=0)

    # Mock time
    current_time = [1000.0]

    def mock_time():
        return current_time[0]

    monkeypatch.setattr(time, "time", mock_time)

    # First call - full fetch
    fetcher()
    assert task_store.get_tasks.call_count == 1

    # Second call shortly after - incremental
    current_time[0] = 1001.0
    fetcher()
    assert task_store.get_tasks.call_count == 1  # No additional full fetch
    assert task_store.get_tasks_updated_since.call_count == 1

    # Third call after MAX_TIME_UNTIL_FULL_REFRESH - should do full fetch
    current_time[0] = 1000.0 + IncrementalTaskFetcher.MAX_TIME_UNTIL_FULL_REFRESH + 1
    fetcher()
    assert task_store.get_tasks.call_count == 2  # Another full fetch


def test_handles_tasks_without_last_updated():
    """Should handle tasks that don't have last_updated set (legacy tasks)."""
    task_store = Mock()
    initial_tasks = [
        make_task("test-job.1", last_updated=None),
        make_task("test-job.2", last_updated=100.0),
    ]
    task_store.get_tasks.return_value = initial_tasks
    task_store.get_tasks_updated_since.return_value = []

    fetcher = IncrementalTaskFetcher(task_store, "test-job", min_delay=0)
    result = fetcher()

    assert len(result) == 2
    # Watermark should only consider tasks with last_updated
    assert fetcher.last_updated_watermark == 100.0


def test_watermark_updates_on_incremental_fetch():
    """Watermark should update when incremental fetch returns newer tasks."""
    task_store = Mock()
    initial_tasks = [make_task("test-job.1", last_updated=100.0)]
    task_store.get_tasks.return_value = initial_tasks

    # Incremental fetch returns task with newer timestamp
    updated_task = make_task("test-job.1", status=STATUS_COMPLETE, last_updated=500.0)
    task_store.get_tasks_updated_since.return_value = [updated_task]

    fetcher = IncrementalTaskFetcher(task_store, "test-job", min_delay=0)

    fetcher()
    assert fetcher.last_updated_watermark == 100.0

    fetcher()
    assert fetcher.last_updated_watermark == 500.0
