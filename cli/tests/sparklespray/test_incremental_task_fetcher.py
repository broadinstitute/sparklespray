from typing import Optional
from queue import Queue

from sparklespray.watch.runner_types import IncrementalTaskFetcher
from sparklespray.task_store import Task, STATUS_PENDING, STATUS_COMPLETE
from unittest.mock import Mock
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
        owned_by_worker_id=None,
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
    assert len(result) == 1
    assert result[0].task_id == "test-job.1"


def test_subsequent_calls_with_queue_fetch_by_id():
    """After first call, should fetch specific tasks from the queue by ID."""
    task_store = Mock()
    initial_tasks = [
        make_task("test-job.1", last_updated=100.0),
        make_task("test-job.2", last_updated=100.0),
    ]
    task_store.get_tasks.return_value = initial_tasks

    # Create a queue and add a task ID
    queue = Queue()
    fetcher = IncrementalTaskFetcher(task_store, "test-job", queue, min_delay=0)

    # First call - full fetch
    fetcher()
    assert task_store.get_tasks.call_count == 1

    # Add a changed task ID to the queue
    queue.put("test-job.1")

    # Mock get_task to return updated task
    updated_task = make_task("test-job.1", status=STATUS_COMPLETE)
    task_store.get_task.return_value = updated_task

    # Second call should fetch the specific task by ID
    result = fetcher()
    task_store.get_task.assert_called_once_with("test-job.1")
    assert task_store.get_tasks.call_count == 1  # No additional full fetch
    assert len(result) == 2


def test_incremental_fetch_merges_changed_tasks():
    """Changed tasks from queue should be merged into the cache."""
    task_store = Mock()
    initial_tasks = [
        make_task("test-job.1", status=STATUS_PENDING, last_updated=100.0),
        make_task("test-job.2", status=STATUS_PENDING, last_updated=100.0),
    ]
    task_store.get_tasks.return_value = initial_tasks

    queue = Queue()
    fetcher = IncrementalTaskFetcher(task_store, "test-job", queue, min_delay=0)

    # First call
    fetcher()

    # Add changed task ID to queue
    queue.put("test-job.1")

    # Task 1 has been updated to complete
    updated_task = make_task("test-job.1", status=STATUS_COMPLETE, last_updated=200.0)
    task_store.get_task.return_value = updated_task

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


def test_full_refresh_after_max_time(monkeypatch):
    """Should do full fetch after MAX_TIME_UNTIL_FULL_REFRESH elapses."""
    task_store = Mock()
    initial_tasks = [make_task("test-job.1", last_updated=100.0)]
    task_store.get_tasks.return_value = initial_tasks

    queue = Queue()
    fetcher = IncrementalTaskFetcher(task_store, "test-job", queue, min_delay=0)

    # Mock time
    current_time = [1000.0]

    def mock_time():
        return current_time[0]

    monkeypatch.setattr(time, "time", mock_time)

    # First call - full fetch
    fetcher()
    assert task_store.get_tasks.call_count == 1

    # Second call shortly after with task in queue - incremental
    current_time[0] = 1001.0
    queue.put("test-job.1")
    task_store.get_task.return_value = make_task("test-job.1")
    fetcher()
    assert task_store.get_tasks.call_count == 1  # No additional full fetch
    assert task_store.get_task.call_count == 1

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

    fetcher = IncrementalTaskFetcher(task_store, "test-job", min_delay=0)
    result = fetcher()

    assert len(result) == 2


def test_queue_drains_all_pending_task_ids():
    """Queue should be fully drained and all tasks fetched."""
    task_store = Mock()
    initial_tasks = [
        make_task("test-job.1"),
        make_task("test-job.2"),
        make_task("test-job.3"),
    ]
    task_store.get_tasks.return_value = initial_tasks

    queue = Queue()
    fetcher = IncrementalTaskFetcher(task_store, "test-job", queue, min_delay=0)

    # First call - full fetch
    fetcher()

    # Add multiple task IDs to queue
    queue.put("test-job.1")
    queue.put("test-job.2")
    queue.put("test-job.1")  # Duplicate - should only fetch once

    task_store.get_task.side_effect = [
        make_task("test-job.1", status=STATUS_COMPLETE),
        make_task("test-job.2", status=STATUS_COMPLETE),
    ]

    # Second call - should fetch both tasks (deduped)
    result = fetcher()

    # Should have called get_task twice (deduplicated)
    assert task_store.get_task.call_count == 2
    assert queue.empty()


def test_no_queue_returns_cached_without_incremental():
    """Without a queue, subsequent calls just return cached results."""
    task_store = Mock()
    initial_tasks = [make_task("test-job.1")]
    task_store.get_tasks.return_value = initial_tasks

    # No queue provided
    fetcher = IncrementalTaskFetcher(task_store, "test-job", None, min_delay=0)

    # First call - full fetch
    fetcher()
    assert task_store.get_tasks.call_count == 1

    # Second call - no queue, so just returns cached
    result = fetcher()
    assert task_store.get_tasks.call_count == 1  # No additional fetch
    assert len(result) == 1


def test_full_fetch_clears_queue():
    """Full fetch should drain the queue without processing individual items."""
    task_store = Mock()
    initial_tasks = [make_task("test-job.1")]
    task_store.get_tasks.return_value = initial_tasks

    queue = Queue()
    fetcher = IncrementalTaskFetcher(task_store, "test-job", queue, min_delay=0)

    # Add items to queue before first fetch
    queue.put("test-job.1")
    queue.put("test-job.2")

    # First call - full fetch should clear queue
    fetcher()
    assert queue.empty()
    # get_task should not have been called since we did a full fetch
    task_store.get_task.assert_not_called()
