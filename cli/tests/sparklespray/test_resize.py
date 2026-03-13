from sparklespray.watch.resize import ResizeCluster, NextPoll
from sparklespray.watch.runner_types import ClusterStateQuery
from sparklespray.task_store import STATUS_PENDING, STATUS_CLAIMED, STATUS_FAILED
from sparklespray.node_req_store import NODE_REQ_RUNNING
from unittest.mock import Mock
from pytest import raises


class MockCluster:
    def __init__(self):
        self.add_nodes_calls = []

    def add_nodes(self, count: int, max_retry_count: int, preemptible: bool):
        self.add_nodes_calls.append(
            {"count": count, "max_retry": max_retry_count, "preemptable": preemptible}
        )
        return f"add-{len(self.add_nodes_calls)}"

    def reset(self):
        self.add_nodes_calls = []


class MockTask:
    def __init__(self, status):
        self.status = status


class MockNode:
    def __init__(self, status):
        self.status = status


def create_mock_state(tasks, nodes, timestamp=1000.0):
    state = Mock(spec=ClusterStateQuery)
    state.get_tasks.return_value = tasks
    state.get_nodes.return_value = nodes
    state.get_time.return_value = timestamp
    return state


def test_resize_cluster_no_additional_nodes_needed():
    """Test when we already have enough nodes"""
    cluster = MockCluster()
    resizer = ResizeCluster(cluster, target_node_count=2, max_preemptable_attempts=5)

    # 2 incomplete tasks, 2 active nodes - no resize needed
    tasks = [MockTask(STATUS_PENDING), MockTask(STATUS_CLAIMED)]
    nodes = [MockNode(NODE_REQ_RUNNING), MockNode(NODE_REQ_RUNNING)]
    state = create_mock_state(tasks, nodes)

    result = resizer.poll(state)

    assert len(cluster.add_nodes_calls) == 0
    assert isinstance(result, NextPoll)


def test_resize_cluster_adds_preemptable_nodes():
    """Test adding preemptable nodes when under target"""
    cluster = MockCluster()
    resizer = ResizeCluster(cluster, target_node_count=5, max_preemptable_attempts=3)

    # 5 incomplete tasks, 2 active nodes - need 3 more, all can be preemptable
    tasks = [MockTask(STATUS_PENDING)] * 5
    nodes = [MockNode(NODE_REQ_RUNNING), MockNode(NODE_REQ_RUNNING)]
    state = create_mock_state(tasks, nodes)

    result = resizer.poll(state)

    assert len(cluster.add_nodes_calls) == 1
    assert cluster.add_nodes_calls[0] == {
        "count": 3,
        "max_retry": 3,
        "preemptable": True,
    }
    assert isinstance(result, NextPoll)


def test_resize_cluster_adds_mixed_nodes():
    """Test adding both preemptable and non-preemptable nodes"""
    cluster = MockCluster()
    resizer = ResizeCluster(cluster, target_node_count=10, max_preemptable_attempts=2)

    # 10 incomplete tasks, 3 active nodes - need 7 more, only 2 can be preemptable
    tasks = [MockTask(STATUS_PENDING)] * 10
    nodes = [MockNode(NODE_REQ_RUNNING)] * 3
    state = create_mock_state(tasks, nodes)

    result = resizer.poll(state)

    assert len(cluster.add_nodes_calls) == 2
    # First call should be for preemptable nodes
    assert cluster.add_nodes_calls[0] == {
        "count": 2,
        "max_retry": 3,
        "preemptable": True,
    }
    # Second call should be for non-preemptable nodes
    assert cluster.add_nodes_calls[1] == {
        "count": 5,
        "max_retry": 3,
        "preemptable": False,
    }
    assert isinstance(result, NextPoll)


def test_resize_cluster_caps_by_incomplete_tasks():
    """Test that target is capped by number of incomplete tasks"""
    cluster = MockCluster()
    resizer = ResizeCluster(cluster, target_node_count=10, max_preemptable_attempts=5)

    # Only 3 incomplete tasks, so target should be capped at 3
    # Open question: Should this really be capped at the number of incomplete tasks, or maybe we should cap the number of nodes at the number
    # of pending tasks?
    tasks = [
        MockTask(STATUS_PENDING),
        MockTask(STATUS_CLAIMED),
        MockTask(STATUS_FAILED),
    ]
    nodes = [MockNode(NODE_REQ_RUNNING)]  # 1 active node
    state = create_mock_state(tasks, nodes)

    result = resizer.poll(state)

    assert len(cluster.add_nodes_calls) == 1
    assert cluster.add_nodes_calls[0]["count"] == 1  # Only need 2 more to reach 3 total
    assert isinstance(result, NextPoll)


def test_resize_cluster_exhausted_preemptable_budget():
    """Test when preemptable budget is exhausted and also verify that we don't create non-preemptable instances forever"""
    cluster = MockCluster()
    resizer = ResizeCluster(cluster, target_node_count=5, max_preemptable_attempts=2)

    # 5 incomplete tasks, 3 active nodes - should request two preemptable nodes, which will exhaust our budget
    tasks = ([MockTask(STATUS_PENDING)] * 2) + ([MockTask(STATUS_CLAIMED)] * 3)
    nodes = [MockNode(NODE_REQ_RUNNING)] * 3
    state = create_mock_state(tasks, nodes)
    result = resizer.poll(state)
    assert isinstance(result, NextPoll)
    assert len(cluster.add_nodes_calls) == 1
    assert cluster.add_nodes_calls[0] == {
        "count": 2,
        "max_retry": 3,
        "preemptable": True,
    }

    # now, let's imagine those 3 running machines died. The two tasks from before were claimed by the preemptable machines. The
    # other three which were running got reset to "pending". Now, we should request 3 new non-preemptable machines to replace those
    # that died
    tasks = ([MockTask(STATUS_CLAIMED)] * 2) + ([MockTask(STATUS_PENDING)] * 3)
    nodes = [MockNode(NODE_REQ_RUNNING)] * 2
    cluster.reset()
    state = create_mock_state(tasks, nodes)
    result = resizer.poll(state)
    assert isinstance(result, NextPoll)
    assert len(cluster.add_nodes_calls) == 1
    assert cluster.add_nodes_calls[0] == {
        "count": 3,
        "max_retry": 3,
        "preemptable": False,
    }

    # lastly, what if all of those machines die again? Three were non-preemptable so there's probably a problem. Make sure that we
    # abort instead of simply creating even more instances.
    tasks = ([MockTask(STATUS_CLAIMED)] * 2) + ([MockTask(STATUS_PENDING)] * 3)
    nodes = []
    cluster.reset()
    state = create_mock_state(tasks, nodes)
    with raises(AssertionError):
        result = resizer.poll(state)
        result = resizer.poll(state)


def test_resize_cluster_partial_preemptable_budget():
    """Test when only part of preemptable budget remains"""
    cluster = MockCluster()
    resizer = ResizeCluster(cluster, target_node_count=6, max_preemptable_attempts=3)

    # Simulate that we've already created 1 preemptable node
    resizer.preemptable_created = 1

    # 6 incomplete tasks, 1 active node - need 5 more, only 2 can be preemptable
    tasks = [MockTask(STATUS_PENDING)] * 6
    nodes = [MockNode(NODE_REQ_RUNNING)]
    state = create_mock_state(tasks, nodes)

    result = resizer.poll(state)

    assert len(cluster.add_nodes_calls) == 2
    # 2 preemptable (budget allows 3-1=2 more)
    assert cluster.add_nodes_calls[0] == {
        "count": 2,
        "max_retry": 3,
        "preemptable": True,
    }
    # 3 non-preemptable (remaining 5-2=3)
    assert cluster.add_nodes_calls[1] == {
        "count": 3,
        "max_retry": 3,
        "preemptable": False,
    }
    assert resizer.preemptable_created == 3  # Should be updated
    assert isinstance(result, NextPoll)
