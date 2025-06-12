from sparklespray.watch.resize import ResizeCluster
from sparklespray.watch.runner_types import ClusterStateQuery
from unittest.mock import Mock


class MockCluster:
    def __init__(self):
        self.add_nodes_calls = []
    
    def add_nodes(self, count, max_retry, preemptable):
        self.add_nodes_calls.append({
            'count': count,
            'max_retry': max_retry,
            'preemptable': preemptable
        })


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
    tasks = [MockTask("pending"), MockTask("running")]
    nodes = [MockNode("running"), MockNode("running")]
    state = create_mock_state(tasks, nodes)
    
    result = resizer.poll(state)
    
    assert len(cluster.add_nodes_calls) == 0
    assert result.seconds == 60  # NextPoll with default seconds_between_modifications


def test_resize_cluster_adds_preemptable_nodes():
    """Test adding preemptable nodes when under target"""
    cluster = MockCluster()
    resizer = ResizeCluster(cluster, target_node_count=5, max_preemptable_attempts=3)
    
    # 5 incomplete tasks, 2 active nodes - need 3 more, all can be preemptable
    tasks = [MockTask("pending")] * 5
    nodes = [MockNode("running"), MockNode("running")]
    state = create_mock_state(tasks, nodes)
    
    result = resizer.poll(state)
    
    assert len(cluster.add_nodes_calls) == 1
    assert cluster.add_nodes_calls[0] == {
        'count': 3,
        'max_retry': 3,
        'preemptable': True
    }
    assert result is None  # Returns None when modification made


def test_resize_cluster_adds_mixed_nodes():
    """Test adding both preemptable and non-preemptable nodes"""
    cluster = MockCluster()
    resizer = ResizeCluster(cluster, target_node_count=10, max_preemptable_attempts=2)
    
    # 10 incomplete tasks, 3 active nodes - need 7 more, only 2 can be preemptable
    tasks = [MockTask("pending")] * 10
    nodes = [MockNode("running")] * 3
    state = create_mock_state(tasks, nodes)
    
    result = resizer.poll(state)
    
    assert len(cluster.add_nodes_calls) == 2
    # First call should be for preemptable nodes
    assert cluster.add_nodes_calls[0] == {
        'count': 2,
        'max_retry': 3,
        'preemptable': True
    }
    # Second call should be for non-preemptable nodes
    assert cluster.add_nodes_calls[1] == {
        'count': 5,
        'max_retry': 3,
        'preemptable': False
    }
    assert result is None


def test_resize_cluster_caps_by_incomplete_tasks():
    """Test that target is capped by number of incomplete tasks"""
    cluster = MockCluster()
    resizer = ResizeCluster(cluster, target_node_count=10, max_preemptable_attempts=5)
    
    # Only 3 incomplete tasks, so target should be capped at 3
    tasks = [MockTask("pending"), MockTask("running"), MockTask("failed")]
    nodes = [MockNode("running")]  # 1 active node
    state = create_mock_state(tasks, nodes)
    
    result = resizer.poll(state)
    
    assert len(cluster.add_nodes_calls) == 1
    assert cluster.add_nodes_calls[0]['count'] == 2  # Only need 2 more to reach 3 total
    assert result is None


def test_resize_cluster_exhausted_preemptable_budget():
    """Test when preemptable budget is exhausted"""
    cluster = MockCluster()
    resizer = ResizeCluster(cluster, target_node_count=5, max_preemptable_attempts=2)
    
    # Simulate that we've already created 2 preemptable nodes
    resizer.preemptable_created = 2
    
    # 5 incomplete tasks, 2 active nodes - need 3 more, but no preemptable budget left
    tasks = [MockTask("pending")] * 5
    nodes = [MockNode("running"), MockNode("running")]
    state = create_mock_state(tasks, nodes)
    
    result = resizer.poll(state)
    
    assert len(cluster.add_nodes_calls) == 1
    assert cluster.add_nodes_calls[0] == {
        'count': 3,
        'max_retry': 3,
        'preemptable': False
    }
    assert result is None


def test_resize_cluster_partial_preemptable_budget():
    """Test when only part of preemptable budget remains"""
    cluster = MockCluster()
    resizer = ResizeCluster(cluster, target_node_count=6, max_preemptable_attempts=3)
    
    # Simulate that we've already created 1 preemptable node
    resizer.preemptable_created = 1
    
    # 6 incomplete tasks, 1 active node - need 5 more, only 2 can be preemptable
    tasks = [MockTask("pending")] * 6
    nodes = [MockNode("running")]
    state = create_mock_state(tasks, nodes)
    
    result = resizer.poll(state)
    
    assert len(cluster.add_nodes_calls) == 2
    # 2 preemptable (budget allows 3-1=2 more)
    assert cluster.add_nodes_calls[0] == {
        'count': 2,
        'max_retry': 3,
        'preemptable': True
    }
    # 3 non-preemptable (remaining 5-2=3)
    assert cluster.add_nodes_calls[1] == {
        'count': 3,
        'max_retry': 3,
        'preemptable': False
    }
    assert resizer.preemptable_created == 3  # Should be updated
    assert result is None


def test_resize_cluster_custom_seconds_between_modifications():
    """Test custom seconds_between_modifications parameter"""
    cluster = MockCluster()
    resizer = ResizeCluster(
        cluster, 
        target_node_count=2, 
        max_preemptable_attempts=5,
        seconds_between_modifications=120
    )
    
    # No resize needed scenario
    tasks = [MockTask("pending"), MockTask("running")]
    nodes = [MockNode("running"), MockNode("running")]
    state = create_mock_state(tasks, nodes)
    
    result = resizer.poll(state)
    
    assert result.seconds == 120
