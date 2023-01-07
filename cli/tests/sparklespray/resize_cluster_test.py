from sparklespray.resize_cluster import ResizeCluster


class MockClusterState:
    def __init__(
        self, incomplete_task_count, requested_node_count, preempt_attempt_count
    ):
        self.incomplete_task_count = incomplete_task_count
        self.requested_node_count = requested_node_count
        self.preempt_attempt_count = preempt_attempt_count

    def get_incomplete_task_count(self) -> int:
        return self.incomplete_task_count

    def get_requested_node_count(self) -> int:
        return self.requested_node_count

    def get_preempt_attempt_count(self) -> int:
        return self.preempt_attempt_count


class MockClusterMod:
    def __init__(self):
        self.calls = []

    def add_node(self, preemptable: bool) -> None:
        self.calls.append(("add", preemptable))

    #    def cancel_last_node(self) -> None:
    #        self.calls.append(("cancel",))

    def cancel_nodes(self, state, needs_cancel):
        self.calls.append(("cancel",))


def test_increase_size():
    r = ResizeCluster(
        target_node_count=2, max_preemptable_attempts=0, seconds_between_modifications=0
    )
    state = MockClusterState(
        incomplete_task_count=2, requested_node_count=0, preempt_attempt_count=0
    )
    mods = MockClusterMod()
    r(state, mods)
    assert mods.calls == [("add", False), ("add", False)]


def test_increase_size_capped_by_tasks():
    r = ResizeCluster(
        target_node_count=2, max_preemptable_attempts=0, seconds_between_modifications=0
    )
    state = MockClusterState(
        incomplete_task_count=1, requested_node_count=0, preempt_attempt_count=0
    )
    mods = MockClusterMod()
    r(state, mods)
    assert mods.calls == [("add", False)]


def test_add_some_preemptable():
    r = ResizeCluster(
        target_node_count=3, max_preemptable_attempts=2, seconds_between_modifications=0
    )
    state = MockClusterState(
        incomplete_task_count=3, requested_node_count=0, preempt_attempt_count=0
    )
    mods = MockClusterMod()
    r(state, mods)
    assert mods.calls == [("add", True), ("add", True), ("add", False)]


def test_decrease_size():
    r = ResizeCluster(
        target_node_count=2, max_preemptable_attempts=0, seconds_between_modifications=0
    )
    state = MockClusterState(
        incomplete_task_count=2, requested_node_count=3, preempt_attempt_count=0
    )
    mods = MockClusterMod()
    r(state, mods)
    assert mods.calls == [("cancel",)]


def test_no_change():
    r = ResizeCluster(
        target_node_count=20,
        max_preemptable_attempts=0,
        seconds_between_modifications=0,
    )
    state = MockClusterState(
        incomplete_task_count=2, requested_node_count=2, preempt_attempt_count=0
    )
    mods = MockClusterMod()
    r(state, mods)
    assert mods.calls == []


def test_frequent_calls():
    r = ResizeCluster(
        target_node_count=2,
        max_preemptable_attempts=0,
        seconds_between_modifications=10,
    )
    state = MockClusterState(
        incomplete_task_count=1, requested_node_count=0, preempt_attempt_count=0
    )
    mods = MockClusterMod()
    r(state, mods)
    assert mods.calls == [("add", False)]
    mods.calls = []
    r(state, mods)
    assert mods.calls == []
