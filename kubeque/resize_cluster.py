import time

from .node_req_store import REQUESTED_NODE_STATES, NODE_REQ_CLASS_PREEMPTIVE
from .task_store import INCOMPLETE_TASK_STATES

class ClusterState:
    def __init__(self, job_id : str, jq, datastore, cluster) -> None:
        self.job_id = job_id
        self.cluster = cluster
        self.datastore = datastore
        self.tasks = []
        self.node_reqs = []

    def update(self):
        # update tasks
        self.tasks = self.jq.get_tasks(self.job_id)

        # poll all the operations which are not marked as dead
        node_reqs = self.datastore.get_node_reqs(self.job_id)
        for node_req in node_reqs:
            if node_req.status in REQUESTED_NODE_STATES:
                op = self.cluster.get_node_req(node_req.operation_id)
                if op.status not in REQUESTED_NODE_STATES:
                    self.datastore.update_node_req_status(node_req.operation_id, op.status)

    def get_incomplete_task_count(self) -> int:
        return len([t for t in self.tasks if t.status in INCOMPLETE_TASK_STATES])

    def get_requested_node_count(self) -> int:
        return len([o for o in self.operations.values() if o.status in REQUESTED_NODE_STATES])

    def get_preempt_attempt_count(self) -> int:
        return len([o for o in self.operations.values() if o.node_class == NODE_REQ_CLASS_PREEMPTIVE ])

class ClusterMod:
    def __init__(self, job_id, cluster, state):
        self.job_id = job_id
        self.cluster = cluster

    def add_node(self, preemptable : bool) -> None:
        self.cluster.add_node(self.job_id, preemptable)

    def cancel_last_node(self) -> None:
        node_reqs = [x for x in self.state.node_reqs if x.status in REQUESTED_NODE_STATES]
        self.cluster.cancel_add(..)

class ResizeCluster:
    # adjust cluster size
    # Given a (target size, a restart-preempt budget, current number of outstanding operations, current number of pending tasks)
    # decide whether to add more add_node operations or remove add_node operations.
    def __init__(self, target_node_count : int, max_preemptable_attempts : int, seconds_between_modifications : int =60, get_time=time.time):
        self.target_node_count = target_node_count
        self.max_preemptable_attempts = max_preemptable_attempts

        self.last_modification = None
        self.seconds_between_modifications = seconds_between_modifications
        self.get_time = get_time

    def __call__(self, state : ClusterState, cluster_mod : ClusterMod) -> None:
        if self.last_modification is not None and (self.get_time() - self.last_modification) < self.seconds_between_modifications:
            return

        modified = False
        # cap our target by the number of tasks which have not finished
        target_node_count = min(self.target_node_count, state.get_incomplete_task_count())

        requested_nodes = state.get_requested_node_count()
        if target_node_count > requested_nodes:
            # Is our target higher than what we have now? Then add that many nodes
            remaining_preempt_attempts = self.max_preemptable_attempts - state.get_preempt_attempt_count()
            for i in range(target_node_count - requested_nodes):
                preemptable = remaining_preempt_attempts > 0
                if preemptable:
                    remaining_preempt_attempts -= 1

                cluster_mod.add_node(preemptable=preemptable)
                modified = True

        elif target_node_count < requested_nodes:
            # We have requested too many. Start cancelling
            for i in range(requested_nodes - target_node_count):
                cluster_mod.cancel_last_node()
                modified = True

        if modified:
            self.last_modification = self.get_time()


class MockClusterState:
    def __init__(self, incomplete_task_count, requested_node_count, preempt_attempt_count):
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

    def add_node(self, preemptable : bool) -> None:
        self.calls.append( ("add",  preemptable) )

    def cancel_last_node(self) -> None:
        self.calls.append( ("cancel",) )

def test_increase_size():
    r = ResizeCluster(target_node_count=2, max_preemptable_attempts=0, seconds_between_modifications=0)
    state = MockClusterState(incomplete_task_count=2, requested_node_count=0, preempt_attempt_count=0)
    mods = MockClusterMod()
    r(state, mods)
    assert mods.calls == [("add", False), ("add", False)]

def test_increase_size_capped_by_tasks():
    r = ResizeCluster(target_node_count=2, max_preemptable_attempts=0, seconds_between_modifications=0)
    state = MockClusterState(incomplete_task_count=1, requested_node_count=0, preempt_attempt_count=0)
    mods = MockClusterMod()
    r(state, mods)
    assert mods.calls == [("add", False)]

def test_add_some_preemptable():
    r = ResizeCluster(target_node_count=3, max_preemptable_attempts=2, seconds_between_modifications=0)
    state = MockClusterState(incomplete_task_count=3, requested_node_count=0, preempt_attempt_count=0)
    mods = MockClusterMod()
    r(state, mods)
    assert mods.calls == [("add", True), ("add", True), ("add", False)]

def test_decrease_size():
    r = ResizeCluster(target_node_count=2, max_preemptable_attempts=0, seconds_between_modifications=0)
    state = MockClusterState(incomplete_task_count=2, requested_node_count=3, preempt_attempt_count=0)
    mods = MockClusterMod()
    r(state, mods)
    assert mods.calls == [("cancel",)]

def test_no_change():
    r = ResizeCluster(target_node_count=20, max_preemptable_attempts=0, seconds_between_modifications=0)
    state = MockClusterState(incomplete_task_count=2, requested_node_count=2, preempt_attempt_count=0)
    mods = MockClusterMod()
    r(state, mods)
    assert mods.calls == []

def test_frequent_calls():
    r = ResizeCluster(target_node_count=2, max_preemptable_attempts=0, seconds_between_modifications=10)
    state = MockClusterState(incomplete_task_count=1, requested_node_count=0, preempt_attempt_count=0)
    mods = MockClusterMod()
    r(state, mods)
    assert mods.calls == [("add", False)]
    mods.calls = []
    r(state, mods)
    assert mods.calls == []
