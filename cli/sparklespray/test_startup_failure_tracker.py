from .startup_failure_tracker import StartupFailureTracker
from unittest.mock import MagicMock

def test_startup_failure_tracker_fails_at_right_time():
    next_id = 0
    node_names = []
    def add_completed(n):
        nonlocal next_id
        for i in range(n):
            node_names.append(f"n{next_id}")
            next_id +=1

    tasks = []
    sft = StartupFailureTracker(node_names)
    node_names.append("orig2")
    start = 100000

    sft.update(start, tasks, node_names)
    assert not sft.is_too_many_failures(start+11)

    add_completed(12)
    sft.update(start+10, tasks, node_names)
    assert not sft.is_too_many_failures(start+11)
    sft.update(start+30, tasks, node_names)
    assert not sft.is_too_many_failures(start+31)
    sft.update(start+40, tasks, node_names)
    assert sft.is_too_many_failures(start+41)

def test_startup_failure_tracker_only_looks_back_15_mins():
    next_id = 0
    node_names = []
    def add_completed(n):
        nonlocal next_id
        for i in range(n):
            node_names.append(f"n{next_id}")
            next_id +=1

    sft = StartupFailureTracker(node_names)
    start = 100000
    t = MagicMock()
    tasks = [t]

    sft.update(start, tasks, node_names)
    assert not sft.is_too_many_failures(start+11)

    add_completed(10)
    t.owner = "x/n1"
    sft.update(start+10, tasks, node_names)
    assert not sft.is_too_many_failures(start+11)
    sft.update(start+30, tasks, node_names)
    assert not sft.is_too_many_failures(start+31)
    sft.update(start+40, tasks, node_names)
    # no failure here because n1 was the owner of a task
    assert not sft.is_too_many_failures(start+41)

    # now these are going to all be bad
    add_completed(10)
    sft.update(start+16*60, tasks, node_names)
    assert not sft.is_too_many_failures(start+16*60+1)
    # no failure yet, because we're now in the grace period
    sft.update(start+16*60+40, tasks, node_names)
    assert sft.is_too_many_failures(start+16*60+41)
