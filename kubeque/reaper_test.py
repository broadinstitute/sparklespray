from kubeque.reaper import reconcile, ClaimedTask, PodStatus, POD_STATUS_DEAD, POD_STATUS_ACTIVE, POD_STATUS_OOM, POD_STATUS_OTHER

def mk_call_recorder(name):
    recorded = []
    def callback(*args):
        print("Called {}: {}".format(name, args))
        recorded.append(args)
    callback.recorded = recorded
    return callback

def test_too_many_tasks():
    claimed_tasks = [ClaimedTask("t1", "pod1"), ClaimedTask("t2", "pod1")]
    pods = [PodStatus("pod1", POD_STATUS_ACTIVE, {})]
    fail_task_callback = mk_call_recorder("fail_task")
    warn_callback = mk_call_recorder("warn")
    reconcile(claimed_tasks, pods, fail_task_callback, warn_callback)
    assert len(warn_callback.recorded) == 1
    assert len(fail_task_callback.recorded) == 0

def test_unused_active_pod():
    claimed_tasks = []
    pods = [PodStatus("pod1", POD_STATUS_ACTIVE, {})]
    fail_task_callback = mk_call_recorder("fail_task")
    warn_callback = mk_call_recorder("warn")
    reconcile(claimed_tasks, pods, fail_task_callback, warn_callback)
    assert len(warn_callback.recorded) == 1
    assert len(fail_task_callback.recorded) == 0

def test_unused_nondead_pod():
    claimed_tasks = []
    pods = [PodStatus("pod1", POD_STATUS_OTHER, {})]
    fail_task_callback = mk_call_recorder("fail_task")
    warn_callback = mk_call_recorder("warn")
    reconcile(claimed_tasks, pods, fail_task_callback, warn_callback)
    assert len(warn_callback.recorded) == 1
    assert len(fail_task_callback.recorded) == 0

def test_missing_pod():
    claimed_tasks = [ClaimedTask("t1", "pod1")]
    pods = []
    fail_task_callback = mk_call_recorder("fail_task")
    warn_callback = mk_call_recorder("warn")
    reconcile(claimed_tasks, pods, fail_task_callback, warn_callback)
    assert len(warn_callback.recorded) == 0
    assert len(fail_task_callback.recorded) == 1

def test_dead_pod():
    claimed_tasks = [ClaimedTask("t1", "pod1")]
    pods = [PodStatus("pod1", POD_STATUS_DEAD, {})]
    fail_task_callback = mk_call_recorder("fail_task")
    warn_callback = mk_call_recorder("warn")
    reconcile(claimed_tasks, pods, fail_task_callback, warn_callback)
    assert len(warn_callback.recorded) == 0
    assert len(fail_task_callback.recorded) == 1

def test_oom_pod():
    claimed_tasks = [ClaimedTask("t1", "pod1")]
    pods = [PodStatus("pod1", POD_STATUS_OOM, {})]
    fail_task_callback = mk_call_recorder("fail_task")
    warn_callback = mk_call_recorder("warn")
    reconcile(claimed_tasks, pods, fail_task_callback, warn_callback)
    assert len(warn_callback.recorded) == 0
    assert len(fail_task_callback.recorded) == 1

def test_all_good():
    claimed_tasks = [ClaimedTask("t1", "pod1")]
    pods = [PodStatus("pod1", POD_STATUS_ACTIVE, {})]
    fail_task_callback = mk_call_recorder("fail_task")
    warn_callback = mk_call_recorder("warn")
    reconcile(claimed_tasks, pods, fail_task_callback, warn_callback)
    assert len(warn_callback.recorded) == 0
    assert len(fail_task_callback.recorded) == 0
