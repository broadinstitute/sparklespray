from kubeque.gcp import create_gcs_job_queue

import uuid
import time

def test_gcs_job_queue():
    task_count = 100

    queue = create_gcs_job_queue("gcs-test-1136")
    jobid = uuid.uuid4().hex
    queue.submit(jobid, ["cmd"] * task_count)
    time.sleep(2)
    status = queue.get_status_counts(jobid)
    print("first status", status)
    status = queue.get_status_counts(jobid)
    assert status["pending"] == task_count

    claimed = set()
    for i in range(task_count):
        t = queue.claim_task(jobid, "owner1")
        assert t is not None
        assert not t in claimed
        print("claimed", 1)
        claimed.add(t)

    assert queue.claim_task(jobid, "owner3") is None

    claimed_list = list(claimed)
    t1 = claimed_list[0]
    t2 = claimed_list[1]

    queue.task_completed(t1[0], True)
    queue.task_completed(t2[0], False)

    time.sleep(2)
    status = queue.get_status_counts(jobid)
    assert status["success"] == 1
    assert status["failed"] == 1
