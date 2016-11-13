from kubeque.gcp import create_gcs_job_queue, IO

import uuid
import time
import json

def test_gs_io(tmpdir):
    prefix = "gs://gcs-test-1136/"+uuid.uuid4().hex
    io = IO("gcs-test-1136", prefix+"/cas")

    # create file which will be uploaded
    fn = str(tmpdir.join("sample"))
    with open(fn, "wt") as fd:
        fd.write("sample")

    # test put
    dest_url = prefix+"/sample"
    io.put(fn, dest_url)

    # test get_as_str
    assert io.get_as_str(dest_url) == "sample"

    # test get()
    dest_fn = str(tmpdir.join("sample_copied"))
    io.get(dest_url, dest_fn)
    with open(dest_fn, "rt") as fd:
        assert fd.read() == "sample"

    # test write_file_to_cas()
    cas_url = io.write_file_to_cas(fn)
    assert io.get_as_str(cas_url) == "sample"

    cas_url = io.write_str_to_cas("sample2")
    assert io.get_as_str(cas_url) == "sample2"

    cas_url = io.write_json_to_cas(dict(a=1))
    assert json.loads(io.get_as_str(cas_url)) == {"a": 1}

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
