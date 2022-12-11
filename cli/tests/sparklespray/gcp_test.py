# from sparklespray.gcp import create_gcs_job_queue, IO

# import uuid
# import time
# import json

# def test_gs_io(tmpdir):
#     prefix = "gs://gcs-test-1136/"+uuid.uuid4().hex
#     io = IO("gcs-test-1136", prefix+"/cas")

#     # create file which will be uploaded
#     fn = str(tmpdir.join("sample"))
#     with open(fn, "wt") as fd:
#         fd.write("sample")

#     # test put
#     dest_url = prefix+"/sample"
#     io.put(fn, dest_url)

#     # test get_as_str
#     assert io.get_as_str(dest_url) == "sample"

#     # test get()
#     dest_fn = str(tmpdir.join("sample_copied"))
#     io.get(dest_url, dest_fn)
#     with open(dest_fn, "rt") as fd:
#         assert fd.read() == "sample"

#     # test write_file_to_cas()
#     cas_url = io.write_file_to_cas(fn)
#     assert io.get_as_str(cas_url) == "sample"

#     cas_url = io.write_str_to_cas("sample2")
#     assert io.get_as_str(cas_url) == "sample2"

#     cas_url = io.write_json_to_cas(dict(a=1))
#     assert json.loads(io.get_as_str(cas_url)) == {"a": 1}

# def test_gcs_job_queue():
#     task_count = 100

#     queue = create_gcs_job_queue("gcs-test-1136")
#     jobid = uuid.uuid4().hex
#     queue.submit(jobid, ["cmd"] * task_count)
#     time.sleep(2)
#     status = queue.get_status_counts(jobid)
#     print("first status", status)
#     status = queue.get_status_counts(jobid)
#     assert status["pending"] == task_count

#     claimed = set()
#     for i in range(task_count):
#         t = queue.claim_task(jobid, "owner1")
#         assert t is not None
#         assert not t in claimed
#         print("claimed", 1)
#         claimed.add(t)

#     assert queue.claim_task(jobid, "owner3") is None

#     claimed_list = list(claimed)
#     t1 = claimed_list[0]
#     t2 = claimed_list[1]

#     queue.task_completed(t1[0], True)
#     queue.task_completed(t2[0], False)

#     time.sleep(2)
#     status = queue.get_status_counts(jobid)
#     assert status["success"] == 1
#     assert status["failed"] == 1

# import threading


# class CountDownLatch(object):
#     def __init__(self, count=1):
#         self.count = count
#         self.lock = threading.Condition()

#     def count_down(self):
#         self.lock.acquire()
#         self.count -= 1
#         if self.count <= 0:
#             self.lock.notifyAll()
#         self.lock.release()

#     def await(self):
#         self.lock.acquire()
#         while self.count > 0:
#             self.lock.wait()
#         self.lock.release()

# def _test_atomic_updates():
#     # This appears to fail due to threading issue https://github.com/GoogleCloudPlatform/google-cloud-python/issues/1214
#     queue1 = create_gcs_job_queue("gcs-test-1136")
#     queue2 = create_gcs_job_queue("gcs-test-1136")
#     jobid = uuid.uuid4().hex
#     queue1.submit(jobid, ["cmd"] )
#     time.sleep(2)
#     tasks = queue1.get_tasks(jobid)
#     task_id = tasks[0].task_id

#     countdown = CountDownLatch(2)
#     semaphore = threading.Semaphore()

#     def mutate_task_callback_1(task):
#         task.failure_reason="update 1"
#         semaphore.release()
#         countdown.count_down()
#         return True

#     def mutate_task_callback_2(task):
#         task.owner="update 2"
#         countdown.count_down()
#         return True

#     def run_update_update(storage, callback):
#         storage = queue1.storage
#         storage.atomic_task_update(task_id, callback)

#     t1 = threading.Thread(target=run_update_update, args=(queue1.storage, mutate_task_callback_1,))
#     t2 = threading.Thread(target=run_update_update, args=(queue2.storage, mutate_task_callback_2,))
#     semaphore.acquire()
#     t1.start()
#     t2.start()
#     t1.join()
#     t2.join()

#     time.sleep(5)

#     task = queue.storage.get_task(task_id)
#     assert task.owner == "update 2"
#     assert task.failure_reason == "update 1"
