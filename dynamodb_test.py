import time
import random
CLAIM_TIMEOUT = 30

from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, JSONAttribute
)

class Task(Model):
    class Meta:
        table_name = "Task"
        host = "http://localhost:8000"
    
    # will be of the form: job_id + task_index
    task_id = UnicodeAttribute(hash_key=True)

    task_index = NumberAttribute()
    job_id = UnicodeAttribute()
    status = UnicodeAttribute() # one of: pending, claimed, success, failed, lost
    owner = UnicodeAttribute(null=True)
    args = UnicodeAttribute()
    history = JSONAttribute() # list of records (timestamp, status)  (maybe include owner?) 
    version = NumberAttribute(default=1)

class Job(Model):
    class Meta:
        table_name = "Job"
        host = "http://localhost:8000"

    job_id = UnicodeAttribute(hash_key=True)
    tasks = JSONAttribute()

class JobQueue:
    def __init__(self):
        pass

    def _find_pending(self, job_id):
        print("_find_pending")
        tasks = Task.scan(status__eq = "pending", job_id__eq = job_id)
        return list(tasks)

    def submit(self, job_id, args):
        tasks = []
        now = time.time()
        with Task.batch_write() as batch:
            for i, arg in enumerate(args):
                task_id = "{}.{}".format(job_id, i)
                task = Task(task_id, task_index=i, job_id=job_id, status="pending", args=arg, history=[dict(timestamp=now, status="pending")])
                tasks.append(task)
                batch.save(task)

        job = Job(job_id, tasks=[t.task_id for t in tasks])
        job.save()


    def claim_task(self, job_id, new_owner):
        claim_start = time.time()
        while True:
            # fetch all pending with this job_id\n",
            tasks = self._find_pending(job_id)
            if len(tasks) == 0:
                return None

            now = time.time()
            if now - claim_start > CLAIM_TIMEOUT:
                raise Exception("Timeout attempting to claim task")

            task = random.choice(tasks)
            original_version = task.version
            task.version = original_version + 1
            task.owner = new_owner
            task.status = "claimed"
            task.history.append( dict(timestamp=now, status="claimed", owner=new_owner) )
            updated = task.save(version__eq = original_version)
            if updated is not None:
                return task.task_id, task.args

            # add exponential backoff?
            print("Update failed")
            time.sleep(random.uniform(0, 1))

    def task_completed(self, task_id, was_successful):
        if was_successful:
            new_status = "success"
        else:
            new_status = "failed"
        self._update_status(task_id, new_status)

    def _update_status(self, task_id, new_status):
        task = Task.get(task_id, consistent_read=True)
        now = time.time()
        original_version = task.version
        task.version += 1
        task.history.append( dict(timestamp=now, status=new_status) )
        task.status = new_status
        task.owner = None
        print("updating status. asserting version ==", original_version, "new", task.version)
        updated = task.save(version__eq = original_version)
        if updated is None:
            # I suppose this is not technically correct. Could be a simultaneous update of "success" or "failed" and "lost"
            raise Exception("Detected concurrent update, which should not be possible")

    def owner_lost(self, owner):
        tasks = Task.scan(owner == owner)
        for task in tasks:
            self._update_status(task_id, "lost")
    
def main():
    if not Job.exists():
        print("creating job")
        Job.create_table(wait=True, read_capacity_units=1, write_capacity_units=1)

    if not Task.exists():
        print("creating task")
        Task.create_table(wait=True, read_capacity_units=1, write_capacity_units=1)

    jq = JobQueue()

    args = ["a", "b", "c"]
    import uuid
    job_id = uuid.uuid4().hex
    print("job_id:", job_id)
    jq.submit(job_id, args)

    while True:
        claimed = jq.claim_task(job_id, "owner")
        print("claimed:", claimed)
        if claimed is None:
            break
        task_id, args = claimed
        print("task_id:", task_id, "args:", args)
        jq.task_completed(task_id, True)

    print ("done")   
    print("Jobs")
    for job in Job.scan():
        print(" ",job.dumps())
    print("Tasks")
    for task in Task.scan():
        print(" ", task.dumps())

if __name__ == "__main__":
    main()
