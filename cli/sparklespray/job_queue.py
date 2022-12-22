# Authorize server-to-server interactions from Google Compute Engine.
from google.cloud import datastore
import google.cloud.exceptions
import logging

from google.cloud.storage.client import Client as GSClient
import os
import re
import hashlib
import json
from .task_store import (
    STATUS_CLAIMED,
    STATUS_FAILED,
    STATUS_COMPLETE,
    STATUS_KILLED,
    STATUS_PENDING,
    INCOMPLETE_TASK_STATES,
)
from .job_store import JobStore, Job, JOB_STATUS_SUBMITTED, JOB_STATUS_KILLED
from .task_store import TaskStore, TaskHistory, Task

from fnmatch import fnmatch
from .datastore_batch import ImmediateBatch, Batch

from contextlib import contextmanager
import collections

import time
from collections import namedtuple
import sys
from typing import Dict, List
CLAIM_TIMEOUT = 5

from .log import log


def get_credentials(account, cred_file="~/.config/gcloud/credentials"):
    return None
    # cred_file = os.path.expanduser(cred_file)
    # with open(cred_file, "rt") as fd:
    #     all_credentials = json.load(fd)
    #
    #     client_credentials = None
    # for c in all_credentials["data"]:
    #     if c["key"]["type"] == "google-cloud-sdk" and c["key"]["account"] == account:
    #         client_credentials = c["credential"]
    #
    # assert client_credentials is not None, "Could not find credentials for {} in {}".format(account, cred_file)
    #
    # return GoogleCredentials(
    #     access_token=None,
    #     client_id=client_credentials['client_id'],
    #     client_secret=client_credentials['client_secret'],
    #     refresh_token=client_credentials['refresh_token'],
    #     token_expiry=None,
    #     token_uri=oauth2client.GOOGLE_TOKEN_URI,
    #     user_agent='Python client library')


class JobQueue:
    def __init__(
        self, client: datastore.Client, job_storage: JobStore, task_storage: TaskStore
    ):
        self.job_storage = job_storage
        self.task_storage = task_storage
        self.client = client

    #     def add_node(self, job_id, cluster, preemptible, debug_log_url, job=None):
    #         return self.storage.add_node(job_id, cluster, preemptible, job, debug_log_url)

    #     def get_node_reqs(self, job_id, status=None):
    #         return self.storage.get_node_reqs(job_id, status=status)

    #     def update_node_reqs(self, job_id, cluster):
    #         return self.storage.update_node_reqs(job_id, cluster)

    #     def get_pending_node_req_count(self, job_id):
    #         return self.storage.get_pending_node_req_count(job_id)

    def get_tasks_for_cluster(self, cluster_name, status, max_fetch=None):
        return self.task_storage.get_tasks_for_cluster(cluster_name, status, max_fetch)

    #     def get_claimed_task_ids(self):
    #         tasks = self.storage.get_tasks(status=STATUS_CLAIMED)
    #         tasks = [t for t in tasks if t.status == STATUS_CLAIMED]
    #         for t in tasks:
    #             assert t.owner is not None
    #         return [(t.task_id, t.owner) for t in tasks]

    #     def get_tasks(self, job_id, status=None):
    #         return self.storage.get_tasks(job_id, status=status)

    def get_job(self, job_id, must=True):
        return self.job_storage.get_job(job_id, must=must)

    def get_last_job(self):
        return self.job_storage.get_last_job()

    def get_jobids(self, job_id_wildcard="*"):
        job_ids = self.job_storage.get_job_ids()
        return [job_id for job_id in job_ids if fnmatch(job_id, job_id_wildcard)]

    #     def get_kube_job_spec(self, job_id):
    #         job = self.storage.get_job(job_id)
    #         return job.kube_job_spec

    # def delete_job(self, job_id):
    #     self.job_storage.delete(job_id)

    def kill_job(self, job_id):
        def mark_killed(job):
            job.status = JOB_STATUS_KILLED
            return True

        return self.job_storage.update_job(job_id, mark_killed)

    def get_status_counts(self, job_id):
        counts = collections.defaultdict(lambda: 0)
        for task in self.task_storage.get_tasks(job_id):
            counts[task.status] += 1
        return dict(counts)

    def reset(
        self,
        jobid: str,
        owner,
        statuses_to_clear=[STATUS_CLAIMED, STATUS_FAILED],
        clear_nonzero_exit=True,
    ):
        tasks = self.task_storage.get_tasks(jobid)
        # for status_to_clear in statuses_to_clear:
        #     tasks.extend(self.task_storage.get_tasks(jobid, status=status_to_clear))

        def needs_clear(task):
            if task.status in statuses_to_clear:
                return True

            if (
                clear_nonzero_exit
                and task.status == STATUS_COMPLETE
                and int(task.exit_code) != 0
            ):
                return True

            return False

        tasks = [t for t in tasks if needs_clear(t)]
        # print("clear", tasks)
        # raise Exception()

        updated = 0
        batch = Batch(self.client)
        for task in tasks:
            if owner is not None and owner != task.owner:
                continue
            self._reset_task(task, STATUS_PENDING, batch=batch)
            updated += 1
        batch.flush()

        def mark_not_killed(job):
            job.status = JOB_STATUS_SUBMITTED
            return True

        self.job_storage.update_job(jobid, mark_not_killed)

        return updated

    def _reset_task(self, task, status, batch=None):
        now = time.time()
        task.owner = None
        task.status = status
        task.history.append(TaskHistory(timestamp=now, status="reset"))
        self.task_storage.update_task(task, batch)

    def reset_task(self, task_id, status=STATUS_PENDING):
        task = self.task_storage.get_task(task_id)
        self._reset_task(task, status)

    def submit(
        self,
        job_id,
        args,
        kube_job_spec,
        metadata : Dict[str, str],
        cluster,
        target_node_count,
        max_preemptable_attempts,
    ):
        kube_job_spec = json.dumps(kube_job_spec)
        tasks : List[Task] = []
        now = time.time()

        batch = Batch(self.client)
        task_index = 1
        for arg, command_result_url, log_url in args:
            task_id = "{}.{}".format(job_id, task_index)
            task = Task(
                task_id=task_id,
                task_index=task_index,
                job_id=job_id,
                status="pending",
                args=arg,
                history=[TaskHistory(timestamp=now, status="pending")],
                owner=None,
                command_result_url=command_result_url,
                cluster=cluster,
                monitor_address=None,
                log_url=log_url,
            )
            self.task_storage.insert(task, batch=batch)
            task_index += 1

        job = Job(
            job_id=job_id,
            tasks=[t.task_id for t in tasks],
            kube_job_spec=kube_job_spec,
            metadata=metadata,
            cluster=cluster,
            status=JOB_STATUS_SUBMITTED,
            submit_time=time.time(),
            target_node_count=target_node_count,
            max_preemptable_attempts=max_preemptable_attempts,
        )
        self.job_storage.insert(job, batch=batch)
        batch.flush()
        # log.info("Saved task definition batch containing %d tasks", len(batch))


#     def test_datastore_api(self, job_id):
#         """Test we the datastore api is enabled by writing a value and deleting a value."""
#         job = Job(job_id=job_id, tasks=[], kube_job_spec=None, metadata={}, cluster=job_id, status=JOB_STATUS_KILLED,
#                   submit_time=time.time())
#         self.storage.store_job(job)
#         fetched_job = self.storage.get_job(job_id)
#         assert fetched_job.job_id == job_id
#         self.storage.delete_job(job_id)

#     def _update_task_status(self, task_id, new_status, failure_reason, retcode):
#         task = self.storage.get_task(task_id)
#         now = time.time()
#         task.history.append( TaskHistory(timestamp=now, status=new_status, failure_reason=failure_reason) )
#         task.status = new_status
#         task.failure_reason = failure_reason
#         task.exit_code = retcode
# #        task.owner = None
#         updated = self.storage.update_task(task)
#         if not updated:
#             # I suppose this is not technically correct. Could be a simultaneous update of "success" or "failed" and "lost"
#             raise Exception("Detected concurrent update, which should not be possible")

#     def owner_lost(self, owner):
#         tasks = self.Task.scan(owner == owner)
#         for task in tasks:
#             self._update_task_status(task.task_id, "lost")


def _gcloud_cmd(args):
    return ["gcloud"] + list(args)
