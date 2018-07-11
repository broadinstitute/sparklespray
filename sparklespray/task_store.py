from google.cloud import datastore
import google.cloud.exceptions
import logging

from google.cloud.storage.client import Client as GSClient
import os
import re
import hashlib
import time
import json
import attr
from typing import List
import logging
from .datastore_batch import ImmediateBatch, Batch

log = logging.getLogger(__name__)

STATUS_CLAIMED = "claimed"
STATUS_PENDING = "pending"
STATUS_FAILED = "failed"
STATUS_COMPLETE = "complete"
STATUS_KILLED = "killed"

INCOMPLETE_TASK_STATES = set([STATUS_CLAIMED, STATUS_PENDING])


@attr.s
class TaskHistory(object):
    timestamp = attr.ib()
    status = attr.ib()
    owner = attr.ib(default=None)
    failure_reason = attr.ib(default=None)


@attr.s
class Task(object):
    # will be of the form: job_id + task_index
    task_id = attr.ib()

    task_index = attr.ib()
    job_id = attr.ib()
    status = attr.ib()  # one of: pending, claimed, success, failed, lost
    owner = attr.ib()
    monitor_address = attr.ib()
    args = attr.ib()
    history = attr.ib()  # list of TaskHistory
    command_result_url = attr.ib()
    cluster = attr.ib()
    failure_reason = attr.ib(default=None)
    version = attr.ib(default=1)
    exit_code = attr.ib(default=None)

    def get_instance_name(self):
        owner = self.owner
        if owner is None:
            return None
        return owner.split("/")[-1]


@attr.s
class TaskStatus(object):
    node_status = attr.ib()
    node = attr.ib()
    task = attr.ib()
    operation_id = attr.ib()


def task_to_entity(client, o):
    entity_key = client.key("Task", o.task_id)
    entity = datastore.Entity(key=entity_key)
    entity['task_index'] = o.task_index
    entity['job_id'] = o.job_id
    entity['status'] = o.status
    assert isinstance(o.status, str)
    entity['owner'] = o.owner
    entity['args'] = o.args
    entity['failure_reason'] = o.failure_reason
    entity['cluster'] = o.cluster
    entity['monitor_address'] = o.monitor_address
    entity['command_result_url'] = o.command_result_url
    history = []
    for h in o.history:
        e = datastore.Entity()
        e['timestamp'] = h.timestamp
        e['status'] = h.status
        history.append(e)

    entity['history'] = history
    entity['version'] = o.version
    entity['exit_code'] = o.exit_code
    return entity


def entity_to_task(entity):
    assert isinstance(entity['status'], str)
    history = []
    for he in entity.get('history', []):
        history.append(TaskHistory(timestamp=he['timestamp'], status=he['status'], owner=he.get(
            'owner'), failure_reason=he.get('failure_reason')))

    return Task(
        task_id=entity.key.name,
        task_index=entity['task_index'],
        job_id=entity['job_id'],
        status=entity['status'],
        owner=entity['owner'],
        args=entity['args'],
        history=history,
        version=entity['version'],
        failure_reason=entity.get('failure_reason'),
        command_result_url=entity.get('command_result_url'),
        exit_code=entity.get('exit_code'),
        cluster=entity.get("cluster"),
        monitor_address=entity.get('monitor_address')
    )


class TaskStore:
    def __init__(self, client: datastore.Client) -> None:
        self.client = client
        self.immediate_batch = ImmediateBatch(client)

    def get_task(self, task_id):
        task_key = self.client.key("Task", task_id)
        return entity_to_task(self.client.get(task_key))

    def insert(self, task, batch=None):
        if batch is None:
            batch = self.immediate_batch

        entity = task_to_entity(self.client, task)
        batch.put(entity)

    def delete(self, task_id, batch=None):
        if batch is None:
            batch = self.immediate_batch

        key = self.client.key("Task", task_id)
        batch.delete(key)

    def get_tasks(self, job_id=None, status=None, max_fetch=None) -> List[Task]:
        query = self.client.query(kind="Task")
        if job_id is not None:
            query.add_filter("job_id", "=", job_id)
        if status is not None:
            query.add_filter("status", "=", status)
        start_time = time.time()
        tasks_it = query.fetch(limit=max_fetch)
        # do I need to use next_page?
        tasks = []
        for entity_task in tasks_it:
            #log.info("fetched: %s", entity_task)
            if status is not None:
                if entity_task["status"] != status:
                    log.warning(
                        "Query returned something that did not match query: %s", entity_task)
                    continue
            tasks.append(entity_to_task(entity_task))
            if max_fetch is not None and len(tasks) >= max_fetch:
                break
        end_time = time.time()
        log.debug("get_tasks took %s seconds", end_time-start_time)
        return tasks

    def get_tasks_for_cluster(self, cluster_name, status, max_fetch=None):
        query = self.client.query(kind="Task")
        query.add_filter("cluster", "=", cluster_name)
        query.add_filter("status", "=", status)
        start_time = time.time()
        tasks_it = query.fetch(limit=max_fetch)
        tasks = []
        for entity_task in tasks_it:
            tasks.append(entity_to_task(entity_task))
            if max_fetch is not None and len(tasks) >= max_fetch:
                break
        end_time = time.time()
        log.debug("get_tasks took %s seconds", end_time-start_time)
        return tasks

    def update_task(self, task):
        original_version = task.version
        task.version = original_version + 1
        self.client.put(task_to_entity(self.client, task))
        return True
