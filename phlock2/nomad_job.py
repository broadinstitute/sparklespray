import requests

DEFAULT_NOMAD_URL = "http://127.0.0.1:4646"



def docker_auth(username, password, email, server_address):
    return {"username": username, "password": password, "email": email, "server_address": server_address}

def create_docker_task(name, image, command, args=[], auth=None):
    task = {
            "Driver": "docker",
            "Config": {
                "Image": image,
                "Command": command,
                "Args": args
            }
    }

    if auth is not None:
        task["Auth"] = auth

    return Task(name, task)

def create_raw_exec_task(name, command, args=[]):
    task = {
            "Driver": "raw_exec",
            "Config": {
                "Command": command,
                "Args": args
            }
    }
    return Task(name, task)

class Task:
    def __init__(self, name, base):
        self.task = {
            "Name": name,
            "Env": {
            },
            "Services": [],
            "Constraints": None,
            "Resources": {
                "CPU": 100,
                "MemoryMB": 10,
                "DiskMB": 300,
                "IOPS": 0,
                "Networks": []
            },
            "Meta": None,
            "KillTimeout": 5000000000,
            "LogConfig": {
                "MaxFiles": 10,
                "MaxFileSizeMB": 10
            },
            "Artifacts": []
            }
        self.task.update(base)

    @property
    def name(self):
        return self.task["Name"]

    def add_env(self, key, value):
        self.task["Env"][key] = value


class Job:
    def __init__(self, job_id, datacenter="dc1"):
        self.job = {"Job":{
            "Region": "global",
            "ID": job_id,
            "ParentID": "",
            "Name": job_id,
            "Type": "batch",
            "Priority": 50,
            "AllAtOnce": False,
            "Datacenters": [
                datacenter
            ],
            "Constraints": None,
            "TaskGroups": [],
            "Update": {
                "Stagger": 0,
                "MaxParallel": 0
            },
            "Periodic": None,
            }
        }

    def add_task(self, task):
        task_group = {
            "Name": task.name,
            "Count": 1,
            "Constraints": None,
            "RestartPolicy": {
                "Attempts": 5,
                "Interval": 604800000000000,
                "Delay": 15000000000,
                "Mode": "delay"
            },
            "Tasks": [
                task.task
            ],
            "Meta": None
        }
        self.job["Job"]["TaskGroups"].append(task_group)

import collections

class Nomad:
    def __init__(self, nomad_url=DEFAULT_NOMAD_URL):
        self.nomad_url = nomad_url

    def run_job(self, job):
        job_json = job.job
        job_id = job_json["Job"]["ID"]
        print("submitting: {}".format(job_id))
        r = requests.post(self.nomad_url+"/v1/job/"+job_id, json=job_json)
        assert r.status_code == 200, "Got status {}".format(r.status_code)

        index = None
        while True:
            params = {}
            if index != None:
                params['index'] = index
            r = requests.get(self.nomad_url+"/v1/job/"+job_id, params=params)
            assert r.status_code == 200
            index=r.headers["X-Nomad-Index"]
            job = r.json()
            #print("Got job state: {}".format(json.dumps(job, indent=2)))
            status = job['Status']
            if status == 'dead':
                break
        return job_id

    def get_allocations(self, job_id):
        AllocationSummary = collections.namedtuple("AllocationSummary", ["id", "status", "node_id", "nomad_job_id", "name"])
        r = requests.get(self.nomad_url+"/v1/job/"+job_id+"/allocations")
        assert r.status_code == 200
        allocs = []
        for rec in r.json():
            allocs.append( AllocationSummary(rec['ID'], rec['ClientStatus'], rec['NodeID'], rec['JobID'], rec['Name']) )
        return allocs

    def kill(self, job_id):
        r = requests.delete(self.nomad_url+"/v1/job/"+job_id)
        assert r.status_code == 200
