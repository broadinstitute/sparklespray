import time
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
import requests
from .spec import rewrite_argv_with_parameters
from .util import get_timestamp, random_string, url_join
from .model import (
    PersistentDiskMount,
    ExistingDiskMount,
    DiskMountT,
    MachineSpec,
    GCSBucketMount,
)
import json
from .io_helper import IO
from .hasher import CachingHashFunction
from .errors import UserError

@dataclass
class V100Job:
    id : str
    name: str
    status : str

    @property
    def is_terminal_state(self) -> bool:
        return self.status in ["success", "error", "failed", "killed"]

class V100Client:
    def __init__(self, base_url, api_key, io:IO, cache_db_path: str, cas_url_prefix:str, target_node_count:int):
        self.base_url = base_url
        self.api_key = api_key
        self.io = io
        self.hash_db = CachingHashFunction(cache_db_path)
        self.cas_url_prefix = cas_url_prefix
        self.target_node_count = target_node_count


    def get_job_by_name(self, name) -> Optional[V100Job]:
        response = requests.get(
            f"{self.base_url}/api/v1/jobs",
            headers={"Authorization": f"Bearer {self.api_key}"},
        )
        response.raise_for_status()
        job_summaries = response.json()

        def is_hidden(job_summary):
            return any(
                label["name"] == "hidden" and label["value"] == "true"
                for label in job_summary.get("labels", [])
            )

        matches = [
            job_summary
            for job_summary in job_summaries
            if not is_hidden(job_summary) and job_summary["name"] == name
        ]

        if len(matches) == 0:
            return None
        
        if len(matches) > 1:
            raise ValueError(f"Multiple jobs found with name {name!r}")

        job_summary = matches[0]
        return V100Job(
            id=job_summary["job_id"],
            name=job_summary["name"],
            status=job_summary["state"],
        )

    def get_job_by_id(self, id) -> V100Job:
        response = requests.get(
            f"{self.base_url}/api/v1/job/{id}/summary",
            headers={"Authorization": f"Bearer {self.api_key}"},
        )
        response.raise_for_status()
        job_summary = response.json()

        return V100Job(
            id=job_summary["job_id"],
            name=job_summary["name"],
            status=job_summary["state"],
        )

    def _stage_file(self, filename) -> str:
        if filename.startswith("gs://"):
            return filename

        h = self.hash_db.get_sha256(filename)
        dest_url = url_join(self.cas_url_prefix, h)
        print(f"Uploading {filename} to {dest_url}")
        self.io.put(filename, dest_url, skip_if_exists=True)

        return dest_url

    def _get_files_in_dir(self, dir_path: str) -> List[str]:
        import os
        result = []
        for root, dirs, files in os.walk(dir_path):
            rel_root = os.path.relpath(root, dir_path)
            for filename in files:
                if rel_root == ".":
                    result.append(filename)
                else:
                    result.append(os.path.join(rel_root, filename))
        return result

    def _expand_directories(self, uploads):
        import os
        result = []
        for source, destination in uploads:
            if os.path.isdir(source):
                for filename in self._get_files_in_dir(source):
                    result.append((os.path.join(source, filename), os.path.join(destination, filename)))
            else:
                result.append((source, destination))
        return result

    def submit(self, name: str, command: List[str],
        params: List[Dict[str, str]],
        image: str,
        uploads: List[Tuple[str, str]],
        machine_type: str,
        project: str,
        region: str,
        boot_volume: PersistentDiskMount,
        max_preemptable_attempts_scale: int,
        mounts: List[DiskMountT],
        provision_mode: str,
        worker_linger: int,
        ) -> V100Job:

        # first check to see if this job already exists
        job = self.get_job_by_name(name)
        if job is not None:
            print("Skipping submission of new job: Found existing job with that name.")
            return job

        assert provision_mode == "preemptible"
        assert len(boot_volume.mount_options) == 0

        list_of_commands = rewrite_argv_with_parameters(command, params)

        uploads = self._expand_directories(uploads)

        files_to_localize = [
            dict(source=self._stage_file(source), destination=destination)
            for source, destination in uploads
        ]

        def _convert_mount(mount : DiskMountT):
            assert len(mount.mount_options) == 0
            return dict(mountPoint=mount.path, type=mount.type, sizeInGB=mount.size_in_gb)
        
        mounts_as_dicts = [
            _convert_mount(x) for x in mounts
        ]

        body = dict(
            name=name,
            tasks=[
                dict(image=image, command=task_command)
                for task_command in list_of_commands
            ],
            workpool=dict(machineType=machine_type, projectID=project, region=region,
                          bootDiskSizeGb=boot_volume.size_in_gb,
                          bootDiskType=boot_volume.type,
                          emptyVolumes=mounts_as_dicts,
                          lingerTimeSec=worker_linger,
                          maxPreemptibleWorkerAttempts=max_preemptable_attempts_scale*self.target_node_count),
            filesToLocalize=files_to_localize,
        )

        print("Submitting job:", json.dumps(body, indent=3))
        response = requests.post(
            f"{self.base_url}/api/v1/job",
            json=body,
            headers={"Authorization": f"Bearer {self.api_key}"},
        )
        response.raise_for_status()
        result = response.json()
        print("Created job submission", result["id"])
        return V100Job(id=result["id"], name=name, status="pending")

# mounts: List[DiskMountT] — the API only supports workpool.emptyVolumes: [{mountPoint, type, sizeInGB}], 
# which corresponds to brand-new empty disks. That's a partial match for PersistentDiskMount only 
# (path→mountPoint, type→type, size_in_gb→sizeInGB), but even then mount_options has no equivalent field. ExistingDiskMount (attach a named pre-existing disk) and GCSBucketMount (fuse-mount a GCS path) have no equivalent at all — emptyVolumes can only create new empty disks, not attach existing disks or mount GCS buckets.

