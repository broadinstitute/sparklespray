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

        def _convert_empty_volume(mount: PersistentDiskMount) -> dict:
            assert len(mount.mount_options) == 0
            return dict(mountPoint=mount.path, type=mount.type, sizeInGB=mount.size_in_gb)

        def _convert_gcs_mount(mount: GCSBucketMount) -> dict:
            return dict(
                mountPath=mount.path,
                gcsPath=f"gs://{mount.remote_path}",
                mountOptions=mount.mount_options,
            )

        empty_volumes = []
        gcs_mounts = []
        for mount in mounts:
            if isinstance(mount, PersistentDiskMount):
                empty_volumes.append(_convert_empty_volume(mount))
            elif isinstance(mount, GCSBucketMount):
                gcs_mounts.append(_convert_gcs_mount(mount))
            else:
                raise UserError(
                    f"sparkles v100 does not support mounts of type {type(mount).__name__} "
                    f"(path={mount.path!r}); only persistent-disk (empty volume) and GCS bucket "
                    f"mounts are supported"
                )

        body = dict(
            name=name,
            tasks=[
                dict(image=image, command=task_command)
                for task_command in list_of_commands
            ],
            workpool=dict(machineType=machine_type, projectID=project, region=region,
                          bootDiskSizeGb=boot_volume.size_in_gb,
                          bootDiskType=boot_volume.type,
                          emptyVolumes=empty_volumes,
                          gcsMounts=gcs_mounts,
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


def wait_for_v100_job(client: "V100Client", job_id: str) -> None:
    "Poll a v100 job until it reaches a terminal state, raising UserError if it did not succeed."
    while True:
        job = client.get_job_by_id(job_id)
        if job.is_terminal_state:
            if job.status != "success":
                raise UserError("Job did not complete successfully")
            return
        time.sleep(5)

# mounts: List[DiskMountT] — PersistentDiskMount maps to workpool.emptyVolumes:
# [{mountPoint, type, sizeInGB}] (brand-new empty disks; mount_options has no equivalent field,
# so it must be empty) and GCSBucketMount maps to workpool.gcsMounts:
# [{mountPath, gcsPath, mountOptions}] (fuse-mounted GCS bucket). ExistingDiskMount (attach a
# named pre-existing disk) has no equivalent at all and is rejected with a UserError.

