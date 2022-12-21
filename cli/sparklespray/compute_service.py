from apiclient.discovery import build
from googleapiclient.errors import HttpError

from googleapiclient.discovery_cache.base import Cache
import os
import hashlib
import tempfile
from dataclasses import dataclass


@dataclass
class VolumeDetails:
    type: str
    size: float
    status: str


class DirCache(Cache):
    def __init__(self, path):
        self.path = path
        # attempt to create if not already existing
        try:
            os.makedirs(self.path)
        except FileExistsError:
            pass

    def _get_filename(self, url):
        return os.path.join(self.path, hashlib.sha256(url.encode("utf8")).hexdigest())

    def get(self, url):
        fn = self._get_filename(url)
        try:
            with open(fn, "rt") as fd:
                return fd.read()
        except FileNotFoundError:
            return None

    def set(self, url, content):
        fn = self._get_filename(url)
        tmp_fd = tempfile.NamedTemporaryFile(mode="wt", dir=self.path, delete=False)
        tmp_fd.write(content)
        tmp_fd.close()
        os.rename(tmp_fd.name, fn)


class ComputeService:
    """Facade/wrapper around GCS compute API"""

    def __init__(self, project: str, credentials=None) -> None:
        self.compute = build(
            "compute",
            "v1",
            credentials=credentials,
            cache_discovery=True,
            cache=DirCache(".sparkles-cache/services"),
        )
        self.project = project

    def get_volume_details(self, zone, name):
        response = (
            self.compute.disks()
            .get(project=self.project, zone=zone, disk=name)
            .execute()
        )
        response = (
            self.compute.disks()
            .list(project=self.project, zone=zone, filter=f'name="{name}"')
            .execute()
        )
        disks = list(response.get("items", []))
        if len(disks) == 0:
            return None
        assert len(disks) == 1
        disk = disks[0]
        return VolumeDetails(disk["type"], float(disk["sizeGb"]), disk["status"])

    def create_volume(self, zone, type, size, name, wait=True):
        assert type in ["pd-standard", "pd-balanced", "pd-ssd"]
        response = (
            self.compute.disks()
            .insert(
                project=self.project,
                zone=zone,
                body={
                    "name": name,
                    "sizeGb": size,
                    "type": f"projects/{self.project}/zones/{zone}/diskTypes/{type}",
                },
            )
            .execute()
        )
        # wait for drive to be created
        while wait:
            response = (
                self.compute.disks()
                .get(project=self.project, zone=zone, disk=name)
                .execute()
            )
            if response["status"] == "READY":
                break
            if response["status"] not in ["CREATING", "RESTORING"]:
                raise Exception(f"bad status: {response}")
            time.sleep(1)

    #        print(self.compute.disks().get(project=self.project,zone=zone,resourceId=response['id']).execute())

    def get_cluster_instances(self, zones, cluster_name):
        instances = []
        for zone in zones:
            i = (
                self.compute.instances()
                .list(
                    project=self.project,
                    zone=zone,
                    filter="labels.kubeque-cluster=" + cluster_name,
                )
                .execute()
                .get("items", [])
            )
            instances.extend(i)
        return instances

    def stop(self, name: str, zone: str) -> None:
        self.compute.instances().delete(
            project=self.project, zone=zone, instance=name
        ).execute()

    def get_instance_status(self, zone: str, instance_name: str) -> str:
        try:
            instance = (
                self.compute.instances()
                .get(project=self.project, zone=zone, instance=instance_name)
                .execute()
            )
            return instance["status"]
        except HttpError as error:
            if error.resp.status == 404:
                return "TERMINATED"
            else:
                raise Exception(
                    "Got HttpError but status was: {}".format(error.resp.status)
                )
