from googleapiclient.discovery import build
#from apiclient.discovery import build
import googleapiclient.errors
from googleapiclient.errors import HttpError

from googleapiclient.discovery_cache.base import Cache
import os
import hashlib
import tempfile
from dataclasses import dataclass
import time

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
        try:
            response = (
                self.compute.disks()
                .get(project=self.project, zone=zone, disk=name)
                .execute()
            )
        except googleapiclient.errors.HttpError as ex:
            if ex.status_code == 404:
                return None
            raise
        disk = response
        return VolumeDetails(disk["type"].split("/")[-1], float(disk["sizeGb"]), disk["status"])


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
