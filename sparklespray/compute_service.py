from apiclient.discovery import build
from googleapiclient.errors import HttpError


class ComputeService:
    """ Facade/wrapper around GCS compute API
    """

    def __init__(self, project: str, credentials=None) -> None:
        self.compute = build(
            'compute', 'v1', credentials=credentials, cache_discovery=False)
        self.project = project

    def get_cluster_instances(self, zones, cluster_name):
        instances = []
        for zone in zones:
            i = self.compute.instances().list(project=self.project, zone=zone,
                                              filter="labels.kubeque-cluster=" + cluster_name).execute().get('items',
                                                                                                             [])
            instances.extend(i)
        return instances

    def stop(self, name: str, zone: str) -> None:
        self.compute.instances().delete(project=self.project,
                                        zone=zone, instance=name).execute()

    def get_instance_status(self, zone: str, instance_name: str) -> str:
        try:
            instance = self.compute.instances().get(project=self.project, zone=zone,
                                                    instance=instance_name).execute()
            return instance['status']
        except HttpError as error:
            if error.resp.status == 404:
                return "TERMINATED"
            else:
                raise Exception(
                    "Got HttpError but status was: {}".format(error.resp.status))
