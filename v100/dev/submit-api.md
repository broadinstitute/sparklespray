```python
class Client:
    def __init__(self, address : str, token : str):
        # where do I get the token from? do a simple API key for now?
        # maybe put a connect string that can be found on the UI? Take from env variable if not provided?
        pass

    def submit_tasks(job_props: JobProps, tasks : List[TaskSpec]):
        pass

    def _get_deployment_settings(self) -> Settings:
        ...

    def submit_tasks_from_template(job_props: JobProps, parameters: List[dict[str, str]], template:    TaskSpecTemplate):
        # stage local files
        # expand variables in template
        return self.submit_tasks(...)

from pydantic import BaseModel


class JobProps:
    name: str
    resources : dict[str, float] = [{"slots":1}]
    labels : dict[str, str] = []
    workpool : WorkpoolSpec


class TaskSpecTemplate(BaseModel):
    localFilesToLocalize : List[FileToLocalize] = []
    gcsFilesToLocalize : List[FileToLocalize] = []
    parameters: list[Record]
    command : list[str]
    dockerImage: str
    upload_destination: str = "{default_gcs_root}/{job_id}/{task_index}"

class TaskSpec(BaseModel):
    filesToLocalize : List[FileToLocalize] = []
    parameters: Record
    command : list[str]
    dockerImage: str
    upload_destination: str

class WorkpoolSpec(BaseModel):
    machineType: str
    rootDir: str = "/mnt/sparkles"
    resources: list[ResourceEntry] = [{"slots":1}]
    emptyVolumes : list[VolumeEntry] = []

on project:
    serviceAccount
    region
    various_defaults
    exegcspath
    default_gcs_path
```
