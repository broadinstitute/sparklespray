from pydantic import BaseModel

Resources = dict[str, float]
Parameters = dict[str, str]


class VolumeEntry(BaseModel):
    pass


class FileToLocalize:
    pass


class WorkpoolSpec(BaseModel):
    machine_type: str
    root_dir: str = "/mnt/sparkles"
    resources: Resources
    empty_volumes: list[VolumeEntry] = []


class JobProps:
    name: str
    resources: Resources
    labels: Parameters
    workpool: WorkpoolSpec


class TaskSpecTemplate(BaseModel):
    local_files_to_localize: list[FileToLocalize] = []
    gcs_files_to_localize: list[FileToLocalize] = []
    parameters: Parameters
    command: list[str]
    docker_image: str
    upload_destination: str = "{default_gcs_root}/{job_id}/{task_index}"


class TaskSpec(BaseModel):
    files_to_localize: list[FileToLocalize] = []
    parameters: Parameters
    command: list[str]
    docker_image: str
    upload_destination: str


class Settings:
    pass


# on project:
#     serviceAccount
#     region
#     various_defaults
#     exegcspath
#     default_gcs_path
# ```

import requests


def _create_requests_session(token: str):
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})
    return session


def _expand_template(parameters: Parameters, template: str) -> str:
    raise NotImplementedError()


def _dict_expand_template_values(
    parameters: Parameters, dict_with_templates: dict[str, str]
):
    return {k: _expand_template(parameters, v) for k, v in dict_with_templates.items()}


def _file_to_localize_expand_template_values(
    parameters: Parameters, file_to_localize: FileToLocalize
) -> FileToLocalize:
    raise NotImplementedError()


def _files_to_localize_expand_template_values(
    parameters: Parameters, files_to_localize: list[FileToLocalize]
):
    return [
        _file_to_localize_expand_template_values(parameters, x)
        for x in files_to_localize
    ]


import urllib.parse
from dataclasses import dataclass


@dataclass
class ParsedSparklesURL:
    address: str
    token: str


def _parse_service_url(url: str) -> ParsedSparklesURL:
    """
    Takes a connection string (which is formatted as a URL) and parses it to extract the
    key information. For example: https://sample.com?token=xyz -> ParsedSparklesURL(address="https://sample.com", token="xyz")
    """
    parsed = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qs(parsed.query)

    tokens = query.get("token")
    if not tokens:
        raise ValueError(
            f"Missing required 'token' query parameter in service url: {url}"
        )
    token = tokens[0]

    address = urllib.parse.urlunsplit(parsed._replace(query="", fragment=""))
    return ParsedSparklesURL(address=str(address), token=token)


class Client:
    def __init__(self, service_url: str):
        service_config = _parse_service_url(service_url)
        self.address = service_config.address
        self.session = _create_requests_session(service_config.token)
        raise NotImplementedError()

    def submit_tasks(self, job_props: JobProps, tasks: list[TaskSpec]):
        raise NotImplementedError()

    def submit_tasks_from_template(
        self,
        job_props: JobProps,
        parameters_list: list[Parameters],
        template: TaskSpecTemplate,
    ):
        settings = self._get_settings()

        tasks: list[TaskSpec] = []
        for task_index, parameters in enumerate(parameters_list):

            # create variables to use in templates
            default_parameters = {
                "task_index": str(task_index),
                "job_id": job_props.job_id,
                "default_gcp_path": settings.default_gcp_path,
            }
            template_vars: dict[str, str] = dict()
            template_vars.update(default_parameters)
            template_vars.update(parameters)

            # process all files to localize
            gcs_files_to_localize = _files_to_localize_expand_template_values(
                template_vars, template.gcs_files_to_localize
            )
            local_files_to_localize = _files_to_localize_expand_template_values(
                template_vars, template.local_files_to_localize
            )
            for local_file_to_localize in local_files_to_localize:
                gcs_files_to_localize.append(self._stage_file(local_file_to_localize))

            # expand remaining variables
            command = [_expand_template(template_vars, x) for x in template.command]
            docker_image = _expand_template(template_vars, template.docker_image)
            upload_destination = _expand_template(
                template_vars, template.upload_destination
            )

            # construct task specification
            task_spec = TaskSpec(
                files_to_localize=gcs_files_to_localize,
                parameters=parameters,
                command=command,
                docker_image=docker_image,
                upload_destination=upload_destination,
            )

            tasks.append(task_spec)

        return self.submit_tasks(job_props, tasks)

    def _get_deployment_settings(self) -> Settings:
        raise NotImplementedError()

    def _stage_file(self, local_file_to_localize: FileToLocalize) -> FileToLocalize:
        raise NotImplementedError()
