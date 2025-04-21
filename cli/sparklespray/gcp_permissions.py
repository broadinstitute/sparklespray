import requests
import re
from dataclasses import dataclass

class DockerImageError(Exception):
    pass

class AccessDenied(DockerImageError):
    pass

class ImageNotFound(DockerImageError):
    pass

def _get_access_token(credentials):
    import google.auth.transport.requests
    request = google.auth.transport.requests.Request()
    credentials.refresh(request)
    access_token = credentials.token
    return access_token


def has_access_to_docker_image(service_account, credentials, docker_image):
    """Returns True if this service account has access to pull the docker image, otherwise
    False."""
    try:
        _check_access_to_docker_image(service_account,credentials, docker_image)
    except DockerImageError as ex:
        return False, ex
    return True, None


def _check_access_to_docker_image(service_account, credentials, docker_image):
    "Tests to make sure that the given service account can read the docker_image. Throws an assertion error if not"
    
    access_token = _get_access_token(credentials)

    parsed_image_name = parse_docker_image_name(docker_image)

    # Now attempt to retreive the docker image manifest to see if we have access. 
    # Expecting either success, manifest doesn't exist, or permission denied
    manifest_url = f"https://{parsed_image_name.host}:{parsed_image_name.port}/v2/{parsed_image_name.path}/manifests/{parsed_image_name.tag}"

    res = requests.get(
        manifest_url,
        headers={"Authorization": f"Bearer {access_token}"},
    )

    reconstructed_image_name = str(parsed_image_name)

    if res.status_code == 200:
        # return if we were successful
        return

    if res.status_code == 404:
        raise ImageNotFound(
            f"Service account ({service_account}) has access to docker repo, but could not find image {reconstructed_image_name}"
        )

    if res.status_code in [403, 401]:
        raise AccessDenied(
            f"Service account ({service_account}) does not access to retreive image {reconstructed_image_name}"
        )

    raise AssertionError(
        f"Unexpected status_code={res.status_code} when fetching manifest from {manifest_url}, response body={res.content}"
    )


@dataclass
class DockerImageName:
    host: str
    port: int
    path: str
    tag: str


@dataclass
class ContainerRegistryPath(DockerImageName):
    region: str  # values like "" (if global), or a region like "asia", "eu", "us" etc
    project: str
    repository: str
    image_name: str


@dataclass
class ArtifactRegistryPath(DockerImageName):
    location: str  # values like "us", "us-central1", etc
    project: str
    repository: str
    image_name: str

def _default_to(value, default):
    if value is None or value == "":
        return default
    return value


def parse_docker_image_name(docker_image):
    m = re.match(
        r"(?:([a-z0-9.-]+)(?::(\\d+))?/)?([a-z0-9-_/]+)(?::([a-z0-9-_/.]+))?",
        docker_image,
    )
    if m is None:
        raise Exception(
            f'"{docker_image}" does not appear to be a valid docker image name'
        )
    host, port, path, tag = m.groups()

    # parse this as a generic name
    generic = DockerImageName(
        host=_default_to(host, "docker.io"),
        port=int(_default_to(port, "443")),
        path=path,
        tag=_default_to(tag, "latest"),
    )

    # Now check, is it a google container registry address like us.gcr.io/cds-docker-containers/gumbopot or gcr.io/cds-docker-containers/gumbopot
    m = re.match(r"^([a-z0-9-]+)\.gcr\.io$", generic.host)
    if m is not None:
        region = m.group(1)
        m = re.match(r"([a-z0-9-]+)/([a-z0-9-/_]+)", generic.path)
        assert (
            m
        ), f"Based on host, looks like GCR name, but the path was invalid: {generic.path}"
        project, image_name = m.groups()
        return ContainerRegistryPath(
            host=generic.host,
            port=generic.port,
            path=generic.path,
            tag=generic.tag,
            region=region,
            project=project,
            repository=generic.host,
            image_name=image_name,
        )

    # is it an artifact registry service like us-central1-docker.pkg.dev or us-docker.pkg.dev
    # example: us-central1-docker.pkg.dev/cds-docker-containers/docker/hermit-dev-env:v1
    m = re.match(r"^([a-z0-9-]+)-docker\.pkg\.dev$", generic.host)
    if m is not None:
        location = m.group(1)
        m = re.match(r"([a-z0-9-]+)/([a-z0-9-._]+)/([a-z0-9-/_]+)", generic.path)
        assert (
            m
        ), f"Based on host, looks like a Artifact Registry name, but path was invalid: {generic.path}"
        project, repository, image_name = m.groups()
        return ArtifactRegistryPath(
            host=generic.host,
            port=generic.port,
            path=generic.path,
            tag=generic.tag,
            location=location,
            project=project,
            repository=repository,
            image_name=image_name,
        )

    # if it's neither, just return the generic parsing
    return generic
