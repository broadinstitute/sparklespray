from ..config import Config
from ..gcp_setup import (
    build_and_push_image,
    get_sparkles_project_settings,
    store_sparkles_project_settings,
)


def prep_image_cmd(args, config: Config):
    project_settings = get_sparkles_project_settings(config.project)

    worker_docker_image = (
        project_settings.get("worker_docker_image") or config.sparklesworker_image
    )
    if not worker_docker_image:
        raise Exception(
            "No worker_docker_image found in project settings or config file"
        )

    print(f"Creating docker image {worker_docker_image}...")

    worker_dockerfile_path = args.worker_dockerfile_path
    build_and_push_image(worker_docker_image, worker_dockerfile_path, False)

    project_settings["worker_docker_image"] = worker_docker_image
    store_sparkles_project_settings(config.project, project_settings, False)

    print(f"Docker image {worker_docker_image} has been created and pushed")


def add_prep_image_cmd(subparser):
    parser = subparser.add_parser(
        "prep-image",
        help="Creates the docker image required by sparkles (name based on the sparkleswork_image parameter in config file)",
    )
    parser.add_argument(
        "--worker-dockerfile-path",
        help="Path to directory containing Dockerfile and go code used to build sparklesworker (defaults to embedded copy)",
        default=None,
    )
    parser.set_defaults(func=prep_image_cmd)
