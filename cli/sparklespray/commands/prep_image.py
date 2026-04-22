import re
from ..config import Config
from ..gcp_setup import setup_project, build_and_push_image
import shutil
import tempfile
import os
import subprocess


def prep_image_cmd(args, config: Config):
    print(f"Creating docker image {config.sparklesworker_image}...")

    worker_dockerfile_path = args.worker_dockerfile_path

    build_and_push_image(config.sparklesworker_image, worker_dockerfile_path, False)

    print(f"Docker image {config.sparklesworker_image} has been created and pushed")


def add_prep_image_cmd(subparser):
    parser = subparser.add_parser(
        "prep-image",
        help="Creates the docker image required by sparkles (name based on the sparkleswork_image parameter in config file)",
    )
    parser.add_argument(
        "--worker-dockerfile-path",
        help="Path to directory containing Dockerfile and go code used to build sparklesworker",
        required=True,
    )
    parser.set_defaults(func=prep_image_cmd)
