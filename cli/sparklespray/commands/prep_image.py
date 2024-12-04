import re
from ..config import Config
from ..gcp_setup import setup_project
import shutil
import tempfile
import os
import subprocess

DOCKERFILE_CONTENT = """
FROM gcr.io/distroless/static-debian11

COPY sparklesworker /sparklesworker
ENTRYPOINT ["/sparklesworker"]
"""


def prep_image_cmd(args, config: Config):
    print(f"Creating docker image {config.sparklesworker_image}...")

    with tempfile.TemporaryDirectory(prefix="sparklesbuild") as tmp_name:
        with open(os.path.join(tmp_name, "Dockerfile"), "wt") as fd:
            fd.write(DOCKERFILE_CONTENT)

        shutil.copy(
            config.sparklesworker_exe_path, os.path.join(tmp_name, "sparklesworker")
        )

        subprocess.check_call(
            ["docker", "build", "-t", config.sparklesworker_image, "."], cwd=tmp_name
        )
        subprocess.check_call(
            ["docker", "push", config.sparklesworker_image], cwd=tmp_name
        )
    print(f"Docker image {config.sparklesworker_image} has been created and pushed")


def add_prep_image_cmd(subparser):
    parser = subparser.add_parser(
        "prep-image",
        help="Creates the docker image required by sparkles (name based on the sparkleswork_image parameter in config file)",
    )
    parser.set_defaults(func=prep_image_cmd)
