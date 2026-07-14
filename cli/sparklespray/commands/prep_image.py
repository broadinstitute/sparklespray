import re
from ..config import Config
from ..gcp_setup import setup_project
import shutil
import tempfile
import os
import subprocess

DOCKERFILE_CONTENT = """
# Stage 1: Build the Go binary
FROM golang:1.19 AS builder

WORKDIR /app
COPY go/src src
WORKDIR /app/src/github.com/broadinstitute/sparklesworker/cli
# build staticly linked executable
RUN GCO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-linkmode 'external' -extldflags '-static'" -o /app/sparklesworker .

# Stage 2: Create the minimal image
FROM debian:bookworm-slim
# Install ca-certificates and clean up the apt cache in a single layer to save space
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/sparklesworker /sparklesworker
ENTRYPOINT ["/sparklesworker"]
"""
import sparklespray
from sparklespray.errors import UserError

def prep_image_cmd(args, config: Config):
    # sanity check the target tag to make sure
    # we're not clobbering a docker image for a different
    # version of sparkles
    sparkles_version = sparklespray.__version__

    if ":" not in config.sparklesworker_image:
        raise UserError(f"sparklesworker_image name was {config.sparklesworker_image} but no tag was specified")

    tag = config.sparklesworker_image.split(":")[-1]
    version_from_tag = tag.split("-")[0]

    if version_from_tag != sparkles_version:
        raise UserError(f"Expected the docker image tag to be {sparkles_version} but was {version_from_tag}")

    print(f"Creating docker image {config.sparklesworker_image}...")

    with tempfile.TemporaryDirectory(prefix="sparklesbuild") as tmp_name:
        with open(os.path.join(tmp_name, "Dockerfile"), "wt") as fd:
            fd.write(DOCKERFILE_CONTENT)

        go_src_dir = os.path.join(
            os.path.dirname(sparklespray.__file__), "sparklesworker", "go", "src"
        )
        shutil.copytree(go_src_dir, os.path.join(tmp_name, "go", "src"))

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
