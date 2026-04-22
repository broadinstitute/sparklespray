import re
from ..config import Config
from ..gcp_setup import setup_project, SetupOptions


def setup_cmd(args):
    options = SetupOptions(
        project=args.project,
        service_account=args.service_account,
        worker_docker_image=args.worker_docker_image,
        dry_run=args.dryrun,
        url_prefix=args.url_prefix,
        region=args.region,
        worker_dockerfile_path=args.worker_dockerfile_path,
        images_for_jobs=[] if args.image_for_job is None else args.image_for_job,
        write_config=args.write_config,
    )
    setup_project(options)


def add_setup_cmd(subparser):
    parser = subparser.add_parser(
        "setup",
        help="Configures the google project chosen in the config to be compatible with sparklespray. (requires gcloud installed in path)",
    )
    parser.add_argument("project", help="The GCP project to use")
    parser.add_argument("--region", help="The region to use", default="us-central1")
    parser.add_argument(
        "--url-prefix",
        help="The url prefix to use when storing/staging files for sparkles. If not specified, a new bucket will be created",
    )
    parser.add_argument(
        "--service-account",
        help="The service account that will be used to run sparkles and therefore should be granted permissions that sparkles requires. (If not specified, a new service account will be created and will receive the required permissions)",
    )
    parser.add_argument(
        "--worker-docker-image",
        help="The docker image to use for the 'sparkles worker'. If not specified, a docker repo will be created within the project and a docker image automatically built and pushed there.",
    )
    parser.add_argument(
        "--dryrun",
        action="store_true",
        help="If set, will only print out commands that would be executed",
    )
    parser.add_argument(
        "--worker-dockerfile-path",
        help="path to the directory which contains the sparkles worker code and Dockerfile",
        required=True,
    )
    parser.add_argument(
        "--image-for-job",
        action="append",
        help="A docker image that we want to use in a job submission (specified so that any necessary grants can be made). This option can be specified multiple times",
    )
    parser.add_argument(
        "--write-config",
        metavar="sparkles_config_file",
        help="If specified, write a sparkles .ini config file to this path at the end of setup",
    )
    parser.set_defaults(func=setup_cmd)
