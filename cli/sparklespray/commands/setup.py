import re
from ..config import Config
from ..gcp_setup import setup_project


def setup_cmd(args, config: Config):
    default_url_prefix = config.default_url_prefix
    m = re.match("^gs://([^/]+)(?:/.*)?$", default_url_prefix)
    assert m is not None, "invalid remote path: {}".format(default_url_prefix)
    bucket_name = m.group(1)

    image_names = [config.sparklesworker_image, config.default_image]

    setup_project(config.project, config.service_account_key, bucket_name, image_names)


def add_setup_cmd(subparser):
    parser = subparser.add_parser(
        "setup",
        help="Configures the google project chosen in the config to be compatible with sparklespray. (requires gcloud installed in path)",
    )
    parser.set_defaults(func=setup_cmd)
