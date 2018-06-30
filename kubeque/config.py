import logging
import os
import sys

import kubeque.gcs_pipeline as pipeline
from configparser import ConfigParser

log = logging.getLogger(__name__)

def load_config(config_file, gcloud_config_file="~/.config/gcloud/configurations/config_default"):
    # first load defaults from gcloud config
    gcloud_config_file = os.path.expanduser(gcloud_config_file)
    defaults = {}
    if os.path.exists(gcloud_config_file):
        gcloud_config = ConfigParser()
        gcloud_config.read(gcloud_config_file)
        defaults = dict(account=gcloud_config.get("core", "account"),
                        project=gcloud_config.get("core", "project"),
                        zones=[gcloud_config.get("compute", "zone")],
                        region=gcloud_config.get("compute", "region"))

    config_file = os.path.expanduser(config_file)

    config = ConfigParser()
    config.read(config_file)
    config_from_file = dict(config.items('config'))
    if 'zones' in config_from_file:
        config_from_file['zones'] = [x.strip() for x in config_from_file['zones'].split(",")]

    merged_config = dict(defaults)
    merged_config.update(config_from_file)

    missing_values = []
    for property in ["default_url_prefix", "project",
                     "default_image", "default_resource_cpu", "default_resource_memory", "zones", "region", "account"]:
        if property not in merged_config or merged_config[property] == "" or merged_config[property] is None:
            missing_values.append(property)

    if len(missing_values) > 0:
        print("Missing the following parameters in {}: {}".format(config_file, ", ".join(missing_values)))
        sys.exit(1)

    if "kubequeconsume_exe_path" not in merged_config:
        merged_config["kubequeconsume_exe_path"] = os.path.join(os.path.dirname(__file__), "bin/kubequeconsume")
        assert os.path.exists(merged_config["kubequeconsume_exe_path"])

    if "cas_url_prefix" not in merged_config:
        merged_config["cas_url_prefix"] = merged_config["default_url_prefix"] + "/CAS/"

    assert isinstance(merged_config['zones'], list)

    jq, io, cluster = load_config_from_dict(merged_config)
    return merged_config, jq, io, cluster


def load_config_from_dict(config):
    credentials = None
    io = IO(config['project'], config['cas_url_prefix'], credentials)
    jq = create_gcs_job_queue(config['project'], credentials, use_pubsub=False)
    cluster = pipeline.Cluster(config['project'], config['zones'], credentials=credentials)

    return jq, io, cluster

def get_config_path(config_path):
    if config_path is not None:
        if not os.path.exists(config_path):
            raise Exception("Could not find config at {}".format(config_path))
        return config_path
    else:
        def possible_config_names():
            for config_name in [".sparkles", ".kubeque"]:
                dirname = os.path.abspath(".")
                while True:
                    yield os.path.join(dirname, config_name)
                    next_dirname = os.path.dirname(dirname)
                    if dirname == next_dirname:
                        break
                    dirname = next_dirname
            return

        checked_paths = []
        for config_path in possible_config_names():
            if os.path.exists(config_path):
                return config_path
            checked_paths.append(config_path)

        raise Exception("Could not find config file at any of locations: {}".format(checked_paths))


