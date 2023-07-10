from sparklespray.config import load_config, UnknownParameters, MissingRequired
import pytest
from sparklespray.model import PersistentDiskMount


def test_basic_config(tmpdir):
    config_file = tmpdir.join("config")
    config_file.write(
        """[config]
default_url_prefix=gs://bananas/1000
project=broad-achilles
default_image=python
machine_type=n1-standard-2
zones=us-central1-b
region=us-central1
account=apple
"""
    )

    config = load_config(
        config_file=str(config_file), overrides={}, gcloud_config_file=str(tmpdir.join("invalid"))
    )

    assert config.default_url_prefix == "gs://bananas/1000"
    assert config.project == "broad-achilles"
    assert config.default_image == "python"
    assert config.zones == ["us-central1-b"]
    assert config.region == "us-central1"
    assert config.mounts == [
        PersistentDiskMount(path="/mnt", size_in_gb=100, type="local-ssd")
    ]


def test_extra_args(tmpdir):
    config_file = tmpdir.join("config")
    config_file.write(
        """[config]
default_url_prefix=gs://bananas/1000
project=broad-achilles
default_image=python
machine_type=n1-standard-2
zones=us-central1-b
region=us-central1
account=apple
extra_arg=yes
"""
    )

    with pytest.raises(UnknownParameters):
        load_config(
            config_file=str(config_file), overrides={}, gcloud_config_file=str(tmpdir.join("invalid"))
        )


def test_missing_args(tmpdir):
    config_file = tmpdir.join("config")
    config_file.write(
        """[config]
project=broad-achilles
default_image=python
machine_type=n1-standard-2
zones=us-central1-b
region=us-central1
"""
    )

    with pytest.raises(MissingRequired):
        load_config(
            config_file=str(config_file), overrides={}, gcloud_config_file=str(tmpdir.join("invalid"))
        )
