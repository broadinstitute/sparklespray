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
region=us-central1
account=apple
sparklesworker_image=invalid-sparklesworker-name
"""
    )

    config = load_config(
        config_file=str(config_file),
        overrides={},
        gcloud_config_file=str(tmpdir.join("invalid")),
    )

    assert config.default_url_prefix == "gs://bananas/1000"
    assert config.project == "broad-achilles"
    assert config.default_image == "python"
    assert config.region == "us-central1"
    assert config.mounts == [
        PersistentDiskMount(
            path="/mnt/disks/mount_1",
            size_in_gb=100,
            type="local-ssd",
            mount_options=[],
        )
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
sparklesworker_image=invalid-sparklesworker-name

"""
    )

    with pytest.raises(UnknownParameters):
        load_config(
            config_file=str(config_file),
            overrides={},
            gcloud_config_file=str(tmpdir.join("invalid")),
        )


def _base_config_text(extra=""):
    return f"""[config]
default_url_prefix=gs://bananas/1000
project=broad-achilles
default_image=python
machine_type=n1-standard-2
region=us-central1
account=apple
sparklesworker_image=invalid-sparklesworker-name
{extra}
"""


def test_provision_mode_defaults_from_preemptible(tmpdir):
    config_file = tmpdir.join("config")
    config_file.write(_base_config_text())

    config = load_config(
        config_file=str(config_file),
        overrides={},
        gcloud_config_file=str(tmpdir.join("invalid")),
    )

    # preemptible defaults to "y", so provision_mode should default to "preemptible"
    assert config.provision_mode == "preemptible"


def test_provision_mode_defaults_to_normal_when_not_preemptible(tmpdir):
    config_file = tmpdir.join("config")
    config_file.write(_base_config_text("preemptible=n"))

    config = load_config(
        config_file=str(config_file),
        overrides={},
        gcloud_config_file=str(tmpdir.join("invalid")),
    )

    assert config.provision_mode == "normal"


def test_provision_mode_explicit_flex_overrides_preemptible(tmpdir):
    config_file = tmpdir.join("config")
    config_file.write(_base_config_text("provision_mode=flex"))

    config = load_config(
        config_file=str(config_file),
        overrides={},
        gcloud_config_file=str(tmpdir.join("invalid")),
    )

    assert config.provision_mode == "flex"


def test_provision_mode_invalid_value_rejected(tmpdir):
    config_file = tmpdir.join("config")
    config_file.write(_base_config_text("provision_mode=bogus"))

    with pytest.raises(AssertionError):
        load_config(
            config_file=str(config_file),
            overrides={},
            gcloud_config_file=str(tmpdir.join("invalid")),
        )


def test_worker_linger_defaults_to_600(tmpdir):
    config_file = tmpdir.join("config")
    config_file.write(_base_config_text())

    config = load_config(
        config_file=str(config_file),
        overrides={},
        gcloud_config_file=str(tmpdir.join("invalid")),
    )

    assert config.worker_linger == 600


def test_worker_linger_explicit_value(tmpdir):
    config_file = tmpdir.join("config")
    config_file.write(_base_config_text("worker_linger=120"))

    config = load_config(
        config_file=str(config_file),
        overrides={},
        gcloud_config_file=str(tmpdir.join("invalid")),
    )

    assert config.worker_linger == 120


def test_when_sub_job_exists_defaults_to_overwrite(tmpdir):
    config_file = tmpdir.join("config")
    config_file.write(_base_config_text())

    config = load_config(
        config_file=str(config_file),
        overrides={},
        gcloud_config_file=str(tmpdir.join("invalid")),
    )

    assert config.when_sub_job_exists == "overwrite"


def test_when_sub_job_exists_explicit_value(tmpdir):
    config_file = tmpdir.join("config")
    config_file.write(_base_config_text("when_sub_job_exists=abort"))

    config = load_config(
        config_file=str(config_file),
        overrides={},
        gcloud_config_file=str(tmpdir.join("invalid")),
    )

    assert config.when_sub_job_exists == "abort"


def test_when_sub_job_exists_invalid_value_rejected(tmpdir):
    config_file = tmpdir.join("config")
    config_file.write(_base_config_text("when_sub_job_exists=bogus"))

    with pytest.raises(AssertionError):
        load_config(
            config_file=str(config_file),
            overrides={},
            gcloud_config_file=str(tmpdir.join("invalid")),
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
            config_file=str(config_file),
            overrides={},
            gcloud_config_file=str(tmpdir.join("invalid")),
        )
