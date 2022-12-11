import os

from sparklespray.main import main

DEFAULT_URL = "gs://broad-achilles-kubeque/test/kube"

CONFIG = """[config]
default_url_prefix={}
project=broad-achilles
default_image=python
machine_type=n1-standard-2
zone=us-east1-b
""".format(
    DEFAULT_URL
)

SCRIPT = """#!/usr/bin/python
import sys, os

x = ""

for fn in sys.argv[1:]:
    with open(fn, "rt") as fd:
        x += fd.read()

with open("output.txt", "wt") as fd:
    fd.write(x)

os.makedirs("outdir/outdir2")
with open("outdir/outdir2/output2.txt", "wt") as fd:
    fd.write("output2")
"""

import pytest

@pytest.mark.skip(reason="test has a lot of hardcoded values. disabling for now")
def test_end_to_end(tmpdir):
    dest_dir = tmpdir.join("dest")
    dest_dir.mkdir()

    # write out config file
    config_file = tmpdir.join("config")
    config_file.write(CONFIG)

    work_dir = tmpdir.join("work")
    work_dir.mkdir()

    # write out script to run
    script = work_dir.join("cat.py")
    script.write(SCRIPT)
    os.chmod(str(script), 0o744)

    # makes dirs to upload
    one_deep = work_dir.join("subdir")
    one_deep.mkdir()
    two_deep = one_deep.join("subdir")
    two_deep.mkdir()
    two_deep_file = two_deep.join("file")
    two_deep_file.write("two_deep_file")

    dir2 = work_dir.join("subdir2")
    dir2.mkdir()
    file2 = dir2.join("file2")
    file2.write("file2")

    file3 = work_dir.join("file3")
    file3.write("file3")

    # change to the work dir where the sparkles config is and the subdirs
    cwd = os.getcwd()
    os.chdir(str(work_dir))

    # sub_opts = ["--fetch", str(dest_dir)]
    # sub_opts += ['--local']
    sub_opts = ["--local", "-n", "test_end_to_end"]

    try:
        retcode = main(
            ["--config", str(config_file), "sub"]
            + sub_opts
            + ["^./cat.py", "^subdir/subdir/file", "^subdir2/file2", "^file3"]
        )
        assert retcode == 0
    finally:
        # return to the original directory the test was running from regardless of execptions
        os.chdir(cwd)

    # fetch results
    cmd = "gsutil rsync -r {}/test_end_to_end {}".format(DEFAULT_URL, dest_dir)
    print("executing:", cmd)
    os.system(cmd)

    # make sure files exist that would be written from successful run.
    fetched = dest_dir.join("1")
    assert fetched.join("stdout.txt").exists()
    assert fetched.join("output.txt").exists()
    assert fetched.join("outdir/outdir2/output2.txt").exists()
