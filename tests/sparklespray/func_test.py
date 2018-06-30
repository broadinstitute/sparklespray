import os

from kubeque.main import main
CONFIG = """[config]
default_url_prefix=gs://broad-achilles-kubeque/test/kube
project=broad-achilles
default_image=python
default_resource_cpu=1
default_resource_memory=100M
zone=us-east1-b
"""

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

def test_end_to_end(tmpdir):
    dest_dir = tmpdir.join('dest')
    dest_dir.mkdir()

    config_file = tmpdir.join("config")
    config_file.write(CONFIG)

    work_dir = tmpdir.join("work")
    work_dir.mkdir()

    script = work_dir.join("cat.py")
    script.write(SCRIPT)
    os.chmod(str(script), 0o744)

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

    cwd = os.getcwd()
    os.chdir(str(work_dir))
    
    use_cluster = False
    
    sub_opts = ["--fetch", str(dest_dir)]
    if not use_cluster:
        sub_opts += ['--local']
    
    try:
        main(["--config", str(config_file), "sub"] +sub_opts+ ['^./cat.py',
              '^subdir/subdir/file', '^subdir2/file2', '^file3'])
    finally:
        os.chdir(cwd)

    fetched = dest_dir.join("1")
    assert fetched.join("stdout.txt").exists()
    assert fetched.join("output.txt").exists()
    assert fetched.join("outdir/outdir2/output2.txt").exists()

