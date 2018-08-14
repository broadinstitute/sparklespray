import os
import json
import time

def run(cmd, expect_ok=True):
    sparkles_cmd = "coverage run --source sparklespray -a -m sparklespray.main_as_module"
    ret = os.system(f"{sparkles_cmd} {cmd}")
    if expect_ok:
        assert ret == 0
    else:
        assert ret != 0


def test_local_running_mode(tmpdir):
    run("sub --local -i python:3.6-stretch -r memory=4G,cpu=1 -n sleeptest python --version")
    assert get_tasks_with_status(tmpdir, "sleeptest", "complete") == 1

def get_tasks_with_status(tmpdir, job_id, status=None):
    # wait a bit for eventual consistency
    time.sleep(2)
    
    status_file = tmpdir.join("status.json")
    run(f"show {job_id} --out {status_file}")
    with open(status_file, "rt") as fd:
        tasks = json.load(fd)
        if status:
            tasks = [task for task in tasks if task['sparklespray_status'] == status]
        return len(tasks)

def test_remote_multitask_run(tmpdir):
    run("sub --seq 3 -i python:3.6-stretch -r memory=4G,cpu=1 -n sleeptest python --version")
    assert get_tasks_with_status(tmpdir, "sleeptest", "complete") == 3

def test_long_run(tmpdir):
    f = tmpdir.join("sleeptest.sh")
    f.write("""echo started ; for ii in `seq 10` ; do echo $ii ; sleep 1 ; done ; echo done""")
    
    run(f"sub -i python:3.6-stretch -r memory=4G,cpu=1 -u {f}:sleep.sh -n sleeptest bash sleep.sh")
    assert get_tasks_with_status(tmpdir, "sleeptest", "complete") == 1


def test_reset(tmpdir):
    run("sub -i python:3.6-stretch -r memory=4G,cpu=1 -n sleeptest python --version")
    assert get_tasks_with_status(tmpdir, "sleeptest", "complete") == 1

# cannot actually check for pending status because a cluster left over from a past run might pick it up
#    run("reset --all sleeptest")
#    assert get_tasks_with_status(tmpdir, "sleeptest", "pending") == 1

    # assuming tasks back in pending, and rerun
    run("watch sleeptest")
    assert get_tasks_with_status(tmpdir, "sleeptest", "complete") == 1


def test_kill(tmpdir):
    run("sub -i python:3.6-stretch -r memory=4G,cpu=1 -n sleeptest --no-wait python --version")
    run("kill sleeptest")
    # like reset, we can't verify killing because tasks may have gotten executed by an already running cluster
    # assert get_tasks_with_status(tmpdir, "sleeptest", "killed") == 1


def test_version():
    run("version")

# test watch and clean


def test_watch(tmpdir):
    run("sub -i python:3.6-stretch -r memory=4G,cpu=1 -n sleeptest --no-wait python --version")
    run("watch sleeptest")
    assert get_tasks_with_status(tmpdir, "sleeptest", "complete") == 1

    # test clean
    run("clean sleeptest")
    run("show sleeptest", expect_ok=False)
