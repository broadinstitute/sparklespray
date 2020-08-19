import os
import json
import time

common_parameters = "-i python:3.6-stretch -m n1-standard-1 -n sleeptest"


def run(cmd, expect_ok=True):
    sparkles_cmd = (
        "coverage run --source sparklespray -a -m sparklespray.main_as_module"
    )
    ret = os.system(f"{sparkles_cmd} {cmd}")
    if expect_ok:
        assert ret == 0
    else:
        assert ret != 0


def test_local_running_mode(tmpdir):
    run(f"sub {common_parameters} --local sleeptest python --version")
    assert get_tasks_with_status(tmpdir, "sleeptest", "complete") == 1


def get_tasks_with_status(tmpdir, job_id, status=None):
    # wait a bit for eventual consistency
    time.sleep(2)

    status_file = tmpdir.join("status.json")
    run(f"show {job_id} --out {status_file}")
    with open(status_file, "rt") as fd:
        tasks = json.load(fd)
        if status:
            tasks = [task for task in tasks if task["sparklespray_status"] == status]
        return len(tasks)


def test_remote_multitask_run(tmpdir):
    run(f"sub {common_parameters} --seq 3 python --version")
    assert get_tasks_with_status(tmpdir, "sleeptest", "complete") == 3


def test_long_run(tmpdir):
    f = tmpdir.join("sleeptest.sh")
    f.write(
        """echo started ; for ii in `seq 10` ; do echo $ii ; sleep 1 ; done ; echo done"""
    )

    run(f"sub {common_parameters} -u {f}:sleep.sh bash sleep.sh")
    assert get_tasks_with_status(tmpdir, "sleeptest", "complete") == 1


def test_reset(tmpdir):
    run(f"sub {common_parameters} python --version")
    assert get_tasks_with_status(tmpdir, "sleeptest", "complete") == 1

    # cannot actually check for pending status because a cluster left over from a past run might pick it up
    #    run("reset --all sleeptest")
    #    assert get_tasks_with_status(tmpdir, "sleeptest", "pending") == 1

    # assuming tasks back in pending, and rerun
    run("watch sleeptest")
    assert get_tasks_with_status(tmpdir, "sleeptest", "complete") == 1


def test_kill(tmpdir):
    run(f"sub {common_parameters} --no-wait python --version")
    run("kill sleeptest")
    # like reset, we can't verify killing because tasks may have gotten executed by an already running cluster
    # assert get_tasks_with_status(tmpdir, "sleeptest", "killed") == 1


def test_version():
    run("version")


# test watch and clean


def test_validate(tmpdir):
    run("validate")


def test_watch(tmpdir):
    run(f"sub {common_parameters}  --no-wait python --version")
    run("watch sleeptest")
    assert get_tasks_with_status(tmpdir, "sleeptest", "complete") == 1

    # test use of "LAST"
    run("watch LAST")

    # test use of status
    # run("status")
    run("status LAST")

    # test clean
    run("clean sleeptest")
    run("show sleeptest", expect_ok=False)
