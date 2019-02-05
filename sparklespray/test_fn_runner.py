from .__sparkles_fn_runner import main
import os
import shutil
import subprocess

PY_SCRIPT = """
from sparklespray import test_fn_runner

def scatter(scatter_param):
    assert scatter_param == "param1"
    return dict(elements=[1,2,3], foreach=foreach, extra_args=["extra"])

def foreach(x, expect_extra):
    assert expect_extra == "extra"
    test_fn_runner.foreach_output.append(x)
"""

R_SCRIPT = """
scatter <- function(tmpdir) {
    list(elements=c(1,2,3), extra_args=list("extra", tmpdir))
}

foreach <- function(x, expect_extra, tmpdir) {
    stopifnot(expect_extra == "extra")
    fn <- file.path(tmpdir, paste0(x, ".txt"))
    print(paste0("writing ", fn))
    cat(x, file=fn)
}
"""


def test_run_py_scatter(tmpdir):
    script1 = tmpdir.join("script1")
    script1.write(PY_SCRIPT)

    dest = str(tmpdir.join("dest"))

    # run 'scatter' function and write out the elements and shared state to dest
    main(["scatter", str(script1), "scatter", dest, "param1"])

    # this is pretty weird, but doing this to communicate with foreach through foreach_output array
    from sparklespray import test_fn_runner
    test_fn_runner.foreach_output = []

    # now, make sure we can read back dest and run foreach on those elements
    main(["foreach", str(script1), "foreach", dest, "1", "2"])

    assert test_fn_runner.foreach_output == [2]


def test_run_r_scatter(tmpdir):
    script1 = tmpdir.join("script1")
    script1.write(R_SCRIPT)

    dest = str(tmpdir.join("dest"))
    script_dest = str(tmpdir.join("runner.R"))
    shutil.copy(os.path.join(os.path.dirname(__file__),
                             "__sparkles_fn_runner.R"), script_dest)

    for filename in ["2.txt", "3.txt"]:
        assert not os.path.exists(str(tmpdir.join(filename)))

    # run 'scatter' function and write out the elements and shared state to dest
    subprocess.check_call(["Rscript", script_dest, "scatter", str(
        script1), "scatter", dest, str(tmpdir)])

    # now, make sure we can read back dest and run foreach on those elements
    subprocess.check_call(["Rscript", script_dest, "foreach", str(
        script1), "foreach", dest, "1", "3"])

    for filename in ["2.txt", "3.txt"]:
        assert os.path.exists(str(tmpdir.join(filename)))
