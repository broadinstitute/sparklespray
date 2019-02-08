import os
import shutil
import subprocess

PY_SCRIPT = """
import os

def scatter(tmpdir):
    return dict(elements=[1,2,3], foreach=foreach, extra_args=["extra", tmpdir])

def foreach(x, expect_extra, tmpdir):
    assert expect_extra == "extra"
    with open(os.path.join(tmpdir, "{}.txt".format(x)), "wt") as fd:
        fd.write(str(x))
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


def _test_run_scatter(executable, suffix, tmpdir, script):
    script1 = tmpdir.join("script1")
    script1.write(script)
    element_count_fn = str(tmpdir.join("elements.txt"))

    dest = str(tmpdir.join("dest"))
    script_dest = str(tmpdir.join("runner."+suffix))
    shutil.copy(os.path.join(os.path.dirname(__file__),
                             "__sparkles_fn_runner."+suffix), script_dest)

    for filename in ["2.txt", "3.txt"]:
        assert not os.path.exists(str(tmpdir.join(filename)))

    # run 'scatter' function and write out the elements and shared state to dest
    subprocess.check_call([executable, script_dest, "scatter", str(
        script1), "scatter", dest, element_count_fn, str(tmpdir)])

    # now, make sure we can read back dest and run foreach on those elements
    subprocess.check_call([executable, script_dest, "foreach", str(
        script1), "foreach", dest, "1", "3"])

    for filename in ["2.txt", "3.txt"]:
        assert os.path.exists(str(tmpdir.join(filename)))


def test_run_py_scatter(tmpdir):
    _test_run_scatter("python", "py", tmpdir, PY_SCRIPT)


def test_run_r_scatter(tmpdir):
    _test_run_scatter("Rscript", "R", tmpdir, R_SCRIPT)
