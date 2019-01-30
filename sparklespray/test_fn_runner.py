from .__sparkles_fn_runner import main

SCRIPT1 = """
from sparklespray import test_fn_runner

def scatter(scatter_param):
    assert scatter_param == "param1"
    return dict(elements=[1,2,3], foreach=foreach, extra_args=["extra"])

def foreach(x, expect_extra):
    assert expect_extra == "extra"
    test_fn_runner.foreach_output.append(x)
"""


def test_run_scatter(tmpdir):
    script1 = tmpdir.join("script1")
    script1.write(SCRIPT1)

    dest = str(tmpdir.join("dest"))

    # run 'scatter' function and write out the elements and shared state to dest
    main(["scatter", str(script1), "scatter", dest, "param1"])

    # this is pretty weird, but doing this to communicate with foreach through foreach_output array
    from sparklespray import test_fn_runner
    test_fn_runner.foreach_output = []

    # now, make sure we can read back dest and run foreach on those elements
    main(["foreach", str(script1), "foreach", dest, "1", "2"])

    assert test_fn_runner.foreach_output == [2]
