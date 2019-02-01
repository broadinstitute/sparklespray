import os
import pickle
import shutil
import csv
from zipfile import ZipFile
import argparse
from .io import IO
from .task_store import Task
from .txtui import user_print
import json


def prepare_scatter(job_name, script_filename, function_name, submission_dir, python_exe, extra_sparkles_options, scatter_function_parameters):
    if not os.path.exists(submission_dir):
        os.makedirs(submission_dir)

    package_filename = os.path.join(submission_dir, "package.zip")
    fn_runner_filename = os.path.join(submission_dir, "run.py")

    shutil.copy(os.path.join(os.path.dirname(
        __file__), "__sparkles_fn_runner.py"), fn_runner_filename)

    return ["sub"] + extra_sparkles_options + [
        "-n", job_name,
        "-u", fn_runner_filename,
        python_exe,
        fn_runner_filename,
        "scatter",
        script_filename,
        function_name,
        package_filename] + scatter_function_parameters


def prepare_foreach(job_name, script_filename, submission_dir, python_exe, foreach_function_name, extra_sparkles_options, batch_size, cluster_name):
    if not os.path.exists(submission_dir):
        os.makedirs(submission_dir)

    package_filename = os.path.join(submission_dir, "package.zip")
    fn_runner_filename = os.path.join(submission_dir, "run.py")
    params_filename = os.path.join(submission_dir, "params.csv")

    shutil.copy(os.path.join(os.path.dirname(
        __file__), "__sparkles_fn_runner.py"), fn_runner_filename)

    with ZipFile(package_filename, "r") as zip:
        with zip.open("element_count.pickle") as fd:
            element_count = pickle.load(fd)

    with open(params_filename, "wt") as fd:
        w = csv.writer(fd)
        w.writerow(["start", "end"])
        for start in range(0, element_count, batch_size):
            end = min(batch_size + start, element_count)
            w.writerow([str(start), str(end)])

    cmd = ["sub"] + extra_sparkles_options + [
        "--params", params_filename,
        "-n", job_name,
        "-u", package_filename,
        "-u", fn_runner_filename,
        "-u", script_filename,
        "--clustername", cluster_name,
        python_exe,
        fn_runner_filename,
        "foreach",
        script_filename,
        foreach_function_name,
        package_filename,
        '{start}', '{end}']

    return cmd


def add_scatter_cmd(subparser):
    parser = subparser.add_parser(
        "scatter", help="py scatter")
    parser.set_defaults(func=scatter_cmd)
    parser.add_argument("job_name")
    parser.add_argument("script_filename")
    parser.add_argument(
        "--scatter", dest="scatter_function_name", default="scatter")
    parser.add_argument(
        "--foreach", dest="foreach_function_name", default="foreach")
    parser.add_argument("--batchsize", dest="batch_size", type=int, default=1)
    parser.add_argument("--submission_dir")
    parser.add_argument("--python", dest="python_exe", default="python")
    parser.add_argument("extra_args", nargs=argparse.REMAINDER)


def _get_uploaded_files(io: IO, task: Task):
    result_spec = json.loads(io.get_as_str(task.command_result_url))
    files = result_spec['files']
    return {f['src']: f['dst_url'] for f in files}


def scatter_cmd(jq, io: IO, args):
    job_name = args.job_name
    script_filename = args.script_filename
    scatter_function_name = args.scatter_function_name
    foreach_function_name = args.foreach_function_name
    batch_size = args.batch_size

    submission_dir = args.submission_dir
    if submission_dir is None:
        submission_dir = job_name

    python_exe = args.python_exe
    extra_sparkles_options = ["-i",
                              "python:3.6-alpine", "-u", script_filename]
    scatter_function_parameters = args.extra_args

    # run scatter phase
    scatter_job_name = job_name+"-scatter"
    cmd = prepare_scatter(scatter_job_name, script_filename, scatter_function_name, submission_dir,
                          python_exe, extra_sparkles_options, scatter_function_parameters)

    from .main import main
    ret_code = main(cmd)
    if ret_code != 0:
        return ret_code

    # copy files back from scatter job. should job_name/1/submission_dir/package.zip
    package_path = os.path.join(submission_dir, "package.zip")
    job = jq.job_storage.get_job(scatter_job_name)
    tasks = jq.task_storage.get_tasks(scatter_job_name)
    assert len(tasks) == 1
    files = _get_uploaded_files(io, tasks[0])

    if tasks[0].exit_code != "0":
        stdout_url = tasks[0].log_url
        user_print("Scatter task failed. Dumping output from script:")
        user_print(io.get_as_str(stdout_url))
        return 1

    package_url = files[package_path]

    # copy the package (the result of the scatter script) to our local submission directory
    io.get(package_url, package_path)

    # run foreach phase
    cmd = prepare_foreach(job_name, script_filename, submission_dir,
                          python_exe, foreach_function_name, extra_sparkles_options, batch_size, job.cluster)
    ret_code = main(cmd)

    return ret_code
