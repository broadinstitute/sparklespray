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
from argparse import Namespace


class Args(object):
    def __init__(self, dict):
        self.dict = dict

    def __getattr__(self, name):
        try:
            return self.dict[name]
        except KeyError:
            raise AttributeError(f"could not find {name}")


def new_args(args, **overrides):
    assert isinstance(args, Namespace)
    m = dict(vars(args))
    for key in overrides.keys():
        assert key in m, f"Tried overriding {key} but was not present in {m}"
    m.update(overrides)
    return Args(m)


def prepare_get_args(args, job_id, script_filename, function_name, submission_dir, interpreter_exe, scatter_function_parameters, script_suffix):
    assert interpreter_exe is not None

    if not os.path.exists(submission_dir):
        os.makedirs(submission_dir)

    package_filename = os.path.join(submission_dir, "package.zip")
    fn_runner_filename = os.path.join(submission_dir, "run.py")
    element_count_filename = os.path.join(submission_dir, "element_count.txt")

    shutil.copy(os.path.join(os.path.dirname(
        __file__), f"__sparkles_fn_runner.{script_suffix}"), fn_runner_filename)

    return new_args(args,
                    push=args.push + [fn_runner_filename, script_filename],
                    command=[interpreter_exe, fn_runner_filename,
                             "scatter",
                             script_filename,
                             function_name,
                             package_filename, element_count_filename],
                    name=job_id,
                    foreach=False)


def prepare_foreach(args, job_id, script_filename, submission_dir, interpreter_exe, foreach_function_name, batch_size, cluster_name, script_suffix, element_count, package_url):
    if not os.path.exists(submission_dir):
        os.makedirs(submission_dir)

    fn_runner_filename = os.path.join(submission_dir, "run.py")
    params_filename = os.path.join(submission_dir, "params.csv")
    package_filename = os.path.join(submission_dir, "package.zip")
    results_filename = os.path.join(submission_dir, "results")

    shutil.copy(os.path.join(os.path.dirname(
        __file__), f"__sparkles_fn_runner.{script_suffix}"), fn_runner_filename)

    with open(params_filename, "wt") as fd:
        w = csv.writer(fd)
        w.writerow(["start", "end"])
        for start in range(0, element_count, batch_size):
            end = min(batch_size + start, element_count)
            w.writerow([str(start), str(end)])

    return new_args(args,
                    name=job_id,
                    push=args.push +
                    [package_url + ":"+package_filename,
                        fn_runner_filename, script_filename],
                    params=params_filename,
                    clustername=cluster_name,
                    command=[interpreter_exe, fn_runner_filename,
                             "foreach",
                             script_filename,
                             foreach_function_name,
                             package_filename,
                             results_filename,
                             '{start}',
                             '{end}'],
                    foreach=False)


def prepare_gather(args, job_id, script_filename, submission_dir, interpreter_exe, foreach_function_name, element_count, package_url, cluster_name, script_suffix, results_from_tasks):
    if not os.path.exists(submission_dir):
        os.makedirs(submission_dir)

    fn_runner_filename = os.path.join(submission_dir, "run.py")
    package_filename = os.path.join(submission_dir, "package.zip")
    results_dir = os.path.join(submission_dir, "results")

    shutil.copy(os.path.join(os.path.dirname(
        __file__), f"__sparkles_fn_runner.{script_suffix}"), fn_runner_filename)

    results_to_pull = []
    for task_index, result_url in results_from_tasks:
        result_path = os.path.join(results_dir, str(task_index))
        results_to_pull.append(f"{result_url}:{result_path}")

    return new_args(args,
                    name=f"{job_id}-gather",
                    push=args.push +
                    [package_url + ":"+package_filename,
                        fn_runner_filename, script_filename] + results_to_pull,
                    clustername=cluster_name,
                    command=[interpreter_exe, fn_runner_filename,
                             "gather",
                             script_filename,
                             foreach_function_name,
                             results_dir,
                             str(element_count),
                             package_filename],
                    foreach=False)


def _get_uploaded_files(io: IO, task: Task):
    result_spec = json.loads(io.get_as_str(task.command_result_url))
    files = result_spec['files']
    return {f['src']: f['dst_url'] for f in files}


def foreach_cmd(jq, io, cluster, args, config, job_id):
    from .submit import submit_cmd

    gather_name = "gather"
    script_filename = args.command[0]
    getargs_function_parameters = args.command[1:]
    scatter_function_name = args.get_foreach_args_name
    foreach_function_name = args.foreach_name
    batch_size = args.foreach_batch_size

    foreach_submission_dir = args.foreach_submission_dir
    if foreach_submission_dir is None:
        foreach_submission_dir = job_id

    interpreter_exe = args.foreach_script_exe
    if interpreter_exe is None:
        if script_filename.lower().endswith(".py"):
            interpreter_exe = "python"
            script_suffix = "py"
        else:
            assert script_filename.lower().endswith(
                ".r"), "script must end with either .py or .R"
            interpreter_exe = "Rscript"
            script_suffix = "R"

    # run scatter phase
    get_args_job_name = job_id+"-getargs"
    sub_args = prepare_get_args(args, get_args_job_name, script_filename, scatter_function_name, foreach_submission_dir,
                                interpreter_exe, getargs_function_parameters, script_suffix)

    from pprint import pprint
    # print("*****")
    # pprint(vars(sub_args))
    ret_code = submit_cmd(jq, io, cluster, sub_args, config)
    if ret_code != 0:
        return ret_code

    # copy files back from scatter job. should job_name/1/submission_dir/package.zip
    job = jq.job_storage.get_job(get_args_job_name)
    tasks = jq.task_storage.get_tasks(get_args_job_name)
    assert len(tasks) == 1
    files = _get_uploaded_files(io, tasks[0])

    if tasks[0].exit_code != "0":
        stdout_url = tasks[0].log_url
        user_print("Scatter task failed. Dumping output from script:")
        user_print(io.get_as_str(stdout_url))
        return 1

    # determine the number of elements in the list, which determines the number of tasks to run
    element_count = int(io.get_as_str(
        files[os.path.join(foreach_submission_dir, "element_count.txt")]))

    # get the url of the elements themselves
    package_url = files[os.path.join(foreach_submission_dir, "package.zip")]

    # run foreach phase
    sub_args = prepare_foreach(args, job_id, script_filename, foreach_submission_dir,
                               interpreter_exe, foreach_function_name, batch_size, job.cluster, script_suffix, element_count, package_url)
    ret_code = submit_cmd(jq, io, cluster, sub_args, config)
    if ret_code != 0:
        return ret_code

    # run gather phase
    results_from_tasks = get_results_from_tasks(
        jq, io, job_id, foreach_submission_dir)

    sub_args = prepare_gather(
        args, job_id, script_filename, foreach_submission_dir, interpreter_exe, gather_name, element_count, package_url, job.cluster, script_suffix, results_from_tasks)
    # print("*****")
    # pprint(vars(sub_args))
    ret_code = submit_cmd(jq, io, cluster, sub_args, config)

    return ret_code


def get_results_from_tasks(jq, io, job_id, foreach_submission_dir):
    tasks = jq.task_storage.get_tasks(job_id)
    result = []
    for task in tasks:
        files = _get_uploaded_files(io, task)
        task_result_url = files[os.path.join(
            foreach_submission_dir, "results")]

        result.append((task.task_index, task_result_url))

    return result
