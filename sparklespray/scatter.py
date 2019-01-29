import os
import pickle
import shutil
import csv


def prepare_scatter(job_name, script_filename, function_name, elements, extra_args, submission_dir, python_exe, extra_sparkles_options, scatter_function_parameters):
    elements_filename = os.path.join(submission_dir, "elements.pickle")
    extra_args_filename = os.path.join(submission_dir, "extra_args.pickle")
    fn_runner_filename = os.path.join(submission_dir, "run.py")

    shutil.copy(os.path.join(os.path.dirname(
        __file__), "__sparkles_fn_runner.py"), fn_runner_filename)

    return ["sub"] + extra_sparkles_options + [
        "-n", job_name+"-scatter",
        "-u", fn_runner_filename,
        python_exe,
        fn_runner_filename,
        "scatter",
        script_filename,
        function_name,
        extra_args_filename,
        elements_filename] + scatter_function_parameters


def prepare_foreach(job_name, submission_dir, python_exe, foreach_function_name, extra_sparkles_options, elements_filename, extra_args_filename, batch_size):
    fn_runner_filename = os.path.join(submission_dir, "run.py")
    params_filename = os.path.join(submission_dir, "params.csv")

    shutil.copy(os.path.join(os.path.dirname(
        __file__), "__sparkles_fn_runner.py"), fn_runner_filename)

    with open(elements_filename, "rb") as fd:
        element_count = pickle.load(fd)

    with open(params_filename, "wt") as fd:
        w = csv.writer(fd)
        w.writerow(["start", "end"])
        for start in range(element_count, batch_size):
            end = min(batch_size + start, element_count)
            w.writerow([str(start), str(end)])

    cmd = ["sub"] + extra_sparkles_options + [
        "--params", params_filename,
        "-n", job_name,
        "-u", elements_filename,
        "-u", extra_args_filename,
        "-u", params_filename,
        "-u", fn_runner_filename,
        python_exe,
        fn_runner_filename,
        "foreach",
        foreach_function_name,
        extra_args_filename,
        elements_filename,
        '{start}', '{end}']

    return cmd


# def scatter(submission_dir, script_filename, function_name, script_args, batch_size):
#     namespace = {}
#     with open(script_filename, "rt") as fd:
#         exec(fd.read(), namespace, namespace)
#     function = namespace[function_name]

#     cmd = prepare_scatter(elements, extra_args, submission_dir, batch_size)


def scatter_cmd(args):
    # job_name = args.job_name
    # assert job_name is not None
    # scatter(args.script_filename, args.function, args.script_args)

    # make params file with batches

    # if --skip-existing: detect whether scatter has already been run. If file exists, do not run scatter, proceed to running foreach
    # submit sparkles scatter job, wait for completion
    # copy back down the pickeled result file
    # submit sparkles foreach jobs wait for completion
