# this file is a little unusual in that it is not a normal module. It actually is
# a script which is copied and used to launch the script inside of a docker image
# as part of the scatter command.

import sys
import argparse
import pickle
import csv
from zipfile import ZipFile
import os


def get_function(script_filename, function_name):
    # do we need to set up sys.path in some way so importing other files works correctly?
    namespace = {}
    with open(script_filename, "rt") as fd:
        exec(fd.read(), namespace, namespace)

    function = namespace[function_name]

    return function


def foreach_cmd(args):
    function = get_function(args.script_filename, args.function_name)

    with ZipFile(args.package_filename, "r") as myzip:
        with myzip.open("extra_args.pickle", "r") as fd:
            common_args = pickle.load(fd)
        with myzip.open('elements.pickle', "r") as fd:
            elements = pickle.load(fd)

    results = []
    for i in range(args.start_index, args.stop_index):
        result = function(elements[i], *common_args)
        results.append(result)

    with open(args.results_filename, "w") as fd:
        pickle.dump(dict(start_index=args.start_index,
                         stop_index=args.stop_index, results=results), fd)


def scatter_cmd(args):
    function = get_function(args.script_filename, args.function_name)
    scatter_def = function(*args.script_args)
    assert isinstance(scatter_def, dict)
    assert "elements" in scatter_def
    elements = scatter_def['elements']
    extra_args = scatter_def.get("extra_args", [])

    with open(args.element_count_filename, "wt") as fd:
        fd.write(str(len(elements)))

    with ZipFile(args.package_filename, "w") as myzip:
        myzip.writestr('elements.pickle', pickle.dumps(elements))
        myzip.writestr('extra_args.pickle', pickle.dumps(extra_args))


def gather_cmd(args):
    function = get_function(args.script_filename, args.function_name)

    with ZipFile(args.package_filename, "r") as myzip:
        with myzip.open("extra_args.pickle", "r") as fd:
            common_args = pickle.load(fd)

    results = [None] * args.element_count

    # read in results from all foreach calls
    for fn in os.listdir(args.results_dir):
        if fn.startswith("."):
            continue
        filename = os.path.join(args.results_dir, fn)
        with open(filename, "rb") as fd:
            block = pickle.load(fd)
            start_index = block['start_index']
            stop_index = block['stop_index']

            for i in range(start_index, stop_index):
                results[i] = block['results'][i - start_index]

    function(results, *common_args)


def main(args=None):
    global_parser = argparse.ArgumentParser()
    subparser = global_parser.add_subparsers()

    parser = subparser.add_parser("scatter")
    parser.add_argument("script_filename")
    parser.add_argument("function_name")
    parser.add_argument("package_filename")
    parser.add_argument("element_count_filename")
    parser.add_argument("script_args", nargs=argparse.REMAINDER)
    parser.set_defaults(func=scatter_cmd)

    parser = subparser.add_parser("foreach")
    parser.add_argument("script_filename")
    parser.add_argument("function_name")
    parser.add_argument("package_filename")
    parser.add_argument("results_filename")
    parser.add_argument("start_index", type=int)
    parser.add_argument("stop_index", type=int)
    parser.set_defaults(func=foreach_cmd)

    parser = subparser.add_parser("gather")
    parser.add_argument("script_filename")
    parser.add_argument("function_name")
    parser.add_argument("results_dir")
    parser.add_argument("element_count", type=int)
    parser.add_argument("package_filename")
    parser.set_defaults(func=gather_cmd)

    args = global_parser.parse_args(args)
    args.func(args)


if __name__ == "__main__":
    main()
