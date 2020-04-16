# this file is a little unusual in that it is not a normal module. It actually is
# a script which is copied and used to launch the script inside of a docker image
# as part of the scatter command.

import sys
import argparse
import pickle
import csv
from zipfile import ZipFile


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

    for i in range(args.start_index, args.stop_index):
        function(elements[i], *common_args)


def scatter_cmd(args):
    function = get_function(args.script_filename, args.function_name)
    scatter_def = function(*args.script_args)
    assert isinstance(scatter_def, dict)
    assert "foreach" in scatter_def
    assert "elements" in scatter_def
    elements = scatter_def['elements']
    extra_args = scatter_def.get("extra_args", [])

    with ZipFile(args.package_filename, "w") as myzip:
        with myzip.open('element_count.pickle', "w") as fd:
            # write out the length of the array so we don't have to deserialize the actual array
            # to do batching. Useful to avoid deserialzing classes which sparklespray doesn't know about.
            pickle.dump(len(elements), fd)

        with myzip.open('elements.pickle', 'w') as fd:
            pickle.dump(elements, fd)

        with myzip.open('extra_args.pickle', 'w') as fd:
            pickle.dump(extra_args, fd)

        with myzip.open('foreach.pickle', 'w') as fd:
            # todo: add validation that we can look up function by module + name and give useful error
            #       if we cannot.
            pickle.dump(dict(module=function.__module__,
                             name=function.__name__,
                             script_filename=args.script_filename), fd)


def main(args=None):
    global_parser = argparse.ArgumentParser()
    subparser = global_parser.add_subparsers()

    parser = subparser.add_parser("scatter")
    parser.add_argument("script_filename")
    parser.add_argument("function_name")
    parser.add_argument("package_filename")
    parser.add_argument("script_args", nargs=argparse.REMAINDER)
    parser.set_defaults(func=scatter_cmd)

    parser = subparser.add_parser("foreach")
    parser.add_argument("script_filename")
    parser.add_argument("function_name")
    parser.add_argument("package_filename")
    parser.add_argument("start_index", type=int)
    parser.add_argument("stop_index", type=int)
    parser.set_defaults(func=foreach_cmd)

    args = global_parser.parse_args(args)
    args.func(args)


if __name__ == "__main__":
    main()
