import sys
import argparse
import pickle
import csv


def get_function(script_filename, function_name):
    # do we need to set up sys.path in some way so importing other files works correctly?
    namespace = {}
    with open(script_filename, "rt") as fd:
        exec(fd.read(), namespace, namespace)

    function = namespace[function_name]

    return function


def run_foreach(args):
    function = get_function(args.script_filename, args.function_name)

    with open(args.common_args_filename, "rb") as fd:
        common_args = pickle.load(fd)
    with open(args.elements_filename, "rb") as fd:
        elements = pickle.load(fd)
    for i in range(args.start_index, args.stop_index):
        function(elements[i], *common_args)


def run_scatter(args):

    function = get_function(args.script_filename, args.function_name)
    scatter_def = function(**args.script_args)
    assert isinstance(scatter_def, dict)
    assert "foreach" in scatter_def
    assert "elements" in scatter_def
    elements = scatter_def['elements']
    extra_args = scatter_def.get("extra_args", [])

    with open(args.elements_filename, "wb") as fd:
        # write out the length of the array so we don't have to deserialize it
        # to do batching. Useful to avoid deserialzing classes which sparklespray doesn't know about.
        pickle.dump(len(elements))
        pickle.dump(elements, fd)

    with open(args.extra_args_filename, "wb") as fd:
        pickle.dump(extra_args, fd)


def main():
    global_parser = argparse.ArgumentParser()
    subparser = global_parser.add_subparsers()

    parser = subparser.add_parser("scatter")
    parser.add_argument("script")
    parser.add_argument("function_name")
    parser.add_argument("common_args_filename")
    parser.add_argument("elements_filename")
    parser.add_argument("script_args", nargs=argparse.REMAINDER)

    parser = subparser.add_parser("foreach")
    parser.add_argument("script")
    parser.add_argument("function_name")
    parser.add_argument("common_args_filename")
    parser.add_argument("elements_filename")
    parser.add_argument("start_index", type=int)
    parser.add_argument("end_index", type=int)

    args = global_parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
