import re
import sys
from . import txtui
import argparse
from .errors import UserError

from .config import load_config, create_func_params

from . import txtui
import inspect
import sys, io

def get_func_parameters(func):
    return inspect.getfullargspec(func)[0]

def sparkles_main():
    # disable stdout/stderr buffering to work better when run non-interactively

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, line_buffering=True)
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, line_buffering=True)

    retcode = main()
    if retcode is not None:
        sys.exit(retcode)




def main(argv=None):
    import warnings

    warnings.filterwarnings(
        "ignore", "Your application has authenticated using end user credentials"
    )
    # silence this annoying warning that is coming from google's library, and we don't have control over (google/cloud/datastore/query.py:234)

    warnings.filterwarnings(
        "ignore",
        "Detected filter using positional arguments. Prefer using the 'filter' keyword argument instead.",
    )

    from .commands.submit import add_submit_cmd
    from .commands.watch import add_watch_cmd
    from .commands.validate import add_validate_cmd
    from .commands.setup import add_setup_cmd, setup_cmd
    from .commands.reset import add_reset_cmd
    from .commands.logs import add_logs_cmd
    from .commands.show import add_show_cmd
    from .commands.status import add_status_cmd
    from .commands.delete import add_delete_cmd
    from .commands.kill import add_kill_cmd
    from .commands.fetch import add_fetch_cmd
    from .commands.version import add_version_cmd
    from .commands.list import add_list_cmd
    from .commands.prep_image import add_prep_image_cmd

    parse = argparse.ArgumentParser()

    # add global options
    parse.add_argument("-c", "--config", default=None)
    parse.add_argument(
        "--debug", action="store_true", help="If set, debug messages will be output"
    )
    parse.add_argument(
        "-o",
        "--override",
        action="append",
        dest="overrides",
        help="override a parameter in the config file. Value should be -o 'param=value'",
    )

    # add subcommands
    subparser = parse.add_subparsers()
    add_submit_cmd(subparser)
    add_list_cmd(subparser)
    add_validate_cmd(subparser)
    add_reset_cmd(subparser)
    add_setup_cmd(subparser)
    add_logs_cmd(subparser)
    add_show_cmd(subparser)
    add_status_cmd(subparser)
    add_watch_cmd(subparser)
    add_delete_cmd(subparser)
    add_kill_cmd(subparser)
    add_fetch_cmd(subparser)
    add_version_cmd(subparser)
    add_prep_image_cmd(subparser)

    args = parse.parse_args(argv)

    overrides = {}
    if args.overrides is not None:
        for override in args.overrides:
            m = re.match("([^=]+)=(.*)", override)
            assert m, f"Could not parse override: {override}"
            overrides[m.group(1)] = m.group(2)

    txtui.config_logging(100 if args.debug else 0)

    if not hasattr(args, "func"):
        parse.print_help()
        sys.exit(1)

    if args.func == setup_cmd:
        # special case, because this is the one command which must work before the service account
        # is set up.
        config = load_config(args.config, verbose=True, overrides=overrides)
        args.func(args, config)
    else:
        func_param_names = get_func_parameters(args.func)
        try:
            func_params = create_func_params(
                args.config,
                overrides=overrides,
                extras={"args": args},
                requested=func_param_names,
            )
        except UserError as ex:
            print(ex.message)
            return 1

        return args.func(**func_params)


if __name__ == "__main__":
    main(sys.argv[1:])
