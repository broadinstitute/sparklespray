import logging
import logging.handlers
from termcolor import colored, cprint


def user_print(msg):
    print(msg)


def print_log_content(timestamp, payload):
    if payload[-1] == "\n":
        payload = payload[:-1]
    payload_lines = payload.split("\n")
    if payload_lines[-1] == "":
        del payload_lines[-1]
    prefix = None
    for line in payload_lines:
        if prefix is None:
            prefix = "[{}]".format(timestamp.strftime("%H:%M:%S"))
            print(colored(prefix, "green"), colored(line, "yellow"))
        else:
            print(colored(" "*len(prefix), "white"), colored(line, "yellow"))


def config_logging(verbosity):
    verbose_fmt = logging.Formatter(
        "%(asctime)s:%(name)s:%(message)s")
    trim_fmt = logging.Formatter("%(asctime)s %(message)s")

    to_file = logging.handlers.RotatingFileHandler(
        "sparkles.log", mode='a', maxBytes=10*1024*1024, backupCount=1)
    to_file.setFormatter(verbose_fmt)
    handlers = [to_file]
    to_stderr = logging.StreamHandler()
    to_stderr.setFormatter(trim_fmt)
    to_stderr.setLevel(logging.WARNING)
    handlers.append(to_stderr)

    if verbosity > 0:
        # logging.basicConfig(
        #     level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
        to_stderr.setLevel(logging.DEBUG)
    else:
        logging.getLogger("googleapiclient.discovery").setLevel(logging.WARN)

    logging.root.handlers = handlers
