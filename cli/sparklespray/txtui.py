import logging
import logging.handlers
from termcolor import colored
from .log import log
import datetime


def user_print(msg):
    print(msg)


# def print_error_lines(lines):
#     from termcolor import colored

#     for line in lines:
#         print(colored(line, "red"))


def print_log_content(timestamp, payload, from_sparkles=False, is_important=True):
    if timestamp is None:
        timestamp = datetime.datetime.now()

    if len(payload) > 0 and payload[-1] == "\n":
        payload = payload[:-1]
    payload_lines = payload.split("\n")
    if payload_lines[-1] == "":
        del payload_lines[-1]
    prefix = None

    message_color = "yellow"
    if from_sparkles:
        message_color = "green"
    attrs = []
    if not is_important:
        attrs = ["dark"]

    for line in payload_lines:
        if prefix is None:
            prefix = "[{}]".format(timestamp.strftime("%H:%M:%S"))
            print(colored(prefix, "green"), colored(line, message_color, attrs=attrs))
        else:
            print(
                colored(" " * len(prefix), "white"),
                colored(line, message_color, attrs=attrs),
            )


def config_logging(verbosity):
    verbose_fmt = logging.Formatter("%(asctime)s:%(name)s:%(message)s")
    trim_fmt = logging.Formatter("%(asctime)s %(message)s")

    to_file = logging.handlers.RotatingFileHandler(
        "sparkles.log", mode="a", maxBytes=10 * 1024 * 1024, backupCount=1
    )
    to_file.setFormatter(verbose_fmt)
    to_file.setLevel(logging.INFO)
    handlers = [to_file]
    to_stderr = logging.StreamHandler()
    to_stderr.setFormatter(trim_fmt)
    to_stderr.setLevel(logging.WARNING)
    handlers.append(to_stderr)  # type: ignore
    log.setLevel(logging.INFO)
    if verbosity > 0:
        to_stderr.setLevel(logging.DEBUG)
    else:
        logging.getLogger("googleapiclient.discovery").setLevel(logging.WARN)

    logging.root.handlers = handlers  # type: ignore
