import sparklespray
from ..log import log

def version_cmd():
    log.info("version command ran")
    print(sparklespray.__version__)

def add_version_cmd(subparser):
    parser = subparser.add_parser("version", help="print the version and exit")
    parser.set_defaults(func=version_cmd)
