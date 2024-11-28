from ..config import Config
from ..gcp_setup import grant


def add_grant_cmd(subparser):
    parser = subparser.add_parser(
        "grant",
        help="Grants additional rights to service account that sparkles is using",
    )
    parser.add_argument("project")
    parser.add_argument("role")
    parser.set_defaults(func=grant_cmd)


def grant_cmd(args, config: Config):
    role = args.role
    project_id = args.project
    service_acct = config.credentials.service_account_email
    grant(service_acct, project_id, role)
