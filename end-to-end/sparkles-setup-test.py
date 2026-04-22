import subprocess
import os
import random
import string
import argparse


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--billing",
        metavar="ID",
        help="Billing account ID; creates a new random project",
    )
    group.add_argument("--project", metavar="ID", help="Existing project ID to use")
    args = parser.parse_args()

    chars = string.digits + string.ascii_lowercase

    env = os.environ.copy()
    env["HOME"] = "/Users/pmontgom/fake-home"
    env["DOCKER_HOST"] = "ssh://pgm-vm"

    def run(cmd, **kwargs):
        subprocess.run(cmd, env=env, check=True, **kwargs)

    if args.billing:
        project_id = "ts-" + ("".join(random.choices(chars, k=14)))
        run(["gcloud", "projects", "create", project_id])
        run(
            [
                "gcloud",
                "billing",
                "projects",
                "link",
                project_id,
                f"--billing-account={args.billing}",
            ]
        )
    else:
        project_id = args.project

    run(
        [
            "sparkles",
            "setup",
            project_id,
            "--worker-dockerfile-path",
            "/Users/pmontgom/dev/sparklespray/go",
            "--write-config",
            ".sparkles",
        ]
    )
    run(["sparkles", "sub", "-i", "ubuntu", "-n", "test-job", "echo", "hello"])


if __name__ == "__main__":
    main()
