import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--longrun",
        action="store_true",
        dest="longrun",
        default=False,
        help="enable longrundecorated tests",
    )
    parser.addoption(
        "--project",
        action="store",
        dest="project",
        default=None,
        help="GCP project ID for integration tests",
    )


@pytest.fixture
def project(request):
    """Fixture to get the --project option value."""
    return request.config.getoption("--project")
