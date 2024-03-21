##
##

from io import TextIOWrapper
import pytest

RESULTS_FILE: TextIOWrapper


def pytest_addoption(parser):
    parser.addoption("--host", action="store", default="127.0.0.1")
    parser.addoption("--bucket", action="store", default="test")
    parser.addoption("--external", action="store_true")
    parser.addoption("--image", action="store", default="mminichino/cbdev:latest")


@pytest.fixture
def hostname(request):
    return request.config.getoption("--host")


@pytest.fixture
def bucket(request):
    return request.config.getoption("--bucket")


@pytest.fixture
def image(request):
    return request.config.getoption("--image")


def pytest_configure():
    pass


def pytest_sessionstart():
    global RESULTS_FILE
    RESULTS_FILE = open("results.log", "w")


def pytest_sessionfinish():
    global RESULTS_FILE
    if RESULTS_FILE:
        RESULTS_FILE.close()
        RESULTS_FILE = None


def pytest_unconfigure():
    pass


def pytest_runtest_logreport(report):
    RESULTS_FILE.write(f"{report.nodeid} {report.when} {report.outcome} {report.duration}\n")
