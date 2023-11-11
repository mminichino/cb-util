##
##

import pytest


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
    pass


def pytest_sessionfinish():
    pass


def pytest_unconfigure():
    pass
