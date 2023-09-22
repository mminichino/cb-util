import os

_ROOT = os.path.abspath(os.path.dirname(__file__))
_PARENT = os.path.dirname(_ROOT)


def get_test_file(file):
    return os.path.join(_ROOT, file)


def get_parent_dir():
    return _PARENT
