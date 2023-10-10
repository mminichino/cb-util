import os
from pkg_resources import parse_version

_ROOT = os.path.abspath(os.path.dirname(__file__))
__version__ = "2.1.1"
VERSION = parse_version(__version__)


def get_config_file(file):
    return os.path.join(_ROOT, 'data', file)
