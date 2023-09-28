##
##

import attr
import attrs
import argparse
from typing import Optional
import cbcmgr.cli.config as config
from cbcmgr.cli.main import MainLoop


@attr.s
class Parameters:
    user: Optional[str] = attr.ib(default=None)
    password: Optional[str] = attr.ib(default=None)
    host: Optional[str] = attr.ib(default=None)
    bucket: Optional[str] = attr.ib(default=None)
    scope: Optional[str] = attr.ib(default=None)
    collection: Optional[str] = attr.ib(default=None)
    project: Optional[str] = attr.ib(default=None)
    db: Optional[str] = attr.ib(default=None)
    tls: Optional[bool] = attr.ib(default=None)
    replica: Optional[int] = attr.ib(default=None)
    quota: Optional[int] = attr.ib(default=None)


class SchemaLoad(object):

    def __init__(self, parameters: Parameters):
        self.parameters = parameters
        # noinspection PyTypeChecker
        self.param_dict = attrs.asdict(parameters)

        parser = argparse.ArgumentParser()
        for k, v in self.param_dict.items():
            parser.add_argument('--' + k, default=v)
        self.options = parser.parse_args()
        config.process_params(self.options)

    @staticmethod
    def load():
        MainLoop().schema_load()
